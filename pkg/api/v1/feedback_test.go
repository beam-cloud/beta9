package apiv1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

const feedbackTestToken = "Xb3kQ9mZ2pL7vR4tN8wY1cF6hJ0sD5gA_Xb3kQ9mZ2pL7vR4tN8wY1cF6hJ0sD5gA-Xb3kQ9mZ2pL7vR4tN8wY1c=="

func TestRedactFeedbackText(t *testing.T) {
	for _, tc := range []struct{ in, out string }{
		{"used " + feedbackTestToken + " from env", "used [redacted] from env"},
		{"Authorization: Bearer abc.def-ghi_123456", "Authorization: Bearer [redacted]"},
		{"DATABASE_URL=postgres://beam:hunter22@db.internal:5432/app", "DATABASE_URL=postgres://[redacted]@db.internal:5432/app"},
		{`{"password": "hunter22", "user": "eli"}`, `{"password": [redacted], "user": "eli"}`},
		{"HF_TOKEN=hf_abc secret_key: s3cr3t", "HF_TOKEN=[redacted] secret_key: [redacted]"},
		{"set OPENAI key sk-proj-abcdefghijklmnopqrstuvwx", "set OPENAI key [redacted]"},
		{"AKIAIOSFODNN7EXAMPLE leaked", "[redacted] leaked"},
		{"-----BEGIN RSA PRIVATE KEY-----\nMIIE\n-----END RSA PRIVATE KEY----- after", "[redacted] after"},
		{"opaque " + strings.Repeat("aB3", 14), "opaque [redacted]"},
		// Prose, paths, digests and ids survive.
		{"max_tokens: 512 and tokenizer=llama", "max_tokens: 512 and tokenizer=llama"},
		{"/Users/eli/beam/beta9-agent-feedback/sdk/src/beta9/cli/main.py", "/Users/eli/beam/beta9-agent-feedback/sdk/src/beta9/cli/main.py"},
		{"sha256:" + strings.Repeat("ab12", 16), "sha256:" + strings.Repeat("ab12", 16)},
		{"deployment 3f0c9a4e-8d1b-4c2a-9e7f-0a1b2c3d4e5f failed", "deployment 3f0c9a4e-8d1b-4c2a-9e7f-0a1b2c3d4e5f failed"},
		{"TestMCPOAuthScopeAndFilterCannotBeBypassed", "TestMCPOAuthScopeAndFilterCannotBeBypassed"},
	} {
		require.Equal(t, tc.out, redactFeedbackText(tc.in, feedbackTestToken), tc.in)
	}
}

func TestFeedbackRequestValidation(t *testing.T) {
	feedback, err := FeedbackRequest{Summary: "  deploy\x00 ignored --port  ", Details: strings.Repeat("x", 9000)}.feedback("")
	require.NoError(t, err)
	require.Equal(t, types.FeedbackKindOther, feedback.Kind)
	require.Equal(t, "deploy ignored --port", feedback.Summary)
	require.Equal(t, 8001, len([]rune(feedback.Details)), "long details are cut, not refused")

	for _, req := range []FeedbackRequest{
		{Summary: " \n "},
		{Summary: "x", Kind: "rant"},
		{Summary: strings.Repeat("x", feedbackSummaryLimit+1)},
		{Summary: "x", IdempotencyKey: strings.Repeat("k", feedbackIdempotencyKeyLimit+1)},
	} {
		_, err := req.feedback("")
		require.Error(t, err, req)
		require.Equal(t, http.StatusBadRequest, err.(*echo.HTTPError).Code, req)
	}
}

type feedbackBackendRepo struct {
	repository.BackendRepository
	recent  []types.Feedback
	created []types.Feedback
	exists  bool
}

func (r *feedbackBackendRepo) ListRecentFeedback(_ context.Context, workspaceId uint, _ time.Time, _ int) ([]types.Feedback, error) {
	return r.recent, nil
}

func (r *feedbackBackendRepo) CreateFeedback(_ context.Context, feedback types.Feedback) (types.Feedback, bool, error) {
	if r.exists {
		feedback.ExternalId = "fb-existing"
		return feedback, false, nil
	}
	feedback.ExternalId = "fb-new"
	feedback.CreatedAt = time.Now()
	r.created = append(r.created, feedback)
	return feedback, true, nil
}

type feedbackEventRepo struct {
	repository.EventRepository
	events []types.EventFeedbackSchema
}

func (r *feedbackEventRepo) PushFeedbackEvent(schema types.EventFeedbackSchema) {
	r.events = append(r.events, schema)
}

func postFeedback(t *testing.T, backend *feedbackBackendRepo, events *feedbackEventRepo, body string) (*httptest.ResponseRecorder, error) {
	t.Helper()
	group := &FeedbackGroup{backendRepo: backend, eventRepo: events}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/feedback/ws", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	ctx := echo.New().NewContext(request, recorder)
	tokenId := uint(3)
	err := group.Create(&auth.HttpAuthContext{
		Context: ctx,
		AuthInfo: &auth.AuthInfo{
			Workspace: &types.Workspace{Id: 7, ExternalId: "ws", Name: "acme"},
			Token:     &types.Token{Id: tokenId, Key: feedbackTestToken, TokenType: types.TokenTypeWorkspace},
			Actor:     types.EventActor{Caller: "cli/0.1.293 (cursor)", AgentSession: "run-1"},
		},
	})
	return recorder, err
}

func TestFeedbackCreate(t *testing.T) {
	backend, events := &feedbackBackendRepo{}, &feedbackEventRepo{}
	recorder, err := postFeedback(t, backend, events, `{"kind":"bug","summary":"deploy ignored --port","blocked":true,"details":"token=`+feedbackTestToken+`"}`)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, recorder.Code)

	var response FeedbackResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, FeedbackResponse{ID: "fb-new", Message: feedbackRecorded}, response)

	require.Len(t, backend.created, 1)
	stored := backend.created[0]
	require.Equal(t, uint(7), stored.WorkspaceId)
	require.Equal(t, uint(3), *stored.TokenId)
	require.Equal(t, "cli/0.1.293 (cursor)", stored.Caller)
	require.Equal(t, "run-1", stored.AgentSession)
	require.Equal(t, "token=[redacted]", stored.Details)

	require.Len(t, events.events, 1)
	event := events.events[0]
	require.Equal(t, "fb-new", event.ID)
	require.Equal(t, "acme", event.WorkspaceName)
	require.Equal(t, "ws", event.WorkspaceID)
	require.True(t, event.Blocked)
	require.Equal(t, "run-1", event.AgentSession)
}

func TestFeedbackDuplicatesReturnTheEarlierReport(t *testing.T) {
	session := types.Feedback{ExternalId: "fb-1", Summary: "Deploy  ignored --PORT", AgentSession: "run-1", CreatedAt: time.Now()}
	keyed := types.Feedback{ExternalId: "fb-2", Summary: "other", AgentSession: "run-2", IdempotencyKey: "retry-1", CreatedAt: time.Now()}
	for name, tc := range map[string]struct {
		body   string
		repo   *feedbackBackendRepo
		wantID string
	}{
		"same session and summary": {`{"summary":"deploy ignored --port"}`, &feedbackBackendRepo{recent: []types.Feedback{session}}, "fb-1"},
		"recent idempotency key":   {`{"summary":"new words","idempotency_key":"retry-1"}`, &feedbackBackendRepo{recent: []types.Feedback{keyed}}, "fb-2"},
		"older idempotency key":    {`{"summary":"new words","idempotency_key":"retry-0"}`, &feedbackBackendRepo{exists: true}, "fb-existing"},
	} {
		t.Run(name, func(t *testing.T) {
			events := &feedbackEventRepo{}
			recorder, err := postFeedback(t, tc.repo, events, tc.body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, recorder.Code)
			var response FeedbackResponse
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			require.Equal(t, FeedbackResponse{ID: tc.wantID, Duplicate: true, Message: feedbackDuplicate}, response)
			require.Empty(t, tc.repo.created)
			require.Empty(t, events.events)
		})
	}

	otherSession := session
	otherSession.AgentSession = "run-9"
	backend := &feedbackBackendRepo{recent: []types.Feedback{otherSession}}
	recorder, err := postFeedback(t, backend, &feedbackEventRepo{}, `{"summary":"deploy ignored --port"}`)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, recorder.Code, "another agent reporting the same problem is a second signal")
}

func TestFeedbackRateLimits(t *testing.T) {
	recent := func(count int, session string, age time.Duration) []types.Feedback {
		out := make([]types.Feedback, count)
		for i := range out {
			out[i] = types.Feedback{ExternalId: "fb", Summary: strings.Repeat("x", i+1), AgentSession: session, CreatedAt: time.Now().Add(-age)}
		}
		return out
	}
	for name, tc := range map[string]struct {
		recent  []types.Feedback
		limited bool
	}{
		"reporter limit":           {recent(feedbackReporterLimit, "run-1", time.Minute), true},
		"workspace limit":          {recent(feedbackWorkspaceLimit, "run-2", time.Minute), true},
		"other reporters below":    {recent(feedbackWorkspaceLimit-1, "run-2", time.Minute), false},
		"older than rate window":   {recent(feedbackWorkspaceLimit, "run-1", 2*time.Hour), false},
		"reporter below its limit": {recent(feedbackReporterLimit-1, "run-1", time.Minute), false},
	} {
		t.Run(name, func(t *testing.T) {
			backend := &feedbackBackendRepo{recent: tc.recent}
			recorder, err := postFeedback(t, backend, &feedbackEventRepo{}, `{"summary":"a new problem"}`)
			if tc.limited {
				require.Error(t, err)
				require.Equal(t, http.StatusTooManyRequests, err.(*echo.HTTPError).Code)
				require.Empty(t, backend.created)
				return
			}
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, recorder.Code)
		})
	}
}

func TestFeedbackRefusesOversizedBodies(t *testing.T) {
	backend := &feedbackBackendRepo{}
	_, err := postFeedback(t, backend, &feedbackEventRepo{}, `{"summary":"x","details":"`+strings.Repeat("x", feedbackBodyLimit)+`"}`)
	require.Error(t, err)
	require.Equal(t, http.StatusRequestEntityTooLarge, err.(*echo.HTTPError).Code)
	require.Empty(t, backend.created)
}
