package mcp

import (
	"context"
	"net/http"
	"testing"
	"time"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type mcpFeedbackRepo struct {
	repository.BackendRepository
	recent  []types.Feedback
	created []types.Feedback
}

func (r *mcpFeedbackRepo) ListRecentFeedback(context.Context, uint, time.Time, int) ([]types.Feedback, error) {
	return r.recent, nil
}

func (r *mcpFeedbackRepo) CreateFeedback(_ context.Context, feedback types.Feedback) (types.Feedback, bool, error) {
	feedback.ExternalId = "fb-1"
	r.created = append(r.created, feedback)
	return feedback, true, nil
}

type mcpFeedbackEvents struct {
	repository.EventRepository
	events []types.EventFeedbackSchema
}

func (r *mcpFeedbackEvents) PushFeedbackEvent(schema types.EventFeedbackSchema) {
	r.events = append(r.events, schema)
}

// send_feedback reaches the REST route with the connection's attribution, so reports from MCP
// and the CLI share one set of checks.
func TestMCPSendFeedback(t *testing.T) {
	g := newTestGroup()
	g.prepareTools()
	repo, events := &mcpFeedbackRepo{}, &mcpFeedbackEvents{}
	identity := mcpIdentity()
	routes := g.router.Group(apiv1.HttpServerBaseRoute+"/feedback", func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			authInfo := *identity
			authInfo.Actor = types.EventActor{Caller: c.Request().Header.Get(auth.CallerHeader), AgentSession: c.Request().Header.Get(auth.AgentSessionHeader)}
			return next(&auth.HttpAuthContext{Context: c, AuthInfo: &authInfo})
		}
	})
	apiv1.NewFeedbackGroup(routes, repo, events)

	headers := http.Header{}
	headers.Set(auth.CallerHeader, "mcp/0.1.293 (claude-code/2.1.306)")
	headers.Set(auth.AgentSessionHeader, "run-1")
	ctx := withIdentityHeaders(context.Background(), headers)
	args := map[string]any{"summary": "wait_deployment hides the last health error", "kind": "error_message", "blocked": true, "command": "wait_deployment"}

	result := g.call(ctx, identity, g.byName["send_feedback"], args)
	require.Equal(t, false, result["isError"], result)
	reply := result["structuredContent"].(map[string]any)
	require.Equal(t, "fb-1", reply["id"])
	require.Equal(t, false, reply["duplicate"])
	require.Len(t, repo.created, 1)
	require.Equal(t, "mcp/0.1.293 (claude-code/2.1.306)", repo.created[0].Caller)
	require.Equal(t, "run-1", repo.created[0].AgentSession)
	require.True(t, repo.created[0].Blocked)
	require.Len(t, events.events, 1)

	result = g.call(ctx, identity, g.byName["send_feedback"], map[string]any{"summary": "x", "kind": "rant"})
	require.Equal(t, "INVALID_ARGS", result["structuredContent"].(map[string]any)["code"])

	repo.recent = make([]types.Feedback, 30)
	for i := range repo.recent {
		repo.recent[i] = types.Feedback{Summary: string(rune('a' + i)), CreatedAt: time.Now()}
	}
	result = g.call(ctx, identity, g.byName["send_feedback"], map[string]any{"summary": "one more", "kind": "bug"})
	structured := result["structuredContent"].(map[string]any)
	require.Equal(t, "RATE_LIMITED", structured["code"])
	require.Contains(t, structured["error"], "Continue with your task")

	tool := g.byName["send_feedback"]
	require.False(t, tool.ReadOnly)
	require.False(t, tool.Idempotent, "a retried report must not be replayed blindly")
	require.False(t, tool.Destructive)
	restricted := mcpIdentity()
	restricted.Token.TokenType = types.TokenTypeWorkspaceRestricted
	require.False(t, g.allowed(restricted, tool))
	oauth := mcpIdentity()
	oauth.OAuth = &auth.OAuthGrant{Scopes: []string{"beam:read"}}
	require.True(t, g.allowed(oauth, tool), "a read-only grant can still report friction")
	only := context.WithValue(context.Background(), mcpSelectionKey{}, mcpSelection{Tools: []string{"whoami"}})
	require.True(t, g.selected(only, tool))
}
