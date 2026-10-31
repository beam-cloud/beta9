package apiv1

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

// FeedbackGroup takes reports about the platform itself from users and their coding agents.
// Each report is stored, then sent to the operator's event callbacks.
type FeedbackGroup struct {
	backendRepo repository.BackendRepository
	eventRepo   repository.EventRepository
}

func NewFeedbackGroup(g *echo.Group, backendRepo repository.BackendRepository, eventRepo repository.EventRepository) *FeedbackGroup {
	group := &FeedbackGroup{backendRepo: backendRepo, eventRepo: eventRepo}

	g.POST("/:workspaceId", auth.WithWorkspaceAuth(group.Create))

	return group
}

type FeedbackRequest struct {
	Kind                 string `json:"kind"`
	Summary              string `json:"summary"`
	Blocked              bool   `json:"blocked"`
	Command              string `json:"command"`
	ErrorCode            string `json:"error_code"`
	ErrorMessage         string `json:"error_message"`
	Details              string `json:"details"`
	SuggestedImprovement string `json:"suggested_improvement"`
	Model                string `json:"model"`
	Environment          string `json:"environment"`
	IdempotencyKey       string `json:"idempotency_key"`
}

type FeedbackResponse struct {
	ID        string `json:"id"`
	Duplicate bool   `json:"duplicate"`
	Message   string `json:"message"`
}

const (
	feedbackBodyLimit           = 64 << 10
	feedbackSummaryLimit        = 300
	feedbackIdempotencyKeyLimit = 128

	// Per workspace, and per agent session (else token), within feedbackRateWindow.
	feedbackRateWindow     = time.Hour
	feedbackWorkspaceLimit = 30
	feedbackReporterLimit  = 10

	// A reporter repeating a summary within this window gets the earlier report back.
	feedbackDuplicateWindow = 24 * time.Hour
	feedbackRecentLimit     = 500

	feedbackRecorded  = "Feedback recorded. Continue with your task."
	feedbackDuplicate = "Already recorded. Continue with your task."
	feedbackLimited   = "Feedback limit reached for now; this report was not recorded. Continue with your task."
)

func (g *FeedbackGroup) Create(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	body, err := io.ReadAll(io.LimitReader(ctx.Request().Body, feedbackBodyLimit+1))
	if err != nil {
		return HTTPBadRequest("Invalid payload")
	}
	if len(body) > feedbackBodyLimit {
		return NewHTTPError(http.StatusRequestEntityTooLarge, "Feedback is limited to 64 KiB")
	}
	var req FeedbackRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return HTTPBadRequest("Invalid payload")
	}

	tokenKey := ""
	var tokenId *uint
	if token := cc.AuthInfo.Token; token != nil {
		tokenKey = token.Key
		if token.Id != 0 {
			id := token.Id
			tokenId = &id
		}
	}
	feedback, err := req.feedback(tokenKey)
	if err != nil {
		return err
	}
	workspace := cc.AuthInfo.Workspace
	feedback.WorkspaceId = workspace.Id
	feedback.TokenId = tokenId
	feedback.Caller = cleanFeedbackField(cc.AuthInfo.Actor.Caller, tokenKey, 200)
	feedback.AgentSession = cleanFeedbackField(cc.AuthInfo.Actor.AgentSession, tokenKey, 200)

	reqCtx := ctx.Request().Context()
	now := time.Now()
	recent, err := g.backendRepo.ListRecentFeedback(reqCtx, workspace.Id, now.Add(-feedbackDuplicateWindow), feedbackRecentLimit)
	if err != nil {
		return HTTPInternalServerError("Failed to record feedback")
	}
	earlier, err := checkRecentFeedback(feedback, recent, now)
	if err != nil {
		return err
	}
	if earlier != nil {
		return ctx.JSON(http.StatusOK, FeedbackResponse{ID: earlier.ExternalId, Duplicate: true, Message: feedbackDuplicate})
	}

	created, isNew, err := g.backendRepo.CreateFeedback(reqCtx, feedback)
	if err != nil {
		return HTTPInternalServerError("Failed to record feedback")
	}
	if !isNew {
		return ctx.JSON(http.StatusOK, FeedbackResponse{ID: created.ExternalId, Duplicate: true, Message: feedbackDuplicate})
	}

	g.eventRepo.PushFeedbackEvent(types.EventFeedbackSchema{
		ID:                   created.ExternalId,
		WorkspaceID:          workspace.ExternalId,
		WorkspaceName:        workspace.Name,
		Kind:                 created.Kind,
		Summary:              created.Summary,
		Blocked:              created.Blocked,
		Command:              created.Command,
		ErrorCode:            created.ErrorCode,
		ErrorMessage:         created.ErrorMessage,
		Details:              created.Details,
		SuggestedImprovement: created.SuggestedImprovement,
		Model:                created.Model,
		Environment:          created.Environment,
		EventActor:           types.EventActor{Caller: created.Caller, AgentSession: created.AgentSession},
		CreatedAt:            created.CreatedAt,
	})
	return ctx.JSON(http.StatusCreated, FeedbackResponse{ID: created.ExternalId, Message: feedbackRecorded})
}

// feedback validates the request and scrubs it of secrets. Only a missing or long summary, an
// unknown kind and a long idempotency key are refused; other long fields are cut short.
func (req FeedbackRequest) feedback(tokenKey string) (types.Feedback, error) {
	kind := strings.ToLower(strings.TrimSpace(req.Kind))
	if kind == "" {
		kind = types.FeedbackKindOther
	}
	if !slices.Contains(types.FeedbackKinds, kind) {
		return types.Feedback{}, HTTPBadRequest("kind must be one of " + strings.Join(types.FeedbackKinds, ", "))
	}

	summary := sanitizeFeedbackText(req.Summary)
	if summary == "" {
		return types.Feedback{}, HTTPBadRequest("summary is required")
	}
	if utf8.RuneCountInString(summary) > feedbackSummaryLimit {
		return types.Feedback{}, HTTPBadRequest(fmt.Sprintf("summary must be at most %d characters; put the rest in details", feedbackSummaryLimit))
	}

	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
	if len(idempotencyKey) > feedbackIdempotencyKeyLimit {
		return types.Feedback{}, HTTPBadRequest(fmt.Sprintf("idempotency_key must be at most %d bytes", feedbackIdempotencyKeyLimit))
	}

	return types.Feedback{
		Kind:                 kind,
		Summary:              redactFeedbackText(summary, tokenKey),
		Blocked:              req.Blocked,
		Command:              cleanFeedbackField(req.Command, tokenKey, 500),
		ErrorCode:            cleanFeedbackField(req.ErrorCode, tokenKey, 64),
		ErrorMessage:         cleanFeedbackField(req.ErrorMessage, tokenKey, 4000),
		Details:              cleanFeedbackField(req.Details, tokenKey, 8000),
		SuggestedImprovement: cleanFeedbackField(req.SuggestedImprovement, tokenKey, 2000),
		Model:                cleanFeedbackField(req.Model, tokenKey, 100),
		Environment:          cleanFeedbackField(req.Environment, tokenKey, 200),
		IdempotencyKey:       idempotencyKey,
	}, nil
}

// checkRecentFeedback returns the earlier report this one repeats, or an error when the
// workspace or reporter has sent too many recently. `recent` is newest first.
func checkRecentFeedback(feedback types.Feedback, recent []types.Feedback, now time.Time) (*types.Feedback, error) {
	reporter := feedbackReporter(feedback)
	summary := normalizedFeedbackSummary(feedback.Summary)
	workspaceCount, reporterCount := 0, 0
	for i := range recent {
		earlier := &recent[i]
		if feedback.IdempotencyKey != "" && earlier.IdempotencyKey == feedback.IdempotencyKey {
			return earlier, nil
		}
		sameReporter := reporter != "" && feedbackReporter(*earlier) == reporter
		if sameReporter && normalizedFeedbackSummary(earlier.Summary) == summary {
			return earlier, nil
		}
		if earlier.CreatedAt.After(now.Add(-feedbackRateWindow)) {
			workspaceCount++
			if sameReporter {
				reporterCount++
			}
		}
	}
	if workspaceCount >= feedbackWorkspaceLimit || reporterCount >= feedbackReporterLimit {
		return nil, NewHTTPError(http.StatusTooManyRequests, feedbackLimited)
	}
	return nil, nil
}

// feedbackReporter names who sent a report: its agent session, else its token.
func feedbackReporter(feedback types.Feedback) string {
	if feedback.AgentSession != "" {
		return "session:" + feedback.AgentSession
	}
	if feedback.TokenId != nil {
		return fmt.Sprintf("token:%d", *feedback.TokenId)
	}
	return ""
}

func normalizedFeedbackSummary(summary string) string {
	return strings.Join(strings.Fields(strings.ToLower(summary)), " ")
}

func cleanFeedbackField(value, tokenKey string, limit int) string {
	value = redactFeedbackText(sanitizeFeedbackText(value), tokenKey)
	if runes := []rune(value); len(runes) > limit {
		return string(runes[:limit]) + "…"
	}
	return value
}

// sanitizeFeedbackText drops control characters other than newlines and tabs; Postgres
// refuses NUL in text columns.
func sanitizeFeedbackText(text string) string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || !unicode.IsControl(r) {
			return r
		}
		return -1
	}, text))
}

const feedbackRedacted = "[redacted]"

var (
	feedbackRedactions = []struct {
		pattern     *regexp.Regexp
		replacement string
	}{
		{regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(?:-----END [A-Z ]*PRIVATE KEY-----|\z)`), feedbackRedacted},
		{regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*://)[^\s/:@]+:[^\s/@]+@`), "${1}" + feedbackRedacted + "@"},
		{regexp.MustCompile(`(?i)\b(bearer|basic)(\s+)[A-Za-z0-9._~+/-]{8,}=*`), "${1}${2}" + feedbackRedacted},
		// HF_TOKEN=, "password":, secret_key: and the like, but not max_tokens or tokenizer.
		{regexp.MustCompile(`(?i)\b([A-Za-z0-9_.-]*(?:password|passwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credentials?)(?:[_.-][A-Za-z0-9_.-]*)?["']?\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s"',;&]+)`), "${1}" + feedbackRedacted},
		{regexp.MustCompile(`\b(?:(?:AKIA|ASIA)[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,}|xox[abposr]-[A-Za-z0-9-]{10,}|sk-[A-Za-z0-9_-]{20,}|sk_(?:live|test)_[A-Za-z0-9]{16,}|hf_[A-Za-z0-9]{30,}|AIza[0-9A-Za-z_-]{35}|eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,})`), feedbackRedacted},
	}

	// Long runs mixing upper case, lower case and digits are random keys, workspace tokens
	// among them. Paths, identifiers, hex digests and UUIDs lack one of the three.
	feedbackOpaqueValue = regexp.MustCompile(`[A-Za-z0-9_+-]{40,}=*`)
)

// redactFeedbackText removes the caller's own token and anything shaped like a credential.
func redactFeedbackText(text, tokenKey string) string {
	if len(tokenKey) >= 16 {
		text = strings.ReplaceAll(text, tokenKey, feedbackRedacted)
	}
	for _, redaction := range feedbackRedactions {
		text = redaction.pattern.ReplaceAllString(text, redaction.replacement)
	}
	return feedbackOpaqueValue.ReplaceAllStringFunc(text, func(value string) string {
		var upper, lower, digit int
		for _, r := range value {
			switch {
			case r >= 'A' && r <= 'Z':
				upper++
			case r >= 'a' && r <= 'z':
				lower++
			case r >= '0' && r <= '9':
				digit++
			}
		}
		if upper >= 4 && lower >= 4 && digit >= 4 {
			return feedbackRedacted
		}
		return value
	})
}
