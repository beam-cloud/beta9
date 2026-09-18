package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type captureEvents struct {
	repository.EventRepository
	workspaces []string
	traces     []types.Trace
}

func (c *captureEvents) PushRouterTrace(workspaceID string, trace types.Trace) {
	c.workspaces = append(c.workspaces, workspaceID)
	c.traces = append(c.traces, trace)
}

func post(t *testing.T, group *Group, authInfo *auth.AuthInfo, body string) (*httptest.ResponseRecorder, error) {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/router/traces", strings.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	return rec, group.PushTraces(&auth.HttpAuthContext{Context: e.NewContext(req, rec), AuthInfo: authInfo})
}

func TestPushTraces(t *testing.T) {
	events := &captureEvents{}
	group := NewGroup(echo.New().Group("/router"), events)
	workspace := &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "ws-1"}}
	valid := `{"id":"t1","harness":"codex","thread":"th","model":"gpt-5","prompt":"hi","start":"2026-09-16T12:00:00Z","status":"ok","tokens":{"source":"provider"},` +
		`"steps":[{"model":{"id":"c1","model":"gpt-5","sent":"2026-09-16T12:00:00Z","status":"ok","tokens":{"source":"provider"},"text":"hello"}}]}`

	rec, err := post(t, group, workspace, `{"traces":[`+valid+`]}`)
	if err != nil || rec.Code != http.StatusAccepted {
		t.Fatalf("code=%d err=%v", rec.Code, err)
	}
	if len(events.traces) != 1 || events.workspaces[0] != "ws-1" || events.traces[0].ID != "t1" || events.traces[0].Steps[0].Model.Text != "hello" {
		t.Fatalf("pushed = %+v for %v", events.traces, events.workspaces)
	}
	if got := events.traces[0].Start; !got.Equal(time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("start = %v", got)
	}
	// Prose with no reasoning: the answer, produced without deliberation.
	if got := events.traces[0].Steps[0].Model.Class; got.Output != types.TraceCallOutputText || got.Thinking != types.TraceCallThinkingNone {
		t.Fatalf("class = %+v", got)
	}

	for name, body := range map[string]string{
		"empty":          `{"traces":[]}`,
		"no id":          `{"traces":[{"harness":"codex","start":"2026-09-16T12:00:00Z","steps":[{"model":{}}]}]}`,
		"no steps":       `{"traces":[{"id":"t2","harness":"codex","start":"2026-09-16T12:00:00Z"}]}`,
		"ambiguous step": `{"traces":[{"id":"t3","harness":"codex","start":"2026-09-16T12:00:00Z","steps":[{"model":{},"tool":{}}]}]}`,
		"not json":       `nope`,
	} {
		if _, err := post(t, group, workspace, body); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := post(t, group, nil, `{"traces":[`+valid+`]}`); err == nil {
		t.Error("accepted without workspace auth")
	}
	if len(events.traces) != 1 {
		t.Fatalf("rejected requests pushed events: %d", len(events.traces))
	}
}
