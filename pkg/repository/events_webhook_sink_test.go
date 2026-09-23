package repository

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type staticWebhookSource struct {
	webhooks []types.WorkspaceWebhook
	calls    atomic.Int32
}

func (s *staticWebhookSource) ListWebhooks(context.Context, string) ([]types.WorkspaceWebhook, error) {
	s.calls.Add(1)
	return s.webhooks, nil
}

func TestWorkspaceWebhookSink(t *testing.T) {
	received := make(chan *http.Request, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	source := &staticWebhookSource{webhooks: []types.WorkspaceWebhook{
		{ExternalId: "hook", URL: server.URL, EventTypes: []string{"stub.*"}, Secret: "whsec_1", Enabled: true},
		{ExternalId: "off", URL: server.URL, EventTypes: []string{"*"}, Secret: "whsec_2", Enabled: false},
	}}
	sink := newWorkspaceWebhookSink(source)
	repo := &EventClientRepo{}
	event := func(eventType string, data any) cloudevents.Event {
		e, err := repo.createEventObject(eventType, "1.0", data)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}

	deploy := event(types.EventStubDeploy, types.EventStubSchema{ID: "stub-1", WorkspaceID: "ws-1"})
	for _, e := range []cloudevents.Event{
		deploy,
		event(types.EventTaskUpdated, types.EventTaskSchema{ID: "task-1", StubID: "stub-1", WorkspaceID: "ws-1"}),
		event(types.EventStubDeploy, types.EventStubSchema{ID: "stub-2"}),
		event(types.EventContainerLog, types.EventContainerLogSchema{WorkspaceID: "ws-1"}),
	} {
		if err := sink.PushEvent(e); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case r := <-received:
		if r.Header.Get(webhookEventHeader) != types.EventStubDeploy || r.Header.Get(webhookDeliveryHeader) != deploy.ID() {
			t.Fatalf("unexpected headers: %v", r.Header)
		}
		if sig := r.Header.Get(webhookSignatureHeader); len(sig) != len("sha256=")+64 {
			t.Fatalf("signature header = %q", sig)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no delivery for stub.deploy")
	}
	select {
	case r := <-received:
		t.Fatalf("unexpected delivery: %s", r.Header.Get(webhookEventHeader))
	case <-time.After(300 * time.Millisecond):
	}

	if got := source.calls.Load(); got != 1 {
		t.Fatalf("source called %d times, want 1 (cached)", got)
	}
}
