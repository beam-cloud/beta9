package repository

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/stretchr/testify/require"
)

type staticWebhookSource struct {
	webhooks []types.WorkspaceWebhook
	calls    atomic.Int32
}

func (s *staticWebhookSource) ListWebhooks(context.Context, string) ([]types.WorkspaceWebhook, error) {
	s.calls.Add(1)
	return s.webhooks, nil
}

func webhookTestClient(t *testing.T, server *httptest.Server) *http.Client {
	t.Helper()
	client := newWebhookClient()
	require.Nil(t, client.Transport.(*http.Transport).Proxy, "environment proxies must not bypass address validation")
	client.Transport.(*http.Transport).DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	t.Cleanup(client.CloseIdleConnections)
	return client
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
	sink := newWorkspaceWebhookSink(source, webhookTestClient(t, server))
	t.Cleanup(func() { close(sink.queue) })
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

func TestWebhookDeliveryRejectsPrivateDestination(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	err := DeliverWebhook(context.Background(), types.WorkspaceWebhook{URL: server.URL}, cloudevents.NewEvent())
	require.ErrorIs(t, err, common.ErrPrivateAddress)
	require.ErrorContains(t, err, "update the webhook URL to a public endpoint")
	require.Zero(t, requests.Load())
}

func TestWebhookDeliveryDoesNotFollowRedirects(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Redirect(w, r, "/internal", http.StatusFound)
	}))
	defer server.Close()
	err := deliverWebhook(context.Background(), webhookTestClient(t, server), types.WorkspaceWebhook{URL: "http://8.8.8.8/hook"}, cloudevents.NewEvent())
	require.ErrorContains(t, err, "status 302")
	require.EqualValues(t, 1, requests.Load())
}
