package repository

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestEventHTTPSinkDeliversCloudEvent(t *testing.T) {
	type callbackRequest struct {
		body []byte
		err  string
	}
	requests := make(chan callbackRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Header.Get("X-Test"), "yes"; got != want {
			requests <- callbackRequest{err: "unexpected header: got " + got + " want " + want}
			return
		}
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			requests <- callbackRequest{err: err.Error()}
			return
		}
		raw, err := json.Marshal(body)
		if err != nil {
			requests <- callbackRequest{err: err.Error()}
			return
		}
		requests <- callbackRequest{body: raw}
	}))
	defer server.Close()

	repo := &EventClientRepo{}
	event, err := repo.createEventObject(types.EventContainerEvent, types.EventContainerEventSchemaVersion, types.EventContainerEventSchema{
		ID:          types.ContainerEventRuntimeExited,
		ContainerID: "container-1",
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		Message:     "runtime exited",
	})
	if err != nil {
		t.Fatal(err)
	}

	sink := newEventHTTPSink(types.EventCallbackConfig{
		URL:        server.URL,
		EventTypes: []string{"container.*"},
		Headers:    map[string]string{"X-Test": "yes"},
	})
	defer close(sink.queue)

	if err := sink.PushEvent(event); err != nil {
		t.Fatal(err)
	}

	select {
	case req := <-requests:
		if req.err != "" {
			t.Fatal(req.err)
		}
		var delivered struct {
			Type string `json:"type"`
			Data struct {
				ContainerID string `json:"container_id"`
				Message     string `json:"message"`
			} `json:"data"`
		}
		if err := json.Unmarshal(req.body, &delivered); err != nil {
			t.Fatal(err)
		}
		if got, want := delivered.Type, types.EventContainerEvent; got != want {
			t.Fatalf("unexpected event type: got %q want %q", got, want)
		}
		if got, want := delivered.Data.ContainerID, "container-1"; got != want {
			t.Fatalf("unexpected container id: got %q want %q", got, want)
		}
		if got, want := delivered.Data.Message, "runtime exited"; got != want {
			t.Fatalf("unexpected message: got %q want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event callback")
	}
}

func TestEventHTTPSinkSkipsUnmatchedEvents(t *testing.T) {
	if !eventHTTPMatches([]string{"container.*"}, types.EventContainerEvent) {
		t.Fatal("expected wildcard callback filter to match container event")
	}
	if eventHTTPMatches([]string{"task.*"}, types.EventContainerEvent) {
		t.Fatal("expected task wildcard callback filter to skip container event")
	}
}

func TestBeta9WebhookPayloadMatchesInternalAPIShape(t *testing.T) {
	repo := &EventClientRepo{}
	event, err := repo.createEventObject(types.EventStubDeploy, types.EventStubSchemaVersion, types.EventStubSchema{
		ID:          "stub-1",
		StubType:    types.StubType(types.StubTypeFunction),
		WorkspaceID: "workspace-1",
		StubConfig:  "{}",
	})
	if err != nil {
		t.Fatal(err)
	}

	body, err := beta9WebhookEventPayload(event)
	if err != nil {
		t.Fatal(err)
	}

	var delivered []struct {
		Type        string          `json:"type"`
		Data        json.RawMessage `json:"data"`
		WorkspaceID string          `json:"workspaceid"`
		StubID      string          `json:"stubid"`
		Date        float64         `json:"date"`
	}
	if err := json.Unmarshal(body, &delivered); err != nil {
		t.Fatal(err)
	}

	if got, want := len(delivered), 1; got != want {
		t.Fatalf("unexpected event count: got %d want %d", got, want)
	}
	if got, want := delivered[0].Type, types.EventStubDeploy; got != want {
		t.Fatalf("unexpected event type: got %q want %q", got, want)
	}
	if got, want := delivered[0].WorkspaceID, "workspace-1"; got != want {
		t.Fatalf("unexpected workspace extension: got %q want %q", got, want)
	}
	if got, want := delivered[0].StubID, "stub-1"; got != want {
		t.Fatalf("unexpected stub extension: got %q want %q", got, want)
	}
	if delivered[0].Date == 0 {
		t.Fatal("expected beta9 webhook callback to include date")
	}
}

func TestInternalAPIEventCallbackFilter(t *testing.T) {
	eventTypes := internalAPICallbackEventTypes()
	for _, eventType := range eventTypes {
		if !eventHTTPMatches(eventTypes, eventType) {
			t.Fatalf("expected callback filter to match %q", eventType)
		}
	}
	if eventHTTPMatches(eventTypes, types.EventWorkerLifecycle) {
		t.Fatal("expected internal API callback filter to skip worker lifecycle events")
	}
}

func internalAPICallbackEventTypes() []string {
	return []string{
		types.EventStubDeploy,
		types.EventStubServe,
		types.EventStubRun,
		types.EventStubClone,
		types.EventGatewayEndpointCalled,
		types.EventFeedbackSubmitted,
	}
}

func TestFeedbackReachesOperatorCallbacksOnly(t *testing.T) {
	storage, callback := &captureEventSink{}, &captureEventSink{}
	repo := &EventClientRepo{storageSinks: []eventSink{storage}, callbackSinks: []eventSink{callback}}
	repo.PushFeedbackEvent(types.EventFeedbackSchema{ID: "fb-1", WorkspaceID: "ws", Kind: types.FeedbackKindBug, Summary: "x"})

	require.Empty(t, storage.events, "feedback stays out of workspace event history")
	require.Len(t, callback.events, 1)
	require.Equal(t, types.EventFeedbackSubmitted, callback.events[0].Type())
	require.Equal(t, "ws", eventMetadataFromCloudEvent(callback.events[0]).WorkspaceID)
	require.True(t, webhookExcluded(types.EventFeedbackSubmitted), "workspace webhooks never receive feedback")
}

func TestSlackFeedbackTextEscapesUserInput(t *testing.T) {
	event := cloudevents.NewEvent()
	event.SetType(types.EventFeedbackSubmitted)
	require.NoError(t, event.SetData(cloudevents.ApplicationJSON, types.EventFeedbackSchema{
		ID:            "fb-1",
		WorkspaceID:   "ws",
		WorkspaceName: "acme",
		Kind:          types.FeedbackKindMissingCapability,
		Summary:       "<!channel> no way to pin a GPU type",
		Blocked:       true,
		Command:       "deploy",
		Details:       "see <https://evil.test|docs> & retry",
		EventActor:    types.EventActor{Caller: "mcp/0.1 (claude-code/2.1)"},
	}))

	text := slackEventText(event)
	require.Contains(t, text, "Feedback `missing_capability` (blocked) | workspace `acme` | `mcp/0.1 (claude-code/2.1)` | id `fb-1`")
	require.Contains(t, text, "> &lt;!channel&gt; no way to pin a GPU type")
	require.Contains(t, text, "see &lt;https://evil.test|docs&gt; &amp; retry")
	require.NotContains(t, text, "<!channel>")
}

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

type lifecyclePushClient struct {
	pb.WorkerRepositoryServiceClient
	mu       sync.Mutex
	calls    int
	requests []*pb.PushContainerLifecycleEventsRequest
}

func (c *lifecyclePushClient) PushContainerLifecycleEvents(_ context.Context, request *pb.PushContainerLifecycleEventsRequest, _ ...grpc.CallOption) (*pb.PushContainerLifecycleEventsResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.requests = append(c.requests, request)
	if c.calls == 1 {
		return nil, errors.New("gateway unavailable")
	}
	return &pb.PushContainerLifecycleEventsResponse{Ok: true}, nil
}

func (c *lifecyclePushClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestWorkerLifecycleRelayRetriesUnacknowledgedBatch(t *testing.T) {
	client := &lifecyclePushClient{}
	relay := &workerLifecycleRelay{
		client:        client,
		workerID:      "worker-1",
		events:        make(chan types.EventContainerLifecycleSchema, 1),
		retryInterval: 10 * time.Millisecond,
		retryBudget:   time.Second,
	}
	go relay.run()

	require.True(t, relay.push(types.EventContainerLifecycleSchema{
		ID:          types.ContainerLifecycleImageLoad,
		ContainerID: "container-1",
		StartTime:   time.Now(),
	}))
	require.Eventually(t, func() bool { return client.callCount() == 2 }, 2*time.Second, 10*time.Millisecond)

	client.mu.Lock()
	defer client.mu.Unlock()
	require.Len(t, client.requests, 2)
	require.Equal(t, client.requests[0].Events, client.requests[1].Events)
}

type poisonLifecyclePushClient struct {
	pb.WorkerRepositoryServiceClient
	mu              sync.Mutex
	poisonAttempts  int
	poisonDeadlines []time.Time
	acknowledged    []*pb.PushContainerLifecycleEventsRequest
}

func (c *poisonLifecyclePushClient) PushContainerLifecycleEvents(ctx context.Context, request *pb.PushContainerLifecycleEventsRequest, _ ...grpc.CallOption) (*pb.PushContainerLifecycleEventsResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(request.Events) > 0 && strings.Contains(string(request.Events[0]), "container-poison") {
		c.poisonAttempts++
		if deadline, ok := ctx.Deadline(); ok {
			c.poisonDeadlines = append(c.poisonDeadlines, deadline)
		}
		return &pb.PushContainerLifecycleEventsResponse{Ok: false, ErrorMsg: "unauthorized worker lifecycle event"}, nil
	}
	c.acknowledged = append(c.acknowledged, request)
	return &pb.PushContainerLifecycleEventsResponse{Ok: true}, nil
}

func (c *poisonLifecyclePushClient) poisonAttemptCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.poisonAttempts
}

func (c *poisonLifecyclePushClient) acknowledgedContainer(containerID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, request := range c.acknowledged {
		for _, data := range request.Events {
			if strings.Contains(string(data), containerID) {
				return true
			}
		}
	}
	return false
}

func TestWorkerLifecycleRelayDropsBatchAfterRetryBudget(t *testing.T) {
	client := &poisonLifecyclePushClient{}
	relay := &workerLifecycleRelay{
		client:        client,
		workerID:      "worker-1",
		events:        make(chan types.EventContainerLifecycleSchema, 16),
		retryInterval: 5 * time.Millisecond,
		retryBudget:   50 * time.Millisecond,
	}
	go relay.run()

	require.True(t, relay.push(types.EventContainerLifecycleSchema{
		ID:          types.ContainerLifecycleImageLoad,
		ContainerID: "container-poison",
		StartTime:   time.Now(),
	}))
	// Once the relay has attempted the poison batch it is sealed inside the
	// retry loop, so the good event cannot be batched with it and must be
	// delivered after the poison batch is dropped.
	require.Eventually(t, func() bool { return client.poisonAttemptCount() >= 1 }, 3*time.Second, time.Millisecond)
	require.True(t, relay.push(types.EventContainerLifecycleSchema{
		ID:          types.ContainerLifecycleImageLoad,
		ContainerID: "container-good",
		StartTime:   time.Now(),
	}))

	require.Eventually(t, func() bool { return client.acknowledgedContainer("container-good") }, 3*time.Second, 10*time.Millisecond)

	// Every attempt, including the last one, was bounded by the batch's retry
	// deadline rather than the full push timeout.
	client.mu.Lock()
	defer client.mu.Unlock()
	require.NotEmpty(t, client.poisonDeadlines)
	for _, deadline := range client.poisonDeadlines {
		require.Equal(t, client.poisonDeadlines[0], deadline)
	}
}

func TestWorkerLifecycleRelayRejectsFullQueue(t *testing.T) {
	relay := &workerLifecycleRelay{
		workerID: "worker-1",
		events:   make(chan types.EventContainerLifecycleSchema, 1),
	}
	relay.events <- types.EventContainerLifecycleSchema{}

	require.False(t, relay.push(types.EventContainerLifecycleSchema{}))
}
