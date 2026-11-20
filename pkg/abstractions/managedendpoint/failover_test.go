package managedendpoint

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	engineJSON   = `{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":10,"completion_tokens":2}}`
	engineStream = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":1}}\n\n" +
		"data: [DONE]\n\n"
)

// engines is an endpoint with fake engines behind it. Each replica answers
// with its handler and counts the requests that reached it.
type engines struct {
	t        *testing.T
	s        *Service
	endpoint *types.ManagedEndpoint
	hits     sync.Map // replica id -> *atomic.Int64
	requests atomic.Int64
}

func newEngines(t *testing.T) *engines {
	s := newServiceForTest(t)
	return &engines{t: t, s: s, endpoint: seedEndpoint(t, s)}
}

func respond(contentType, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write([]byte(body))
	}
}

// add starts a ready replica served by handler; a nil handler is a machine
// that refuses connections.
func (e *engines) add(id string, handler http.HandlerFunc) *types.EndpointReplica {
	e.t.Helper()
	replica := &types.EndpointReplica{
		ID: id, EndpointID: e.endpoint.Spec.ID, Version: 1, GPU: "H100", Address: id, Status: types.ReplicaStatusReady,
		ContainerID: "managed-" + id, StartedAt: time.Now(), ReadyAt: time.Now(), Capacity: types.ReplicaCapacity{MaxConcurrency: 8},
	}
	require.NoError(e.t, e.s.repo.SaveReplica(context.Background(), replica))
	hits := counter(&e.hits, id)
	target := "127.0.0.1:1" // nothing listens here
	if handler != nil {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			hits.Add(1)
			handler(w, req)
		}))
		e.t.Cleanup(upstream.Close)
		target = upstream.Listener.Addr().String()
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", target)
	}}
	e.t.Cleanup(transport.CloseIdleConnections)
	e.s.transports.Store(replica.Address, transport)
	return replica
}

func (e *engines) hit(id string) int64 { return counter(&e.hits, id).Load() }

// send serves one chat request through the router and returns the response
// with the replica that settled it.
func (e *engines) send(r *router, body string) (*httptest.ResponseRecorder, string) {
	e.t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx := &auth.HttpAuthContext{Context: echo.New().NewContext(req, rec), AuthInfo: userInfo}
	route := types.EndpointRouteChatCompletions
	rq := &routeRequest{
		ctx: ctx, auth: userInfo, requestID: fmt.Sprint("req-", e.requests.Add(1)), app: e.endpoint,
		route: route, proto: protocols[route], startedAt: time.Now(), body: []byte(body),
	}
	var err error
	rq.payload, err = decodeRequestJSON(rq.body)
	require.NoError(e.t, err)
	rq.stream, _ = rq.payload["stream"].(bool)
	rq.charge = &types.Charge{ID: rq.requestID, WorkspaceID: "user-ws", AppID: e.endpoint.Spec.ID, Pricing: e.endpoint.Pricing, AcceptedAt: rq.startedAt}
	require.NoError(e.t, r.serve(rq, e.endpoint))
	return rec, charge(e.t, e.s, rq.requestID).ReplicaID
}

func conversation(name string, turns int) string {
	messages := []string{`{"role":"system","content":"You are a careful assistant."}`}
	for turn := range turns {
		if turn > 0 {
			messages = append(messages, fmt.Sprintf(`{"role":"assistant","content":"answer %d"}`, turn))
		}
		messages = append(messages, fmt.Sprintf(`{"role":"user","content":"%s question %d"}`, name, turn+1))
	}
	return `{"model":"acme/model","messages":[` + strings.Join(messages, ",") + `]}`
}

func TestConversationKeepsItsReplica(t *testing.T) {
	e := newEngines(t)
	for _, id := range []string{"a", "b", "c"} {
		e.add(id, respond("application/json", engineJSON))
	}
	gateway, otherGateway := newRouter(e.s), newRouter(e.s)
	homes := map[string]bool{}
	for i := range 12 {
		name := fmt.Sprint("conversation-", i)
		rec, home := e.send(gateway, conversation(name, 1))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		homes[home] = true
		for turn := 2; turn <= 4; turn++ {
			_, replica := e.send(gateway, conversation(name, turn))
			assert.Equal(t, home, replica, "turn %d follows the conversation's cache", turn)
		}
		_, replica := e.send(otherGateway, conversation(name, 5))
		assert.Equal(t, home, replica, "a gateway that never saw the conversation computes the same home")
	}
	assert.Len(t, homes, 3, "conversations spread over the replicas")
}

func TestFailedReplicaIsReplacedAndSitsOut(t *testing.T) {
	for name, broken := range map[string]http.HandlerFunc{
		"refuses connections": nil,
		"answers 503":         func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) },
	} {
		t.Run(name, func(t *testing.T) {
			e := newEngines(t)
			e.add("healthy", respond("application/json", engineJSON))
			e.add("broken", broken)
			r := newRouter(e.s)
			for i := range 30 {
				rec, replica := e.send(r, conversation(fmt.Sprint("c", i), 1))
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				assert.Equal(t, "healthy", replica)
			}
			until, ejected := r.ejected.Load("broken")
			require.True(t, ejected, "some conversation's home was the broken replica")
			assert.WithinDuration(t, time.Now().Add(ejectFor), until.(time.Time), 5*time.Second)
			if broken != nil {
				assert.EqualValues(t, 1, e.hit("broken"), "one failure takes the replica out of this gateway's rotation")
			}

			// It is tried again once it has sat out, and is the last resort before that.
			r.ejected.Store("broken", time.Now().Add(-time.Second))
			replicas, err := r.servingReplicas(context.Background(), e.endpoint, "", nil)
			require.NoError(t, err)
			assert.Len(t, replicas, 2)
			r.ejected.Store("broken", time.Now().Add(time.Minute))
			replicas, err = r.servingReplicas(context.Background(), e.endpoint, "", map[string]bool{"healthy": true})
			require.NoError(t, err)
			require.Len(t, replicas, 1)
			assert.Equal(t, "broken", replicas[0].ID)
		})
	}
}

// closesIdle answers the first request on a connection and hangs up on the
// next: an engine whose keep-alive ran out as the request was written.
func closesIdle(body string) http.HandlerFunc {
	var used sync.Map
	return func(w http.ResponseWriter, req *http.Request) {
		if _, reused := used.LoadOrStore(req.RemoteAddr, true); reused {
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
			return
		}
		respond("application/json", body)(w, req)
	}
}

func TestClosedIdleConnectionIsNotAFailedReplica(t *testing.T) {
	e := newEngines(t)
	e.add("a", closesIdle(engineJSON))
	e.add("b", closesIdle(engineJSON))
	r := newRouter(e.s)
	for i := range 4 {
		name := fmt.Sprint("conversation-", i)
		_, home := e.send(r, conversation(name, 1))
		for turn := 2; turn <= 4; turn++ {
			rec, replica := e.send(r, conversation(name, turn))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Equal(t, home, replica, "turn %d stays with the conversation's cache", turn)
		}
	}
	assert.Greater(t, e.hit("a")+e.hit("b"), int64(16), "some requests met a connection their engine had closed")
	for _, id := range []string{"a", "b"} {
		_, ejected := r.ejected.Load(id)
		assert.False(t, ejected, "replica %s answers on a new connection", id)
	}
}

// hangUp sends the stream's headers and, optionally, some events, then drops
// the connection the way a lost machine's proxy does.
func hangUp(events string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(events))
		w.(http.Flusher).Flush()
		conn, _, _ := w.(http.Hijacker).Hijack()
		_ = conn.Close()
	}
}

// busy makes a replica look loaded enough that the other one is tried first.
func (e *engines) busy(replica *types.EndpointReplica) {
	replica.Capacity.Running = replica.Capacity.MaxConcurrency - 1
	require.NoError(e.t, e.s.repo.SaveReplica(context.Background(), replica))
}

func TestStreamMovesToAnotherReplicaBeforeItsFirstEvent(t *testing.T) {
	body := strings.Replace(conversation("stream", 1), `{"model"`, `{"stream":true,"model"`, 1)

	e := newEngines(t)
	e.add("lost", hangUp(""))
	e.busy(e.add("healthy", respond("text/event-stream", engineStream)))
	rec, served := e.send(newRouter(e.s), body)
	assert.EqualValues(t, 1, e.hit("lost"), "the stream was opened by the replica that then went away")
	assert.Equal(t, "healthy", served)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"content":"hi"`)
	assert.Contains(t, rec.Body.String(), "data: [DONE]")
	assert.NotContains(t, rec.Body.String(), `"error"`)
	assert.Equal(t, types.ChargeSettled, charge(t, e.s, "req-1").Status)

	// The same loss after output began cannot be hidden: the caller is told,
	// nothing is billed and no other replica is tried.
	e = newEngines(t)
	e.add("lost", hangUp("data: {\"choices\":[{\"delta\":{\"content\":\"par\"}}]}\n\n"))
	e.busy(e.add("healthy", respond("text/event-stream", engineStream)))
	rec, served = e.send(newRouter(e.s), body)
	assert.Equal(t, "lost", served)
	assert.Contains(t, rec.Body.String(), `"content":"par"`)
	assert.Contains(t, rec.Body.String(), "upstream_stream_interrupted")
	assert.NotContains(t, rec.Body.String(), "[DONE]")
	assert.Zero(t, e.hit("healthy"))
	assert.Equal(t, types.ChargeVoid, charge(t, e.s, "req-1").Status)

	// When no replica can finish the stream it ends with the reason.
	e = newEngines(t)
	e.add("lost", hangUp(""))
	e.add("gone", nil)
	rec, _ = e.send(newRouter(e.s), body)
	assert.Equal(t, http.StatusOK, rec.Code, "the stream had already opened")
	assert.Contains(t, rec.Body.String(), "upstream_unavailable")
}

func TestLostReplicaEndsItsRequests(t *testing.T) {
	stuck := make(chan struct{})
	hang := func(_ http.ResponseWriter, req *http.Request) {
		select {
		case <-req.Context().Done():
		case <-stuck:
		}
	}
	e := newEngines(t)
	defer close(stuck)
	dead := e.add("dead", hang)
	r := newRouter(e.s)
	done := make(chan string, 1)
	go func() {
		_, served := e.send(r, conversation("waiting", 1))
		done <- served
	}()
	require.Eventually(t, func() bool { return e.hit("dead") == 1 }, 2*time.Second, time.Millisecond)

	// A machine that vanished sends nothing. The request waits until the
	// replica is known lost, then runs on the one that replaced it.
	r.dropLost(context.Background())
	select {
	case <-done:
		t.Fatal("a live replica's request was ended")
	case <-time.After(20 * time.Millisecond):
	}
	e.add("replacement", respond("application/json", engineJSON))
	dead.Status = types.ReplicaStatusFailed
	require.NoError(t, e.s.repo.SaveReplica(context.Background(), dead))
	r.dropLost(context.Background())
	select {
	case served := <-done:
		assert.Equal(t, "replacement", served)
	case <-time.After(2 * time.Second):
		t.Fatal("the lost replica's request was not ended")
	}
	r.running.Range(func(_, _ any) bool {
		t.Error("a request that finished is still in flight")
		return false
	})

	// A harness replica is lost as soon as its heartbeats stop, before the
	// controller records it.
	silent := e.add("silent", hang)
	silent.HarnessEnabled, silent.LastHeartbeat = true, time.Now().Add(-e.s.config.ReplicaStaleAfter-time.Second)
	require.NoError(t, e.s.repo.SaveReplica(context.Background(), silent))
	e.s.controller.startedAt = time.Now().Add(-time.Hour)
	replicas, err := r.servingReplicas(context.Background(), e.endpoint, "", nil)
	require.NoError(t, err)
	require.Len(t, replicas, 1)
	assert.Equal(t, "replacement", replicas[0].ID)
}
