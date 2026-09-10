package managedendpoint

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestAdmittedInferenceSurvivesReadinessDrain(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint("stream=", stream), func(t *testing.T) {
			s := newServiceForTest(t)
			drain, beginDrain := context.WithCancel(context.Background())
			defer beginDrain()
			s.drainCtx = drain
			started := make(chan struct{})
			encoding := make(chan string, 1)
			complete := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				encoding <- req.Header.Get("Accept-Encoding")
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"start\"}}]}\n\n")
					w.(http.Flusher).Flush()
				}
				close(started)
				select {
				case <-complete:
					if stream {
						_, _ = io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n")
					} else {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"complete"}}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`)
					}
				case <-req.Context().Done():
				}
			}))
			defer upstream.Close()
			transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", upstream.Listener.Addr().String())
			}}
			defer transport.CloseIdleConnections()
			s.transports.Store("engine", transport)
			router := newRouter(s)
			httpCtx, rec := coldRouteContext()
			httpCtx.Request().Header.Set("Accept-Encoding", "gzip, br")
			rq := &routeRequest{ctx: httpCtx, auth: httpCtx.AuthInfo, adapter: adapters[types.EndpointRouteChatCompletions], route: types.EndpointRouteChatCompletions, requestID: "drain-request", body: []byte(`{}`), startedAt: time.Now(), stream: stream}
			endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "acme/model"}}
			replica := &types.EndpointReplica{ID: "replica", Address: "engine"}
			done := make(chan error, 1)
			go func() { _, err := router.proxy(httpCtx.Request().Context(), rq, endpoint, replica); done <- err }()
			<-started
			require.Equal(t, "identity", <-encoding, "metered JSON/SSE must not arrive compressed")
			beginDrain()
			select {
			case err := <-done:
				t.Fatalf("readiness drain interrupted active inference: %v", err)
			case <-time.After(30 * time.Millisecond):
			}
			close(complete)
			require.NoError(t, <-done)
			if stream {
				require.Contains(t, rec.Body.String(), "[DONE]")
			} else {
				require.Contains(t, rec.Body.String(), "complete")
			}
		})
	}
}

func TestDemandLifetimeSurvivesDrainButEndsOnShutdownOrClientCancellation(t *testing.T) {
	for _, finalShutdown := range []bool{false, true} {
		t.Run(fmt.Sprint("finalShutdown=", finalShutdown), func(t *testing.T) {
			s := newServiceForTest(t)
			serviceCtx, shutdown := context.WithCancel(s.ctx)
			defer shutdown()
			s.ctx = serviceCtx
			drain, beginDrain := context.WithCancel(context.Background())
			defer beginDrain()
			s.drainCtx = drain
			httpCtx, _ := coldRouteContext()
			clientCtx, clientCancel := context.WithCancel(httpCtx.Request().Context())
			defer clientCancel()
			httpCtx.SetRequest(httpCtx.Request().WithContext(clientCtx))
			rq := &routeRequest{ctx: httpCtx, model: "model", requestID: "active"}
			release, err := newRouter(s).holdDemand(rq)
			require.NoError(t, err)
			beginDrain()
			select {
			case <-httpCtx.Request().Context().Done():
				t.Fatal("readiness drain canceled an admitted demand lease")
			case <-time.After(30 * time.Millisecond):
			}
			demand, err := s.demand(context.Background(), "model", "read", "", 0)
			require.NoError(t, err)
			require.EqualValues(t, 1, demand.active)
			if finalShutdown {
				shutdown()
			} else {
				clientCancel()
			}
			select {
			case <-httpCtx.Request().Context().Done():
			case <-time.After(time.Second):
				t.Fatal("request survived final cancellation")
			}
			release()
			demand, err = s.demand(context.Background(), "model", "read", "", 0)
			require.NoError(t, err)
			require.Zero(t, demand.active)
		})
	}
}

func TestStreamWriteFailureClosesBlockedUpstreamReader(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	body := &observedBody{ReadCloser: reader, closed: make(chan struct{})}
	w := echo.NewResponse(failingStreamWriter{HeaderMap: make(http.Header)}, echo.New())
	_, _, err := relayStream(w, body, "gen-test", time.Now(), nil)
	require.Error(t, err)
	select {
	case <-body.closed:
	case <-time.After(time.Second):
		t.Fatal("upstream body was not closed after client write failure")
	}
	_, err = writer.Write([]byte("data: waiting\n\n"))
	require.Error(t, err)
}

type observedBody struct {
	io.ReadCloser
	closed chan struct{}
}

func (b *observedBody) Close() error { close(b.closed); return b.ReadCloser.Close() }

type failingStreamWriter struct{ HeaderMap http.Header }

func (w failingStreamWriter) Header() http.Header     { return w.HeaderMap }
func (failingStreamWriter) WriteHeader(int)           {}
func (failingStreamWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestProxyCancellationStopsBlockedUpstream(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprint("serviceShutdown=", shutdown), func(t *testing.T) {
			s := newServiceForTest(t)
			serviceCtx, serviceCancel := context.WithCancel(s.ctx)
			defer serviceCancel()
			s.ctx = serviceCtx
			upstreamCanceled := make(chan struct{})
			started := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				close(started)
				<-req.Context().Done()
				close(upstreamCanceled)
			}))
			defer upstream.Close()
			transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", upstream.Listener.Addr().String())
			}}
			defer transport.CloseIdleConnections()
			s.transports.Store("engine", transport)
			httpCtx, _ := coldRouteContext()
			clientCtx, clientCancel := context.WithCancel(httpCtx.Request().Context())
			defer clientCancel()
			httpCtx.SetRequest(httpCtx.Request().WithContext(clientCtx))
			rq := &routeRequest{ctx: httpCtx, auth: httpCtx.AuthInfo, adapter: adapters[types.EndpointRouteChatCompletions], requestID: "cancel-request", body: []byte(`{}`), startedAt: time.Now(), stream: true}
			endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "acme/model"}}
			done := make(chan error, 1)
			go func() {
				_, err := newRouter(s).proxy(clientCtx, rq, endpoint, &types.EndpointReplica{ID: "replica", Address: "engine"})
				done <- err
			}()
			<-started
			if shutdown {
				serviceCancel()
			} else {
				clientCancel()
			}
			select {
			case <-upstreamCanceled:
			case <-time.After(time.Second):
				t.Fatal("engine survived cancellation")
			}
			require.Error(t, <-done)
		})
	}
}
