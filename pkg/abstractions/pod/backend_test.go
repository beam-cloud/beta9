package pod

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// relay stands in for an agent relay or worker port proxy: until the app is
// ready it accepts connections and hangs up, optionally after a partial reply.
type relay struct {
	net.Listener
	ready   atomic.Bool
	partial []byte
}

func (l *relay) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil || l.ready.Load() {
			return conn, err
		}
		if l.partial != nil {
			io.ReadAll(io.LimitReader(conn, 1))
			conn.Write(l.partial)
		}
		conn.Close()
	}
}

func newRelay(t *testing.T, partial []byte) *relay {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	r := &relay{Listener: listener, partial: partial}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "served") })}
	go server.Serve(r)
	t.Cleanup(func() { server.Close() })
	return r
}

func routeTo(t *testing.T, repo repository.ContainerRepository, target string) string {
	t.Helper()
	routeID := "machine:worker:container:container:8000"
	require.NoError(t, repo.SetBackendRoute(context.Background(), types.BackendRoute{
		RouteID:     routeID,
		Port:        8000,
		Transport:   types.BackendRouteTransportDirect,
		ProxyTarget: target,
		State:       types.BackendRouteStateReady,
	}))
	return types.BackendRouteAddress(routeID)
}

func TestReadinessWaitsForARelayToHangUpOnlyOnRoutes(t *testing.T) {
	app := newRelay(t, nil)
	repo := repository.NewContainerRedisRepositoryForTest(newPodProxyTestRedis(t))
	routed := routeTo(t, repo, app.Addr().String())
	pb := &PodProxyBuffer{ctx: context.Background(), workspace: &types.Workspace{Name: "workspace"}, stubConfig: &types.StubConfigV1{}, containerRepo: repo}

	require.False(t, pb.checkContainerAvailable(routed), "a relay that hangs up is not ready")

	require.True(t, pb.checkContainerAvailable(app.Addr().String()), "a direct address keeps the connect-only check")

	app.ready.Store(true)
	require.True(t, pb.checkContainerAvailable(routed), "a relay that keeps the connection open is ready")
}

func TestForwardRequestResendsOnlyRequestsARelayNeverAnswered(t *testing.T) {
	replacement := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "served") }))
	t.Cleanup(replacement.Close)

	for _, tt := range []struct {
		name    string
		method  string
		body    []byte
		direct  bool
		partial []byte
		resent  bool
	}{
		{name: "get", method: http.MethodGet, resent: true},
		{name: "head", method: http.MethodHead, resent: true},
		{name: "post with a body", method: http.MethodPost, body: []byte("payload")},
		{name: "get after a partial reply", method: http.MethodGet, partial: []byte("HTTP/1.1 ")},
		{name: "get to a direct address", method: http.MethodGet, direct: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			app := newRelay(t, tt.partial)
			repo := repository.NewContainerRedisRepositoryForTest(newPodProxyTestRedis(t))
			address := app.Addr().String()
			if !tt.direct {
				address = routeTo(t, repo, address)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			pb := &PodProxyBuffer{
				ctx:           ctx,
				workspace:     &types.Workspace{Name: "workspace"},
				stubId:        "stub",
				stubConfig:    &types.StubConfigV1{},
				containerRepo: repo,
				buffer:        abstractions.NewRingBuffer[*connection](2),
				workReady:     make(chan struct{}, 1),
				discoverReady: make(chan struct{}, 1),
				availableContainers: []container{{
					id:              "starting",
					addressMap:      map[int32]string{8000: address},
					readyAddressMap: map[int32]string{8000: address},
				}},
			}
			var resent atomic.Bool
			pb.onBackendUnavailable = func() error {
				resent.Store(true)
				address := replacement.Listener.Addr().String()
				pb.availableContainersLock.Lock()
				pb.availableContainers = []container{{
					id:              "ready",
					addressMap:      map[int32]string{8000: address},
					readyAddressMap: map[int32]string{8000: address},
				}}
				pb.availableContainersLock.Unlock()
				return nil
			}
			go pb.processBuffer()

			rec := httptest.NewRecorder()
			requestCtx := echo.New().NewContext(httptest.NewRequest(tt.method, "/", bytes.NewReader(tt.body)), rec)
			requestCtx.SetParamNames("port")
			requestCtx.SetParamValues("8000")
			require.NoError(t, pb.ForwardRequest(requestCtx))

			want := http.StatusBadGateway
			if tt.resent {
				want = http.StatusOK
			}
			require.Equal(t, want, rec.Code, rec.Body.String())
			require.Equal(t, tt.resent, resent.Load())
		})
	}
}
