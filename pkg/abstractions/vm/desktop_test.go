package vm

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestStreamingDesktopAllowsAuthenticatedFrames(t *testing.T) {
	for _, mode := range []string{"desktop-webrtc", "desktop-websockets", "kiosk"} {
		t.Run(mode, func(t *testing.T) {
			s, v, _, runtime, _ := fixture()
			v.Spec.DesktopTransport = types.VMDesktopTransportWebRTC
			if mode == "desktop-websockets" {
				v.Spec.DesktopTransport = types.VMDesktopTransportWebSockets
			} else if mode == "kiosk" {
				v.Spec.Kiosk = &types.VMKioskSpec{Command: []string{"application"}}
			}
			v.Spec.ProtectedPorts = []uint32{types.VMDesktopPort}
			v.TrafficAccessToken = "private"
			runtime.forwardHeaders = http.Header{
				"Content-Security-Policy": {"default-src 'self'; frame-ancestors 'self'; object-src 'none'", "script-src 'self'"},
			}
			request := func(path, token string) *httptest.ResponseRecorder {
				req := httptest.NewRequest(http.MethodGet, "/vm/"+v.Handle+"/8080/"+path, nil)
				req.Header.Set("Accept", "text/html")
				req.Header.Set("X-Beam-VM-Token", token)
				req.Header.Set("Referer", "https://portal.example/")
				rec := httptest.NewRecorder()
				proxyAPI(s).ServeHTTP(rec, req)
				return rec
			}
			require.Equal(t, http.StatusForbidden, request("stream/", "").Code)
			require.Empty(t, runtime.forwarded)
			entry := request("", "private")
			require.Equal(t, http.StatusOK, entry.Code, entry.Body.String())
			require.Equal(t, "frame-ancestors *", entry.Header().Get("Content-Security-Policy"))
			stream := request("stream/", "private")
			require.Equal(t, http.StatusOK, stream.Code, stream.Body.String())
			require.Equal(t, []string{
				"default-src 'self'; frame-ancestors *; object-src 'none'",
				"script-src 'self'",
			}, stream.Result().Header.Values("Content-Security-Policy"))
			// A terminal or ordinary published application keeps its own policy.
			terminal := vmRequest(proxyAPI(s), http.MethodGet, "/vm/"+v.Handle+"/7681/", "")
			require.Equal(t, runtime.forwardHeaders.Values("Content-Security-Policy"), terminal.Header().Values("Content-Security-Policy"))
		})
	}
}

func TestDesktopPreservesExplicitFramePolicy(t *testing.T) {
	for _, policy := range []string{"frame-ancestors 'none'", "frame-ancestors https://portal.example.com", "default-src 'self'"} {
		t.Run(policy, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c := echo.New().NewContext(httptest.NewRequest(http.MethodGet, "/", nil), rec)
			allowDesktopFrame(c)
			c.Response().Header().Set("Content-Security-Policy", policy)
			require.NoError(t, c.String(http.StatusOK, "desktop"))
			require.Equal(t, policy, rec.Result().Header.Get("Content-Security-Policy"))
		})
	}
}
