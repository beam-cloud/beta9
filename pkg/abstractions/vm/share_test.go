package vm

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExpiredOrRevokedShareShowsAUsefulBrowserPage(t *testing.T) {
	for _, revoked := range []bool{false, true} {
		for _, domain := range []string{"app.beam.cloud", "vm.example.com"} {
			s, v, _, runtime, _ := fixture()
			s.domain = domain
			v.TrafficAccessToken = "private-traffic-token"
			v.Spec.ProtectedPorts = []uint32{7681}
			expiry := time.Now().Add(-time.Minute)
			if revoked {
				expiry = time.Now().Add(time.Hour)
			}
			session := accessSession(v, 7681, expiry.Unix())
			if revoked {
				v.TrafficAccessToken = "rotated-token"
			}
			req := browserRequest(s, v, 7681, "/?theme=dark&"+sessionParameter+"="+session)
			rec := httptest.NewRecorder()
			browserProxy(s).ServeHTTP(rec, req)
			require.Equal(t, http.StatusForbidden, rec.Code)
			require.Contains(t, rec.Header().Get("Content-Type"), "text/html")
			require.Contains(t, rec.Body.String(), "This share link is no longer valid.")
			require.Contains(t, rec.Body.String(), "Ask the sender for a new link.")
			if domain == "app.beam.cloud" {
				require.Contains(t, rec.Body.String(), `href="/?theme=dark"`)
				require.Contains(t, rec.Body.String(), "Open with my Beam account")
			} else {
				require.NotContains(t, rec.Body.String(), "Open with my Beam account")
			}
			require.NotContains(t, rec.Body.String(), session)
			require.NotContains(t, rec.Body.String(), sessionParameter)
			require.NotContains(t, rec.Body.String(), "private-traffic-token")
			require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
			require.Equal(t, "no-referrer", rec.Header().Get("Referrer-Policy"))
			require.Contains(t, rec.Header().Get("Content-Security-Policy"), "default-src 'none'")
			require.Empty(t, rec.Result().Cookies())
			require.Empty(t, runtime.forwarded)
		}
	}
}

func TestExpiredShareAPIsAndFramesKeepTheirAuthErrors(t *testing.T) {
	for _, scenario := range []string{"API", "iframe", "websocket"} {
		t.Run(scenario, func(t *testing.T) {
			s, v, _, runtime, _ := fixture()
			s.domain = "app.beam.cloud"
			v.TrafficAccessToken = "private-token"
			v.Spec.ProtectedPorts = []uint32{7681}
			session := accessSession(v, 7681, time.Now().Add(-time.Minute).Unix())
			req := browserRequest(s, v, 7681, "/?"+sessionParameter+"="+session)
			switch scenario {
			case "API":
				req.Header.Set("Accept", "application/json")
			case "iframe":
				req.Header.Set("Sec-Fetch-Dest", "iframe")
			case "websocket":
				req.Header.Set("Upgrade", "websocket")
			}
			rec := httptest.NewRecorder()
			browserProxy(s).ServeHTTP(rec, req)
			require.Equal(t, http.StatusForbidden, rec.Code)
			require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
			require.NotContains(t, rec.Body.String(), "<!doctype html>")
			require.Empty(t, rec.Header().Get("Location"))
			require.Empty(t, runtime.forwarded)
		})
	}
}
