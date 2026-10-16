package vm

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func browserRequest(s *Service, v *types.VM, port uint32, path string) *http.Request {
	s.urls(v)
	origin, _ := url.Parse(v.URLs[port])
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Host = origin.Host
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Dest", "document")
	return req
}

func browserProxy(s *Service) *echo.Echo {
	e := proxyAPI(s)
	e.Pre(s.hostRoute)
	return e
}

func TestDirectVMURLUsesExplicitBrowserAuthProvider(t *testing.T) {
	for _, endpoint := range []string{"https://login.example.net/authorize-vm?tenant=example", "https://identity.example.org/sessions/new?tenant=example"} {
		t.Run(endpoint, func(t *testing.T) {
			s, v, info, runtime, _ := fixture()
			s.domain = "vm.example.com"
			s.browserAuthURL = endpoint
			provider, err := url.Parse(endpoint)
			require.NoError(t, err)
			v.WorkspaceExternalID, v.TrafficAccessToken = info.Workspace.ExternalId, "traffic-secret"
			v.Spec.ProtectedPorts = []uint32{7681, 8080}
			for _, port := range []uint32{7681, 8080} {
				req := browserRequest(s, v, port, "/files/a%20b?theme=dark")
				rec := httptest.NewRecorder()
				browserProxy(s).ServeHTTP(rec, req)
				require.Equal(t, http.StatusSeeOther, rec.Code, rec.Body.String())
				location, err := url.Parse(rec.Header().Get("Location"))
				require.NoError(t, err)
				require.Equal(t, "https", location.Scheme)
				require.Equal(t, provider.Host, location.Host)
				require.Equal(t, provider.Path, location.Path)
				require.Equal(t, "example", location.Query().Get("tenant"))
				require.Equal(t, v.ID, location.Query().Get("vm"))
				require.Equal(t, v.WorkspaceExternalID, location.Query().Get("workspace"))
				require.Equal(t, strconv.Itoa(int(port)), location.Query().Get("port"))
				require.Equal(t, "/files/a%20b?theme=dark", location.Query().Get("next"))
				require.NotContains(t, rec.Header().Get("Location"), "traffic-secret")
				require.NotContains(t, rec.Header().Get("Location"), sessionParameter)
				require.Empty(t, rec.Result().Cookies(), "sign-in alone does not grant VM access")
				require.Empty(t, runtime.forwarded)
				require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
				require.True(t, s.response(v).BrowserAuth)
			}
		})
	}
}

func TestBrowserAuthIsDisabledWithoutExplicitConfiguration(t *testing.T) {
	// No deployment's domain may implicitly select an external auth service.
	for _, domain := range []string{"vm.example.com", "app.beam.cloud", "app.stage.beam.cloud"} {
		t.Run(domain, func(t *testing.T) {
			s, v, info, runtime, _ := fixture()
			s.domain = domain
			v.WorkspaceExternalID, v.TrafficAccessToken = info.Workspace.ExternalId, "traffic-secret"
			v.Spec.ProtectedPorts = []uint32{7681}
			req := browserRequest(s, v, 7681, "/")
			rec := httptest.NewRecorder()
			browserProxy(s).ServeHTTP(rec, req)
			require.Equal(t, http.StatusForbidden, rec.Code)
			require.Empty(t, rec.Header().Get("Location"))
			require.False(t, s.response(v).BrowserAuth)
			require.Empty(t, runtime.forwarded)

			// Disabling delegation does not disable the existing session flow.
			session := accessSession(v, 7681, time.Now().Add(time.Minute).Unix())
			req = browserRequest(s, v, 7681, "/?"+sessionParameter+"="+session)
			rec = httptest.NewRecorder()
			browserProxy(s).ServeHTTP(rec, req)
			require.Equal(t, http.StatusSeeOther, rec.Code)
			require.Equal(t, "/", rec.Header().Get("Location"))
			cookies := rec.Result().Cookies()
			require.Len(t, cookies, 1)
			req = browserRequest(s, v, 7681, "/")
			req.AddCookie(cookies[0])
			rec = httptest.NewRecorder()
			browserProxy(s).ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.NotEmpty(t, runtime.forwarded)
		})
	}
}

func TestBrowserSessionsDoNotTakeLifecycleLock(t *testing.T) {
	s, v, info, _, _ := fixture()
	s.domain = "vm.example.com"
	v.TrafficAccessToken = "traffic-secret"
	v.Spec.ProtectedPorts = []uint32{7681, 8080}
	store := &createRaceStore{vmStore: s.repo.(*vmStore), locked: true}
	s.repo = store
	for _, port := range []uint32{7681, 8080} {
		response := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId+"/"+v.ID+"/access-session", `{"port":`+strconv.Itoa(int(port))+`}`)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var entry struct {
			URL string `json:"url"`
		}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &entry))
		target, err := url.Parse(entry.URL)
		require.NoError(t, err)
		require.True(t, validAccessSession(v, port, target.Query().Get(sessionParameter), time.Now()))
	}
	// Lifecycle mutations must still respect the held lock.
	response := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId+"/"+v.ID+"/rotate-access-token", `{}`)
	require.Equal(t, http.StatusConflict, response.Code)
	require.True(t, store.locked)
}

func TestAuthorizedBrowserCookieReturnsToCleanDirectVMURL(t *testing.T) {
	s, v, info, runtime, _ := fixture()
	s.domain = "vm.example.com"
	s.browserAuthURL = "https://login.example.net/authorize-vm"
	v.WorkspaceExternalID, v.TrafficAccessToken = info.Workspace.ExternalId, "traffic-secret"
	v.Spec.ProtectedPorts = []uint32{7681}
	req := browserRequest(s, v, 7681, "/?theme=dark")
	rec := httptest.NewRecorder()
	browserProxy(s).ServeHTTP(rec, req)
	require.Equal(t, http.StatusSeeOther, rec.Code)
	require.Empty(t, runtime.forwarded)

	// Only the authenticated management API can issue this port's cookie entry
	// link; redirecting to the external provider alone grants no access.
	response := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId+"/"+v.ID+"/access-session", `{"port":7681,"ttl":3600}`)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var entry struct {
		URL string `json:"url"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &entry))
	target, err := url.Parse(entry.URL)
	require.NoError(t, err)
	query := target.Query()
	query.Set("theme", "dark")
	req = browserRequest(s, v, 7681, "/?"+query.Encode())
	rec = httptest.NewRecorder()
	browserProxy(s).ServeHTTP(rec, req)
	require.Equal(t, http.StatusSeeOther, rec.Code)
	require.Equal(t, "/?theme=dark", rec.Header().Get("Location"))
	cookies := rec.Result().Cookies()
	require.Len(t, cookies, 1)
	require.True(t, cookies[0].HttpOnly)
	require.True(t, cookies[0].Secure)

	req = browserRequest(s, v, 7681, rec.Header().Get("Location"))
	req.AddCookie(cookies[0])
	rec = httptest.NewRecorder()
	browserProxy(s).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Empty(t, rec.Header().Get("Location"))
	require.NotEmpty(t, runtime.forwarded)
	require.Empty(t, req.Header.Get("Cookie"), "the guest never receives the VM authentication cookie")
	require.Empty(t, req.URL.Query().Get(sessionParameter))

	// An expired port cookie goes back through account authentication.
	req = browserRequest(s, v, 7681, "/")
	req.AddCookie(&http.Cookie{Name: sessionCookie(v, 7681), Value: accessSession(v, 7681, time.Now().Add(-time.Minute).Unix())})
	rec = httptest.NewRecorder()
	browserProxy(s).ServeHTTP(rec, req)
	require.Equal(t, http.StatusSeeOther, rec.Code)
	require.Contains(t, rec.Header().Get("Location"), s.browserAuthURL+"?")
}

func TestBrowserLoginDoesNotReplaceAPIWebsocketOrEmbeddedAuthFailures(t *testing.T) {
	cases := []struct{ name, method, header, value string }{
		{"API", "GET", "Accept", "application/json"},
		{"POST", "POST", "", ""},
		{"HEAD", "HEAD", "", ""},
		{"fetch", "GET", "Sec-Fetch-Mode", "cors"},
		{"iframe", "GET", "Sec-Fetch-Dest", "iframe"},
		{"websocket", "GET", "Upgrade", "websocket"},
		{"explicit traffic token", "GET", "X-Beam-VM-Token", "invalid"},
		{"explicit bearer", "GET", "Authorization", "Bearer invalid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, v, info, runtime, _ := fixture()
			s.domain = "vm.example.com"
			s.browserAuthURL = "https://login.example.net/authorize-vm"
			v.WorkspaceExternalID, v.TrafficAccessToken = info.Workspace.ExternalId, "traffic-secret"
			v.Spec.ProtectedPorts = []uint32{7681}
			req := browserRequest(s, v, 7681, "/")
			req.Method = tc.method
			if tc.header != "" {
				req.Header.Set(tc.header, tc.value)
			}
			rec := httptest.NewRecorder()
			browserProxy(s).ServeHTTP(rec, req)
			require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
			require.Empty(t, rec.Header().Get("Location"))
			require.Empty(t, runtime.forwarded)
		})
	}
}

func TestBrowserLoginRequiresPublishedOriginAndLiveCredential(t *testing.T) {
	for _, scenario := range []string{"shared host", "foreign origin", "revoked", "unconfigured", "invalid return", "insecure auth endpoint"} {
		t.Run(scenario, func(t *testing.T) {
			s, v, info, runtime, _ := fixture()
			s.domain = "vm.example.com"
			s.browserAuthURL = "https://login.example.net/authorize-vm"
			v.WorkspaceExternalID, v.TrafficAccessToken = info.Workspace.ExternalId, "traffic-secret"
			v.Spec.ProtectedPorts = []uint32{7681}
			req := browserRequest(s, v, 7681, "/")
			want := http.StatusForbidden
			switch scenario {
			case "shared host":
				req.Host = "gateway.example.com"
			case "foreign origin":
				req.Header.Set("Origin", "https://other.example")
			case "revoked":
				s.backend.(*vmBackend).token = &types.Token{Active: false}
			case "unconfigured":
				s.browserAuthURL = ""
			case "invalid return":
				req.RequestURI = "//other.example/"
				want = http.StatusBadRequest
			case "insecure auth endpoint":
				s.browserAuthURL = "http://other.example/sign-in"
				want = http.StatusServiceUnavailable
			}
			// Exercise the handler directly for shared-host validation: the
			// host router correctly won't map that host onto this VM at all.
			req.URL.Path = "/vm/" + v.Handle + "/7681/"
			rec := httptest.NewRecorder()
			proxyAPI(s).ServeHTTP(rec, req)
			require.Equal(t, want, rec.Code, rec.Body.String())
			require.Empty(t, rec.Header().Get("Location"))
			require.Empty(t, runtime.forwarded)
		})
	}
}
