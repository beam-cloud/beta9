package vm

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestVMHostRoutingPreservesAppPaths(t *testing.T) {
	s := &Service{domain: "app.example.com"}
	for _, host := range []string{
		"sandbox-11111111-2222-3333-4444-555555555555-12345678-8080",
		"11111111-2222-3333-4444-555555555555-8080",
		"my-app-abcdefg-latest-8080",
	} {
		t.Run(host, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "https://"+host+"."+s.domain+"/files/a%20b?x=1", nil)
			err := s.hostRoute(func(c echo.Context) error {
				require.Equal(t, "/files/a b", c.Request().URL.Path)
				require.Equal(t, "/files/a%20b", c.Request().URL.EscapedPath())
				require.Equal(t, "x=1", c.Request().URL.RawQuery)
				return nil
			})(e.NewContext(req, httptest.NewRecorder()))
			require.NoError(t, err)
		})
	}
	_, v, _, _, _ := fixture()
	v.Handle = "sandbox-long-friendly-name-0123456789abcdef0123456789abcdef"
	req := httptest.NewRequest(http.MethodGet, "https://"+v.Handle+"-8080."+s.domain+"/files/a%20b", nil)
	err := s.hostRoute(func(c echo.Context) error {
		require.Equal(t, "/vm/"+v.Handle+"/8080/files/a b", c.Request().URL.Path)
		return nil
	})(echo.New().NewContext(req, httptest.NewRecorder()))
	require.NoError(t, err)
	require.Equal(t, "/files/a b", req.URL.Path)
}

func TestBrowserSessionBoundToVMPortExpiryAndRotation(t *testing.T) {
	v := &types.VM{Handle: "dev-random", TrafficAccessToken: "owner-secret"}
	now := time.Now()
	session := accessSession(v, 8080, now.Add(time.Minute).Unix())
	require.True(t, validAccessSession(v, 8080, session, now))
	require.False(t, validAccessSession(v, 7681, session, now))
	require.False(t, validAccessSession(v, 8080, session, now.Add(2*time.Minute)))
	v.Handle = "other-random"
	require.False(t, validAccessSession(v, 8080, session, now))
	v.Handle, v.TrafficAccessToken = "dev-random", "rotated"
	require.False(t, validAccessSession(v, 8080, session, now))
}

func TestBrowserSessionExchangesAndNeverReachesGuest(t *testing.T) {
	s, v, _, _, _ := fixture()
	s.domain = "vm.example.com"
	v.TrafficAccessToken = "owner-secret"
	v.Spec.ProtectedPorts = []uint32{8080}
	e := proxyAPI(s)
	session := accessSession(v, 8080, time.Now().Add(time.Minute).Unix())
	path := "/vm/" + v.Handle + "/8080/desktop?existing=value&" + sessionParameter + "=" + session
	req := httptest.NewRequest("GET", "https://"+v.Handle+"-8080."+s.domain+path, nil)
	req.RequestURI = path
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusSeeOther, rec.Code, rec.Body.String())
	require.Equal(t, "/vm/"+v.Handle+"/8080/desktop?existing=value", rec.Header().Get("Location"))
	cookies := rec.Result().Cookies()
	require.Len(t, cookies, 1)
	require.True(t, cookies[0].Secure)
	require.True(t, cookies[0].HttpOnly)
	require.Equal(t, http.SameSiteLaxMode, cookies[0].SameSite)
	req = httptest.NewRequest("GET", "https://"+v.Handle+"-8080."+s.domain+rec.Header().Get("Location"), nil)
	req.AddCookie(cookies[0])
	req.AddCookie(&http.Cookie{Name: "application", Value: "preserved"})
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, 200, rec.Code, rec.Body.String())
	_, err := req.Cookie(sessionCookie(v, 8080))
	require.Error(t, err)
	own, err := req.Cookie("application")
	require.NoError(t, err)
	require.Equal(t, "preserved", own.Value)
}

func TestSessionCookieUsesPublishedOriginBehindH2C(t *testing.T) {
	for _, test := range []struct {
		domain, base string
		secure       bool
	}{
		{"vm.example.com", "https://gateway.example.com", true},
		{"vm.localhost:1994", "http://localhost:1994", false},
	} {
		t.Run(test.base, func(t *testing.T) {
			s, v, _, _, _ := fixture()
			s.domain, s.baseURL = test.domain, test.base
			v.TrafficAccessToken = "owner-secret"
			v.Spec.ProtectedPorts = []uint32{8080}
			path := "/vm/" + v.Handle + "/8080/?" + sessionParameter + "=" + accessSession(v, 8080, time.Now().Add(time.Minute).Unix())
			rec := httptest.NewRecorder()
			s.urls(v)
			origin, err := url.Parse(v.URLs[8080])
			require.NoError(t, err)
			req := httptest.NewRequest("GET", path, nil)
			req.Host = origin.Host
			proxyAPI(s).ServeHTTP(rec, req)
			require.Equal(t, http.StatusSeeOther, rec.Code)
			require.Len(t, rec.Result().Cookies(), 1)
			require.Equal(t, test.secure, rec.Result().Cookies()[0].Secure)
		})
	}
}

func TestSessionCookiesDoNotCollideOnSharedGatewayHost(t *testing.T) {
	v := &types.VM{Handle: "first"}
	other := &types.VM{Handle: "second"}
	require.NotEqual(t, sessionCookie(v, 8080), sessionCookie(v, 8000))
	require.NotEqual(t, sessionCookie(v, 8080), sessionCookie(other, 8080))
}

func TestDesktopServesViewerBeforeBackendReady(t *testing.T) {
	for _, state := range []types.ContainerStatus{types.ContainerStatusPending, types.ContainerStatusRunning} {
		t.Run(string(state), func(t *testing.T) {
			s, v, _, runtime, _ := fixture()
			v.Status = types.VMStateStarting
			runtime.containers.states[v.ContainerID].Status = state
			runtime.forwardStatus = http.StatusBadGateway
			e := proxyAPI(s)
			path := "/vm/" + v.Handle + "/8080/"
			request := func(path string) *httptest.ResponseRecorder {
				req := httptest.NewRequest(http.MethodGet, path, nil)
				req.Header.Set("Accept", "text/html")
				rec := httptest.NewRecorder()
				e.ServeHTTP(rec, req)
				return rec
			}

			response := request(path)
			require.Equal(t, http.StatusOK, response.Code)
			require.Contains(t, response.Body.String(), "Connecting to desktop…")
			require.Contains(t, response.Body.String(), "aria-label=\"Beam\"")
			require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
			require.Empty(t, runtime.forwarded)
			response = request(path + "vnc.html")
			if state == types.ContainerStatusPending {
				require.Equal(t, http.StatusServiceUnavailable, response.Code)
			} else {
				require.Equal(t, http.StatusBadGateway, response.Code)
			}

			runtime.containers.states[v.ContainerID].Status = types.ContainerStatusRunning
			runtime.forwardStatus = http.StatusOK
			response = request(path + "vnc.html")
			require.Equal(t, http.StatusOK, response.Code)
			require.NotEmpty(t, runtime.forwarded, "readiness requires a response from the desktop HTTP server")
			response = request(path)
			require.Equal(t, http.StatusOK, response.Code)
			require.Contains(t, response.Body.String(), "Connecting to desktop…")
		})
	}
}

func TestDesktopStartupStillRequiresItsTrafficCredential(t *testing.T) {
	s, v, _, runtime, _ := fixture()
	v.Spec.ProtectedPorts = []uint32{8080}
	v.TrafficAccessToken = "owner-secret"
	runtime.forwardStatus = http.StatusBadGateway
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		req := httptest.NewRequest(method, "/vm/"+v.Handle+"/8080/", nil)
		req.Header.Set("Accept", "text/html")
		rec := httptest.NewRecorder()
		proxyAPI(s).ServeHTTP(rec, req)
		require.Equal(t, http.StatusForbidden, rec.Code)
		require.NotContains(t, rec.Body.String(), "Starting desktop")
		require.Empty(t, runtime.forwarded)
	}
}

func TestDesktopStartupPageDoesNotReplaceOtherRequests(t *testing.T) {
	for _, test := range []struct {
		method, path, accept string
		desktop              bool
	}{
		{http.MethodGet, "/8080/vnc.html", "text/html", true},
		{http.MethodGet, "/8080/", "application/json", true},
		{http.MethodPost, "/8080/", "text/html", true},
		{http.MethodGet, "/7681/", "text/html", true},
		{http.MethodGet, "/8080/", "text/html", false},
	} {
		t.Run(test.method+test.path+test.accept, func(t *testing.T) {
			s, v, _, runtime, _ := fixture()
			v.Spec.Desktop = test.desktop
			runtime.forwardStatus = http.StatusBadGateway
			req := httptest.NewRequest(test.method, "/vm/"+v.Handle+test.path, nil)
			req.Header.Set("Accept", test.accept)
			rec := httptest.NewRecorder()
			proxyAPI(s).ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadGateway, rec.Code)
			require.NotEmpty(t, runtime.forwarded)
		})
	}
}

func TestAccessUsesLiveLaunchStateBeforeReconciliation(t *testing.T) {
	for _, test := range []struct {
		name, desired string
		state         types.ContainerStatus
		wantProxy     int
		wantTunnel    int
	}{
		{"ready", "running", types.ContainerStatusRunning, 200, 200},
		{"pending", "running", types.ContainerStatusPending, 503, 409},
		{"stopped intent", "stopped", types.ContainerStatusRunning, 503, 409},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, v, info, runtime, _ := fixture()
			v.Status, v.DesiredState = "starting", test.desired
			runtime.containers.states[v.ContainerID].Status = test.state
			response := vmRequest(proxyAPI(s), "GET", "/vm/"+v.Handle+"/8080/", "")
			require.Equal(t, test.wantProxy, response.Code, response.Body.String())
			e := managementAPI(s, info)
			e.GET("/:workspaceId/:name/tunnel/:port", auth.WithStrictWorkspaceAuth(s.tunnel))
			response = vmRequest(e, "GET", "/"+info.Workspace.ExternalId+"/"+v.ID+"/tunnel/7681", "")
			require.Equal(t, test.wantTunnel, response.Code, response.Body.String())
			require.Equal(t, "starting", v.Status, "access must not persist lifecycle/checkpoint state")
		})
	}
}

func TestAccessFailsClosedWhenRuntimeStateIsUnavailable(t *testing.T) {
	s, v, info, runtime, _ := fixture()
	v.Status = "starting"
	runtime.containers.readError = errors.New("redis unavailable")
	response := vmRequest(proxyAPI(s), "GET", "/vm/"+v.Handle+"/8080/", "")
	require.Equal(t, 503, response.Code)
	require.Empty(t, runtime.forwarded)
	e := managementAPI(s, info)
	e.GET("/:workspaceId/:name/tunnel/:port", auth.WithStrictWorkspaceAuth(s.tunnel))
	response = vmRequest(e, "GET", "/"+info.Workspace.ExternalId+"/"+v.ID+"/tunnel/7681", "")
	require.Equal(t, 503, response.Code)
}

func TestBrowserSessionRejectsSharedHostAndOtherOrigins(t *testing.T) {
	for _, test := range []struct {
		name, host, origin string
		status             int
	}{
		{"same origin", "dev-random-8080.vm.example.com", "https://dev-random-8080.vm.example.com", http.StatusOK},
		{"shared gateway", "gateway.example.com", "", http.StatusForbidden},
		{"other VM", "dev-random-8080.vm.example.com", "https://other-random-8080.vm.example.com", http.StatusForbidden},
		{"other port", "dev-random-8080.vm.example.com", "https://dev-random-8000.vm.example.com", http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, v, _, runtime, _ := fixture()
			s.domain = "vm.example.com"
			v.Handle = "dev-random"
			v.TrafficAccessToken = "owner-secret"
			v.Spec.ProtectedPorts = []uint32{8080}
			req := httptest.NewRequest("GET", "/vm/"+v.Handle+"/8080/", nil)
			req.Host = test.host
			req.Header.Set("Origin", test.origin)
			req.AddCookie(&http.Cookie{Name: sessionCookie(v, 8080), Value: accessSession(v, 8080, time.Now().Add(time.Minute).Unix())})
			rec := httptest.NewRecorder()
			proxyAPI(s).ServeHTTP(rec, req)
			require.Equal(t, test.status, rec.Code, rec.Body.String())
			if test.status != http.StatusOK {
				require.Empty(t, runtime.forwarded)
			}
		})
	}
}

func TestSharedHostCannotExchangeBrowserSession(t *testing.T) {
	s, v, _, _, _ := fixture()
	s.domain = "vm.example.com"
	v.TrafficAccessToken = "owner-secret"
	path := "/vm/" + v.Handle + "/8080/?" + sessionParameter + "=" + accessSession(v, 8080, time.Now().Add(time.Minute).Unix())
	req := httptest.NewRequest("GET", "https://gateway.example.com"+path, nil)
	rec := httptest.NewRecorder()
	proxyAPI(s).ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, rec.Result().Cookies())
}

func TestPathModeRequiresExplicitTrafficToken(t *testing.T) {
	s, v, info, _, _ := fixture()
	s.domain = ""
	v.TrafficAccessToken = "owner-secret"
	v.Spec.ProtectedPorts = []uint32{8080}
	rec := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId+"/"+v.ID+"/access-session", `{"port":8080}`)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

	e := proxyAPI(s)
	path := "/vm/" + v.Handle + "/8080/"
	for _, token := range []string{"", "owner-secret"} {
		req := httptest.NewRequest("GET", path, nil)
		req.AddCookie(&http.Cookie{Name: sessionCookie(v, 8080), Value: accessSession(v, 8080, time.Now().Add(time.Minute).Unix())})
		req.Header.Set("X-Beam-VM-Token", token)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if token == "" {
			require.Equal(t, http.StatusForbidden, rec.Code)
		} else {
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Empty(t, req.Header.Get("X-Beam-VM-Token"))
			require.Empty(t, req.Header.Get("Cookie"))
		}
	}
}
