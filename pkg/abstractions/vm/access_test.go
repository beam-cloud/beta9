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
	"github.com/stretchr/testify/require"
)

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
