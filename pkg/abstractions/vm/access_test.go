package vm

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
	v.TrafficAccessToken = "owner-secret"
	v.Spec.ProtectedPorts = []uint32{8080}
	e := proxyAPI(s)
	session := accessSession(v, 8080, time.Now().Add(time.Minute).Unix())
	path := "/vm/" + v.Handle + "/8080/desktop?existing=value&" + sessionParameter + "=" + session
	req := httptest.NewRequest("GET", "https://vm.example.com"+path, nil)
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
	req = httptest.NewRequest("GET", rec.Header().Get("Location"), nil)
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

func TestSessionCookiesDoNotCollideOnSharedGatewayHost(t *testing.T) {
	v := &types.VM{Handle: "first"}
	other := &types.VM{Handle: "second"}
	require.NotEqual(t, sessionCookie(v, 8080), sessionCookie(v, 8000))
	require.NotEqual(t, sessionCookie(v, 8080), sessionCookie(other, 8080))
}
