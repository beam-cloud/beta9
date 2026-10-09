package vm

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

const sessionParameter = "beam_vm_session"

func accessSession(v *types.VM, port uint32, expires int64) string {
	payload := fmt.Sprintf("%s:%d:%d", v.Handle, port, expires)
	mac := hmac.New(sha256.New, []byte(v.TrafficAccessToken))
	mac.Write([]byte(payload))
	return strconv.FormatInt(expires, 10) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func sessionExpiry(session string) int64 {
	expiry, _, ok := strings.Cut(session, ".")
	expires, err := strconv.ParseInt(expiry, 10, 64)
	if !ok || err != nil {
		return 0
	}
	return expires
}

func sessionCookie(v *types.VM, port uint32) string {
	return fmt.Sprintf("%s_%s_%d", sessionParameter, v.Handle, port)
}

func validAccessSession(v *types.VM, port uint32, session string, now time.Time) bool {
	expires := sessionExpiry(session)
	return expires > now.Unix() && v.TrafficAccessToken != "" && hmac.Equal([]byte(session), []byte(accessSession(v, port, expires)))
}

// Exchange a short-lived URL for a host-only cookie before application traffic
// (including desktop WebSockets). Neither credential reaches the guest.
func (s *Service) acceptBrowserSession(c echo.Context, v *types.VM, port uint32) (bool, error) {
	if session := c.QueryParam(sessionParameter); session != "" {
		if c.Request().Method != http.MethodGet || !validAccessSession(v, port, session, time.Now()) {
			return false, echo.NewHTTPError(403, "invalid or expired VM access session")
		}
		expires := sessionExpiry(session)
		s.urls(v)
		origin, err := url.Parse(v.URLs[port])
		if err != nil {
			return false, echo.NewHTTPError(503, "VM URL unavailable")
		}
		// The configured public origin remains authoritative behind h2c/TLS
		// termination. Only explicitly local HTTP origins omit Secure.
		host := origin.Hostname()
		ip := net.ParseIP(host)
		localHTTP := origin.Scheme == "http" && (host == "localhost" || strings.HasSuffix(host, ".localhost") || (ip != nil && ip.IsLoopback()))
		c.SetCookie(&http.Cookie{Name: sessionCookie(v, port), Value: session, Path: "/", Expires: time.Unix(expires, 0), Secure: !localHTTP, HttpOnly: true, SameSite: http.SameSiteLaxMode})
		query := c.Request().URL.Query()
		query.Del(sessionParameter)
		// RequestURI preserves the original browser path before host rewriting.
		u, err := url.ParseRequestURI(c.Request().RequestURI)
		if err != nil || u.IsAbs() || u.Host != "" || strings.HasPrefix(u.Path, "//") {
			return false, echo.NewHTTPError(400, "invalid request URI")
		}
		u.RawQuery = query.Encode()
		c.Response().Header().Set("Referrer-Policy", "no-referrer")
		c.Response().Header().Set("Cache-Control", "no-store")
		return true, c.Redirect(http.StatusSeeOther, u.String())
	}
	return false, nil
}

func browserSession(c echo.Context, v *types.VM, port uint32) bool {
	cookie, err := c.Cookie(sessionCookie(v, port))
	valid := err == nil && validAccessSession(v, port, cookie.Value, time.Now())
	// Preserve the application's own cookies without forwarding ours.
	cookies := c.Request().Cookies()
	c.Request().Header.Del("Cookie")
	for _, other := range cookies {
		if !strings.HasPrefix(other.Name, sessionParameter+"_") {
			c.Request().AddCookie(other)
		}
	}
	return valid
}
