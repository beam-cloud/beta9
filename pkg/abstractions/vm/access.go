package vm

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
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

func validAccessSession(v *types.VM, port uint32, session string, now time.Time) bool {
	expiry, _, ok := strings.Cut(session, ".")
	expires, err := strconv.ParseInt(expiry, 10, 64)
	return ok && err == nil && expires > now.Unix() && v.TrafficAccessToken != "" && hmac.Equal([]byte(session), []byte(accessSession(v, port, expires)))
}

// Exchange a short-lived URL for a host-only cookie before application traffic
// (including desktop WebSockets). Neither credential reaches the guest.
func acceptBrowserSession(c echo.Context, v *types.VM, port uint32) (bool, error) {
	if session := c.QueryParam(sessionParameter); session != "" {
		if c.Request().Method != http.MethodGet || !validAccessSession(v, port, session, time.Now()) {
			return false, echo.NewHTTPError(403, "invalid or expired VM access session")
		}
		expiry, _, _ := strings.Cut(session, ".")
		expires, _ := strconv.ParseInt(expiry, 10, 64)
		c.SetCookie(&http.Cookie{Name: sessionParameter, Value: session, Path: "/", Expires: time.Unix(expires, 0), Secure: c.Scheme() == "https", HttpOnly: true, SameSite: http.SameSiteStrictMode})
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
	cookie, err := c.Cookie(sessionParameter)
	valid := err == nil && validAccessSession(v, port, cookie.Value, time.Now())
	// Preserve the application's own cookies without forwarding ours.
	cookies := c.Request().Cookies()
	c.Request().Header.Del("Cookie")
	for _, other := range cookies {
		if other.Name != sessionParameter {
			c.Request().AddCookie(other)
		}
	}
	return valid
}
