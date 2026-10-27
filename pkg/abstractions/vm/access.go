package vm

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
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

// Browser credentials are confined to the VM's configured host and origin.
// Cookie paths cannot isolate applications that share a gateway origin.
func (s *Service) browserOrigin(c echo.Context, v *types.VM, port uint32) (*url.URL, error) {
	if s.config.Domain == "" {
		return nil, echo.NewHTTPError(http.StatusForbidden, "browser sessions require a VM subdomain")
	}

	s.urls(v)
	origin, err := url.Parse(v.URLs[port])
	if err != nil || origin.Host == "" {
		return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "VM URL unavailable")
	}

	if !strings.EqualFold(c.Request().Host, origin.Host) {
		return nil, echo.NewHTTPError(http.StatusForbidden, "browser session requires the VM's published host")
	}

	if requested := c.Request().Header.Get("Origin"); requested != "" && !strings.EqualFold(requested, origin.Scheme+"://"+origin.Host) {
		return nil, echo.NewHTTPError(http.StatusForbidden, "browser session origin does not match the VM")
	}

	return origin, nil
}

// Exchange a short-lived URL for a host-only cookie before application traffic
// (including desktop WebSockets). Neither credential reaches the guest.
func (s *Service) acceptBrowserSession(c echo.Context, v *types.VM, port uint32) (bool, error) {
	session := c.QueryParam(sessionParameter)
	if session == "" {
		return false, nil
	}
	if c.Request().Method != http.MethodGet {
		return false, echo.NewHTTPError(http.StatusForbidden, "invalid or expired VM access session")
	}
	origin, err := s.browserOrigin(c, v, port)
	if err != nil {
		return false, err
	}
	clean, err := cleanBrowserURL(c)
	if err != nil {
		return false, err
	}
	if !validAccessSession(v, port, session, time.Now()) {
		if browserNavigation(c.Request()) {
			return true, s.invalidShareLink(c, clean.String())
		}
		return false, echo.NewHTTPError(http.StatusForbidden, "invalid or expired VM access session")
	}

	// The configured public origin remains authoritative behind h2c/TLS
	// termination. Only explicitly local HTTP origins omit Secure.
	host := origin.Hostname()
	ip := net.ParseIP(host)
	localHTTP := origin.Scheme == "http" && (host == "localhost" || strings.HasSuffix(host, ".localhost") || (ip != nil && ip.IsLoopback()))
	c.SetCookie(&http.Cookie{
		Name:     sessionCookie(v, port),
		Value:    session,
		Path:     "/",
		Expires:  time.Unix(sessionExpiry(session), 0),
		Secure:   !localHTTP,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})

	setBrowserHeaders(c)
	return true, c.Redirect(http.StatusSeeOther, clean.String())
}

func (s *Service) browserSession(c echo.Context, v *types.VM, port uint32) bool {
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

	if !valid {
		return false
	}

	_, err = s.browserOrigin(c, v, port)
	return err == nil
}

// An operator may delegate top-level browser sign-in to an external provider.
// The provider must authorize the viewer before requesting an access session
// through the authenticated management API. Redirecting grants no VM access.
// APIs, WebSockets and embedded apps retain their explicit authentication errors.
func (s *Service) redirectBrowserLogin(c echo.Context, v *types.VM, port uint32) (bool, error) {
	r := c.Request()
	if !browserNavigation(r) || r.Header.Get("X-Beam-VM-Token") != "" || r.Header.Get("Authorization") != "" || v.WorkspaceExternalID == "" || v.TrafficAccessToken == "" {
		return false, nil
	}

	if s.config.BrowserAuthURL == "" {
		return false, nil
	}
	if _, err := s.browserOrigin(c, v, port); err != nil {
		return false, err
	}
	login, err := url.Parse(s.config.BrowserAuthURL)
	if err != nil || login.Host == "" || login.User != nil || login.Fragment != "" ||
		(login.Scheme != "https" && !(login.Scheme == "http" && (login.Hostname() == "localhost" || net.ParseIP(login.Hostname()).IsLoopback()))) {
		return false, echo.NewHTTPError(http.StatusServiceUnavailable, "VM browser sign-in unavailable")
	}
	next, err := cleanBrowserURL(c)
	if err != nil {
		return false, err
	}

	query := login.Query()
	query.Set("workspace", v.WorkspaceExternalID)
	query.Set("vm", v.ID)
	query.Set("port", strconv.FormatUint(uint64(port), 10))
	query.Set("next", next.String())
	login.RawQuery = query.Encode()
	setBrowserHeaders(c)
	return true, c.Redirect(http.StatusSeeOther, login.String())
}

func (s *Service) hostRoute(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		host := strings.ToLower(c.Request().Host)
		domain := strings.ToLower(s.config.Domain)
		if strings.HasSuffix(host, "."+domain) {
			label := strings.TrimSuffix(host, "."+domain)
			handle, port, ok := splitHost(label)
			_, appRouteError := common.ParseSubdomainFields(label)
			if ok && appRouteError != nil {
				// Route through Echo normally so host-based access retains
				// the gateway's recovery, tracing and other middleware.
				u := c.Request().URL
				path, rawPath := u.Path, u.RawPath
				prefix := "/vm/" + handle + "/" + port
				u.Path = prefix + "/" + strings.TrimPrefix(path, "/")
				if rawPath != "" {
					u.RawPath = prefix + "/" + strings.TrimPrefix(rawPath, "/")
				}

				defer func() { u.Path, u.RawPath = path, rawPath }()
			}
		}

		return next(c)
	}
}

func splitHost(label string) (string, string, bool) {
	i := strings.LastIndex(label, "-")
	if i < 0 {
		return "", "", false
	}

	p, err := strconv.Atoi(label[i+1:])
	return label[:i], label[i+1:], err == nil && p > 0 && p <= 65535
}

func (s *Service) proxy(c echo.Context) error {
	ctx := c.Request().Context()
	v, err := s.repo.GetVMByHandle(ctx, c.Param("handle"))
	if err != nil || v.DesiredState == types.VMStateDeleted {
		return echo.NewHTTPError(http.StatusNotFound, "VM not found")
	}

	port, err := strconv.Atoi(c.Param("port"))
	if err != nil || port == types.VMSSHPort {
		return echo.NewHTTPError(http.StatusNotFound)
	}

	if !slices.Contains(v.Spec.Ports, uint32(port)) {
		return echo.NewHTTPError(http.StatusNotFound)
	}

	token, err := s.backend.GetTokenByExternalId(ctx, v.WorkspaceID, v.TokenID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "VM authorization unavailable")
	}

	if token == nil || !token.Active || token.DisabledByClusterAdmin {
		return echo.NewHTTPError(http.StatusForbidden, "VM access is revoked")
	}

	if handled, err := s.acceptBrowserSession(c, v, uint32(port)); handled || err != nil {
		return err
	}

	validSession := s.browserSession(c, v, uint32(port))
	if slices.Contains(v.Spec.ProtectedPorts, uint32(port)) {
		provided := c.Request().Header.Get("X-Beam-VM-Token")
		if !validSession && (v.TrafficAccessToken == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(v.TrafficAccessToken)) != 1) {
			if handled, err := s.redirectBrowserLogin(c, v, uint32(port)); handled || err != nil {
				return err
			}
			return echo.NewHTTPError(http.StatusForbidden, "VM traffic token required")
		}
	}

	c.Request().Header.Del("X-Beam-VM-Token")
	release, err := s.keepActive(ctx, v.ID, v.Spec.IdleTimeout)
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "VM activity unavailable")
	}

	defer release()
	// Capability URLs contain only the VM's random identity, never a workspace
	// token. Authenticated management and raw SSH retain workspace auth.
	status, err := s.launchStatus(v)
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "VM runtime unavailable")
	}

	if desktopEntry(c, v, port) {
		return s.serveDesktop(c, v, status)
	}

	if v.DesiredState != types.VMStateRunning || status != types.VMStateRunning {
		if !v.Spec.AutoResume {
			return echo.NewHTTPError(http.StatusServiceUnavailable, "VM is not running; start it to use this URL")
		}

		v, err = s.wakeForAccess(ctx, v, uint32(port))
		if err != nil {
			return err
		}
	}

	// Hold activity while a desktop/terminal websocket is open, even when the
	// user is watching without sending input.
	if v.Spec.Kiosk != nil && port == types.VMDesktopPort {
		return s.proxyKiosk(c, v)
	}
	return s.forwardVM(c, v, uint32(port), c.Param("*"))
}

func (s *Service) forwardVM(c echo.Context, v *types.VM, port uint32, subPath string) error {
	c.SetParamNames("port", "subPath")
	c.SetParamValues(strconv.FormatUint(uint64(port), 10), subPath)
	return s.runtime.ForwardVM(c, v.StubID, v.ContainerID)
}

func (s *Service) tunnel(c echo.Context) error {
	ctx, info := requestContext(c)
	v, err := s.repo.GetVM(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}

	port, err := strconv.Atoi(c.Param("port"))
	if err != nil || port < 1 || port > 65535 {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid port")
	}

	if port == types.VMSSHPort && !v.Spec.SSH {
		return echo.NewHTTPError(http.StatusBadRequest, "SSH is disabled")
	}

	status, err := s.launchStatus(v)
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "VM runtime unavailable")
	}

	if v.DesiredState != types.VMStateRunning || status != types.VMStateRunning {
		return echo.NewHTTPError(http.StatusConflict, "VM is not running")
	}

	if !slices.Contains(v.Spec.RuntimePorts(), uint32(port)) {
		return echo.NewHTTPError(http.StatusBadRequest, "expose the port before opening a tunnel")
	}

	release, err := s.keepActive(ctx, v.ID, v.Spec.IdleTimeout)
	if err != nil {
		return apiError(err)
	}

	defer release()
	return s.runtime.TunnelVM(c, v.ContainerID, uint32(port))
}

func (s *Service) createAccessSession(c echo.Context, v *types.VM, port uint32, ttl int64) error {
	if s.config.Domain == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "browser access sessions require a VM subdomain")
	}

	if !slices.Contains(v.Spec.ProtectedPorts, port) {
		return echo.NewHTTPError(http.StatusBadRequest, "access sessions require a protected published port")
	}

	if ttl == 0 {
		ttl = 600
	}

	if ttl < 1 || ttl > 3600 {
		return echo.NewHTTPError(http.StatusBadRequest, "session ttl must be 1–3600 seconds")
	}

	s.urls(v)
	expires := time.Now().Add(time.Duration(ttl) * time.Second).Unix()
	return c.JSON(http.StatusOK, map[string]any{"url": v.URLs[port] + "?" + sessionParameter + "=" + accessSession(v, port, expires), "expires_at": expires})
}

func (s *Service) urls(v *types.VM) {
	v.BrowserAuth = s.config.Domain != "" && s.config.BrowserAuthURL != ""
	v.URLs = map[uint32]string{}
	for _, port := range v.Spec.Ports {
		if port == types.VMSSHPort {
			continue
		}

		if s.config.Domain != "" {
			scheme := "https"
			if strings.HasSuffix(strings.Split(s.config.Domain, ":")[0], ".localhost") || strings.Split(s.config.Domain, ":")[0] == "localhost" {
				scheme = "http"
			}

			v.URLs[port] = fmt.Sprintf("%s://%s-%d.%s/", scheme, v.Handle, port, s.config.Domain)
		} else {
			v.URLs[port] = fmt.Sprintf("%s/vm/%s/%d/", s.config.BaseURL, v.Handle, port)
		}
	}

	v.TerminalURL = v.URLs[types.VMTerminalPort]
	if v.Spec.Desktop {
		v.DesktopURL = v.URLs[types.VMDesktopPort]
	}
	if v.Spec.Kiosk != nil {
		v.KioskURL = v.DesktopURL
		if v.Spec.Kiosk.MCP != nil && v.URLs[v.Spec.Kiosk.MCP.Port] != "" {
			v.MCPURL = strings.TrimRight(v.URLs[v.Spec.Kiosk.MCP.Port], "/") + v.Spec.Kiosk.MCP.Path
		}
	}
}
