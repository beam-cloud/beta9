package vm

import (
	_ "embed"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

//go:embed desktop.html
var desktopPage string

//go:embed kiosk.html
var streamingDesktopPage string

//go:embed kiosk-webrtc.js
var desktopWebRTC []byte

func desktopEntry(c echo.Context, v *types.VM, port int) bool {
	r := c.Request()
	return v.Spec.Desktop && port == types.VMDesktopPort && c.Param("*") == "" &&
		r.Method == http.MethodGet && strings.Contains(r.Header.Get("Accept"), "text/html")
}

func (s *Service) serveDesktop(c echo.Context, v *types.VM, status string) error {
	if v.Spec.StreamingDesktop() && !strings.HasSuffix(c.Request().URL.Path, "/") {
		canonical := *c.Request().URL
		canonical.Path += "/"
		if canonical.RawPath != "" {
			canonical.RawPath += "/"
		}
		return c.Redirect(http.StatusTemporaryRedirect, canonical.String())
	}
	if v.DesiredState != types.VMStateRunning || (status != types.VMStateRunning && status != types.VMStateStarting) {
		if !v.Spec.AutoResume {
			return echo.NewHTTPError(http.StatusServiceUnavailable, "VM is not running; start it to use this URL")
		}
		if _, err := s.wakeForAccess(c.Request().Context(), v, types.VMDesktopPort); err != nil {
			return err
		}
	}

	c.Response().Header().Set("Cache-Control", "no-store")
	if v.Spec.StreamingDesktop() {
		allowDesktopFrame(c)
		return c.HTML(http.StatusOK, streamingDesktopPage)
	}
	return c.HTML(http.StatusOK, desktopPage)
}

// Creation and port updates enforce the same private and protected endpoints.
func validateDesktopPort(spec types.VMSpec, port uint32, public bool) error {
	if slices.Contains(spec.DesktopPrivatePorts(), port) {
		return fmt.Errorf("port %d is reserved for desktop services", port)
	}
	if spec.StreamingDesktop() && public && port == types.VMDesktopPort {
		return fmt.Errorf("desktop browser sessions must be protected")
	}
	return nil
}

type desktopContext struct {
	echo.Context
	*Service
	vm *types.VM
}

func (c *desktopContext) forward(port uint32, path string) error {
	if port == types.VMDesktopPort {
		allowDesktopFrame(c)
	}
	return c.forwardVM(c, c.vm, port, path)
}

// Authenticated desktop links can be embedded on any site. Selkies defaults to
// frame-ancestors 'self'; adapt that default after upstream headers arrive so
// existing VM images work without coupling embedding to the sign-in provider.
func allowDesktopFrame(c echo.Context) {
	c.Response().Before(func() {
		headers := c.Response().Header()
		policies := headers.Values("Content-Security-Policy")
		if len(policies) == 0 {
			headers.Set("Content-Security-Policy", "frame-ancestors *")
			return
		}
		headers.Del("Content-Security-Policy")
		for _, policy := range policies {
			directives := strings.Split(policy, ";")
			for i, directive := range directives {
				fields := strings.Fields(directive)
				// Only relax the native default, preserving all other directives
				// and any explicitly configured frame-ancestors policy.
				if len(fields) == 2 && strings.EqualFold(fields[0], "frame-ancestors") && fields[1] == "'self'" {
					directives[i] = strings.Replace(directive, "'self'", "*", 1)
				}
			}
			headers.Add("Content-Security-Policy", strings.Join(directives, ";"))
		}
	})
}

var desktopRoutes = newDesktopRouter()

func newDesktopRouter() *echo.Router {
	e := echo.New()
	e.Any(types.VMKioskControlPath+"/webrtc.js", func(c echo.Context) error {
		c.Response().Header().Set("Cache-Control", "no-store")
		return c.Blob(http.StatusOK, "text/javascript; charset=utf-8", desktopWebRTC)
	})
	e.Any(types.VMKioskControlPath+"/webrtc/session", func(c echo.Context) error {
		session := c.(*desktopContext)
		return session.webrtc.register(c, session.vm)
	})
	e.Any(types.VMKioskControlPath+"/status", func(c echo.Context) error {
		if c.Request().Method != http.MethodGet {
			return echo.NewHTTPError(http.StatusMethodNotAllowed)
		}
		return c.(*desktopContext).forward(types.VMKioskStatusPort, "status")
	})
	e.Any(types.VMKioskControlPath+"/restart", func(c echo.Context) error {
		if c.Request().Method != http.MethodPost || c.Request().Header.Get(types.VMKioskActionHeader) != "restart" {
			return echo.NewHTTPError(http.StatusForbidden, "explicit desktop restart required")
		}
		return c.(*desktopContext).forward(types.VMKioskStatusPort, "restart")
	})
	e.Any(types.VMKioskMCPPath, func(c echo.Context) error {
		if c.(*desktopContext).vm.Spec.Kiosk == nil {
			return echo.NewHTTPError(http.StatusNotFound)
		}
		return c.(*desktopContext).forward(types.VMKioskMCPPort, strings.TrimPrefix(types.VMKioskMCPPath, "/"))
	})
	for _, path := range []string{types.VMKioskStreamPath + "/*", "/*"} {
		e.Any(path, func(c echo.Context) error {
			return c.(*desktopContext).forward(types.VMDesktopPort, c.Param("*"))
		})
	}
	return e.Router()
}

// proxyDesktop runs after the published desktop port's authentication and VM
// lifecycle checks. Its private control routes inherit that same authority.
func (s *Service) proxyDesktop(c echo.Context, v *types.VM) error {
	path := "/" + c.Param("*")
	// Echo matches against its native context; attach the VM after selection.
	desktopRoutes.Find(c.Request().Method, path, c)
	return c.Handler()(&desktopContext{Context: c, Service: s, vm: v})
}
