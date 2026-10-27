package vm

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type kioskContext struct {
	echo.Context
	*Service
	vm *types.VM
}

func (c *kioskContext) forward(port uint32, path string) error {
	return c.forwardVM(c, c.vm, port, path)
}

var kioskRoutes = newKioskRouter()

func newKioskRouter() *echo.Router {
	e := echo.New()
	e.Any(types.VMKioskControlPath+"/webrtc.js", func(c echo.Context) error {
		c.Response().Header().Set("Cache-Control", "no-store")
		return c.Blob(http.StatusOK, "text/javascript; charset=utf-8", kioskWebRTC)
	})
	e.Any(types.VMKioskControlPath+"/webrtc/session", func(c echo.Context) error {
		session := c.(*kioskContext)
		return session.webrtc.register(c, session.vm)
	})
	e.Any(types.VMKioskControlPath+"/status", func(c echo.Context) error {
		if c.Request().Method != http.MethodGet {
			return echo.NewHTTPError(http.StatusMethodNotAllowed)
		}
		return c.(*kioskContext).forward(types.VMKioskStatusPort, "status")
	})
	e.Any(types.VMKioskControlPath+"/restart", func(c echo.Context) error {
		if c.Request().Method != http.MethodPost || c.Request().Header.Get(types.VMKioskActionHeader) != "restart" {
			return echo.NewHTTPError(http.StatusForbidden, "explicit kiosk restart required")
		}
		return c.(*kioskContext).forward(types.VMKioskStatusPort, "restart")
	})
	for _, path := range []string{types.VMKioskStreamPath + "/*", "/*"} {
		e.Any(path, func(c echo.Context) error {
			return c.(*kioskContext).forward(types.VMDesktopPort, c.Param("*"))
		})
	}
	return e.Router()
}

// proxyKiosk runs after the published desktop port's authentication and VM
// lifecycle checks. Its private control routes inherit that same authority.
func (s *Service) proxyKiosk(c echo.Context, v *types.VM) error {
	path := "/" + c.Param("*")
	// Echo matches against its native context; attach the VM after selection.
	kioskRoutes.Find(c.Request().Method, path, c)
	return c.Handler()(&kioskContext{Context: c, Service: s, vm: v})
}
