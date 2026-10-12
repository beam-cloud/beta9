package vm

import (
	_ "embed"
	"net/http"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

//go:embed desktop.html
var desktopPage string

func desktopEntry(c echo.Context, v *types.VM, port int) bool {
	r := c.Request()
	return v.Spec.Desktop && port == 8080 && c.Param("*") == "" &&
		r.Method == http.MethodGet && strings.Contains(r.Header.Get("Accept"), "text/html")
}

func (s *Service) serveDesktop(c echo.Context, v *types.VM, status string) error {
	if v.DesiredState != types.VMStateRunning || (status != types.VMStateRunning && status != types.VMStateStarting) {
		if !v.Spec.AutoResume {
			return echo.NewHTTPError(http.StatusServiceUnavailable, "VM is not running; start it to use this URL")
		}
		if _, err := s.wakeForAccess(c.Request().Context(), v, 8080); err != nil {
			return err
		}
	}

	c.Response().Header().Set("Cache-Control", "no-store")
	return c.HTML(http.StatusOK, desktopPage)
}
