package gateway

import (
	"log/slog"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func configureEchoLogger(e *echo.Echo, debug bool) {
	if debug {
		e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
			LogStatus:    true,
			LogRoutePath: true,
			LogURIPath:   true,
			LogError:     true,
			LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
				if v.Error != nil {
					slog.Error("request error", "error", v.Error, "method", c.Request().Method, "uri", v.URIPath, "status", v.Status)
				} else {
					slog.Info("request", "method", c.Request().Method, "uri", v.URIPath, "status", v.Status)
				}
				return nil
			},
			Skipper: func(c echo.Context) bool {
				return c.Request().URL.Path == "/api/v1/health"
			},
		}))
	} else {
		e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
			Skipper: func(c echo.Context) bool {
				return c.Request().URL.Path == "/api/v1/health"
			},
		}))
	}
}
