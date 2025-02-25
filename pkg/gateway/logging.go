package gateway

import (
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog/log"
)

func configureEchoLogger(e *echo.Echo, debug bool) {
	if debug {
		e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
			LogStatus:    true,
			LogRoutePath: true,
			LogURIPath:   true,
			LogError:     true,
			LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
				stubId, ok := c.Get("stubId").(string)
				if !ok {
					stubId = "unknown"
				}

				if v.Error != nil {
					log.Error().
						Str("error", v.Error.Error()).
						Str("method", c.Request().Method).
						Str("route", v.RoutePath).
						Str("path", v.URIPath).
						Str("stub_id", stubId).
						Int("status", v.Status).
						Msg("request error")
				} else {
					log.Info().
						Str("method", c.Request().Method).
						Str("route", v.RoutePath).
						Str("path", v.URIPath).
						Str("stub_id", stubId).
						Int("status", v.Status).
						Msg("request")
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
