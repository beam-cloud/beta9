package gateway

import (
	"os"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog"
)

func configureEchoLogger(e *echo.Echo, debug bool) {
	if debug {
		// Configure logger with better UI for debugging purposes (less efficient than the default logger)
		logger := zerolog.New(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02T15:04:05",
		}).With().Timestamp().Logger()

		e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
			LogStatus:    true,
			LogRoutePath: true,
			LogURIPath:   true,
			LogError:     true,
			LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
				if v.Error != nil {
					logger.Err(v.Error).
						Str("method", c.Request().Method).
						Str("URI", v.URIPath).
						Int("status", v.Status).
						Msg("")
				} else {
					logger.Info().
						Str("method", c.Request().Method).
						Str("URI", v.URIPath).
						Int("status", v.Status).
						Msg("")
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
