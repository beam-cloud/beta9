package apiv1

import "github.com/labstack/echo/v4"

const (
	HttpServerBaseRoute string = "/api/v1"
	HttpServerRootRoute string = ""
)

func NewHTTPError(code int, message string) error {
	return echo.NewHTTPError(code, map[string]interface{}{
		"message": message,
	})
}
