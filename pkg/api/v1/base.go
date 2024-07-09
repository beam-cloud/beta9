package apiv1

import "github.com/labstack/echo/v4"

const (
	HttpServerBaseRoute string = "/api/v1"
	HttpServerRootRoute string = ""
)

func NewHTTPError(ctx echo.Context, code int, message string) error {
	return ctx.JSON(code, map[string]interface{}{
		"message": message,
	})
}
