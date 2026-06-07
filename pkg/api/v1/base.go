package apiv1

import (
	"context"
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"
)

const (
	HttpServerBaseRoute       string = "/api/v1"
	HttpServerRootRoute       string = ""
	statusClientClosedRequest        = 499
)

func NewHTTPError(code int, message string) error {
	return echo.NewHTTPError(code, map[string]interface{}{
		"message": message,
	})
}

func HTTPClientClosedRequest(ctx echo.Context) error {
	if ctx.Request().Context().Err() != nil {
		return nil
	}
	return ctx.NoContent(statusClientClosedRequest)
}

func IsRequestCanceled(ctx echo.Context, err error) bool {
	requestErr := ctx.Request().Context().Err()
	return errors.Is(requestErr, context.Canceled) ||
		errors.Is(requestErr, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled)
}

func HTTPBadRequest(message string) error {
	return NewHTTPError(http.StatusBadRequest, message)
}

func HTTPInternalServerError(message string) error {
	return NewHTTPError(http.StatusInternalServerError, message)
}

func HTTPConflict(message string) error {
	return NewHTTPError(http.StatusConflict, message)
}

func HTTPUnauthorized(message string) error {
	return NewHTTPError(http.StatusUnauthorized, message)
}

func HTTPForbidden(message string) error {
	return NewHTTPError(http.StatusForbidden, message)
}

func HTTPNotFound() error {
	return NewHTTPError(http.StatusNotFound, "")
}
