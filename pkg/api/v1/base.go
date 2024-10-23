package apiv1

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

const (
	HttpServerBaseRoute string = "/api/v1"
	HttpServerRootRoute string = ""
)

func NewHTTPError(code int, message string) error {
	return echo.NewHTTPError(code, map[string]interface{}{
		"message": message,
	})
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
