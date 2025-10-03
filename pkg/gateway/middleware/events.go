package middleware

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

// GatewayEvents returns middleware that tracks grpc-gateway endpoint calls by sending events to the event repository
func GatewayEvents(eventRepo repository.EventRepository) func(http.Handler) echo.HandlerFunc {
	return func(handler http.Handler) echo.HandlerFunc {
		return func(c echo.Context) error {
			handler.ServeHTTP(c.Response(), c.Request())

			method := c.Request().Method
			path := c.Request().URL.Path
			userAgent := c.Request().UserAgent()
			remoteIP := c.RealIP()
			requestID := c.Response().Header().Get(echo.HeaderXRequestID)
			contentType := c.Request().Header.Get(echo.HeaderContentType)
			accept := c.Request().Header.Get(echo.HeaderAccept)

			statusCode := c.Response().Status

			workspaceID := ""
			if workspace := c.Get("workspace"); workspace != nil {
				if ws, ok := workspace.(*types.Workspace); ok {
					workspaceID = ws.ExternalId
				}
			}

			errorMessage := ""
			if statusCode >= 400 {
				errorMessage = http.StatusText(statusCode)
			}

			go eventRepo.PushGatewayEndpointCalledEvent(
				method,
				path,
				workspaceID,
				statusCode,
				userAgent,
				remoteIP,
				requestID,
				contentType,
				accept,
				errorMessage,
			)

			return nil
		}
	}
}
