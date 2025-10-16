package middleware

import (
	"net/http"
	"strings"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/labstack/echo/v4"
)

// GatewayEvents returns middleware that tracks grpc-gateway endpoint calls by sending events to the event repository
func GatewayEvents(eventRepo repository.EventRepository, backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository) func(http.Handler) echo.HandlerFunc {
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
			authHeader := c.Request().Header.Get("Authorization")
			tokenKey := strings.TrimPrefix(authHeader, "Bearer ")

			if tokenKey != "" {
				if _, ws, err := workspaceRepo.AuthorizeToken(tokenKey); err == nil && ws != nil {
					workspaceID = ws.ExternalId
				} else {
					if _, ws, err := backendRepo.AuthorizeToken(c.Request().Context(), tokenKey); err == nil && ws != nil {
						workspaceID = ws.ExternalId
					}
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
