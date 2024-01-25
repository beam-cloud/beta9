package auth

import (
	"strings"

	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/labstack/echo/v4"
)

type HttpAuthContext struct {
	echo.Context
	AuthInfo *AuthInfo
}

func AuthMiddleware(backendRepo repository.BackendRepository) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			req := c.Request()
			authHeader := req.Header.Get("Authorization")
			tokenKey := strings.TrimPrefix(authHeader, "Bearer ")

			token, workspace, err := backendRepo.AuthorizeToken(c.Request().Context(), tokenKey)
			if err != nil {
				return echo.ErrUnauthorized
			}

			authInfo := &AuthInfo{
				Token:     token,
				Workspace: workspace,
			}

			cc := &HttpAuthContext{c, authInfo}
			return next(cc)
		}
	}
}
