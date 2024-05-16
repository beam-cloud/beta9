package auth

import (
	"net/http"
	"strings"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
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

func WithWorkspaceAuth(next func(ctx echo.Context) error) func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		workspaceId := ctx.Param("workspaceId")

		cc, _ := ctx.(*HttpAuthContext)
		if cc.AuthInfo.Workspace.ExternalId != workspaceId && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
			return echo.NewHTTPError(http.StatusUnauthorized)
		}

		return next(ctx)
	}
}
