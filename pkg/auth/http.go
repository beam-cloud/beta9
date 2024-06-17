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

			if authHeader == "" || tokenKey == "" {
				return next(c)
			}

			token, workspace, err := backendRepo.AuthorizeToken(c.Request().Context(), tokenKey)
			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized)
			}

			if !token.Active || token.DisabledByClusterAdmin {
				return echo.NewHTTPError(http.StatusUnauthorized)
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

func WithAuth(next func(ctx echo.Context) error) func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		cc, ok := ctx.(*HttpAuthContext)
		if !ok {
			return echo.NewHTTPError(http.StatusUnauthorized)
		}

		return next(cc)
	}
}

func WithAssumedStubAuth(next func(ctx echo.Context) error, isPublic func(stubId string) (*types.Workspace, error)) func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		stubId := ctx.Param("stubId")

		workspace, err := isPublic(stubId)
		if err != nil {
			return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
				"error": "invalid stub id",
			})
		}

		authInfo := &AuthInfo{
			Workspace: workspace,

			// We do not need the users token for assumed auth endpoints
			// since we can look up a valid token by workspace ID
			Token: nil,
		}

		cc := &HttpAuthContext{ctx, authInfo}
		return next(cc)
	}
}

func WithWorkspaceAuth(next func(ctx echo.Context) error) func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		workspaceId := ctx.Param("workspaceId")

		cc, ok := ctx.(*HttpAuthContext)
		if !ok {
			return echo.NewHTTPError(http.StatusUnauthorized)
		}

		if cc.AuthInfo.Workspace.ExternalId != workspaceId && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
			return echo.NewHTTPError(http.StatusUnauthorized)
		}

		return next(ctx)
	}
}

func WithClusterAdminAuth(next func(ctx echo.Context) error) func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		cc, ok := ctx.(*HttpAuthContext)
		if !ok {
			return echo.NewHTTPError(http.StatusUnauthorized)
		}

		if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
			return echo.NewHTTPError(http.StatusUnauthorized)
		}

		return next(ctx)
	}
}
