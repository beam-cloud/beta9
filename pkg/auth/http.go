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

func AuthMiddleware(backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			var tokenKey string
			req := c.Request()
			authHeader := req.Header.Get("Authorization")
			tokenKey = strings.TrimPrefix(authHeader, "Bearer ")

			if authHeader == "" || tokenKey == "" {
				// Check query param for token
				tokenKey = req.URL.Query().Get("auth_token")
				if tokenKey == "" {
					return next(c)
				}
			}

			var token *types.Token
			var workspace *types.Workspace
			var err error

			token, workspace, err = workspaceRepo.AuthorizeToken(tokenKey)
			if err != nil {
				token, workspace, err = backendRepo.AuthorizeToken(c.Request().Context(), tokenKey)
				if err != nil {
					return echo.NewHTTPError(http.StatusUnauthorized)
				}

				err = workspaceRepo.SetAuthorizationToken(token, workspace)
				if err != nil {
					return echo.NewHTTPError(http.StatusInternalServerError)
				}
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

func verifyWorkspaceAuth(ctx echo.Context, workspaceId string) error {
	cc, ok := ctx.(*HttpAuthContext)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	if cc.AuthInfo == nil {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	if cc.AuthInfo.Workspace.ExternalId != workspaceId && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	return nil
}

func WithWorkspaceAuth(next func(ctx echo.Context) error) func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		if err := verifyWorkspaceAuth(ctx, ctx.Param("workspaceId")); err != nil {
			return err
		}

		return next(ctx)
	}
}

// This prevents users with restricted tokens from accessing an api endpoint even if they have access to the workspace.
func WithRestrictedWorkspaceAuth(next func(ctx echo.Context) error) func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		if err := verifyWorkspaceAuth(ctx, ctx.Param("workspaceId")); err != nil {
			return err
		}

		cc, _ := ctx.(*HttpAuthContext)
		if cc.AuthInfo.Token.TokenType == types.TokenTypeWorkspaceRestricted {
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
