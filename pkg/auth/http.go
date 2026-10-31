package auth

import (
	"net/http"
	"strings"
	"time"

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
			// WithAssumedStubAuth checks visibility; the app owns this header.
			if kind, route := serviceInvocationRoute(c.Path()); kind != "" && route == "public" {
				return next(c)
			}

			req := c.Request()
			authHeader := req.Header.Get("Authorization")
			tokenKey := strings.TrimPrefix(authHeader, "Bearer ")

			if authHeader == "" || tokenKey == "" {
				// Check query param for token
				tokenKey = req.URL.Query().Get("auth_token")
				if tokenKey == "" {
					return next(c)
				}
			}

			if strings.HasPrefix(tokenKey, serviceCredentialPrefix) {
				authInfo, err := authorizeServiceCredential(c, backendRepo, tokenKey)
				if err != nil {
					return echo.NewHTTPError(http.StatusUnauthorized, "invalid service credential")
				}
				return next(&HttpAuthContext{c, authInfo})
			}

			token, workspace, err := workspaceRepo.AuthorizeToken(tokenKey)
			if err != nil {
				token, workspace, err = backendRepo.AuthorizeToken(c.Request().Context(), tokenKey)
				if err != nil {
					return echo.NewHTTPError(http.StatusUnauthorized)
				}
				if err := workspaceRepo.SetAuthorizationToken(token, workspace); err != nil {
					return echo.NewHTTPError(http.StatusInternalServerError)
				}
			} else if err := validateCachedToken(c.Request().Context(), backendRepo, token); err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized)
			}

			if !token.ValidAt(time.Now()) {
				return echo.NewHTTPError(http.StatusUnauthorized)
			}
			if !httpTokenAllowed(token.TokenType, c.Path()) {
				return echo.NewHTTPError(http.StatusForbidden)
			}

			if !workspace.StorageAvailable() {
				if err := ensureWorkspaceStorage(c.Request().Context(), workspace); err != nil {
					return echo.NewHTTPError(http.StatusInternalServerError)
				}
				_ = workspaceRepo.SetAuthorizationToken(token, workspace)
			}
			authInfo := &AuthInfo{
				Token:     token,
				Workspace: workspace,
				Actor: types.EventActor{
					Caller:       c.Request().Header.Get(CallerHeader),
					AgentSession: c.Request().Header.Get(AgentSessionHeader),
				},
			}

			live, cancel, _ := guardCredential(c.Request().Context(), backendRepo, token)
			defer cancel()
			c.SetRequest(c.Request().WithContext(live))
			return next(&HttpAuthContext{c, authInfo})
		}
	}
}

func httpTokenAllowed(kind, route string) bool {
	switch kind {
	case types.TokenTypeClusterAdmin, types.TokenTypeWorkspacePrimary, types.TokenTypeWorkspace:
		return true
	case types.TokenTypeWorkspaceRestricted:
		if invocation, _ := serviceInvocationRoute(route); invocation != "" {
			return true
		}
		parts := strings.Split(strings.TrimPrefix(route, "/"), "/")
		if len(parts) >= 2 && (parts[0] == "function" || parts[0] == "taskqueue" || parts[0] == "schedule") {
			return parts[1] == "id" || parts[1] == "public" || parts[1] == ":deploymentName"
		}
		return strings.HasPrefix(route, "/api/v1/task/")
	default:
		return false
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

	if cc.AuthInfo == nil || cc.AuthInfo.Token == nil || cc.AuthInfo.Workspace == nil {
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
func WithStrictWorkspaceAuth(next func(ctx echo.Context) error) func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		if err := verifyWorkspaceAuth(ctx, ctx.Param("workspaceId")); err != nil {
			return err
		}

		cc, _ := ctx.(*HttpAuthContext)
		if !HasPermission(cc.AuthInfo) {
			return echo.NewHTTPError(http.StatusUnauthorized)
		}

		return next(ctx)
	}
}

func WithClusterAdminAuth(next func(ctx echo.Context) error) func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		cc, ok := ctx.(*HttpAuthContext)
		if !ok || cc.AuthInfo == nil || cc.AuthInfo.Token == nil {
			return echo.NewHTTPError(http.StatusUnauthorized)
		}

		if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
			return echo.NewHTTPError(http.StatusUnauthorized)
		}

		return next(ctx)
	}
}
