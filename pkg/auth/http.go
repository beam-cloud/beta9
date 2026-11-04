package auth

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

type HttpAuthContext struct {
	echo.Context
	AuthInfo *AuthInfo
}

func AuthMiddleware(backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository) echo.MiddlewareFunc {
	tokens := NewTokens(backendRepo, workspaceRepo)
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			trace := common.NewStartupTrace(c.Request().Header.Get("X-Beam-Startup-Trace") == "1" && strings.Contains(c.Path(), "/vm"))
			traceCtx := common.ContextWithStartupTrace(c.Request().Context(), trace)
			if trace != nil {
				c.SetRequest(c.Request().WithContext(traceCtx))
			}
			finishAuth := common.StartupSpan(traceCtx, "http.authenticate")
			var poolStats func() sql.DBStats
			if trace != nil {
				if pool, ok := backendRepo.(interface{ DBStats() sql.DBStats }); ok {
					poolStats = pool.DBStats
					trace.Attribute("postgres_pool_before", pool.DBStats())
				}
			}
			defer func() {
				finishAuth()
				if trace != nil {
					if poolStats != nil {
						trace.Attribute("postgres_pool_after", poolStats())
					}
					trace.Log(log.Info().Str("component", "gateway").Str("path", c.Path()))
				}
			}()
			invoke := func(context echo.Context) error {
				finishAuth()
				defer common.StartupSpan(traceCtx, "http.handler")()
				return next(context)
			}
			// WithAssumedStubAuth checks visibility; the app owns this header.
			if kind, route := serviceInvocationRoute(c.Path()); kind != "" && route == "public" {
				return invoke(c)
			}

			req := c.Request()
			authHeader := req.Header.Get("Authorization")
			tokenKey := strings.TrimPrefix(authHeader, "Bearer ")

			if authHeader == "" || tokenKey == "" {
				// Check query param for token
				tokenKey = req.URL.Query().Get("auth_token")
				if tokenKey == "" {
					return invoke(c)
				}
			}

			if strings.HasPrefix(tokenKey, serviceCredentialPrefix) {
				authInfo, err := authorizeServiceCredential(c, backendRepo, tokenKey)
				if err != nil {
					return echo.NewHTTPError(http.StatusUnauthorized, "invalid service credential")
				}
				return invoke(&HttpAuthContext{c, authInfo})
			}

			token, workspace, err := tokens.AuthenticateUser(c.Request().Context(), tokenKey)
			if errors.Is(err, errWorkspaceStorage) {
				return echo.NewHTTPError(http.StatusInternalServerError)
			}
			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized)
			}
			authInfo := &AuthInfo{
				Token:     token,
				Workspace: workspace,
				Actor: types.EventActor{
					Caller:       c.Request().Header.Get(CallerHeader),
					AgentSession: c.Request().Header.Get(AgentSessionHeader),
				},
			}

			return invoke(&HttpAuthContext{c, authInfo})
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
func WithStrictWorkspaceAuth(next func(ctx echo.Context) error) func(ctx echo.Context) error {
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

// WithCredentialAuth guards routes that reveal or manage credentials, which
// deploy tokens never need.
func WithCredentialAuth(next func(ctx echo.Context) error) func(ctx echo.Context) error {
	return WithStrictWorkspaceAuth(func(ctx echo.Context) error {
		cc, _ := ctx.(*HttpAuthContext)
		if cc.AuthInfo.Token.TokenType == types.TokenTypePlatformDeployer {
			return echo.NewHTTPError(http.StatusUnauthorized)
		}
		return next(ctx)
	})
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
