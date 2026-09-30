package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
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
			// Public routes check stub visibility and leave authentication to the app.
			if strings.HasPrefix(c.Path(), "/pod/public/") ||
				strings.HasPrefix(c.Path(), "/asgi/public/") ||
				strings.HasPrefix(c.Path(), "/endpoint/public/") {
				return next(c)
			}

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

			if strings.HasPrefix(tokenKey, serviceCredentialPrefix) {
				authInfo, err := authorizeServiceCredential(c, backendRepo, tokenKey)
				if err != nil {
					return echo.NewHTTPError(http.StatusUnauthorized, "invalid service credential")
				}
				return next(&HttpAuthContext{c, authInfo})
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
				if err := workspaceRepo.SetAuthorizationToken(token, workspace); err != nil {
					return echo.NewHTTPError(http.StatusInternalServerError)
				}
			}

			if !token.Active || token.DisabledByClusterAdmin {
				return echo.NewHTTPError(http.StatusUnauthorized)
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

			cc := &HttpAuthContext{c, authInfo}
			return next(cc)
		}
	}
}

const serviceCredentialPrefix = "bsvc_"

type serviceCredentialScope struct {
	WorkspaceID string `json:"workspace_id"`
	AppID       string `json:"app_id"`
	ConsumerID  string `json:"consumer_id"`
}

func (s serviceCredentialScope) secretName() string {
	digest := sha256.Sum256([]byte(s.AppID + ":" + s.ConsumerID))
	return "BEAM_SERVICE_" + hex.EncodeToString(digest[:16])
}

// NewServiceCredential issues an invocation-only bearer credential. Its full
// value must be saved under the returned secret name; deleting or rotating that
// secret revokes it immediately. No workspace token is embedded in the value.
func NewServiceCredential(workspaceID, appID, consumerID string) (string, string, error) {
	scope := serviceCredentialScope{WorkspaceID: workspaceID, AppID: appID, ConsumerID: consumerID}
	encoded, err := json.Marshal(scope)
	if err != nil {
		return "", "", err
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return "", "", err
	}

	value := serviceCredentialPrefix + base64.RawURLEncoding.EncodeToString(encoded) + "." + hex.EncodeToString(key)
	return scope.secretName(), value, nil
}

func authorizeServiceCredential(c echo.Context, backend repository.BackendRepository, value string) (*AuthInfo, error) {
	kind, err := serviceInvocationKind(c.Path())
	if err != nil {
		return nil, err
	}

	scope, err := parseServiceCredential(value)
	if err != nil {
		return nil, err
	}

	ctx := c.Request().Context()
	workspace, err := backend.GetWorkspaceByExternalIdWithSigningKey(ctx, scope.WorkspaceID)
	if err != nil {
		return nil, err
	}
	workspace.ExternalId = scope.WorkspaceID

	secret, err := backend.GetSecretByNameDecrypted(ctx, &workspace, scope.secretName())
	if err != nil || secret == nil || subtle.ConstantTimeCompare([]byte(secret.Value), []byte(value)) != 1 {
		return nil, fmt.Errorf("credential revoked or invalid")
	}

	stubID, err := serviceInvocationStub(c, backend, &workspace, kind)
	if err != nil {
		return nil, err
	}
	stub, err := backend.GetStubByExternalId(ctx, stubID)
	if err != nil || stub == nil || stub.App == nil || stub.App.ExternalId != scope.AppID || stub.WorkspaceId != workspace.Id {
		return nil, fmt.Errorf("credential does not authorize this application")
	}

	return &AuthInfo{
		Workspace: &workspace,
		Token:     &types.Token{Active: true, TokenType: types.TokenTypeWorkspaceRestricted},
	}, nil
}

func serviceInvocationKind(route string) (string, error) {
	parts := strings.Split(strings.Trim(route, "/"), "/")
	if len(parts) < 2 || parts[1] == "run" || parts[1] == "container" {
		return "", fmt.Errorf("not an invocation route")
	}

	switch parts[0] {
	case "pod":
		return types.StubTypePodDeployment, nil
	case "asgi":
		return types.StubTypeASGIDeployment, nil
	case "endpoint":
		return types.StubTypeEndpointDeployment, nil
	default:
		return "", fmt.Errorf("not an invocation route")
	}
}

func parseServiceCredential(value string) (*serviceCredentialScope, error) {
	if len(value) > 2048 {
		return nil, fmt.Errorf("invalid credential")
	}
	payload, _, ok := strings.Cut(strings.TrimPrefix(value, serviceCredentialPrefix), ".")
	if !ok {
		return nil, fmt.Errorf("invalid credential")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return nil, err
	}

	var scope serviceCredentialScope
	if err := json.Unmarshal(decoded, &scope); err != nil {
		return nil, err
	}
	if scope.WorkspaceID == "" || scope.AppID == "" || scope.ConsumerID == "" {
		return nil, fmt.Errorf("incomplete credential scope")
	}
	return &scope, nil
}

func serviceInvocationStub(c echo.Context, backend repository.BackendRepository, workspace *types.Workspace, kind string) (string, error) {
	if stubID := c.Param("stubId"); stubID != "" {
		return stubID, nil
	}

	ctx := c.Request().Context()
	var deployment *types.DeploymentWithRelated
	var err error
	if version := c.Param("version"); version != "" {
		number, parseErr := strconv.ParseUint(version, 10, 32)
		if parseErr != nil {
			return "", parseErr
		}
		deployment, err = backend.GetDeploymentByNameAndVersion(ctx, workspace.Id, c.Param("deploymentName"), uint(number), kind)
	} else {
		deployment, err = backend.GetLatestDeploymentByName(ctx, workspace.Id, c.Param("deploymentName"), kind, true)
	}
	if err != nil || deployment == nil || !deployment.Active {
		return "", fmt.Errorf("deployment unavailable")
	}
	return deployment.Stub.ExternalId, nil
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
