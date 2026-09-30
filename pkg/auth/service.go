package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

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
	kind, _ := serviceInvocationRoute(c.Path())
	if kind == "" {
		return nil, errors.New("not an invocation route")
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
		return nil, errors.New("credential revoked or invalid")
	}

	stubID, err := serviceInvocationStub(c, backend, &workspace, kind)
	if err != nil {
		return nil, err
	}
	stub, err := backend.GetStubByExternalId(ctx, stubID)
	if err != nil || stub == nil || stub.App == nil || stub.App.ExternalId != scope.AppID || stub.WorkspaceId != workspace.Id {
		return nil, errors.New("credential does not authorize this application")
	}

	return &AuthInfo{
		Workspace: &workspace,
		Token:     &types.Token{Active: true, TokenType: types.TokenTypeWorkspaceRestricted},
	}, nil
}

// Match registered invocation routes, excluding run, sandbox and management APIs.
func serviceInvocationRoute(route string) (kind, target string) {
	parts := strings.SplitN(strings.TrimPrefix(route, "/"), "/", 3)
	if len(parts) < 2 {
		return "", ""
	}
	switch parts[1] {
	case "id", "public", ":deploymentName":
	default:
		return "", ""
	}

	switch parts[0] {
	case "pod":
		return types.StubTypePodDeployment, parts[1]
	case "asgi":
		return types.StubTypeASGIDeployment, parts[1]
	case "endpoint":
		return types.StubTypeEndpointDeployment, parts[1]
	default:
		return "", ""
	}
}

func parseServiceCredential(value string) (*serviceCredentialScope, error) {
	payload, key, ok := strings.Cut(strings.TrimPrefix(value, serviceCredentialPrefix), ".")
	if !ok || key == "" || len(value) > 2048 {
		return nil, errors.New("invalid credential")
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
		return nil, errors.New("incomplete credential scope")
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
		return "", errors.New("deployment unavailable")
	}
	return deployment.Stub.ExternalId, nil
}
