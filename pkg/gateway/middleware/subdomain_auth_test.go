package middleware_test

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/gateway/middleware"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type crossWorkspaceTestWorkspaceRepo struct {
	repository.WorkspaceRepository
	tokens map[string]struct {
		token     *types.Token
		workspace *types.Workspace
	}
}

func (r *crossWorkspaceTestWorkspaceRepo) AuthorizeToken(key string) (*types.Token, *types.Workspace, error) {
	entry, ok := r.tokens[key]
	if !ok {
		return nil, nil, errors.New("invalid token")
	}
	return entry.token, entry.workspace, nil
}

func (r *crossWorkspaceTestWorkspaceRepo) SetAuthorizationToken(*types.Token, *types.Workspace) error {
	return nil
}

type crossWorkspaceTestBackendRepo struct {
	repository.BackendRepository
	customDomains map[string]*types.DeploymentWithRelated
	subdomains    map[string]*types.DeploymentWithRelated
	deployments   []types.DeploymentWithRelated
	stubs         map[string]*types.StubWithRelated
}

func (r *crossWorkspaceTestBackendRepo) GetDeploymentByCustomDomain(_ context.Context, domain string) (*types.DeploymentWithRelated, *types.CustomDomain, error) {
	dep, ok := r.customDomains[domain]
	if !ok {
		return nil, nil, types.ErrCustomDomainNotFound
	}
	return dep, &types.CustomDomain{
		WorkspaceId:    dep.Deployment.WorkspaceId,
		Domain:         domain,
		DeploymentName: dep.Name,
		StubType:       dep.StubType,
		Version:        0,
		Status:         types.CustomDomainStatusActive,
		Subdomain:      dep.Subdomain,
	}, nil
}

func (r *crossWorkspaceTestBackendRepo) GetDeploymentBySubdomain(_ context.Context, subdomain string, version uint) (*types.DeploymentWithRelated, error) {
	dep, ok := r.subdomains[subdomain]
	if !ok || (version > 0 && dep.Version != version) {
		return nil, sql.ErrNoRows
	}
	return dep, nil
}

func (r *crossWorkspaceTestBackendRepo) GetLatestDeploymentByName(_ context.Context, workspaceId uint, name string, stubType string, _ bool) (*types.DeploymentWithRelated, error) {
	for i := range r.deployments {
		dep := r.deployments[i]
		if dep.Deployment.WorkspaceId == workspaceId && dep.Name == name && dep.StubType == stubType && dep.Active {
			return &dep, nil
		}
	}
	return nil, sql.ErrNoRows
}

func (r *crossWorkspaceTestBackendRepo) GetDeploymentByNameAndVersion(_ context.Context, workspaceId uint, name string, version uint, stubType string) (*types.DeploymentWithRelated, error) {
	for i := range r.deployments {
		dep := r.deployments[i]
		if dep.Deployment.WorkspaceId == workspaceId && dep.Name == name && dep.Version == version && dep.StubType == stubType {
			return &dep, nil
		}
	}
	return nil, sql.ErrNoRows
}

func (r *crossWorkspaceTestBackendRepo) GetStubByExternalId(_ context.Context, externalId string, _ ...types.QueryFilter) (*types.StubWithRelated, error) {
	stub, ok := r.stubs[externalId]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return stub, nil
}

func TestSubdomainMiddlewarePreventsCrossWorkspaceRouting(t *testing.T) {
	redisClient, err := middleware.NewRedisClientForTest()
	require.NoError(t, err)

	common.InvalidateCustomDomainCache(context.Background(), redisClient, "api.acme.com", "pod.acme.com", "public.acme.com")

	storage1, storage2 := uint(1), uint(2)
	wsA := &types.Workspace{
		Id:         1,
		ExternalId: "ws-a-uuid",
		Name:       "acme",
		StorageId:  &storage1,
		Storage:    &types.WorkspaceStorage{Id: &storage1},
	}
	wsB := &types.Workspace{
		Id:         2,
		ExternalId: "ws-b-uuid",
		Name:       "evil",
		StorageId:  &storage2,
		Storage:    &types.WorkspaceStorage{Id: &storage2},
	}

	stubEpA := &types.StubWithRelated{
		Stub:      types.Stub{Id: 101, ExternalId: "stub-ep-a", Type: types.StubType(types.StubTypeEndpointDeployment), Config: `{"authorized":true}`},
		Workspace: *wsA,
	}
	stubEpB := &types.StubWithRelated{
		Stub:      types.Stub{Id: 102, ExternalId: "stub-ep-b", Type: types.StubType(types.StubTypeEndpointDeployment), Config: `{"authorized":true}`},
		Workspace: *wsB,
	}
	stubPodA := &types.StubWithRelated{
		Stub:      types.Stub{Id: 103, ExternalId: "stub-pod-a", Type: types.StubType(types.StubTypePodDeployment), Config: `{"authorized":true,"ports":[8000]}`},
		Workspace: *wsA,
	}
	stubPodB := &types.StubWithRelated{
		Stub:      types.Stub{Id: 104, ExternalId: "stub-pod-b", Type: types.StubType(types.StubTypePodDeployment), Config: `{"authorized":true,"ports":[8000]}`},
		Workspace: *wsB,
	}
	stubPubA := &types.StubWithRelated{
		Stub:      types.Stub{Id: 105, ExternalId: "stub-pub-a", Type: types.StubType(types.StubTypeEndpointDeployment), Config: `{"authorized":false}`},
		Workspace: *wsA,
	}

	depEpA := types.DeploymentWithRelated{
		Deployment: types.Deployment{Id: 1, Name: "api", Subdomain: "api-aaaaaaa", WorkspaceId: wsA.Id, StubType: types.StubTypeEndpointDeployment, Version: 1, Active: true},
		Workspace:  *wsA,
		Stub:       stubEpA.Stub,
	}
	depEpB := types.DeploymentWithRelated{
		Deployment: types.Deployment{Id: 2, Name: "api", Subdomain: "api-bbbbbbb", WorkspaceId: wsB.Id, StubType: types.StubTypeEndpointDeployment, Version: 1, Active: true},
		Workspace:  *wsB,
		Stub:       stubEpB.Stub,
	}
	depPodA := types.DeploymentWithRelated{
		Deployment: types.Deployment{Id: 3, Name: "web", Subdomain: "web-aaaaaaa", WorkspaceId: wsA.Id, StubType: types.StubTypePodDeployment, Version: 1, Active: true},
		Workspace:  *wsA,
		Stub:       stubPodA.Stub,
	}
	depPodB := types.DeploymentWithRelated{
		Deployment: types.Deployment{Id: 4, Name: "web", Subdomain: "web-bbbbbbb", WorkspaceId: wsB.Id, StubType: types.StubTypePodDeployment, Version: 1, Active: true},
		Workspace:  *wsB,
		Stub:       stubPodB.Stub,
	}
	depPubA := types.DeploymentWithRelated{
		Deployment: types.Deployment{Id: 5, Name: "public-api", Subdomain: "pub-aaaaaaa", WorkspaceId: wsA.Id, StubType: types.StubTypeEndpointDeployment, Version: 1, Active: true},
		Workspace:  *wsA,
		Stub:       stubPubA.Stub,
	}

	backendRepo := &crossWorkspaceTestBackendRepo{
		customDomains: map[string]*types.DeploymentWithRelated{
			"api.acme.com":    &depEpA,
			"pod.acme.com":    &depPodA,
			"public.acme.com": &depPubA,
		},
		subdomains: map[string]*types.DeploymentWithRelated{
			"api-aaaaaaa": &depEpA,
		},
		deployments: []types.DeploymentWithRelated{depEpA, depEpB, depPodA, depPodB, depPubA},
		stubs: map[string]*types.StubWithRelated{
			"stub-ep-a":  stubEpA,
			"stub-ep-b":  stubEpB,
			"stub-pod-a": stubPodA,
			"stub-pod-b": stubPodB,
			"stub-pub-a": stubPubA,
		},
	}
	workspaceRepo := &crossWorkspaceTestWorkspaceRepo{
		tokens: map[string]struct {
			token     *types.Token
			workspace *types.Workspace
		}{
			"tok-a":       {token: &types.Token{Active: true, TokenType: types.TokenTypeWorkspacePrimary}, workspace: wsA},
			"tok-b":       {token: &types.Token{Active: true, TokenType: types.TokenTypeWorkspacePrimary}, workspace: wsB},
			"tok-admin-b": {token: &types.Token{Active: true, TokenType: types.TokenTypeClusterAdmin}, workspace: wsB},
		},
	}

	e := echo.New()
	e.Use(middleware.SubdomainWithReservedHosts("https://app.example.com", nil, backendRepo, redisClient))
	authMW := auth.AuthMiddleware(backendRepo, workspaceRepo)
	stubCache := abstractions.NewDeploymentStubCache()

	makeHandler := func(stubType string) echo.HandlerFunc {
		return func(ctx echo.Context) error {
			cc, _ := ctx.(*auth.HttpAuthContext)
			stubID, err := abstractions.ParseAndValidateDeploymentStubId(
				ctx.Request().Context(),
				stubCache,
				cc.AuthInfo,
				ctx.Param("stubId"),
				ctx.Param("deploymentName"),
				ctx.Param("version"),
				stubType,
				backendRepo,
			)
			if err != nil {
				return err
			}
			return ctx.String(http.StatusOK, stubID)
		}
	}

	epGroup := e.Group("/endpoint", authMW)
	epGroup.POST("/:deploymentName/latest", auth.WithAuth(makeHandler(types.StubTypeEndpointDeployment)))
	epGroup.POST("/:deploymentName/v:version", auth.WithAuth(makeHandler(types.StubTypeEndpointDeployment)))
	epGroup.POST("/public/:stubId", auth.WithAssumedStubAuth(makeHandler(types.StubTypeEndpointDeployment), func(stubID string) (*types.Workspace, error) {
		return wsA, nil
	}))

	podGroup := e.Group("/pod", authMW)
	podGroup.GET("/:deploymentName/latest/:port", auth.WithAuth(makeHandler(types.StubTypePodDeployment)))
	podGroup.GET("/:deploymentName/v:version/:port", auth.WithAuth(makeHandler(types.StubTypePodDeployment)))

	doReq := func(method, host, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/", nil)
		req.Host = host
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		return rec
	}

	// 1. Workspace A token succeeds on its own authorized custom domain (cold cache -> warms Redis).
	rec := doReq(http.MethodPost, "api.acme.com", "tok-a")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "stub-ep-a", rec.Body.String())

	// 2. Workspace B token (even though WS B also has an "api" deployment) MUST get 401 on warm Redis cache.
	rec = doReq(http.MethodPost, "api.acme.com", "tok-b")
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	// 3. Cluster admin token scoped to Workspace B MUST also get 401 on Workspace A's domain.
	rec = doReq(http.MethodPost, "api.acme.com", "tok-admin-b")
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	// 4. Missing token gets 401.
	rec = doReq(http.MethodPost, "api.acme.com", "")
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	// 5. Cold cache check with WS B token first, plus legacy Redis cache entry (path without wsID| prefix).
	common.InvalidateCustomDomainCache(context.Background(), redisClient, "api.acme.com")
	rec = doReq(http.MethodPost, "api.acme.com", "tok-b")
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	require.NoError(t, redisClient.Set(context.Background(), common.CustomDomainHandlerCacheKey("api.acme.com"), "/endpoint/api/latest", time.Minute).Err())
	rec = doReq(http.MethodPost, "api.acme.com", "tok-b")
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	rec = doReq(http.MethodPost, "api.acme.com", "tok-a")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "stub-ep-a", rec.Body.String())

	// 6. Pod custom domain rejects WS B token and allows WS A token.
	rec = doReq(http.MethodGet, "pod.acme.com", "tok-b")
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	rec = doReq(http.MethodGet, "pod.acme.com", "tok-a")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "stub-pod-a", rec.Body.String())

	// 7. Standard subdomain routes also enforce workspace ownership across cold and cached versioned lookups.
	rec = doReq(http.MethodPost, "api-aaaaaaa.app.example.com", "tok-b")
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	rec = doReq(http.MethodPost, "api-aaaaaaa-v1.app.example.com", "tok-a")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "stub-ep-a", rec.Body.String())
	rec = doReq(http.MethodPost, "api-aaaaaaa-v1.app.example.com", "tok-b")
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	// 8. Public custom domain remains accessible without a token and with any unrelated token.
	rec = doReq(http.MethodPost, "public.acme.com", "")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "stub-pub-a", rec.Body.String())
	rec = doReq(http.MethodPost, "public.acme.com", "tok-b")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "stub-pub-a", rec.Body.String())
}
