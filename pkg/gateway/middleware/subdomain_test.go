package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
)

func NewRedisClientForTest() (*common.RedisClient, error) {
	s, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		return nil, err
	}

	return rdb, nil
}

type middlewareRepoForTest struct {
	deployName        string
	stub              *types.Stub
	customDomains     map[string]*types.CustomDomain
	customDeployments map[string]*types.DeploymentWithRelated
}

func (r *middlewareRepoForTest) GetStubByExternalId(ctx context.Context, externalId string, queryFilters ...types.QueryFilter) (*types.StubWithRelated, error) {
	return &types.StubWithRelated{
		Stub: *r.stub,
	}, nil
}

func (r *middlewareRepoForTest) GetDeploymentBySubdomain(ctx context.Context, subdomain string, version uint) (*types.DeploymentWithRelated, error) {
	return &types.DeploymentWithRelated{
		Deployment: types.Deployment{
			Name: r.deployName,
		},
		Stub: *r.stub,
	}, nil
}

func (r *middlewareRepoForTest) GetDeploymentByCustomDomain(ctx context.Context, domain string) (*types.DeploymentWithRelated, *types.CustomDomain, error) {
	cd, ok := r.customDomains[domain]
	if !ok {
		return nil, nil, types.ErrCustomDomainNotFound
	}
	if dep, ok := r.customDeployments[domain]; ok {
		return dep, cd, nil
	}
	return &types.DeploymentWithRelated{
		Deployment: types.Deployment{
			Name:      cd.DeploymentName,
			Subdomain: cd.Subdomain,
		},
		Stub: *r.stub,
	}, cd, nil
}

func TestSubdomainMiddleware(t *testing.T) {
	tests := []struct {
		name       string
		host       string
		deployName string
		stubType   string
		expected   map[string]string
	}{
		{
			name:       "name-and-hash",
			host:       "task2-7a7db8c.app.example.com",
			deployName: "task2",
			stubType:   types.StubTypeTaskQueueDeployment,
			expected: map[string]string{
				"path": "/taskqueue/task2/latest",
			},
		},
		{
			name:       "name-hash-and-latest-version",
			host:       "task2-7a7db8c-latest.app.example.com",
			deployName: "task2",
			stubType:   types.StubTypeTaskQueueDeployment,
			expected: map[string]string{
				"path": "/taskqueue/task2/latest",
			},
		},
		{
			name:       "name-hash-and-specific-version",
			host:       "task2-7a7db8c-v1.app.example.com",
			deployName: "task2",
			stubType:   types.StubTypeTaskQueueDeployment,
			expected: map[string]string{
				"path": "/taskqueue/task2/v1",
			},
		},
		{
			name:       "stub-id",
			host:       "8f32e485-2b2e-4238-9878-490eb9b0a9d3.app.example.com",
			deployName: "task2",
			stubType:   types.StubTypeTaskQueueDeployment,
			expected: map[string]string{
				"path": "/taskqueue/id/8f32e485-2b2e-4238-9878-490eb9b0a9d3",
			},
		},
		{
			name:       "hyphened-name-and-hash",
			host:       "hello-world-7a7db8c.app.example.com",
			deployName: "hello-world",
			stubType:   types.StubTypeTaskQueueDeployment,
			expected: map[string]string{
				"path": "/taskqueue/hello-world/latest",
			},
		},
		{
			name:       "hyphened-name-hash-and-specific-version",
			host:       "hello-world-123-7a7db8c-v3.app.example.com",
			deployName: "hello-world-123",
			stubType:   types.StubTypeTaskQueueDeployment,
			expected: map[string]string{
				"path": "/taskqueue/hello-world-123/v3",
			},
		},
		{
			name:       "hyphened-name-hash-and-latest-version",
			host:       "hello-world-123-7a7db8c-latest.app.example.com",
			deployName: "hello-world-123",
			stubType:   types.StubTypeTaskQueueDeployment,
			expected: map[string]string{
				"path": "/taskqueue/hello-world-123/latest",
			},
		},
		{
			name:       "hyphened-name-hash-and-specific-version-with-deployment-name",
			host:       "hello-world-again-and-again-7a7db8c-v10.app.example.com",
			deployName: "my-name",
			stubType:   types.StubTypeTaskQueueDeployment,
			expected: map[string]string{
				"path": "/taskqueue/my-name/v10",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			redisClient, err := NewRedisClientForTest()
			assert.NoError(t, err)

			e := echo.New()
			backendRepo := &middlewareRepoForTest{
				deployName: test.deployName,
				stub: &types.Stub{
					Type: types.StubType(test.stubType),
				},
			}

			handler := func(ctx echo.Context) error {
				assert.Equal(t, test.expected["path"], ctx.Request().URL.Path)
				return ctx.String(http.StatusOK, "OK")
			}

			g := e.Group("/taskqueue")
			g.GET("/:deploymentName", handler)
			g.GET("/:deploymentName/latest", handler)
			g.GET("/:deploymentName/v:version", handler)
			g.GET("/id/:stubId", handler)

			mw := Subdomain("https://app.example.com", backendRepo, redisClient)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Host = test.host
			rec := httptest.NewRecorder()

			c := e.NewContext(req, rec)
			h := mw(handler)
			err = h(c)
			assert.NoError(t, err)
			e.Shutdown(context.Background())
		})
	}
}

func TestSubdomainMiddlewareRoutesSandboxContainerHost(t *testing.T) {
	redisClient, err := NewRedisClientForTest()
	assert.NoError(t, err)

	stubID := "e9c29586-c465-4a67-9c9b-25293d1ce77b"
	containerID := "sandbox-" + stubID + "-abc12345"
	e := echo.New()
	backendRepo := &middlewareRepoForTest{
		stub: &types.Stub{
			Type:       types.StubType(types.StubTypeSandbox),
			ExternalId: stubID,
		},
	}

	handler := func(ctx echo.Context) error {
		assert.Equal(t, "/sandbox/container/"+stubID+"/"+containerID+"/8765", ctx.Request().URL.Path)
		return ctx.String(http.StatusOK, "OK")
	}

	g := e.Group("/sandbox")
	g.GET("/container/:stubId/:containerId/:port", handler)

	mw := Subdomain("https://app.example.com", backendRepo, redisClient)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = containerID + "-8765.app.example.com"
	rec := httptest.NewRecorder()

	c := e.NewContext(req, rec)
	h := mw(handler)
	err = h(c)
	assert.NoError(t, err)
	e.Shutdown(context.Background())
}

func TestParseSubdomain(t *testing.T) {
	tests := []struct {
		host     string
		base     string
		expected string
	}{
		{
			host:     "task2-7a7db8c.app.example.com",
			base:     "app.example.com",
			expected: "task2-7a7db8c",
		},
		{
			host:     "task2-7a7db8c-latest.app.example.com",
			base:     "app.example.com",
			expected: "task2-7a7db8c-latest",
		},
		{
			host:     "task2-7a7db8c-v1.app.example.com",
			base:     "app.example.com",
			expected: "task2-7a7db8c-v1",
		},
		{
			host:     "task2-7a7db8c-8f32e485-2b2e-4238-9878-490eb9b0a9d3.app.example.com",
			base:     "app.example.com",
			expected: "task2-7a7db8c-8f32e485-2b2e-4238-9878-490eb9b0a9d3",
		},
		{
			host:     "sandbox-e9c29586-c465-4a67-9c9b-25293d1ce77b-abc12345-8765.app.example.com",
			base:     "app.example.com",
			expected: "sandbox-e9c29586-c465-4a67-9c9b-25293d1ce77b-abc12345-8765",
		},
		{
			host:     "task2-7a7db8c-v1.app.example.com",
			base:     "fail.example.com",
			expected: "",
		},
		{
			host:     "task2-7a7db8c-v1.app.example.com",
			base:     ".app.example.com",
			expected: "",
		},
	}

	for _, test := range tests {
		t.Run(test.host, func(t *testing.T) {
			assert.Equal(t, test.expected, ParseSubdomain(test.host, test.base))
		})
	}
}

func TestParseSubdomainFields(t *testing.T) {
	tests := []struct {
		subdomain   string
		expected    *common.SubdomainFields
		expectedErr error
	}{
		{
			subdomain: "app1-9a7dbcc",
			expected: &common.SubdomainFields{
				Subdomain: "app1-9a7dbcc",
				Version:   0,
			},
		},
		{
			subdomain: "app2-7a7db8c-latest",
			expected: &common.SubdomainFields{
				Subdomain: "app2-7a7db8c",
				Version:   0,
			},
		},
		{
			subdomain: "my-app-7a7db8c-v1",
			expected: &common.SubdomainFields{
				Subdomain: "my-app-7a7db8c",
				Version:   1,
			},
		},
		{
			subdomain: "8f32e485-2b2e-4238-9878-490eb9b0a9d3",
			expected: &common.SubdomainFields{
				StubId: "8f32e485-2b2e-4238-9878-490eb9b0a9d3",
			},
		},
		{
			subdomain: "sandbox-e9c29586-c465-4a67-9c9b-25293d1ce77b-abc12345-8765",
			expected: &common.SubdomainFields{
				StubId:      "e9c29586-c465-4a67-9c9b-25293d1ce77b",
				ContainerId: "sandbox-e9c29586-c465-4a67-9c9b-25293d1ce77b-abc12345",
				Port:        8765,
			},
		},
		{
			subdomain:   "invalid-subdomain",
			expectedErr: common.ErrSubdomainDoesNotMatchRegex,
		},
		{
			subdomain:   "invalid-123f",
			expectedErr: common.ErrSubdomainDoesNotMatchRegex,
		},
		{
			subdomain:   "invalid-2b2e-4238-9878-111122222333",
			expectedErr: common.ErrSubdomainDoesNotMatchRegex,
		},
		{
			subdomain:   "invalid-8f32e485-2b2e-4238-9878-490eb9b0a9d3",
			expectedErr: common.ErrSubdomainDoesNotMatchRegex,
		},
	}

	for _, test := range tests {
		t.Run(test.subdomain, func(t *testing.T) {
			fields, err := common.ParseSubdomainFields(test.subdomain)

			if test.expectedErr != nil {
				assert.Error(t, err)
				assert.Equal(t, test.expectedErr, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, test.expected, fields)
		})
	}
}

func TestBuildHandlerPath(t *testing.T) {
	tests := []struct {
		name       string
		stub       *types.Stub
		fields     *common.SubdomainFields
		extraPaths []string
		expected   string
	}{
		{
			name: "public-path",
			stub: &types.Stub{
				Type:       types.StubType(types.StubTypeEndpointDeployment),
				Config:     `{"authorized": false}`,
				ExternalId: "49a41870-bb61-4c45-aad1-68aaa7073c2e",
			},
			expected: "/endpoint/public/49a41870-bb61-4c45-aad1-68aaa7073c2e",
		},
		{
			name: "stub-id-path",
			stub: &types.Stub{
				Type: types.StubType(types.StubTypeEndpointDeployment),
			},
			fields: &common.SubdomainFields{
				StubId: "4ec446ce-3fd1-41a8-9f70-4d25b9224821",
			},
			expected: "/endpoint/id/4ec446ce-3fd1-41a8-9f70-4d25b9224821",
		},
		{
			name: "sandbox-container-path",
			stub: &types.Stub{
				Type: types.StubType(types.StubTypeSandbox),
			},
			fields: &common.SubdomainFields{
				StubId:      "e9c29586-c465-4a67-9c9b-25293d1ce77b",
				ContainerId: "sandbox-e9c29586-c465-4a67-9c9b-25293d1ce77b-abc12345",
				Port:        8765,
			},
			expected: "/sandbox/container/e9c29586-c465-4a67-9c9b-25293d1ce77b/sandbox-e9c29586-c465-4a67-9c9b-25293d1ce77b-abc12345/8765",
		},
		{
			name: "name-and-version-path",
			stub: &types.Stub{
				Type: types.StubType(types.StubTypeTaskQueueDeployment),
			},
			fields: &common.SubdomainFields{
				Name:    "tq",
				Version: 55,
			},
			expected: "/taskqueue/tq/v55",
		},
		{
			name: "name-and-version-path-with-extra-paths",
			stub: &types.Stub{
				Type: types.StubType(types.StubTypeASGIDeployment),
			},
			fields: &common.SubdomainFields{
				Name:    "tq",
				Version: 55,
			},
			extraPaths: []string{"api", "users"},
			expected:   "/asgi/tq/v55/api/users",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, common.BuildHandlerPath(test.stub, test.fields, test.extraPaths...))
		})
	}
}

func TestParseHostFromURL(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		{
			url:      "https://app.example.com",
			expected: "app.example.com",
		},
		{
			url:      "http://example.com.uk",
			expected: "example.com.uk",
		},
		{
			url:      "example.org",
			expected: "example.org",
		},
	}

	for _, test := range tests {
		t.Run(test.url, func(t *testing.T) {
			assert.Equal(t, test.expected, ParseHostFromURL(test.url))
		})
	}
}

func TestParseVersion(t *testing.T) {
	tests := []struct {
		version  string
		expected uint
	}{
		{
			version:  "",
			expected: 0,
		},
		{
			version:  "latest",
			expected: 0,
		},
		{
			version:  "v1",
			expected: 1,
		},
		{
			version:  "v2",
			expected: 2,
		},
		{
			version:  "invalid",
			expected: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.version, func(t *testing.T) {
			assert.Equal(t, test.expected, common.ParseVersion(test.version))
		})
	}
}

func TestSubdomainMiddlewareCustomDomain(t *testing.T) {
	publicStubID := "49a41870-bb61-4c45-aad1-68aaa7073c2e"
	tests := []struct {
		name           string
		host           string
		requestPath    string
		customDomain   *types.CustomDomain
		stub           *types.Stub
		expectedPath   string
		expectFallback bool
	}{
		{
			name:        "authorized-endpoint-custom-domain",
			host:        "api.mycompany.com",
			requestPath: "/",
			customDomain: &types.CustomDomain{
				Domain:         "api.mycompany.com",
				DeploymentName: "api-deploy",
				Status:         types.CustomDomainStatusActive,
			},
			stub: &types.Stub{
				Type:       types.StubType(types.StubTypeEndpointDeployment),
				Config:     `{"authorized": true}`,
				ExternalId: "11111111-1111-4111-8111-111111111111",
			},
			expectedPath: "/endpoint/api-deploy/latest",
		},
		{
			name:        "public-endpoint-custom-domain",
			host:        "public.mycompany.com",
			requestPath: "/",
			customDomain: &types.CustomDomain{
				Domain:         "public.mycompany.com",
				DeploymentName: "public-deploy",
				Status:         types.CustomDomainStatusActive,
			},
			stub: &types.Stub{
				Type:       types.StubType(types.StubTypeEndpointDeployment),
				Config:     `{"authorized": false}`,
				ExternalId: publicStubID,
			},
			expectedPath: "/endpoint/public/" + publicStubID,
		},
		{
			name:        "asgi-custom-domain-preserves-subpath-and-trailing-slash",
			host:        "asgi.mycompany.com:443",
			requestPath: "/v1/items/",
			customDomain: &types.CustomDomain{
				Domain:         "asgi.mycompany.com",
				DeploymentName: "asgi-deploy",
				Status:         types.CustomDomainStatusActive,
			},
			stub: &types.Stub{
				Type:       types.StubType(types.StubTypeASGIDeployment),
				Config:     `{"authorized": true}`,
				ExternalId: "22222222-2222-4222-8222-222222222222",
			},
			expectedPath: "/asgi/asgi-deploy/latest/v1/items/",
		},
		{
			name:        "pod-custom-domain-auto-resolves-single-port",
			host:        "pod.mycompany.com",
			requestPath: "/",
			customDomain: &types.CustomDomain{
				Domain:         "pod.mycompany.com",
				DeploymentName: "pod-deploy",
				Status:         types.CustomDomainStatusActive,
			},
			stub: &types.Stub{
				Type:       types.StubType(types.StubTypePodDeployment),
				Config:     `{"authorized": true, "ports": [8000]}`,
				ExternalId: "33333333-3333-4333-8333-333333333333",
			},
			expectedPath: "/pod/pod-deploy/latest/8000",
		},
		{
			name:        "pod-custom-domain-explicit-port-and-version",
			host:        "pod-explicit.mycompany.com",
			requestPath: "/",
			customDomain: &types.CustomDomain{
				Domain:         "pod-explicit.mycompany.com",
				DeploymentName: "pod-deploy",
				Version:        2,
				Port:           9000,
				Status:         types.CustomDomainStatusActive,
			},
			stub: &types.Stub{
				Type:       types.StubType(types.StubTypePodDeployment),
				Config:     `{"authorized": true, "ports": [8000, 9000]}`,
				ExternalId: "44444444-4444-4444-8444-444444444444",
			},
			expectedPath: "/pod/pod-deploy/v2/9000",
		},
		{
			name:           "unknown-custom-domain-falls-through",
			host:           "unknown.mycompany.com",
			requestPath:    "/",
			expectedPath:   "/",
			expectFallback: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			redisClient, err := NewRedisClientForTest()
			assert.NoError(t, err)

			e := echo.New()
			backendRepo := &middlewareRepoForTest{
				customDomains: map[string]*types.CustomDomain{},
				stub:          test.stub,
			}
			if test.customDomain != nil {
				backendRepo.customDomains[test.customDomain.Domain] = test.customDomain
			}

			routed := false
			fallbackCalled := false
			handler := func(ctx echo.Context) error {
				routed = true
				assert.Equal(t, test.expectedPath, ctx.Request().URL.Path)
				return ctx.String(http.StatusOK, "OK")
			}
			fallbackHandler := func(ctx echo.Context) error {
				fallbackCalled = true
				assert.Equal(t, test.expectedPath, ctx.Request().URL.Path)
				return ctx.String(http.StatusNotFound, "not found")
			}

			epGroup := e.Group("/endpoint")
			epGroup.GET("/:deploymentName/latest", handler)
			epGroup.GET("/public/:stubId", handler)

			asgiGroup := e.Group("/asgi")
			asgiGroup.GET("/:deploymentName/latest", handler)
			asgiGroup.GET("/:deploymentName/latest/:subPath", handler)

			podGroup := e.Group("/pod")
			podGroup.GET("/:deploymentName/latest/:port", handler)
			podGroup.GET("/:deploymentName/latest/:port/:subPath", handler)
			podGroup.GET("/:deploymentName/v:version/:port", handler)
			podGroup.GET("/:deploymentName/v:version/:port/:subPath", handler)

			mw := Subdomain("https://app.example.com", backendRepo, redisClient)

			req := httptest.NewRequest(http.MethodGet, test.requestPath, nil)
			req.Host = test.host
			rec := httptest.NewRecorder()

			c := e.NewContext(req, rec)
			h := mw(fallbackHandler)
			err = h(c)
			assert.NoError(t, err)
			assert.Equal(t, test.expectFallback, fallbackCalled)
			assert.Equal(t, !test.expectFallback, routed)
			_ = e.Shutdown(context.Background())
		})
	}
}

func TestIsCandidateCustomDomain(t *testing.T) {
	tests := []struct {
		host     string
		base     string
		expected bool
	}{
		{host: "app.example.com", base: "app.example.com", expected: false},
		{host: "sub.app.example.com", base: "app.example.com", expected: false},
		{host: "localhost:1993", base: "app.example.com", expected: false},
		{host: "127.0.0.1:1993", base: "app.example.com", expected: false},
		{host: "10.0.0.1", base: "app.example.com", expected: false},
		{host: "api.customer.io:443", base: "app.example.com", expected: true},
	}

	for _, test := range tests {
		t.Run(test.host, func(t *testing.T) {
			assert.Equal(t, test.expected, IsCandidateCustomDomain(test.host, test.base))
		})
	}

	assert.False(t, IsCandidateCustomDomain("grpc.example.com", "app.example.com", "grpc.example.com", "tcp.example.com"))
	assert.False(t, IsCandidateCustomDomain("sub.tcp.example.com", "app.example.com", "grpc.example.com", "tcp.example.com"))
	assert.False(t, IsCandidateCustomDomain("beta9-gateway.beta9.svc.cluster.local", "app.example.com"))
}

type countingRepo struct {
	*middlewareRepoForTest
	lookups int
}

func (r *countingRepo) GetDeploymentByCustomDomain(ctx context.Context, domain string) (*types.DeploymentWithRelated, *types.CustomDomain, error) {
	r.lookups++
	return r.middlewareRepoForTest.GetDeploymentByCustomDomain(ctx, domain)
}

func TestSubdomainMiddlewareNegativeCachesUnknownCustomDomain(t *testing.T) {
	redisClient, err := NewRedisClientForTest()
	assert.NoError(t, err)

	// The miss cache is process-wide; start from a clean slate.
	common.InvalidateCustomDomainCache(context.Background(), redisClient, "unknown.mycompany.com")

	repo := &countingRepo{middlewareRepoForTest: &middlewareRepoForTest{
		customDomains: map[string]*types.CustomDomain{},
		stub:          &types.Stub{Type: types.StubType(types.StubTypeEndpointDeployment)},
	}}

	e := echo.New()
	mw := Subdomain("https://app.example.com", repo, redisClient)
	next := func(ctx echo.Context) error { return ctx.String(http.StatusNotFound, "nf") }

	call := func() {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Host = "unknown.mycompany.com"
		assert.NoError(t, mw(next)(e.NewContext(req, httptest.NewRecorder())))
	}

	call()
	call()
	call()
	assert.Equal(t, 1, repo.lookups, "unknown domain should hit the repo once within the negative TTL")

	common.InvalidateCustomDomainCache(context.Background(), redisClient, "unknown.mycompany.com")
	call()
	assert.Equal(t, 2, repo.lookups, "invalidation must clear the negative entry")
}

func TestSubdomainMiddlewareMultiReplicaSharedNegativeCache(t *testing.T) {
	redisClient, err := NewRedisClientForTest()
	assert.NoError(t, err)

	common.InvalidateCustomDomainCache(context.Background(), redisClient, "shared.mycompany.com")

	repo := &countingRepo{middlewareRepoForTest: &middlewareRepoForTest{
		customDomains: map[string]*types.CustomDomain{},
		stub:          &types.Stub{Type: types.StubType(types.StubTypeEndpointDeployment)},
	}}

	e := echo.New()
	mw := Subdomain("https://app.example.com", repo, redisClient)
	next := func(ctx echo.Context) error { return ctx.String(http.StatusNotFound, "nf") }

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "shared.mycompany.com"
	assert.NoError(t, mw(next)(e.NewContext(req, httptest.NewRecorder())))
	assert.Equal(t, 1, repo.lookups)

	// Simulate another gateway replica verifying the domain and deleting ONLY the
	// shared Redis key (without touching this replica's in-process memory).
	assert.NoError(t, redisClient.Del(context.Background(), common.CustomDomainHandlerCacheKey("shared.mycompany.com")).Err())

	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Host = "shared.mycompany.com"
	assert.NoError(t, mw(next)(e.NewContext(req2, httptest.NewRecorder())))
	assert.Equal(t, 2, repo.lookups, "healthy Redis should bypass in-process miss cache when shared key is deleted")
}

func TestSubdomainMiddlewareIsolatesVerifiedCustomDomainFromGatewayRoutes(t *testing.T) {
	redisClient, err := NewRedisClientForTest()
	assert.NoError(t, err)
	common.InvalidateCustomDomainCache(context.Background(), redisClient, "api.isolated.io", "stopped.isolated.io", "portless.isolated.io")

	repo := &middlewareRepoForTest{
		customDomains: map[string]*types.CustomDomain{
			"api.isolated.io": {
				Domain:         "api.isolated.io",
				DeploymentName: "ep-app",
				Status:         types.CustomDomainStatusActive,
				Port:           8080, // should be ignored for non-Pod deployments
			},
			"portless.isolated.io": {
				Domain:         "portless.isolated.io",
				DeploymentName: "pod-app",
				Status:         types.CustomDomainStatusActive,
			},
		},
		customDeployments: map[string]*types.DeploymentWithRelated{
			"api.isolated.io": {
				Deployment: types.Deployment{Name: "ep-app", Subdomain: "ep-app-1234567"},
				Stub:       types.Stub{Type: types.StubType(types.StubTypeEndpointDeployment), Config: `{"authorized": true}`},
			},
			"portless.isolated.io": {
				Deployment: types.Deployment{Name: "pod-app", Subdomain: "pod-app-1234567"},
				Stub:       types.Stub{Type: types.StubType(types.StubTypePodDeployment), Config: `{"authorized": true}`},
			},
		},
	}

	e := echo.New()
	e.Use(Subdomain("https://app.example.com", repo, redisClient))
	gatewayControlPlaneHit := false
	e.GET("/api/v1/health", func(ctx echo.Context) error {
		gatewayControlPlaneHit = true
		return ctx.String(http.StatusOK, "gw-health")
	})
	epHit := false
	e.GET("/endpoint/:deploymentName/latest", func(ctx echo.Context) error {
		epHit = true
		return ctx.String(http.StatusOK, "ep")
	})

	// 1. Valid root request routes to /endpoint/ep-app/latest (ignoring stray port 8080).
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "api.isolated.io"
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, epHit)
	assert.False(t, gatewayControlPlaneHit)

	// 2. Request to /api/v1/health on verified custom domain MUST return 404, not Gateway health.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	req.Host = "api.isolated.io"
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.False(t, gatewayControlPlaneHit)

	// 3. Dot-segment traversal cannot escape handlerPath back to /api/v1/health.
	req = httptest.NewRequest(http.MethodGet, "/../../../../api/v1/health", nil)
	req.Host = "api.isolated.io"
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.False(t, gatewayControlPlaneHit)

	// 4. Pod with no resolved port, missing pinned port, or corrupt config returns 404.
	repo.customDomains["staleport.isolated.io"] = &types.CustomDomain{
		Domain: "staleport.isolated.io", DeploymentName: "pod-app", Port: 9090, Status: types.CustomDomainStatusActive,
	}
	repo.customDeployments["staleport.isolated.io"] = &types.DeploymentWithRelated{
		Deployment: types.Deployment{Name: "pod-app", Subdomain: "pod-app-1234567"},
		Stub:       types.Stub{Type: types.StubType(types.StubTypePodDeployment), Config: `{"authorized": true, "ports": [8000]}`},
	}
	repo.customDomains["corrupt.isolated.io"] = &types.CustomDomain{
		Domain: "corrupt.isolated.io", DeploymentName: "pod-app", Port: 8000, Status: types.CustomDomainStatusActive,
	}
	repo.customDeployments["corrupt.isolated.io"] = &types.DeploymentWithRelated{
		Deployment: types.Deployment{Name: "pod-app", Subdomain: "pod-app-1234567"},
		Stub:       types.Stub{Type: types.StubType(types.StubTypePodDeployment), Config: `{not-json`},
	}
	for _, host := range []string{"portless.isolated.io", "staleport.isolated.io", "corrupt.isolated.io"} {
		common.InvalidateCustomDomainCache(context.Background(), redisClient, host)
		req = httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
		req.Host = host
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.False(t, gatewayControlPlaneHit)
	}

	// 5. Verified domain with inactive deployment returns 404 and does not hit Gateway routes.
	repo.customDomains["stopped.isolated.io"] = &types.CustomDomain{Domain: "stopped.isolated.io", Status: types.CustomDomainStatusActive}
	repo.customDeployments["stopped.isolated.io"] = nil
	// Simulate GetDeploymentByCustomDomain returning inactive error when cd exists.
	inactiveRepo := &inactiveCustomDomainRepo{middlewareRepoForTest: repo}
	e2 := echo.New()
	e2.Use(Subdomain("https://app.example.com", inactiveRepo, redisClient))
	e2.GET("/api/v1/health", func(ctx echo.Context) error {
		gatewayControlPlaneHit = true
		return ctx.String(http.StatusOK, "gw-health")
	})
	req = httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	req.Host = "stopped.isolated.io"
	rec = httptest.NewRecorder()
	e2.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.False(t, gatewayControlPlaneHit)
}

type inactiveCustomDomainRepo struct {
	*middlewareRepoForTest
}

func (r *inactiveCustomDomainRepo) GetDeploymentByCustomDomain(_ context.Context, domain string) (*types.DeploymentWithRelated, *types.CustomDomain, error) {
	if cd, ok := r.customDomains[domain]; ok {
		return nil, cd, types.ErrCustomDomainDeploymentInactive
	}
	return nil, nil, types.ErrCustomDomainNotFound
}
