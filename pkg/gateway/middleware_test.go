package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
)

type middlewareRepoForTest struct {
	deployName string
	stub       *types.Stub
}

func (r *middlewareRepoForTest) GetStubByExternalId(ctx context.Context, externalId string, queryFilters ...types.QueryFilter) (*types.StubWithRelated, error) {
	return &types.StubWithRelated{
		Stub: *r.stub,
	}, nil
}

func (r *middlewareRepoForTest) GetDeploymentByStubGroup(ctx context.Context, version uint, stubGroup string) (*types.DeploymentWithRelated, error) {
	return &types.DeploymentWithRelated{
		Deployment: types.Deployment{
			Name: r.deployName,
		},
		Stub: *r.stub,
	}, nil
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
				"path": "/taskqueue/task2",
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
				"path": "/taskqueue/hello-world",
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
			e := echo.New()
			repo := &middlewareRepoForTest{
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

			mw := subdomainMiddleware("https://app.example.com", repo)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Host = test.host
			rec := httptest.NewRecorder()

			c := e.NewContext(req, rec)
			h := mw(handler)
			err := h(c)
			assert.NoError(t, err)
			e.Shutdown(context.Background())
		})
	}
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
			assert.Equal(t, test.expected, parseSubdomain(test.host, test.base))
		})
	}
}

func TestParseSubdomainFields(t *testing.T) {
	tests := []struct {
		subdomain   string
		expected    *SubdomainFields
		expectedErr error
	}{
		{
			subdomain: "app1-9a7dbcc",
			expected: &SubdomainFields{
				StubGroup: "app1-9a7dbcc",
			},
		},
		{
			subdomain: "app2-7a7db8c-latest",
			expected: &SubdomainFields{
				StubGroup: "app2-7a7db8c",
				Version:   "latest",
			},
		},
		{
			subdomain: "my-app-7a7db8c-v1",
			expected: &SubdomainFields{
				StubGroup: "my-app-7a7db8c",
				Version:   "v1",
			},
		},
		{
			subdomain: "8f32e485-2b2e-4238-9878-490eb9b0a9d3",
			expected: &SubdomainFields{
				StubId: "8f32e485-2b2e-4238-9878-490eb9b0a9d3",
			},
		},
		{
			subdomain:   "invalid-subdomain",
			expectedErr: ErrSubdomainDoesNotMatchRegex,
		},
		{
			subdomain:   "invalid-123f",
			expectedErr: ErrSubdomainDoesNotMatchRegex,
		},
		{
			subdomain:   "invalid-2b2e-4238-9878-111122222333",
			expectedErr: ErrSubdomainDoesNotMatchRegex,
		},
		{
			subdomain:   "invalid-8f32e485-2b2e-4238-9878-490eb9b0a9d3",
			expectedErr: ErrSubdomainDoesNotMatchRegex,
		},
	}

	for _, test := range tests {
		t.Run(test.subdomain, func(t *testing.T) {
			fields, err := parseSubdomainFields(test.subdomain)

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
		fields     *SubdomainFields
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
			fields: &SubdomainFields{
				StubId: "4ec446ce-3fd1-41a8-9f70-4d25b9224821",
			},
			expected: "/endpoint/id/4ec446ce-3fd1-41a8-9f70-4d25b9224821",
		},
		{
			name: "name-and-version-path",
			stub: &types.Stub{
				Type: types.StubType(types.StubTypeTaskQueueDeployment),
			},
			fields: &SubdomainFields{
				Name:    "tq",
				Version: "v55",
			},
			expected: "/taskqueue/tq/v55",
		},
		{
			name: "name-and-version-path-with-extra-paths",
			stub: &types.Stub{
				Type: types.StubType(types.StubTypeASGIDeployment),
			},
			fields: &SubdomainFields{
				Name:    "tq",
				Version: "v55",
			},
			extraPaths: []string{"api", "users"},
			expected:   "/asgi/tq/v55/api/users",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, buildHandlerPath(test.stub, test.fields, test.extraPaths...))
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
			assert.Equal(t, test.expected, parseHostFromURL(test.url))
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
			assert.Equal(t, test.expected, parseVersion(test.version))
		})
	}
}
