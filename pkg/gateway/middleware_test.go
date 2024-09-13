package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
)

type middlewareRepoForTest struct {
	stub *types.Stub
}

func (r *middlewareRepoForTest) GetStubByExternalId(ctx context.Context, externalId string, queryFilters ...types.QueryFilter) (*types.StubWithRelated, error) {
	return &types.StubWithRelated{
		Stub: *r.stub,
	}, nil
}
func (r *middlewareRepoForTest) GetDeploymentByStubGroup(ctx context.Context, name string, version uint, stubGroup string) (*types.DeploymentWithRelated, error) {
	return &types.DeploymentWithRelated{
		Stub: *r.stub,
	}, nil
}

func TestSubdomainMiddleware(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		stubType types.StubType
		expected map[string]string
	}{
		{
			name:     "subdomain-with-name-hash",
			host:     "task2-7a7db8c.app.example.com",
			stubType: "taskqueue/deployment",
			expected: map[string]string{
				"path": "/taskqueue/task2",
			},
		},
		{
			name:     "subdomain-with-name-hash-and-latest",
			host:     "task2-7a7db8c-latest.app.example.com",
			stubType: "taskqueue/deployment",
			expected: map[string]string{
				"path": "/taskqueue/task2/latest",
			},
		},
		{
			name:     "subdomain-with-name-hash-and-version",
			host:     "task2-7a7db8c-v1.app.example.com",
			stubType: "taskqueue/deployment",
			expected: map[string]string{
				"path": "/taskqueue/task2/v1",
			},
		},
		{
			name:     "subdomain-with-name-hash-and-stubid",
			host:     "task2-7a7db8c-8f32e485-2b2e-4238-9878-490eb9b0a9d3.app.example.com",
			stubType: "taskqueue/deployment",
			expected: map[string]string{
				"path": "/taskqueue/id/8f32e485-2b2e-4238-9878-490eb9b0a9d3",
			},
		},
		{
			name:     "subdomain-with-hyphen-name-and-hash",
			host:     "hello-world-7a7db8c.app.example.com",
			stubType: "taskqueue/deployment",
			expected: map[string]string{
				"path": "/taskqueue/hello-world",
			},
		},
		{
			name:     "subdomain-with-hyphen-name-and-hash-and-version",
			host:     "hello-world-123-7a7db8c-v3.app.example.com",
			stubType: "taskqueue/deployment",
			expected: map[string]string{
				"path": "/taskqueue/hello-world-123/v3",
			},
		},
		{
			name:     "subdomain-with-hyphen-name-and-hash-and-latest",
			host:     "hello-world-123-7a7db8c-latest.app.example.com",
			stubType: "taskqueue/deployment",
			expected: map[string]string{
				"path": "/taskqueue/hello-world-123/latest",
			},
		},
		{
			name:     "subdomain-with-hyphen-name-and-hash-and-stubid",
			host:     "hello-world-again-and-again-7a7db8c-8f32e485-2b2e-4238-9878-490eb9b0a9d3.app.example.com",
			stubType: "taskqueue/deployment",
			expected: map[string]string{
				"path": "/taskqueue/id/8f32e485-2b2e-4238-9878-490eb9b0a9d3",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e := echo.New()
			repo := &middlewareRepoForTest{
				stub: &types.Stub{
					Type: test.stubType,
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
			subdomain: "task1-9a7dbcc",
			expected: &SubdomainFields{
				Name:      "task1",
				StubGroup: "task1-9a7dbcc",
				Version:   "",
			},
		},
		{

			subdomain: "task2-7a7db8c-latest",
			expected: &SubdomainFields{
				Name:      "task2",
				StubGroup: "task2-7a7db8c",
				Version:   "latest",
			},
		},
		{
			subdomain: "task3-7a7db8c-v1",
			expected: &SubdomainFields{
				Name:      "task3",
				StubGroup: "task3-7a7db8c",
				Version:   "v1",
			},
		},
		{
			subdomain: "task4-4b7df8c-8f32e485-2b2e-4238-9878-490eb9b0a9d3",
			expected: &SubdomainFields{
				Name:      "task4",
				StubGroup: "task4-4b7df8c",
				Version:   "8f32e485-2b2e-4238-9878-490eb9b0a9d3",
			},
		},
		{
			subdomain:   "task5",
			expectedErr: errors.New("subdomain does not match regex"),
		},
		{
			subdomain:   "task6-8f32e485-2b2e-4238-9878-490eb9b0a9d3",
			expectedErr: errors.New("subdomain does not match regex"),
		},
	}

	for _, test := range tests {
		t.Run(test.subdomain, func(t *testing.T) {
			fields, err := parseSubdomainFields(subdomainRegex, test.subdomain)

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

func TestIsValidExternalId(t *testing.T) {
	tests := []struct {
		externalId string
		expected   bool
	}{
		{
			externalId: "task1-9a7dbcc",
			expected:   false,
		},
		{
			externalId: "1",
			expected:   false,
		},
		{
			externalId: "v1",
			expected:   false,
		},
		{
			externalId: "8f32e485-2b2e-4238-9878-490eb9b0a9d3",
			expected:   true,
		},
		{
			externalId: "565fe2e4-c0ae-4153-813a-97a8eb818422",
			expected:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.externalId, func(t *testing.T) {
			assert.Equal(t, test.expected, isValidExternalID(test.externalId))
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
			name: "endpoint-noauth-with-extra-paths",
			stub: &types.Stub{
				Type:       "endpoint/deployment",
				Config:     `{"authorized": false}`,
				ExternalId: "49a41870-bb61-4c45-aad1-68aaa7073c2e",
			},
			extraPaths: []string{"api", "users"},
			expected:   "/endpoint/public/49a41870-bb61-4c45-aad1-68aaa7073c2e/api/users",
		},
		{
			name: "endpoint-with-stubid-for-version",
			stub: &types.Stub{
				Type: "endpoint/deployment",
			},
			fields: &SubdomainFields{
				Name:      "handler",
				StubGroup: "handler-332db1c",
				Version:   "4ec446ce-3fd1-41a8-9f70-4d25b9224821",
			},
			extraPaths: []string{},
			expected:   "/endpoint/id/4ec446ce-3fd1-41a8-9f70-4d25b9224821",
		},
		{
			name: "taskqueue-with-normal-version",
			stub: &types.Stub{
				Type: "taskqueue/deployment",
			},
			fields: &SubdomainFields{
				Name:      "tq",
				StubGroup: "tq-9a7dbcc",
				Version:   "v55",
			},
			extraPaths: []string{},
			expected:   "/taskqueue/tq/v55",
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
