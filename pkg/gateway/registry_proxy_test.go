package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type registryProxyWorkspaceRepo struct{ repository.WorkspaceRepository }

func (registryProxyWorkspaceRepo) AuthorizeToken(key string) (*types.Token, *types.Workspace, error) {
	workspace := &types.Workspace{ExternalId: "workspace-1"}
	switch key {
	case "private-token":
		return &types.Token{Key: key, Active: true, TokenType: types.TokenTypeWorkerPrivate, WorkerID: "worker-1"}, workspace, nil
	case "unbound-token":
		return &types.Token{Key: key, Active: true, TokenType: types.TokenTypeWorkerPrivate}, workspace, nil
	case "trusted-token":
		return &types.Token{Key: key, Active: true, TokenType: types.TokenTypeWorker, WorkerID: "worker-1"}, workspace, nil
	}
	return nil, nil, errors.New("not cached")
}

func (registryProxyWorkspaceRepo) SetAuthorizationToken(*types.Token, *types.Workspace) error {
	return nil
}

type registryProxyBackendRepo struct{ repository.BackendRepository }

func (registryProxyBackendRepo) AuthorizeToken(context.Context, string) (*types.Token, *types.Workspace, error) {
	return nil, nil, errors.New("token not found")
}

func (registryProxyBackendRepo) GetStubByExternalId(_ context.Context, id string, _ ...types.QueryFilter) (*types.StubWithRelated, error) {
	return &types.StubWithRelated{Stub: types.Stub{ExternalId: id, Config: `{"runtime":{"image_id":"image-1"}}`}}, nil
}

type registryProxyContainerRepo struct{ repository.ContainerRepository }

func (registryProxyContainerRepo) GetActiveContainersByWorkerId(workerID string) ([]types.ContainerState, error) {
	if workerID != "worker-1" {
		return nil, nil
	}
	return []types.ContainerState{
		{ContainerId: "container-1", StubId: "stub-1", WorkerId: workerID, WorkspaceId: "workspace-1"},
		{ContainerId: "container-2", StubId: "stub-2", WorkerId: workerID, WorkspaceId: "workspace-2"},
	}, nil
}

func TestRegistryProxy(t *testing.T) {
	var forwarded []*http.Request
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded = append(forwarded, r)
		switch {
		case strings.Contains(r.URL.Path, "/blobs/uploads/"):
			w.Header().Set("Location", "http://"+r.Host+r.URL.Path+"session?state=1")
			w.WriteHeader(http.StatusAccepted)
		case strings.Contains(r.URL.Path, "/blobs/"):
			http.Redirect(w, r, "https://objects.example.com/layer", http.StatusTemporaryRedirect)
		}
	}))
	defer upstream.Close()
	upstreamURL, err := url.Parse(upstream.URL)
	require.NoError(t, err)

	proxy := newRegistryProxy(types.AppConfig{ImageService: types.ImageServiceConfig{
		BuildRegistry:         upstreamURL.Host,
		BuildRegistryInsecure: true,
		BuildRepositoryName:   "stage/images",
		BuildRegistryCredentials: types.BuildRegistryCredentialsConfig{
			Type:        "basic",
			Credentials: map[string]string{"USERNAME": "registry-user", "PASSWORD": "registry-pass"},
		},
	}}, registryProxyBackendRepo{}, registryProxyWorkspaceRepo{}, registryProxyContainerRepo{})

	digest := "sha256:" + strings.Repeat("a", 64)
	serve := func(method, target, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, target, nil)
		if token != "" {
			r.SetBasicAuth("beta9", token)
		}
		w := httptest.NewRecorder()
		proxy.ServeHTTP(w, r)
		return w
	}

	for _, token := range []string{"", "trusted-token", "unbound-token", "unknown-token"} {
		w := serve(http.MethodGet, "/v2", token)
		require.Equal(t, http.StatusUnauthorized, w.Code, token)
		require.Equal(t, `Basic realm="beta9"`, w.Header().Get("WWW-Authenticate"))
	}
	require.Equal(t, http.StatusOK, serve(http.MethodGet, "/v2", "private-token").Code)
	require.Empty(t, forwarded)

	for _, tc := range []struct {
		method, target string
		forwarded      bool
	}{
		{http.MethodGet, "/v2/stage/images/manifests/image-1", true},
		{http.MethodHead, "/v2/stage/images/manifests/" + digest, true},
		{http.MethodGet, "/v2/stage/images/manifests/image-2", false},
		{http.MethodGet, "/v2/stage/images/blobs/" + digest, true},
		{http.MethodGet, "/v2/stage/images/blobs/latest", false},
		{http.MethodGet, "/v2/other/manifests/image-1", false},
		{http.MethodGet, "/v2/stage/images/../other/manifests/image-1", false},
		{http.MethodGet, "/v2/stage/images/%2e%2e/other/manifests/image-1", false},
		{http.MethodPost, "/v2/stage/images/blobs/uploads", true},
		{http.MethodPatch, "/v2/stage/images/blobs/uploads/session", true},
		{http.MethodPost, "/v2/stage/images/blobs/uploads?mount=" + digest + "&from=other", false},
		{http.MethodPut, "/v2/stage/images/manifests/stub-1-1700000000", true},
		{http.MethodPut, "/v2/stage/images/manifests/stub-2-1700000000", false},
		{http.MethodPut, "/v2/stage/images/manifests/image-1", false},
		{http.MethodDelete, "/v2/stage/images/manifests/" + digest, false},
	} {
		forwarded = nil
		w := serve(tc.method, tc.target, "private-token")
		if !tc.forwarded {
			require.Equal(t, http.StatusForbidden, w.Code, "%s %s", tc.method, tc.target)
			require.Empty(t, forwarded, "%s %s", tc.method, tc.target)
			continue
		}
		require.Len(t, forwarded, 1, "%s %s", tc.method, tc.target)
		user, password, ok := forwarded[0].BasicAuth()
		require.True(t, ok)
		require.Equal(t, "registry-user", user)
		require.Equal(t, "registry-pass", password)
	}

	w := serve(http.MethodGet, "/v2/stage/images/blobs/"+digest, "private-token")
	require.Equal(t, http.StatusTemporaryRedirect, w.Code)
	require.Equal(t, "https://objects.example.com/layer", w.Header().Get("Location"))

	forwarded = nil
	w = serve(http.MethodPost, "/v2/stage/images/blobs/uploads", "private-token")
	require.Equal(t, "/v2/stage/images/blobs/uploads/", forwarded[0].URL.Path)
	require.Equal(t, "/v2/stage/images/blobs/uploads/session?state=1", w.Header().Get("Location"))
}
