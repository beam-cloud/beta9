package repository_services

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/require"
)

type registryBackend struct {
	repository.BackendRepository
	tokens map[string]*types.Token
}

func (b *registryBackend) AuthorizeToken(_ context.Context, key string) (*types.Token, *types.Workspace, error) {
	if token := b.tokens[key]; token != nil && token.Active {
		return token, &types.Workspace{ExternalId: "workspace-id"}, nil
	}
	return nil, nil, errors.New("invalid token")
}

func TestWorkerRegistryAssignments(t *testing.T) {
	upstream := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(upstream.Close)
	upstreamHost := strings.TrimPrefix(upstream.URL, "http://")
	image, err := random.Image(1024, 1)
	require.NoError(t, err)
	foreign, err := random.Image(1024, 1)
	require.NoError(t, err)
	assignedRef, err := name.NewTag(upstreamHost+"/images:image-id", name.Insecure)
	require.NoError(t, err)
	require.NoError(t, remote.Write(assignedRef, image))
	foreignRef, err := name.NewTag(upstreamHost+"/images:foreign", name.Insecure)
	require.NoError(t, err)
	require.NoError(t, remote.Write(foreignRef, foreign))

	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	backend := &registryBackend{tokens: map[string]*types.Token{
		"worker-key":  {Key: "worker-key", TokenType: types.TokenTypeWorker, WorkerID: "worker-id", Active: true},
		"runtime-key": {Key: "runtime-key", TokenType: types.TokenTypeWorkspaceRestricted, Active: true},
		"admin-key":   {Key: "admin-key", TokenType: types.TokenTypeClusterAdmin, Active: true},
	}}
	config := types.AppConfig{ImageService: types.ImageServiceConfig{BuildRegistry: upstreamHost, BuildRepositoryName: "images", BuildRegistryInsecure: true}}
	handler, err := NewWorkerRegistryProxy(config, backend, &assignedOriginContainerRepo{}, rdb)
	require.NoError(t, err)
	proxy := handler.(*registryProxy)
	proxy.service.imageGrants = fixtureImageGrants{}
	broker := httptest.NewServer(handler)
	t.Cleanup(broker.Close)
	get := func(method, path, container, key string, body []byte) *http.Response {
		t.Helper()
		r, err := http.NewRequest(method, broker.URL+path, bytes.NewReader(body))
		require.NoError(t, err)
		r.SetBasicAuth(container, key)
		response, err := broker.Client().Do(r)
		require.NoError(t, err)
		t.Cleanup(func() { response.Body.Close() })
		return response
	}
	for _, key := range []string{"runtime-key", "admin-key", "invalid"} {
		require.Equal(t, http.StatusUnauthorized, get("GET", "/v2/", "container-id", key, nil).StatusCode)
	}
	require.Equal(t, http.StatusUnauthorized, get("GET", "/v2/", "missing", "worker-key", nil).StatusCode)
	require.Equal(t, http.StatusOK, get("GET", "/v2/images/manifests/image-id", "container-id", "worker-key", nil).StatusCode)
	for _, path := range []string{"/v2/_catalog", "/v2/images/tags/list", "/v2/other/manifests/image-id", "/v2/images/manifests/foreign"} {
		require.Equal(t, http.StatusForbidden, get("GET", path, "container-id", "worker-key", nil).StatusCode, path)
	}
	layers, err := image.Layers()
	require.NoError(t, err)
	digest, err := layers[0].Digest()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, get("GET", "/v2/images/blobs/"+digest.String(), "container-id", "worker-key", nil).StatusCode)
	foreignLayers, err := foreign.Layers()
	require.NoError(t, err)
	foreignDigest, err := foreignLayers[0].Digest()
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, get("GET", "/v2/images/blobs/"+foreignDigest.String(), "container-id", "worker-key", nil).StatusCode)

	// Snapshot uploads use ordinary registry clients; their blobs, manifest and
	// upload sessions remain scoped to the assigned container.
	output, err := name.NewTag(strings.TrimPrefix(broker.URL, "http://")+"/images:stub-id-1780000000", name.Insecure)
	require.NoError(t, err)
	require.NoError(t, remote.Write(output, foreign, remote.WithAuth(&authn.Basic{Username: "container-id", Password: "worker-key"})))
	require.Equal(t, http.StatusForbidden, get("GET", "/v2/images/blobs/"+foreignDigest.String(), "export", "worker-key", nil).StatusCode)
	manifest, err := foreign.RawManifest()
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, get("PUT", "/v2/images/manifests/foreign", "container-id", "worker-key", manifest).StatusCode)
	require.Equal(t, http.StatusForbidden, get("PUT", "/v2/images/manifests/stub-id-1780000001", "export", "worker-key", manifest).StatusCode)
	backend.tokens["worker-key"].Active = false
	require.Equal(t, http.StatusUnauthorized, get("GET", "/v2/images/manifests/image-id", "container-id", "worker-key", nil).StatusCode)
}
