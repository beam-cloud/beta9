package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestS3ClientUsesPathStyleForExplicitEndpoint(t *testing.T) {
	var gotHost string
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		gotPath = r.URL.Path
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewS3Client(context.Background(), S3SourceConfig{
		BucketName:  "workspace-local-9202152b-e63e-47fd-825b-a193913e7403",
		Region:      "us-east-1",
		EndpointURL: server.URL,
		AccessKey:   "access-key",
		SecretKey:   "secret-key",
	}, ServerConfig{})
	require.NoError(t, err)

	_, _, err = client.Head(context.Background(), "volumes/run/files/workspace_fuse/sequential/32mb.bin")
	require.NoError(t, err)
	require.Equal(t, strings.TrimPrefix(server.URL, "http://"), gotHost)
	require.Equal(t, "/workspace-local-9202152b-e63e-47fd-825b-a193913e7403/volumes/run/files/workspace_fuse/sequential/32mb.bin", gotPath)
}
