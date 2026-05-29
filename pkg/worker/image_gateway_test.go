package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	reg "github.com/beam-cloud/beta9/pkg/registry"
)

func TestPullImageArchiveFromGateway(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agent/images/image-a/image-a.clip" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer worker-token" {
			t.Fatalf("authorization = %q, want worker token", got)
		}
		_, _ = w.Write([]byte("clip-archive"))
	}))
	t.Cleanup(server.Close)
	t.Setenv(agentGatewayHTTPURLEnv, server.URL)
	t.Setenv("WORKER_TOKEN", "worker-token")

	client := &ImageClient{
		registry: &reg.ImageRegistry{ImageFileExtension: reg.LocalImageFileExtension},
	}
	path := filepath.Join(t.TempDir(), "cache", "image-a.clip.tmp")

	if err := client.pullImageArchiveFromGateway(context.Background(), path, "image-a"); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "clip-archive" {
		t.Fatalf("archive contents = %q", data)
	}
}
