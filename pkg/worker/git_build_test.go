package worker

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFiles(t *testing.T, files map[string]string) string {
	dir := t.TempDir()
	for name, text := range files {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(text), 0o644))
	}
	return dir
}

func TestResolveGitDockerfileFollowsRailwayOrder(t *testing.T) {
	out := slog.New(slog.NewTextHandler(io.Discard, nil))
	resolve := func(dir string, src *types.GitSource) (string, error) {
		return resolveGitDockerfile(context.Background(), out, dir, src)
	}

	dir := writeFiles(t, map[string]string{
		"Dockerfile":            "# repo",
		"docker/Dockerfile.web": "# web",
		"Dockerfile.api":        "# api",
		"railway.toml":          "[build]\ndockerfilePath = 'Dockerfile.api'\n\n[deploy]\nstartCommand = \"hypercorn main:app --bind \\\"[::]:$PORT\\\"\"\n",
	})
	text, err := resolve(dir, &types.GitSource{DockerfilePath: "docker/Dockerfile.web"})
	require.NoError(t, err)
	assert.Equal(t, "# web", text, "an explicit path wins")

	text, err = resolve(dir, &types.GitSource{})
	require.NoError(t, err)
	assert.Equal(t, "# api", text, "then railway config")
	assert.Equal(t, `hypercorn main:app --bind \"[::]:$PORT\"`, readRailwayConfig(dir).startCommand, "quotes inside a TOML string survive")

	_, err = resolve(dir, &types.GitSource{DockerfilePath: "../../etc/passwd"})
	assert.ErrorContains(t, err, "not found", "paths cannot escape the checkout")

	plain := writeFiles(t, map[string]string{"Dockerfile": "# repo", "railway.json": `{"deploy":{"startCommand":"gunicorn app:app"}}`})
	text, err = resolve(plain, &types.GitSource{})
	require.NoError(t, err)
	assert.Equal(t, "# repo", text, "then the repository's own Dockerfile")
	assert.Equal(t, "gunicorn app:app --bind 0.0.0.0:$PORT", gunicornBound(readRailwayConfig(plain).startCommand))
}

func TestGitCheckoutFetchesResolvedCommit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	origin := t.TempDir()
	git := func(args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = origin
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "main")
	require.NoError(t, os.WriteFile(filepath.Join(origin, "Dockerfile"), []byte("# v1"), 0o644))
	git("add", ".")
	git("commit", "-q", "-m", "v1")
	first := git("rev-parse", "HEAD")
	require.NoError(t, os.WriteFile(filepath.Join(origin, "Dockerfile"), []byte("# v2"), 0o644))
	git("commit", "-q", "-am", "v2")
	git("config", "uploadpack.allowReachableSHA1InWant", "true")

	checkout := func(src *types.GitSource) string {
		dir := t.TempDir()
		require.NoError(t, gitCheckout(context.Background(), dir, src))
		text, err := os.ReadFile(filepath.Join(dir, "Dockerfile"))
		require.NoError(t, err)
		return string(text)
	}
	assert.Equal(t, "# v1", checkout(&types.GitSource{RepoURL: "file://" + origin, Ref: "main", Commit: first}), "builds the commit the gateway resolved, not the branch tip")
	assert.Equal(t, "# v2", checkout(&types.GitSource{RepoURL: "file://" + origin, Ref: "main", Commit: strings.Repeat("0", 40)}), "a hash the server cannot serve falls back to cloning the ref")
	assert.Error(t, gitCheckout(context.Background(), t.TempDir(), &types.GitSource{RepoURL: "file://" + origin, Ref: "nope"}))
}
