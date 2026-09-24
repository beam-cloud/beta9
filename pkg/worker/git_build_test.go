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

func TestReadRailwayConfig(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "railway.json"), []byte(`{
		"build": {"buildCommand": "npm run build", "dockerfilePath": "docker/Dockerfile.web"},
		"deploy": {"startCommand": "npm start"}
	}`), 0o644))
	cfg := readRailwayConfig(dir)
	assert.Equal(t, railwayConfig{startCommand: "npm start", buildCommand: "npm run build", dockerfilePath: "docker/Dockerfile.web"}, cfg)

	tomlDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(tomlDir, "railway.toml"), []byte(`
[build]
builder = "nixpacks"
buildCommand = "make build"

[deploy]
startCommand = 'hypercorn main:app --bind "[::]:$PORT"'
`), 0o644))
	cfg = readRailwayConfig(tomlDir)
	assert.Equal(t, "make build", cfg.buildCommand)
	assert.Equal(t, `hypercorn main:app --bind "[::]:$PORT"`, cfg.startCommand)
	assert.Empty(t, cfg.dockerfilePath)

	assert.Equal(t, railwayConfig{}, readRailwayConfig(t.TempDir()))
}

func TestGunicornBound(t *testing.T) {
	assert.Equal(t, "gunicorn app:app --bind 0.0.0.0:$PORT", gunicornBound("gunicorn app:app"))
	assert.Equal(t, "gunicorn -b :9000 app:app", gunicornBound("gunicorn -b :9000 app:app"))
	assert.Equal(t, "gunicorn --bind=0.0.0.0:80 app:app", gunicornBound("gunicorn --bind=0.0.0.0:80 app:app"))
	assert.Equal(t, "uvicorn app:app", gunicornBound("uvicorn app:app"))
	assert.Equal(t, "", gunicornBound(""))
}

func TestResolveGitDockerfilePrefersExplicitThenRepoThenRailway(t *testing.T) {
	out := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM scratch\n# repo"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "docker"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "docker", "Dockerfile.web"), []byte("FROM scratch\n# web"), 0o644))

	text, err := resolveGitDockerfile(context.Background(), out, dir, &types.GitSource{DockerfilePath: "docker/Dockerfile.web"})
	require.NoError(t, err)
	assert.Contains(t, text, "# web")

	text, err = resolveGitDockerfile(context.Background(), out, dir, &types.GitSource{})
	require.NoError(t, err)
	assert.Contains(t, text, "# repo")

	_, err = resolveGitDockerfile(context.Background(), out, dir, &types.GitSource{DockerfilePath: "missing/Dockerfile"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"missing/Dockerfile" not found`)

	_, err = resolveGitDockerfile(context.Background(), out, dir, &types.GitSource{DockerfilePath: "../../etc/passwd"})
	require.Error(t, err, "paths cannot escape the checkout")

	railwayDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(railwayDir, "Dockerfile"), []byte("FROM scratch\n# root"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(railwayDir, "Dockerfile.api"), []byte("FROM scratch\n# api"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(railwayDir, "railway.json"), []byte(`{"build":{"dockerfilePath":"Dockerfile.api"}}`), 0o644))
	text, err = resolveGitDockerfile(context.Background(), out, railwayDir, &types.GitSource{})
	require.NoError(t, err)
	assert.Contains(t, text, "# api", "railway.json picks the Dockerfile like it does on Railway")
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
		outBytes, err := cmd.CombinedOutput()
		require.NoError(t, err, string(outBytes))
		return strings.TrimSpace(string(outBytes))
	}
	git("init", "-q", "-b", "main")
	require.NoError(t, os.WriteFile(filepath.Join(origin, "Dockerfile"), []byte("FROM scratch\n# v1"), 0o644))
	git("add", ".")
	git("commit", "-q", "-m", "v1")
	first := git("rev-parse", "HEAD")
	require.NoError(t, os.WriteFile(filepath.Join(origin, "Dockerfile"), []byte("FROM scratch\n# v2"), 0o644))
	git("commit", "-q", "-am", "v2")
	git("config", "uploadpack.allowReachableSHA1InWant", "true")

	dir := t.TempDir()
	src := &types.GitSource{RepoURL: "file://" + origin, Ref: "main", Commit: first}
	require.NoError(t, gitCheckout(context.Background(), dir, src))
	text, err := os.ReadFile(filepath.Join(dir, "Dockerfile"))
	require.NoError(t, err)
	assert.Contains(t, string(text), "# v1", "the build uses the commit the gateway resolved, not the branch tip")

	// A ref the server cannot serve by hash still clones by branch.
	dir = t.TempDir()
	src = &types.GitSource{RepoURL: "file://" + origin, Ref: "main", Commit: strings.Repeat("0", 40)}
	require.NoError(t, gitCheckout(context.Background(), dir, src))
	text, err = os.ReadFile(filepath.Join(dir, "Dockerfile"))
	require.NoError(t, err)
	assert.Contains(t, string(text), "# v2")

	err = gitCheckout(context.Background(), t.TempDir(), &types.GitSource{RepoURL: "file://" + origin, Ref: "nope"})
	require.Error(t, err)
}

func TestGitRedactHidesToken(t *testing.T) {
	assert.Equal(t, "fatal: auth *** failed", gitRedact("fatal: auth ghs_secret failed", "ghs_secret"))
	assert.Equal(t, "plain", gitRedact("plain", ""))
}
