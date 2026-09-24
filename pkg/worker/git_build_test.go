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
		return resolveGitDockerfile(out, dir, src)
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

	plain := writeFiles(t, map[string]string{"Dockerfile": "# repo", "requirements.txt": "flask"})
	text, err = resolve(plain, &types.GitSource{})
	require.NoError(t, err)
	assert.Equal(t, "# repo", text, "then the repository's own Dockerfile")

	_, err = resolve(writeFiles(t, map[string]string{"README.md": ""}), &types.GitSource{})
	assert.ErrorContains(t, err, "no recognised stack")
}

func TestRepoFilesCannotEscapeThroughSymlinks(t *testing.T) {
	out := slog.New(slog.NewTextHandler(io.Discard, nil))
	outside := writeFiles(t, map[string]string{"secret": "host file"})
	dir := writeFiles(t, map[string]string{"app/Dockerfile.real": "# inside", "README.md": ""})
	require.NoError(t, os.Symlink(filepath.Join(outside, "secret"), filepath.Join(dir, "Dockerfile")))
	require.NoError(t, os.Symlink(filepath.Join(outside, "secret"), filepath.Join(dir, "railway.json")))
	require.NoError(t, os.Symlink("app/Dockerfile.real", filepath.Join(dir, "Dockerfile.link")))
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, "hostdir")))

	_, err := readRepoFile(dir, "Dockerfile")
	assert.ErrorContains(t, err, "outside the repository", "a committed link to a host file is not read")
	_, err = readRepoFile(dir, "hostdir/secret")
	assert.ErrorContains(t, err, "outside the repository", "nor through a linked directory")
	text, err := readRepoFile(dir, "Dockerfile.link")
	require.NoError(t, err)
	assert.Equal(t, "# inside", text, "links that stay inside the checkout still work")

	_, err = resolveGitDockerfile(out, dir, &types.GitSource{DockerfilePath: "Dockerfile"})
	assert.ErrorContains(t, err, "not found")
	assert.Equal(t, railwayConfig{}, readRailwayConfig(dir), "a linked railway.json is ignored")

	_, err = repoPath(dir, "hostdir")
	assert.ErrorContains(t, err, "outside the repository", "a linked working_dir cannot become the build context")
	path, err := repoPath(dir, "app")
	require.NoError(t, err)
	assert.Equal(t, "Dockerfile.real", func() string { e, _ := os.ReadDir(path); return e[0].Name() }())
}

func TestRenderDockerfileFromDetectedStack(t *testing.T) {
	flask := writeFiles(t, map[string]string{
		"requirements.txt": "flask\n",
		"app.py":           "from flask import Flask\napp = Flask(__name__)\n",
		"runtime.txt":      "python-3.11.4\n",
		"railway.json":     `{"build":{"buildCommand":"python manage.py collectstatic"}}`,
	})
	text, err := renderDockerfile(flask, "", readRailwayConfig(flask).buildCommand)
	require.NoError(t, err)
	assert.Contains(t, text, "FROM python:3.11-slim\n")
	assert.Contains(t, text, "RUN pip install --no-cache-dir -r requirements.txt gunicorn\n", "the chosen server is installed")
	assert.Contains(t, text, "RUN python manage.py collectstatic\n")
	assert.Contains(t, text, `CMD ["sh", "-c", "gunicorn app:app --bind 0.0.0.0:$PORT"]`)

	procfile := writeFiles(t, map[string]string{"requirements.txt": "", "Procfile": "web: gunicorn wsgi\n"})
	text, err = renderDockerfile(procfile, procfileCommand(procfile), "")
	require.NoError(t, err)
	assert.Contains(t, text, `"gunicorn wsgi --bind 0.0.0.0:$PORT"`, "gunicorn is bound to the service port")

	node := writeFiles(t, map[string]string{
		"package.json": `{"engines":{"node":">=18"},"scripts":{"build":"tsc","start":"node dist/index.js"}}`,
		"yarn.lock":    "",
	})
	text, err = renderDockerfile(node, "", "")
	require.NoError(t, err)
	assert.Contains(t, text, "FROM node:18-slim\n")
	assert.Contains(t, text, "RUN corepack enable && yarn install --frozen-lockfile\nRUN yarn run build\nENV NODE_ENV=production", "dev dependencies are present for the build")
	assert.Contains(t, text, `CMD ["sh", "-c", "yarn run start"]`)

	goMod := writeFiles(t, map[string]string{"go.mod": "module example.com/v2\n\ngo 1.22\n", "main.go": ""})
	text, err = renderDockerfile(goMod, "", "")
	require.NoError(t, err)
	assert.Contains(t, text, "FROM golang:1.22 AS build\n", "the go directive, not the module path, names the version")
	assert.Contains(t, text, "COPY --from=build /out /out\n")
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

	client := &ImageClient{imageCachePath: t.TempDir()}
	out := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, _, _, err := client.prepareGitBuild(context.Background(), out, &types.GitSource{RepoURL: "file://" + origin, Ref: "main", Commit: first, WorkingDir: "missing"})
	assert.ErrorContains(t, err, `working directory "missing" not found`, "a failed prepare returns an error, not a panic")
	entries, _ := os.ReadDir(filepath.Join(client.imageCachePath, "spool"))
	assert.Empty(t, entries, "and removes its checkout")
}
