package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	gitCloneTimeout    = 10 * time.Minute
	nixpacksTimeout    = 5 * time.Minute
	nixpacksDockerfile = ".nixpacks/Dockerfile"
)

// gitBuild is a build whose context is a checkout of a repository: the
// repository's Dockerfile when it has one, otherwise one nixpacks generates
// from what it detects (Procfile, nixpacks.toml, the language toolchain).
type gitBuild struct {
	root       string // the checkout
	contextDir string // root/working_dir, what buildah gets as its context
	dockerfile string
}

// prepareGitBuild clones the source and settles on a Dockerfile. The checkout
// lives on the disk-backed spool dir, not the RAM-backed build dir.
func (c *ImageClient) prepareGitBuild(ctx context.Context, out *slog.Logger, src *types.GitSource) (*gitBuild, func(), error) {
	root, err := os.MkdirTemp(c.layerSpoolDir(), "git-src-")
	if err != nil {
		return nil, nil, fmt.Errorf("create checkout dir: %w", err)
	}
	cleanup := func() { os.RemoveAll(root) }

	out.Info(fmt.Sprintf("Cloning %s at %s\n", src.RepoURL, shortCommit(src)))
	if err := gitCheckout(ctx, root, src); err != nil {
		cleanup()
		return nil, nil, err
	}

	contextDir := root
	if src.WorkingDir != "" {
		contextDir = filepath.Join(root, filepath.Clean("/"+src.WorkingDir))
		if info, err := os.Stat(contextDir); err != nil || !info.IsDir() {
			cleanup()
			return nil, nil, fmt.Errorf("working directory %q not found in repository", src.WorkingDir)
		}
	}

	dockerfile, err := resolveGitDockerfile(ctx, out, contextDir, src)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return &gitBuild{root: root, contextDir: contextDir, dockerfile: dockerfile}, cleanup, nil
}

// gitCheckout fetches exactly the resolved commit into dir. Fetching a commit by
// hash needs the server to allow it (GitHub and GitLab do); when it refuses,
// the ref is cloned shallowly instead.
func gitCheckout(ctx context.Context, dir string, src *types.GitSource) error {
	ctx, cancel := context.WithTimeout(ctx, gitCloneTimeout)
	defer cancel()

	env, cleanupEnv, err := gitAuthEnv(dir, src.Token)
	if err != nil {
		return err
	}
	defer cleanupEnv()

	remote := gitRemoteURL(src)
	run := func(args ...string) error {
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = dir
		cmd.Env = env
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("git %s: %s", args[0], strings.TrimSpace(gitRedact(stderr.String(), src.Token)))
		}
		return nil
	}

	if src.Commit != "" {
		if err := run("init", "-q"); err != nil {
			return err
		}
		if err := run("remote", "add", "origin", remote); err != nil {
			return err
		}
		if err := run("fetch", "-q", "--depth", "1", "origin", src.Commit); err == nil {
			return run("checkout", "-q", "--recurse-submodules", "FETCH_HEAD")
		}
		os.RemoveAll(filepath.Join(dir, ".git"))
	}

	args := []string{"clone", "-q", "--depth", "1", "--recurse-submodules", "--shallow-submodules"}
	if src.Ref != "" {
		args = append(args, "--branch", src.Ref)
	}
	if err := run(append(args, remote, ".")...); err != nil {
		return fmt.Errorf("clone %s: %w", src.RepoURL, err)
	}
	return nil
}

// gitAuthEnv hands the token to git through GIT_ASKPASS so it never lands in
// the checkout's config or a process argument.
func gitAuthEnv(dir, token string) ([]string, func(), error) {
	env := append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_LFS_SKIP_SMUDGE=1")
	if token == "" {
		return env, func() {}, nil
	}
	askpass, err := os.CreateTemp("", "git-askpass-")
	if err != nil {
		return nil, nil, fmt.Errorf("create askpass helper: %w", err)
	}
	if _, err := askpass.WriteString("#!/bin/sh\nprintf '%s' \"$GIT_SOURCE_TOKEN\"\n"); err != nil {
		askpass.Close()
		os.Remove(askpass.Name())
		return nil, nil, fmt.Errorf("write askpass helper: %w", err)
	}
	askpass.Close()
	os.Chmod(askpass.Name(), 0o700)
	env = append(env, "GIT_ASKPASS="+askpass.Name(), "GIT_SOURCE_TOKEN="+token)
	return env, func() { os.Remove(askpass.Name()) }, nil
}

// gitRemoteURL names the token user GitHub and GitLab expect; the password
// itself comes from GIT_ASKPASS.
func gitRemoteURL(src *types.GitSource) string {
	if src.Token == "" {
		return src.RepoURL
	}
	u, err := url.Parse(src.RepoURL)
	if err != nil {
		return src.RepoURL
	}
	u.User = url.User("x-access-token")
	return u.String()
}

func gitRedact(s, token string) string {
	if token == "" {
		return s
	}
	return strings.ReplaceAll(s, token, "***")
}

func shortCommit(src *types.GitSource) string {
	switch {
	case len(src.Commit) >= 12:
		return src.Commit[:12]
	case src.Ref != "":
		return src.Ref
	default:
		return "HEAD"
	}
}

// resolveGitDockerfile returns the Dockerfile to build: the one requested,
// the repository's own, or the one nixpacks generates into the context.
func resolveGitDockerfile(ctx context.Context, out *slog.Logger, contextDir string, src *types.GitSource) (string, error) {
	cfg := readRailwayConfig(contextDir)

	if path := firstNonEmpty(src.DockerfilePath, cfg.dockerfilePath); path != "" {
		text, err := readInContext(contextDir, path)
		if err != nil {
			return "", fmt.Errorf("dockerfile %q not found in repository", path)
		}
		out.Info(fmt.Sprintf("Building %s\n", path))
		return text, nil
	}
	if text, err := readInContext(contextDir, "Dockerfile"); err == nil {
		out.Info("Building the repository's Dockerfile\n")
		return text, nil
	}

	start := gunicornBound(firstNonEmpty(src.StartCommand, cfg.startCommand))
	build := firstNonEmpty(src.BuildCommand, cfg.buildCommand)
	return nixpacksDockerfileFor(ctx, out, contextDir, start, build)
}

// nixpacksDockerfileFor writes .nixpacks/ into the context (its Dockerfile
// COPYs the nix plan from there) and returns the Dockerfile.
func nixpacksDockerfileFor(ctx context.Context, out *slog.Logger, contextDir, startCommand, buildCommand string) (string, error) {
	nixpacks, err := exec.LookPath("nixpacks")
	if err != nil {
		return "", fmt.Errorf("repository has no Dockerfile and this worker cannot generate one (nixpacks missing)")
	}
	ctx, cancel := context.WithTimeout(ctx, nixpacksTimeout)
	defer cancel()

	args := []string{"build", contextDir, "--out", contextDir}
	if startCommand != "" {
		args = append(args, "--start-cmd", startCommand)
	}
	if buildCommand != "" {
		args = append(args, "--build-cmd", buildCommand)
	}
	cmd := exec.CommandContext(ctx, nixpacks, args...)
	cmd.Env = append(os.Environ(), "NIXPACKS_NO_COLOR=1")
	var stderr bytes.Buffer
	cmd.Stdout = &stderr
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("repository has no Dockerfile and nixpacks could not detect how to build it: %s", strings.TrimSpace(stderr.String()))
	}
	text, err := readInContext(contextDir, nixpacksDockerfile)
	if err != nil {
		return "", fmt.Errorf("nixpacks produced no Dockerfile: %s", strings.TrimSpace(stderr.String()))
	}
	out.Info("Generated a Dockerfile with nixpacks\n")
	if plan := nixpacksPlanSummary(stderr.String()); plan != "" {
		out.Info(plan + "\n")
	}
	return text, nil
}

// nixpacksPlanSummary keeps the plan box nixpacks prints, which tells the user
// what was detected, and drops the progress noise around it.
func nixpacksPlanSummary(output string) string {
	start := strings.Index(output, "╔")
	end := strings.LastIndex(output, "╝")
	if start < 0 || end < start {
		return ""
	}
	return output[start : end+len("╝")]
}

// gunicornBound makes gunicorn listen where the platform routes traffic; its
// default is 127.0.0.1:8000, which nothing outside the container reaches.
func gunicornBound(command string) string {
	if strings.Contains(command, "gunicorn") && !gunicornBindFlag.MatchString(command) {
		return command + " --bind 0.0.0.0:$PORT"
	}
	return command
}

var gunicornBindFlag = regexp.MustCompile(`(^|\s)(-b|--bind)(\s|=)`)

// railwayConfig is the part of railway.json / railway.toml that decides how
// a repository builds and starts, so repositories written for Railway deploy
// unchanged.
type railwayConfig struct {
	startCommand   string
	buildCommand   string
	dockerfilePath string
}

func readRailwayConfig(dir string) railwayConfig {
	if text, err := readInContext(dir, "railway.json"); err == nil {
		var doc struct {
			Build  struct{ BuildCommand, DockerfilePath string } `json:"build"`
			Deploy struct{ StartCommand string }                 `json:"deploy"`
		}
		if json.Unmarshal([]byte(text), &doc) == nil {
			return railwayConfig{startCommand: doc.Deploy.StartCommand, buildCommand: doc.Build.BuildCommand, dockerfilePath: doc.Build.DockerfilePath}
		}
	}
	if text, err := readInContext(dir, "railway.toml"); err == nil {
		return railwayConfig{
			startCommand:   tomlString(text, "deploy", "startCommand"),
			buildCommand:   tomlString(text, "build", "buildCommand"),
			dockerfilePath: tomlString(text, "build", "dockerfilePath"),
		}
	}
	return railwayConfig{}
}

// tomlString reads a quoted string key from a TOML table; railway.toml is
// flat enough that this is all it needs.
func tomlString(text, table, key string) string {
	inTable := false
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			inTable = strings.Trim(line, "[] ") == table
			continue
		}
		if !inTable {
			continue
		}
		if k, v, ok := strings.Cut(line, "="); ok && strings.TrimSpace(k) == key {
			return tomlUnquote(strings.TrimSpace(v))
		}
	}
	return ""
}

func tomlUnquote(v string) string {
	if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
		return v[1 : len(v)-1]
	}
	return v
}

// readInContext reads a file by path relative to dir, refusing paths that
// escape it.
func readInContext(dir, rel string) (string, error) {
	full := filepath.Join(dir, filepath.Clean("/"+rel))
	data, err := os.ReadFile(full)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
