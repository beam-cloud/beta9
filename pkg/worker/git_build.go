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

// prepareGitBuild clones the source and settles on a Dockerfile. It returns
// the directory buildah gets as its context (the checkout, or working_dir
// within it) and the Dockerfile text. The checkout lives on the disk-backed
// spool dir, not the RAM-backed build dir.
func (c *ImageClient) prepareGitBuild(ctx context.Context, out *slog.Logger, src *types.GitSource) (contextDir, dockerfile string, cleanup func(), err error) {
	root, err := os.MkdirTemp(c.layerSpoolDir(), "git-src-")
	if err != nil {
		return "", "", nil, fmt.Errorf("create checkout dir: %w", err)
	}
	cleanup = func() { os.RemoveAll(root) }
	defer func() {
		if err != nil {
			cleanup()
		}
	}()

	out.Info(fmt.Sprintf("Cloning %s at %.12s\n", src.RepoURL, src.Commit))
	if err = gitCheckout(ctx, root, src); err != nil {
		return "", "", nil, err
	}

	contextDir = root
	if src.WorkingDir != "" {
		contextDir = filepath.Join(root, filepath.Clean("/"+src.WorkingDir))
		if info, statErr := os.Stat(contextDir); statErr != nil || !info.IsDir() {
			return "", "", nil, fmt.Errorf("working directory %q not found in repository", src.WorkingDir)
		}
	}

	dockerfile, err = resolveGitDockerfile(ctx, out, contextDir, src)
	if err != nil {
		return "", "", nil, err
	}
	return contextDir, dockerfile, cleanup, nil
}

// gitCheckout fetches exactly the commit the gateway resolved. Fetching by hash
// needs the server to allow it (GitHub and GitLab do); when it refuses, the ref
// is cloned shallowly instead.
func gitCheckout(ctx context.Context, dir string, src *types.GitSource) error {
	ctx, cancel := context.WithTimeout(ctx, gitCloneTimeout)
	defer cancel()

	env, cleanupEnv, err := gitAuthEnv(src.Token)
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
// the checkout's config (which a COPY . would ship) or in a process argument.
func gitAuthEnv(token string) ([]string, func(), error) {
	env := append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_LFS_SKIP_SMUDGE=1")
	if token == "" {
		return env, func() {}, nil
	}
	askpass, err := os.CreateTemp("", "git-askpass-")
	if err != nil {
		return nil, nil, fmt.Errorf("create askpass helper: %w", err)
	}
	_, err = askpass.WriteString("#!/bin/sh\nprintf '%s' \"$GIT_SOURCE_TOKEN\"\n")
	askpass.Close()
	if err == nil {
		err = os.Chmod(askpass.Name(), 0o700)
	}
	if err != nil {
		os.Remove(askpass.Name())
		return nil, nil, fmt.Errorf("write askpass helper: %w", err)
	}
	env = append(env, "GIT_ASKPASS="+askpass.Name(), "GIT_SOURCE_TOKEN="+token)
	return env, func() { os.Remove(askpass.Name()) }, nil
}

// gitRemoteURL names the token user GitHub and GitLab expect; the password
// itself comes from GIT_ASKPASS.
func gitRemoteURL(src *types.GitSource) string {
	u, err := url.Parse(src.RepoURL)
	if src.Token == "" || err != nil {
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

// resolveGitDockerfile returns the Dockerfile to build, in the order Railway
// resolves it: the path requested or set in railway.json, the repository's
// own Dockerfile, else one nixpacks generates into the context.
func resolveGitDockerfile(ctx context.Context, out *slog.Logger, contextDir string, src *types.GitSource) (string, error) {
	cfg := readRailwayConfig(contextDir)

	if path := firstNonEmpty(src.DockerfilePath, cfg.dockerfilePath); path != "" {
		text, err := readRepoFile(contextDir, path)
		if err != nil {
			return "", fmt.Errorf("dockerfile %q not found in repository", path)
		}
		out.Info(fmt.Sprintf("Building %s\n", path))
		return text, nil
	}
	if text, err := readRepoFile(contextDir, "Dockerfile"); err == nil {
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
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("repository has no Dockerfile and nixpacks could not detect how to build it: %s", strings.TrimSpace(output.String()))
	}
	text, err := readRepoFile(contextDir, nixpacksDockerfile)
	if err != nil {
		return "", fmt.Errorf("nixpacks produced no Dockerfile: %s", strings.TrimSpace(output.String()))
	}
	out.Info("Generated a Dockerfile with nixpacks\n")
	// The plan box nixpacks prints tells the user what it detected; the
	// progress noise around it does not.
	if start, end := strings.Index(output.String(), "╔"), strings.LastIndex(output.String(), "╝"); start >= 0 && end > start {
		out.Info(output.String()[start:end+len("╝")] + "\n")
	}
	return text, nil
}

var gunicornBindFlag = regexp.MustCompile(`(^|\s)(-b|--bind)(\s|=)`)

// gunicornBound makes gunicorn listen where the platform routes traffic; its
// default is 127.0.0.1:8000, which nothing outside the container reaches.
func gunicornBound(command string) string {
	if strings.Contains(command, "gunicorn") && !gunicornBindFlag.MatchString(command) {
		return command + " --bind 0.0.0.0:$PORT"
	}
	return command
}

// railwayConfig is the part of railway.json / railway.toml that decides how a
// repository builds and starts, so repositories written for Railway deploy
// unchanged.
type railwayConfig struct {
	startCommand   string
	buildCommand   string
	dockerfilePath string
}

func readRailwayConfig(dir string) railwayConfig {
	if text, err := readRepoFile(dir, "railway.json"); err == nil {
		var doc struct {
			Build  struct{ BuildCommand, DockerfilePath string } `json:"build"`
			Deploy struct{ StartCommand string }                 `json:"deploy"`
		}
		if json.Unmarshal([]byte(text), &doc) == nil {
			return railwayConfig{startCommand: doc.Deploy.StartCommand, buildCommand: doc.Build.BuildCommand, dockerfilePath: doc.Build.DockerfilePath}
		}
	}
	if text, err := readRepoFile(dir, "railway.toml"); err == nil {
		return railwayConfig{
			startCommand:   tomlString(text, "deploy", "startCommand"),
			buildCommand:   tomlString(text, "build", "buildCommand"),
			dockerfilePath: tomlString(text, "build", "dockerfilePath"),
		}
	}
	return railwayConfig{}
}

// tomlString reads a quoted string key from a TOML table; railway.toml is flat
// enough that this is all it needs.
func tomlString(text, table, key string) string {
	inTable := false
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			inTable = strings.Trim(line, "[] ") == table
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !inTable || !ok || strings.TrimSpace(k) != key {
			continue
		}
		v = strings.TrimSpace(v)
		if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
			v = v[1 : len(v)-1]
		}
		return v
	}
	return ""
}

// readRepoFile reads a file by path relative to dir, refusing paths that
// escape it.
func readRepoFile(dir, rel string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, filepath.Clean("/"+rel)))
	return string(data), err
}
