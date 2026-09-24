package image

import (
	"context"
	"fmt"
	"net/url"
	"os/exec"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const gitResolveTimeout = 30 * time.Second

func gitSourceFromProto(in *pb.GitBuildSource) *types.GitSource {
	if in == nil || in.RepoUrl == "" {
		return nil
	}
	return &types.GitSource{
		RepoURL:        strings.TrimSpace(in.RepoUrl),
		Ref:            strings.TrimSpace(in.Ref),
		Token:          in.Token,
		WorkingDir:     strings.Trim(strings.TrimSpace(in.WorkingDir), "/"),
		DockerfilePath: strings.TrimSpace(in.DockerfilePath),
		StartCommand:   in.StartCommand,
		BuildCommand:   in.BuildCommand,
	}
}

// resolveGitCommit pins the source to the commit its ref points at right now,
// so the image id (and the build cache) keys on content, not on a moving branch.
func resolveGitCommit(ctx context.Context, src *types.GitSource) error {
	if _, err := url.ParseRequestURI(src.RepoURL); err != nil || !strings.HasPrefix(src.RepoURL, "https://") {
		return fmt.Errorf("git repository must be an https URL")
	}
	ctx, cancel := context.WithTimeout(ctx, gitResolveTimeout)
	defer cancel()

	lsRemote := func(patterns ...string) (string, error) {
		cmd := exec.CommandContext(ctx, "git", append([]string{"ls-remote", "--exit-code", gitAuthURL(src)}, patterns...)...)
		cmd.Env = []string{"GIT_TERMINAL_PROMPT=0", "PATH=/usr/bin:/bin:/usr/local/bin"}
		out, err := cmd.Output()
		if err != nil {
			return "", err
		}
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if sha, _, ok := strings.Cut(line, "\t"); ok && isFullCommit(sha) {
				return sha, nil
			}
		}
		return "", fmt.Errorf("no match")
	}

	if src.Ref == "" {
		sha, err := lsRemote("HEAD")
		if err != nil {
			return fmt.Errorf("repository %s not found or not accessible", src.RepoURL)
		}
		src.Commit = sha
		return nil
	}
	if sha, err := lsRemote(src.Ref, "refs/heads/"+src.Ref, "refs/tags/"+src.Ref); err == nil {
		src.Commit = sha
		return nil
	}
	if _, err := lsRemote("HEAD"); err != nil {
		return fmt.Errorf("repository %s not found or not accessible", src.RepoURL)
	}
	if isFullCommit(src.Ref) {
		src.Commit = src.Ref // a commit the worker fetches by hash
		return nil
	}
	return fmt.Errorf("ref %q not found in %s", src.Ref, src.RepoURL)
}

func isFullCommit(s string) bool {
	if len(s) != 40 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// gitAuthURL embeds the token the way GitHub and GitLab accept it over https.
func gitAuthURL(src *types.GitSource) string {
	if src.Token == "" {
		return src.RepoURL
	}
	u, err := url.Parse(src.RepoURL)
	if err != nil {
		return src.RepoURL
	}
	u.User = url.UserPassword("x-access-token", src.Token)
	return u.String()
}

// gitImageIDInput is what makes two git builds the same image: the commit and
// how it is built, never the token.
type gitImageIDInput struct {
	ClipVersion    uint32
	RepoURL        string
	Commit         string
	WorkingDir     string
	DockerfilePath string
	StartCommand   string
	BuildCommand   string
}

func (o *BuildOpts) gitImageIDInput() gitImageIDInput {
	return gitImageIDInput{
		ClipVersion:    o.ClipVersion,
		RepoURL:        o.GitSource.RepoURL,
		Commit:         o.GitSource.Commit,
		WorkingDir:     o.GitSource.WorkingDir,
		DockerfilePath: o.GitSource.DockerfilePath,
		StartCommand:   o.GitSource.StartCommand,
		BuildCommand:   o.GitSource.BuildCommand,
	}
}
