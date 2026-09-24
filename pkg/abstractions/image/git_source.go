package image

import (
	"context"
	"fmt"
	"net/url"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/mitchellh/hashstructure/v2"
)

const gitResolveTimeout = 30 * time.Second

var fullCommit = regexp.MustCompile(`^[0-9a-f]{40}$`)

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
// so the image id (and the build cache) keys on content, not on a moving
// branch. A repository or ref that does not exist fails here, before a build
// worker is involved.
func resolveGitCommit(ctx context.Context, src *types.GitSource) error {
	if !strings.HasPrefix(src.RepoURL, "https://") {
		return fmt.Errorf("git repository must be an https URL")
	}
	ctx, cancel := context.WithTimeout(ctx, gitResolveTimeout)
	defer cancel()

	lsRemote := func(patterns ...string) (string, bool) {
		cmd := exec.CommandContext(ctx, "git", append([]string{"ls-remote", "--exit-code", gitAuthURL(src)}, patterns...)...)
		// A bare environment: the token in the URL is the only credential git may use.
		cmd.Env = []string{"GIT_TERMINAL_PROMPT=0", "PATH=/usr/bin:/bin:/usr/local/bin"}
		out, err := cmd.Output()
		if err != nil {
			return "", false
		}
		sha, _, _ := strings.Cut(strings.TrimSpace(string(out)), "\t")
		return sha, fullCommit.MatchString(sha)
	}

	if src.Ref == "" {
		sha, ok := lsRemote("HEAD")
		if !ok {
			return fmt.Errorf("repository %s not found or not accessible", src.RepoURL)
		}
		src.Commit = sha
		return nil
	}
	if sha, ok := lsRemote(src.Ref, "refs/heads/"+src.Ref, "refs/tags/"+src.Ref); ok {
		src.Commit = sha
		return nil
	}
	if _, ok := lsRemote("HEAD"); !ok {
		return fmt.Errorf("repository %s not found or not accessible", src.RepoURL)
	}
	if fullCommit.MatchString(src.Ref) {
		src.Commit = src.Ref // the worker fetches it by hash
		return nil
	}
	return fmt.Errorf("ref %q not found in %s", src.Ref, src.RepoURL)
}

// gitAuthURL embeds the token the way GitHub and GitLab accept it over https.
func gitAuthURL(src *types.GitSource) string {
	u, err := url.Parse(src.RepoURL)
	if src.Token == "" || err != nil {
		return src.RepoURL
	}
	u.User = url.UserPassword("x-access-token", src.Token)
	return u.String()
}

// gitImageID is what makes two git builds the same image: the commit and how
// it is built. The token and runtime env play no part.
func gitImageID(opts *BuildOpts) (string, error) {
	hash, err := hashstructure.Hash(struct {
		ClipVersion    uint32
		RepoURL        string
		Commit         string
		WorkingDir     string
		DockerfilePath string
		StartCommand   string
		BuildCommand   string
	}{
		opts.ClipVersion,
		opts.GitSource.RepoURL,
		opts.GitSource.Commit,
		opts.GitSource.WorkingDir,
		opts.GitSource.DockerfilePath,
		opts.GitSource.StartCommand,
		opts.GitSource.BuildCommand,
	}, hashstructure.FormatV2, nil)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%016x", hash), nil
}
