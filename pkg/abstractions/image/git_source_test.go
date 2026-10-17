package image

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGitImageIDKeysOnCommitNotTokenOrEnv(t *testing.T) {
	opts := func(commit, token, start string, env ...string) *BuildOpts {
		return &BuildOpts{
			ClipVersion: 2,
			EnvVars:     env,
			GitSource:   &types.GitSource{RepoURL: "https://github.com/acme/app", Commit: commit, Token: token, StartCommand: start},
		}
	}
	id := func(o *BuildOpts) string {
		id, err := getImageID(o)
		require.NoError(t, err)
		return id
	}
	base := id(opts("0123456789abcdef0123456789abcdef01234567", "secret-a", "", "PORT=8000"))
	assert.Equal(t, base, id(opts("0123456789abcdef0123456789abcdef01234567", "secret-b", "", "PORT=9000")), "token and runtime env must not change the image")
	assert.NotEqual(t, base, id(opts("89abcdef0123456789abcdef0123456789abcdef", "secret-a", "")), "a new commit is a new image")
	assert.NotEqual(t, base, id(opts("0123456789abcdef0123456789abcdef01234567", "secret-a", "gunicorn app:app")), "the start command is baked in")
}

func TestGitSourceRequestHandling(t *testing.T) {
	assert.Nil(t, gitSourceFromProto(&pb.GitBuildSource{}), "no repo, no source")

	src := gitSourceFromProto(&pb.GitBuildSource{RepoUrl: " https://github.com/acme/app ", Ref: " main ", WorkingDir: "/services/api/", Token: "ghs_abc"})
	assert.Equal(t, "https://github.com/acme/app", src.RepoURL)
	assert.Equal(t, "services/api", src.WorkingDir)
	assert.Equal(t, "https://x-access-token:ghs_abc@github.com/acme/app", gitAuthURL(src))

	err := resolveGitCommit(t.Context(), &types.GitSource{RepoURL: "git@github.com:acme/app.git"})
	assert.ErrorContains(t, err, "https")
}

func TestPickLsRemoteRefMatchesTheWholeRefName(t *testing.T) {
	const (
		copilotMain = "fd1b4f2a6d532779f516b8630abdbeee5e46af02"
		main        = "7a72a0b242a1fb2e01bb2b96e0607ecf1c5f43ad"
		tag         = "0123456789abcdef0123456789abcdef01234567"
	)
	// What `git ls-remote <url> main refs/heads/main refs/tags/main` prints for
	// a repository that also has a copilot/main branch.
	out := copilotMain + "\trefs/heads/copilot/main\n" + main + "\trefs/heads/main\n"

	sha, ok := pickLsRemoteRef(out, "main", "refs/heads/main", "refs/tags/main")
	assert.True(t, ok)
	assert.Equal(t, main, sha, "ref main is refs/heads/main, not refs/heads/copilot/main")

	sha, ok = pickLsRemoteRef(out, "copilot/main", "refs/heads/copilot/main", "refs/tags/copilot/main")
	assert.True(t, ok)
	assert.Equal(t, copilotMain, sha)

	sha, ok = pickLsRemoteRef(tag+"\trefs/tags/v1.2\n", "v1.2", "refs/heads/v1.2", "refs/tags/v1.2")
	assert.True(t, ok)
	assert.Equal(t, tag, sha)

	sha, ok = pickLsRemoteRef(main+"\tHEAD\n", "HEAD")
	assert.True(t, ok)
	assert.Equal(t, main, sha)

	_, ok = pickLsRemoteRef(copilotMain+"\trefs/heads/copilot/main\n", "main", "refs/heads/main", "refs/tags/main")
	assert.False(t, ok, "a branch that only ends in main is not main")
}
