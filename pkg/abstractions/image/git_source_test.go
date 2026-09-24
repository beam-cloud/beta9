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
