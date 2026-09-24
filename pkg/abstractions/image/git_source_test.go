package image

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGitImageIDKeysOnCommitNotTokenOrEnv(t *testing.T) {
	base := func() *BuildOpts {
		return &BuildOpts{
			ClipVersion: 2,
			GitSource: &types.GitSource{
				RepoURL: "https://github.com/acme/app",
				Ref:     "main",
				Commit:  "0123456789abcdef0123456789abcdef01234567",
				Token:   "secret-a",
			},
			EnvVars: []string{"PORT=8000"},
		}
	}

	id, err := getImageID(base())
	require.NoError(t, err)

	other := base()
	other.GitSource.Token = "secret-b"
	other.EnvVars = []string{"PORT=9000"}
	sameID, err := getImageID(other)
	require.NoError(t, err)
	assert.Equal(t, id, sameID, "token and runtime env must not change the image")

	moved := base()
	moved.GitSource.Commit = "89abcdef0123456789abcdef0123456789abcdef"
	movedID, err := getImageID(moved)
	require.NoError(t, err)
	assert.NotEqual(t, id, movedID, "a new commit is a new image")

	cmd := base()
	cmd.GitSource.StartCommand = "gunicorn app:app"
	cmdID, err := getImageID(cmd)
	require.NoError(t, err)
	assert.NotEqual(t, id, cmdID, "the start command is baked into the image")
}

func TestGitSourceFromProtoNormalizes(t *testing.T) {
	assert.Nil(t, gitSourceFromProto(nil))
	assert.Nil(t, gitSourceFromProto(&pb.GitBuildSource{}))

	src := gitSourceFromProto(&pb.GitBuildSource{
		RepoUrl:    " https://github.com/acme/app ",
		Ref:        " main ",
		WorkingDir: "/services/api/",
	})
	require.NotNil(t, src)
	assert.Equal(t, "https://github.com/acme/app", src.RepoURL)
	assert.Equal(t, "main", src.Ref)
	assert.Equal(t, "services/api", src.WorkingDir)
}

func TestGitAuthURLEmbedsTokenAsAccessTokenUser(t *testing.T) {
	src := &types.GitSource{RepoURL: "https://github.com/acme/app"}
	assert.Equal(t, "https://github.com/acme/app", gitAuthURL(src))

	src.Token = "ghs_abc"
	assert.Equal(t, "https://x-access-token:ghs_abc@github.com/acme/app", gitAuthURL(src))
}

func TestResolveGitCommitRejectsNonHTTPS(t *testing.T) {
	err := resolveGitCommit(t.Context(), &types.GitSource{RepoURL: "git@github.com:acme/app.git"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "https")
}

func TestIsFullCommit(t *testing.T) {
	assert.True(t, isFullCommit("0123456789abcdef0123456789abcdef01234567"))
	assert.False(t, isFullCommit("0123456"))
	assert.False(t, isFullCommit("0123456789ABCDEF0123456789abcdef01234567"))
}
