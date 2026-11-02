package image

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type snapshotOwnerRepository struct{ repository.BackendRepository }

func (snapshotOwnerRepository) GetStubByExternalId(_ context.Context, stubID string, filters ...types.QueryFilter) (*types.StubWithRelated, error) {
	if stubID == "workspace-a-stub" && len(filters) == 1 && filters[0].Value == "workspace-a" {
		return &types.StubWithRelated{}, nil
	}
	return nil, nil
}

func TestVerifyImageBuildRejectsAnotherWorkspacesSnapshot(t *testing.T) {
	service := &ContainerImageService{backendRepo: snapshotOwnerRepository{}}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "workspace-b"}})
	imageID := types.SnapshotImageID("workspace-a-stub", time.Unix(1700000000, 0))
	_, err := service.VerifyImageBuild(ctx, &pb.VerifyImageBuildRequest{ImageId: &imageID})
	require.ErrorContains(t, err, "not found")
}

func scopeTestOpts() (generic, dockerfile, git *BuildOpts) {
	generic = &BuildOpts{ClipVersion: 1, BaseImageRegistry: "docker.io", BaseImageName: "library/python", BaseImageTag: "3.12", PythonVersion: "python3.12", PythonPackages: []string{"requests"}, Commands: []string{"echo hi"}, EnvVars: []string{"A=1"}}
	dockerfile = &BuildOpts{ClipVersion: 2, Dockerfile: "FROM python:3.12\nRUN pip install requests\n", BuildCtxObject: "ctx-1", EnvVars: []string{"A=1"}}
	git = &BuildOpts{ClipVersion: 2, GitSource: &types.GitSource{RepoURL: "https://github.com/example/app", Commit: "0123456789abcdef0123456789abcdef01234567", WorkingDir: "svc", Dockerfile: "FROM scratch\n"}}
	return generic, dockerfile, git
}

// Images without private inputs keep the IDs and prefix keys they had before
// scoping, so existing images stay valid.
func TestUnscopedImageIDsAreUnchanged(t *testing.T) {
	generic, dockerfile, git := scopeTestOpts()
	for want, opts := range map[string]*BuildOpts{"75e3505294109883": generic, "0028649f8c5689f7": dockerfile, "22567e9d9df1323f": git} {
		id, err := getImageID(opts)
		require.NoError(t, err)
		require.Equal(t, want, id)
	}
	require.Equal(t,
		"image:built_dockerfile:740a54e2dced08baaf9c5c52662ce4a3672d696f20b70d72aee8b63341ef73f9",
		builtDockerfileKey(dockerfile, []string{"FROM python:3.12", "RUN pip install requests"}),
	)
}

func TestImageScopeCoversPrivateInputs(t *testing.T) {
	workspaceContext := func(signingKey string) context.Context {
		return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Workspace: &types.Workspace{SigningKey: &signingKey}})
	}
	workspaceA, workspaceB := workspaceContext("signing-key-a"), workspaceContext("signing-key-b")

	_, public, _ := scopeTestOpts()
	scope, err := imageScope(context.Background(), public)
	require.NoError(t, err)
	require.Empty(t, scope)

	for name, private := range map[string]*BuildOpts{
		"build secrets":        {BuildSecrets: []string{"PIP_TOKEN=secret"}},
		"registry credentials": {ExistingImageCreds: map[string]string{"USERNAME": "user", "PASSWORD": "secret"}},
		"git token":            {GitSource: &types.GitSource{RepoURL: "https://github.com/example/private", Token: "secret"}},
	} {
		scopeA, err := imageScope(workspaceA, private)
		require.NoError(t, err, name)
		require.NotEmpty(t, scopeA, name)
		again, err := imageScope(workspaceA, private)
		require.NoError(t, err, name)
		require.Equal(t, scopeA, again, name)
		scopeB, err := imageScope(workspaceB, private)
		require.NoError(t, err, name)
		require.NotEqual(t, scopeA, scopeB, name)

		_, err = imageScope(context.Background(), private)
		require.Error(t, err, name)
	}
}

func TestScopedImageIDsDifferByWorkspace(t *testing.T) {
	generic, dockerfile, git := scopeTestOpts()
	for _, opts := range []*BuildOpts{generic, dockerfile, git} {
		unscoped, err := getImageID(opts)
		require.NoError(t, err)
		scopedA, scopedB := *opts, *opts
		scopedA.Scope, scopedB.Scope = "scope-a", "scope-b"
		idA, err := getImageID(&scopedA)
		require.NoError(t, err)
		idB, err := getImageID(&scopedB)
		require.NoError(t, err)
		require.NotEqual(t, unscoped, idA)
		require.NotEqual(t, idA, idB)
		require.Len(t, idA, 16)
	}

	lines := []string{"FROM python:3.12", "RUN pip install requests"}
	scoped := *dockerfile
	scoped.Scope = "scope-a"
	require.NotEqual(t, builtDockerfileKey(dockerfile, lines), builtDockerfileKey(&scoped, lines))

	// A v1 build starts from its base image, which is scoped with it.
	base := &Build{opts: generic, config: types.AppConfig{ImageService: types.ImageServiceConfig{ClipVersion: 1}}}
	unscopedBase, err := base.getContainerImageID()
	require.NoError(t, err)
	scopedGeneric := *generic
	scopedGeneric.Scope = "scope-a"
	base.opts = &scopedGeneric
	scopedBase, err := base.getContainerImageID()
	require.NoError(t, err)
	require.NotEqual(t, unscopedBase, scopedBase)
}
