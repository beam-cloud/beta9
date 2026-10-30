package image

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestDockerfileCredentialSource(t *testing.T) {
	for _, test := range []struct{ dockerfile, source string }{
		{"FROM registry.example/private:tag AS base\nFROM base", "registry.example/private:tag"},
		{"FROM --platform=linux/amd64 registry.example/private@sha256:abc", "registry.example/private@sha256:abc"},
		{"FROM scratch AS empty\nFROM empty AS other\nFROM registry.example/private", "registry.example/private"},
		{"ARG BASE=private.example/image\nFROM ${BASE}\nFROM public.example/image", ""},
		{"FROM scratch", ""},
	} {
		source, err := dockerfileCredentialSource(test.dockerfile)
		if test.source == "" {
			require.ErrorContains(t, err, "literal Dockerfile FROM")
		} else {
			require.NoError(t, err)
			require.Equal(t, test.source, source)
		}
	}
}

func TestPrivateDockerfileCredentialsPrecedePinningAndStayRegistryScoped(t *testing.T) {
	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	inspected := make(map[string]string)
	service := &ContainerImageService{builder: &Builder{skopeoClient: &recordingSkopeoClient{
		digest: digest,
		onInspect: func(ref, creds string) error {
			inspected[ref] = creds
			if strings.HasPrefix(ref, "private.example/") && creds != "user:secret" {
				return errors.New("unauthorized")
			}
			return nil
		},
	}}, baseImageDigests: newBaseImageDigestCache(nil)}
	input := "FROM private.example/base:tag AS base\nFROM public.example/other:tag\nFROM base\n"
	opts := &BuildOpts{Dockerfile: input, IgnorePython: true, ExistingImageCreds: map[string]string{"USERNAME": "user", "PASSWORD": "secret"}}
	require.NoError(t, service.prepareBuildOptionsForImageID(context.Background(), &pb.VerifyImageBuildRequest{Dockerfile: input}, opts))
	require.Equal(t, "user:secret", inspected["private.example/base:tag"])
	require.Empty(t, inspected["public.example/other:tag"])
	require.Equal(t, "private.example", opts.BaseImageRegistry)
	require.Equal(t, "base", opts.BaseImageName)
	require.Equal(t, digest, opts.BaseImageDigest)
	require.Equal(t, "user:secret", opts.BaseImageCreds)
	require.Contains(t, opts.Dockerfile, "FROM private.example/base@"+digest)
	require.Empty(t, service.baseImageDigests.get(context.Background(), "private.example/base:tag"), "authenticated digests must not enter the public cache")
	require.Equal(t, digest, service.baseImageDigests.get(context.Background(), "public.example/other:tag"))

	service.baseImageDigests.set(context.Background(), "private.example/base:tag", digest)
	opts = &BuildOpts{Dockerfile: input, IgnorePython: true, ExistingImageCreds: map[string]string{"USERNAME": "user", "PASSWORD": "wrong"}}
	require.NoError(t, service.prepareBuildOptionsForImageID(context.Background(), &pb.VerifyImageBuildRequest{Dockerfile: input}, opts))
	require.Equal(t, "private.example", opts.BaseImageRegistry, "failed auth cannot select a later public registry")
	require.Empty(t, opts.BaseImageDigest, "explicit auth must not accept a cached public digest")
	require.Contains(t, opts.Dockerfile, "FROM private.example/base:tag")
}

type privateImageRepository struct {
	repository.BackendRepository
	imageID string
}

func (r *privateImageRepository) GetSecretsByNameDecrypted(context.Context, *types.Workspace, []string) ([]types.Secret, error) {
	return []types.Secret{{Name: "USERNAME", Value: "user"}, {Name: "PASSWORD", Value: "secret"}}, nil
}

func (r *privateImageRepository) GetImageClipVersion(_ context.Context, imageID string) (uint32, error) {
	if imageID == r.imageID {
		return uint32(types.ClipVersion2), nil
	}
	return 0, sql.ErrNoRows
}

type imageBuildStream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*pb.BuildImageResponse
}

func (s *imageBuildStream) Context() context.Context { return s.ctx }
func (s *imageBuildStream) Send(response *pb.BuildImageResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

func TestBuildPrivateDockerfileWarmIdentityUsesWorkspaceCredentials(t *testing.T) {
	const digest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	config := types.AppConfig{ImageService: types.ImageServiceConfig{
		ClipVersion: uint32(types.ClipVersion2), RegistryStore: reg.S3ImageRegistryStore,
		Runner: types.RunnerConfig{Tags: map[string]string{"python3.11": "3.11"}},
	}}
	repo := &privateImageRepository{}
	inspections := 0
	service := &ContainerImageService{
		config: config, backendRepo: repo, baseImageDigests: newBaseImageDigestCache(nil),
		builder: &Builder{config: config, skopeoClient: &recordingSkopeoClient{
			digest: digest,
			onInspect: func(_ string, creds string) error {
				inspections++
				require.Equal(t, "user:secret", creds, "pinning must use workspace values before computing identity")
				return nil
			},
		}},
	}
	dockerfile := "FROM private.example/image:tag\nRUN true\n"
	opts := &BuildOpts{Dockerfile: "FROM private.example/image@" + digest + "\nRUN true\n", ClipVersion: uint32(types.ClipVersion2)}
	var err error
	repo.imageID, err = getImageID(opts)
	require.NoError(t, err)
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Workspace: &types.Workspace{}})
	request := &pb.BuildImageRequest{PythonVersion: "python3.11", Dockerfile: dockerfile, IgnorePython: true,
		ExistingImageCreds: map[string]string{"USERNAME": "untrusted-client-value", "PASSWORD": "untrusted-client-value"}}
	for range 2 {
		stream := &imageBuildStream{ctx: ctx}
		require.NoError(t, service.BuildImage(request, stream))
		require.NotEmpty(t, stream.responses)
		last := stream.responses[len(stream.responses)-1]
		require.True(t, last.Success)
		require.Equal(t, repo.imageID, last.ImageId)
	}
	require.Equal(t, 2, inspections, "each authenticated warm lookup must resolve without public cache reuse")
	require.Empty(t, service.baseImageDigests.get(context.Background(), "private.example/image:tag"))
}
