package image

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func prefixTestBuilder(t *testing.T, existing map[string]bool) *Builder {
	server := miniredis.RunT(t)
	rdb := &common.RedisClient{UniversalClient: redis.NewClient(&redis.Options{Addr: server.Addr()})}
	return &Builder{
		config: types.AppConfig{ImageService: types.ImageServiceConfig{
			ClipVersion:         uint32(types.ClipVersion2),
			BuildRegistry:       "registry.example.com",
			BuildRepositoryName: "beta9-build",
		}},
		rdb: rdb,
		existsOverride: func(_ context.Context, id string) (bool, error) {
			return existing[id], nil
		},
	}
}

func renderedOpts(dockerfile string) *BuildOpts {
	return &BuildOpts{
		ClipVersion:       uint32(types.ClipVersion2),
		BaseImageName:     "beta9-runner",
		BaseImageRegistry: "registry.localhost:5000",
		BaseImageTag:      "py312-latest",
		Dockerfile:        dockerfile,
		BuildCtxObject:    "ctx-1",
	}
}

const baseDockerfile = "FROM registry.localhost:5000/beta9-runner:py312-latest\n" +
	"ENV APP_ENV=bench\n" +
	"ARG TOKEN\n" +
	"RUN apt-get install -y git curl\n" +
	"RUN uv-b9 pip install requests numpy\n"

func TestReuseBuiltPrefixExtendsTheLongestBuiltImage(t *testing.T) {
	ctx := context.Background()
	b := prefixTestBuilder(t, map[string]bool{"img-a": true, "img-b": true})
	b.recordBuiltDockerfile(ctx, renderedOpts(baseDockerfile), "img-a")
	b.recordBuiltDockerfile(ctx, renderedOpts(baseDockerfile+"RUN echo hi > /hi\n"), "img-b")

	dockerfile, source, ok := b.reuseBuiltPrefix(ctx, renderedOpts(baseDockerfile+"RUN echo hi > /hi\nRUN echo bye > /bye\n"))
	require.True(t, ok)
	require.Equal(t, "registry.example.com/beta9-build:img-b", source)
	require.Equal(t, "FROM registry.example.com/beta9-build:img-b\nARG TOKEN\nRUN echo bye > /bye\n", dockerfile)

	// A change in the middle only matches the shorter prefix.
	dockerfile, source, ok = b.reuseBuiltPrefix(ctx, renderedOpts(baseDockerfile+"RUN echo other\n"))
	require.True(t, ok)
	require.Equal(t, "registry.example.com/beta9-build:img-a", source)
	require.Equal(t, "FROM registry.example.com/beta9-build:img-a\nARG TOKEN\nRUN echo other\n", dockerfile)
}

func TestReuseBuiltPrefixSkipsMissingImagesAndOtherContexts(t *testing.T) {
	ctx := context.Background()
	b := prefixTestBuilder(t, map[string]bool{"img-a": false})
	b.recordBuiltDockerfile(ctx, renderedOpts(baseDockerfile), "img-a")
	_, _, ok := b.reuseBuiltPrefix(ctx, renderedOpts(baseDockerfile+"RUN echo hi\n"))
	require.False(t, ok, "an image gone from the registry is not a base")

	b = prefixTestBuilder(t, map[string]bool{"img-a": true})
	b.recordBuiltDockerfile(ctx, renderedOpts(baseDockerfile), "img-a")
	other := renderedOpts(baseDockerfile + "RUN echo hi\n")
	other.BuildCtxObject = "ctx-2"
	_, _, ok = b.reuseBuiltPrefix(ctx, other)
	require.False(t, ok, "a different build context may change what COPY produced")

	// The identical Dockerfile is not its own prefix, and custom Dockerfiles are left alone.
	_, _, ok = b.reuseBuiltPrefix(ctx, renderedOpts(baseDockerfile))
	require.False(t, ok)
	custom := renderedOpts(baseDockerfile + "RUN echo hi\n")
	custom.BaseImageName = ""
	_, _, ok = b.reuseBuiltPrefix(ctx, custom)
	require.False(t, ok)
}

func TestPrefixReuseLeavesCustomDockerfilesAlone(t *testing.T) {
	ctx := context.Background()
	b := prefixTestBuilder(t, map[string]bool{"img-a": true})
	b.recordBuiltDockerfile(ctx, renderedOpts(baseDockerfile), "img-a")

	// Base image pinning fills in the base image fields for custom
	// Dockerfiles too, so their shape is what tells them apart.
	for name, dockerfile := range map[string]string{
		"multi-stage":  baseDockerfile + "FROM scratch AS builder\nCOPY --from=builder /x /y\n",
		"continuation": baseDockerfile + "RUN echo hi && \\\n    echo bye\n",
		"comment":      baseDockerfile + "# a comment\nRUN echo hi\n",
		"indented":     baseDockerfile + "  RUN echo hi\n",
		"onbuild":      baseDockerfile + "ONBUILD RUN echo hi\n",
		"other-from":   "FROM python:3.12\n" + strings.SplitN(baseDockerfile, "\n", 2)[1] + "RUN echo hi\n",
	} {
		opts := renderedOpts(dockerfile)
		require.False(t, b.prefixReusable(opts), name)
		_, _, ok := b.reuseBuiltPrefix(ctx, opts)
		require.False(t, ok, name)
		b.recordBuiltDockerfile(ctx, opts, "img-custom")
	}
	// A rendered Dockerfile still qualifies.
	require.True(t, b.prefixReusable(renderedOpts(baseDockerfile+"RUN echo hi\n")))
	_, _, ok := b.reuseBuiltPrefix(ctx, renderedOpts(baseDockerfile+"RUN echo hi\n"))
	require.True(t, ok)
}
