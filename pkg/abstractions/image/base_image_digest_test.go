package image

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPinDockerfileFromDigests(t *testing.T) {
	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	resolved := make([]string, 0, 2)
	dockerfile, base := pinDockerfileFromDigests(
		"FROM --platform=linux/amd64 vllm/vllm-openai:latest AS runtime\n"+
			"FROM runtime AS final\n"+
			"CMD [\"python\"]\n",
		func(imageRef string) string {
			resolved = append(resolved, imageRef)
			return digest
		},
	)

	require.Equal(t, []string{"vllm/vllm-openai:latest"}, resolved)
	require.Equal(t,
		"FROM --platform=linux/amd64 vllm/vllm-openai@"+digest+" AS runtime\n"+
			"FROM runtime AS final\n"+
			"CMD [\"python\"]\n",
		dockerfile,
	)
	require.NotNil(t, base)
	require.Equal(t, "docker.io", base.Registry)
	require.Equal(t, "vllm/vllm-openai", base.Repo)
	require.Equal(t, digest, base.Digest)
}

func TestPinDockerfileFromDigestsPreservesPinnedAndDynamicBases(t *testing.T) {
	const digest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	input := "FROM alpine@" + digest + " AS pinned\n" +
		"FROM ${BASE_IMAGE} AS dynamic\n" +
		"FROM scratch AS empty\n"

	dockerfile, base := pinDockerfileFromDigests(input, func(string) string {
		t.Fatal("resolver must not be called")
		return ""
	})

	require.Equal(t, input, dockerfile)
	require.NotNil(t, base)
	require.Equal(t, digest, base.Digest)
}

func TestPinImageRefToDigestPreservesRegistryPort(t *testing.T) {
	const digest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	require.Equal(t,
		"registry.example.com:5000/team/image@"+digest,
		pinImageRefToDigest("registry.example.com:5000/team/image:latest", digest),
	)
}
