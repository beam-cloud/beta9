package worker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestAptBuildVolumesSkipsImagesWithoutApt(t *testing.T) {
	rootfs := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(rootfs, "etc"), 0o755))
	volumes := aptBuildVolumes(types.BuildAptConfig{TimeoutS: 20, Retries: 3, Mirror: "http://mirror/ubuntu"}, rootfs, t.TempDir())
	require.Empty(t, volumes)
}

func TestAptBuildVolumesMountsConfAndRewrittenSources(t *testing.T) {
	rootfs := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(rootfs, "etc", "apt", "sources.list.d"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(rootfs, "etc", "apt", "sources.list"), []byte(
		"deb http://archive.ubuntu.com/ubuntu/ jammy main restricted\n"+
			"deb http://security.ubuntu.com/ubuntu/ jammy-security main restricted\n"+
			"deb https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu jammy main\n"), 0o644))

	tmpdir := t.TempDir()
	volumes := aptBuildVolumes(types.BuildAptConfig{TimeoutS: 20, Retries: 3, Proxy: "http://apt-cache:3142", Mirror: "http://mirrors.edge.kernel.org/ubuntu/"}, rootfs, tmpdir)
	require.Len(t, volumes, 2)

	conf, target := splitVolume(t, volumes[0])
	require.Equal(t, aptConfMountPath, target)
	body, err := os.ReadFile(conf)
	require.NoError(t, err)
	require.Equal(t, "Acquire::http::Timeout \"20\";\nAcquire::https::Timeout \"20\";\nAcquire::Retries \"3\";\nAcquire::http::Proxy \"http://apt-cache:3142\";\n", string(body))

	sources, target := splitVolume(t, volumes[1])
	require.Equal(t, aptSourcesMountPath, target)
	body, err = os.ReadFile(sources)
	require.NoError(t, err)
	require.Equal(t, "deb http://mirrors.edge.kernel.org/ubuntu/ jammy main restricted\n"+
		"deb http://mirrors.edge.kernel.org/ubuntu/ jammy-security main restricted\n"+
		"deb https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu jammy main\n", string(body))

	// The image's own file is untouched: the rewrite is a build-time mount.
	original, err := os.ReadFile(filepath.Join(rootfs, "etc", "apt", "sources.list"))
	require.NoError(t, err)
	require.Contains(t, string(original), "archive.ubuntu.com")
}

func TestAptBuildVolumesLeavesNonUbuntuSourcesAlone(t *testing.T) {
	rootfs := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(rootfs, "etc", "apt"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(rootfs, "etc", "apt", "sources.list"), []byte("deb http://deb.debian.org/debian bookworm main\n"), 0o644))
	volumes := aptBuildVolumes(types.BuildAptConfig{Mirror: "http://mirror/ubuntu"}, rootfs, t.TempDir())
	require.Empty(t, volumes, "no timeouts configured and nothing to rewrite")
}

func TestAptBuildVolumesRefusesSymlinkedAptPaths(t *testing.T) {
	// The rootfs is the user's base image; a symlink there must not lead the
	// worker to read its own files.
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "sources.list"), []byte("deb http://archive.ubuntu.com/ubuntu jammy main\n"), 0o644))

	rootfs := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(rootfs, "etc", "apt"), 0o755))
	require.NoError(t, os.Symlink(filepath.Join(outside, "sources.list"), filepath.Join(rootfs, "etc", "apt", "sources.list")))
	volumes := aptBuildVolumes(types.BuildAptConfig{Mirror: "http://mirror/ubuntu"}, rootfs, t.TempDir())
	require.Empty(t, volumes)

	rootfs = t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(rootfs, "etc"), 0o755))
	require.NoError(t, os.Symlink(outside, filepath.Join(rootfs, "etc", "apt")))
	volumes = aptBuildVolumes(types.BuildAptConfig{TimeoutS: 20, Mirror: "http://mirror/ubuntu"}, rootfs, t.TempDir())
	require.Empty(t, volumes, "a symlinked /etc/apt counts as no apt")
}

func TestRunStepsTouchAptConfig(t *testing.T) {
	plan, err := parseDockerfilePlan("FROM ubuntu\nENV A=/etc/apt\nRUN apt-get update && apt-get install -y curl\nCOPY sources.list /opt/\n", nil)
	require.NoError(t, err)
	require.False(t, runStepsTouchAptConfig(plan.steps), "using apt is not configuring it")

	for _, text := range []string{
		"FROM ubuntu\nRUN sed -i 's|archive.ubuntu.com|mirror|' /etc/apt/sources.list\n",
		"FROM ubuntu\nRUN add-apt-repository ppa:deadsnakes/ppa\n",
		"FROM ubuntu\nRUN [\"tee\", \"/etc/apt/sources.list.d/ubuntu.sources\"]\n",
		"FROM ubuntu\nCOPY sources.list /etc/apt/sources.list\n",
	} {
		plan, err := parseDockerfilePlan(text, nil)
		require.NoError(t, err)
		require.True(t, runStepsTouchAptConfig(plan.steps), text)
	}
}

func splitVolume(t *testing.T, spec string) (source, target string) {
	t.Helper()
	parts := strings.Split(spec, ":")
	require.Len(t, parts, 3)
	require.Equal(t, "ro", parts[2])
	return parts[0], parts[1]
}
