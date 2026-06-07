package worker

import (
	"path/filepath"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestWithVolumeCacheEnvAddsPreloadAndMap(t *testing.T) {
	env := withVolumeCacheEnv([]string{"PATH=/usr/bin"}, `{"data":"/volumes/data"}`)

	require.Contains(t, env, "PATH=/usr/bin")
	require.Contains(t, env, "LD_PRELOAD=/usr/local/lib/volume_cache.so")
	require.Contains(t, env, `VOLUME_CACHE_MAP={"data":"/volumes/data"}`)
}

func TestWithLDPreloadAppendsWithoutDuplicating(t *testing.T) {
	env := withLDPreload([]string{"LD_PRELOAD=/lib/existing.so"}, volumeCacheLibraryPath)
	require.Equal(t, []string{"LD_PRELOAD=/lib/existing.so:/usr/local/lib/volume_cache.so"}, env)

	env = withLDPreload(env, volumeCacheLibraryPath)
	require.Equal(t, []string{"LD_PRELOAD=/lib/existing.so:/usr/local/lib/volume_cache.so"}, env)
}

func TestVolumeCacheMounts(t *testing.T) {
	mounts := volumeCacheMounts("/volumes/workspace")

	require.Len(t, mounts, 2)
	require.Equal(t, filepath.Join(types.AgentCacheFSMountPath, "volumes", "workspace"), mounts[0].Source)
	require.Equal(t, types.AgentCacheFSMountPath, mounts[0].Destination)
	require.Equal(t, volumeCacheLibraryPath, mounts[1].Source)
	require.Equal(t, volumeCacheLibraryPath, mounts[1].Destination)
}
