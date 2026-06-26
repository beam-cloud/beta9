package worker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDevDurableDiskRestoresMissingPrimaryFromReplica(t *testing.T) {
	primary := filepath.Join(t.TempDir(), "pg-data")
	mount := devDurableDiskTestMount(primary)

	require.NoError(t, prepareDevDurableDiskMount(mount))
	require.NoError(t, os.MkdirAll(filepath.Join(primary, "pgdata"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(primary, "pgdata", "state"), []byte("postgres-value"), 0o644))
	require.NoError(t, syncDevDurableDiskMount(mount))

	require.NoError(t, os.RemoveAll(primary))
	require.NoError(t, prepareDevDurableDiskMount(mount))

	data, err := os.ReadFile(filepath.Join(primary, "pgdata", "state"))
	require.NoError(t, err)
	require.Equal(t, "postgres-value", string(data))
}

func TestDevDurableDiskSyncRecreatesMissingReplicaFromPrimary(t *testing.T) {
	primary := filepath.Join(t.TempDir(), "redis-data")
	mount := devDurableDiskTestMount(primary)

	require.NoError(t, prepareDevDurableDiskMount(mount))
	require.NoError(t, os.MkdirAll(filepath.Join(primary, "appendonlydir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(primary, "appendonlydir", "state"), []byte("redis-value"), 0o644))
	require.NoError(t, syncDevDurableDiskMount(mount))

	replica := devDurableDiskReplicaPaths(mount)[0]
	require.NoError(t, os.RemoveAll(replica))
	require.NoError(t, syncDevDurableDiskMount(mount))

	data, err := os.ReadFile(filepath.Join(replica, "appendonlydir", "state"))
	require.NoError(t, err)
	require.Equal(t, "redis-value", string(data))
}

func devDurableDiskTestMount(primary string) *types.Mount {
	return &types.Mount{
		LocalPath: primary,
		MountPath: "/data",
		DurableDisk: &types.DurableDiskMountConfig{
			Name:     filepath.Base(primary),
			Driver:   "dev",
			Replicas: 3,
			Mode:     "sync",
			Quorum:   "majority",
		},
	}
}
