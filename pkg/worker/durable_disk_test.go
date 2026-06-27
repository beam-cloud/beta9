package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
			Driver:   types.DurableDiskDriverDev,
			Replicas: 3,
			Mode:     types.DurableDiskReplicationModeSync,
			Quorum:   types.DurableDiskReplicationQuorumMajority,
		},
	}
}

func TestDRBDDurableDiskRejectsNonPrimaryWorker(t *testing.T) {
	mount := drbdDurableDiskTestMount(filepath.Join(t.TempDir(), "pg-data"))
	worker := &Worker{workerId: "worker-b"}

	_, err := worker.newDRBDDiskConfig(mount, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "refusing attach")
}

func TestDRBDDurableDiskRequiresReplicaAddresses(t *testing.T) {
	mount := drbdDurableDiskTestMount(filepath.Join(t.TempDir(), "pg-data"))
	worker := &Worker{workerId: "worker-a"}

	_, err := worker.newDRBDDiskConfig(mount, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), drbdNodeAddressesEnv)
}

func TestDRBDDurableDiskPreparesPrimary(t *testing.T) {
	stateDir := t.TempDir()
	configDir := t.TempDir()
	t.Setenv(drbdStateDirEnv, stateDir)
	t.Setenv(drbdConfigDirEnv, configDir)
	t.Setenv(drbdNodeAddressesEnv, `{"worker-a":"10.0.0.1","worker-b":"10.0.0.2","worker-c":"10.0.0.3"}`)
	t.Setenv(drbdNodeNamesEnv, `{"worker-a":"node-a","worker-b":"node-b","worker-c":"node-c"}`)

	runner := &fakeDurableDiskRunner{}
	withDurableDiskTestHooks(t, runner)

	mount := drbdDurableDiskTestMount(filepath.Join(t.TempDir(), "pg-data"))
	worker := &Worker{workerId: "worker-a", ctx: context.Background()}

	require.NoError(t, worker.prepareDRBDDurableDiskMount(&types.ContainerRequest{StubId: "stub-id"}, mount))

	files, err := filepath.Glob(filepath.Join(configDir, "*.res"))
	require.NoError(t, err)
	require.Len(t, files, 1)

	config, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.Contains(t, string(config), "protocol C;")
	require.Contains(t, string(config), "quorum majority;")
	require.Contains(t, string(config), "on-no-quorum io-error;")
	require.Contains(t, string(config), "on node-a")
	require.Contains(t, string(config), "address ipv4 10.0.0.2:")

	require.True(t, runner.hasCallPrefix("truncate -s 1073741824 "))
	require.True(t, runner.hasCallPrefix("losetup --find --show "))
	require.True(t, runner.hasCallPrefix("drbdadm create-md beta9_"))
	require.True(t, runner.hasCallPrefix("drbdadm primary --force beta9_"))
	require.True(t, runner.hasCallPrefix("mkfs.ext4 -F /dev/drbd"))
	require.True(t, runner.hasCallPrefix("mount /dev/drbd"))
}

func TestDRBDDurableDiskTeardownDemotesPrimary(t *testing.T) {
	t.Setenv(drbdNodeAddressesEnv, `{"worker-a":"10.0.0.1","worker-b":"10.0.0.2","worker-c":"10.0.0.3"}`)
	t.Setenv(drbdNodeNamesEnv, `{"worker-a":"node-a","worker-b":"node-b","worker-c":"node-c"}`)

	runner := &fakeDurableDiskRunner{mounted: true}
	withDurableDiskTestHooks(t, runner)

	mount := drbdDurableDiskTestMount(filepath.Join(t.TempDir(), "pg-data"))
	worker := &Worker{workerId: "worker-a", ctx: context.Background()}

	require.NoError(t, worker.teardownDRBDDurableDiskMount(&types.ContainerRequest{ContainerId: "container-id"}, mount))
	require.True(t, runner.hasCallPrefix("umount "))
	require.True(t, runner.hasCallPrefix("drbdadm secondary beta9_"))
}

func TestDRBDDurableDiskRefusesBackingFileShrink(t *testing.T) {
	backingPath := filepath.Join(t.TempDir(), "backing.img")
	require.NoError(t, os.WriteFile(backingPath, []byte("existing-state"), 0o644))

	config := &drbdDiskConfig{
		BackingPath: backingPath,
		SizeBytes:   1,
	}
	err := config.ensureBackingFile(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "refusing to shrink")
}

func TestDRBDAddressWithPortHandlesIPv6(t *testing.T) {
	require.Equal(t, drbdNetAddress{Value: "[fd00::1]:7799", Family: "ipv6"}, drbdAddressWithPort("fd00::1", 7799))
	require.Equal(t, drbdNetAddress{Value: "[fd00::1]:7799", Family: "ipv6"}, drbdAddressWithPort("[fd00::1]", 7799))
	require.Equal(t, drbdNetAddress{Value: "[fd00::1]:7788", Family: "ipv6"}, drbdAddressWithPort("[fd00::1]:7788", 7799))
	require.Equal(t, drbdNetAddress{Value: "10.0.0.1:7799", Family: "ipv4"}, drbdAddressWithPort("10.0.0.1", 7799))
}

func drbdDurableDiskTestMount(primary string) *types.Mount {
	return &types.Mount{
		LocalPath: primary,
		MountPath: "/var/lib/postgresql/data",
		DurableDisk: &types.DurableDiskMountConfig{
			Name:             filepath.Base(primary),
			Size:             "1Gi",
			Filesystem:       "ext4",
			Driver:           types.DurableDiskDriverDRBD,
			Replicas:         3,
			Mode:             types.DurableDiskReplicationModeSync,
			Quorum:           types.DurableDiskReplicationQuorumMajority,
			PrimaryWorkerID:  "worker-a",
			ReplicaWorkerIDs: []string{"worker-a", "worker-b", "worker-c"},
		},
	}
}

type fakeDurableDiskRunner struct {
	calls   []string
	mounted bool
}

func (f *fakeDurableDiskRunner) run(_ context.Context, name string, args ...string) (string, error) {
	call := strings.TrimSpace(name + " " + strings.Join(args, " "))
	f.calls = append(f.calls, call)

	switch {
	case callHasPrefix(call, "losetup -j "):
		return "", nil
	case callHasPrefix(call, "losetup --find --show "):
		return "/dev/loop7\n", nil
	case callHasPrefix(call, "findmnt --mountpoint "):
		if f.mounted {
			return "mounted\n", nil
		}
		return "", fmt.Errorf("not mounted")
	case callHasPrefix(call, "blkid -o value -s TYPE "):
		return "", fmt.Errorf("no filesystem")
	default:
		return "", nil
	}
}

func (f *fakeDurableDiskRunner) hasCallPrefix(prefix string) bool {
	for _, call := range f.calls {
		if callHasPrefix(call, prefix) {
			return true
		}
	}
	return false
}

func callHasPrefix(call, prefix string) bool {
	return strings.HasPrefix(strings.Join(strings.Fields(call), " "), strings.Join(strings.Fields(prefix), " "))
}

func withDurableDiskTestHooks(t *testing.T, runner *fakeDurableDiskRunner) {
	t.Helper()

	oldRun := durableDiskRun
	oldLookPath := durableDiskLookPath
	oldEUID := durableDiskEUID
	oldHostname := durableDiskHostname

	durableDiskRun = runner.run
	durableDiskLookPath = func(name string) (string, error) {
		return "/usr/bin/" + name, nil
	}
	durableDiskEUID = func() int {
		return 0
	}
	durableDiskHostname = func() (string, error) {
		return "node-a", nil
	}

	t.Cleanup(func() {
		durableDiskRun = oldRun
		durableDiskLookPath = oldLookPath
		durableDiskEUID = oldEUID
		durableDiskHostname = oldHostname
	})
}
