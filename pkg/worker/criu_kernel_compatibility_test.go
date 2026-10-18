package worker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestMicroVMCheckpointProfileBindsExactGuestKernel(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"cloud-hypervisor", "virtiofsd"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\necho version-test\n"), 0755))
	}
	t.Setenv("PATH", dir)
	initPath, kernelPath := filepath.Join(dir, "init"), filepath.Join(dir, "vmlinux")
	require.NoError(t, os.WriteFile(initPath, []byte("init-a"), 0755))
	require.NoError(t, os.WriteFile(kernelPath, []byte("kernel-a"), 0644))
	worker := &Worker{ctx: context.Background(), runtime: &mockRuntime{name: "microvm"}}
	before, err := worker.readCheckpointHostProfileWithGuestFiles(initPath, kernelPath)
	require.NoError(t, err)
	require.Contains(t, before.Version, fmt.Sprintf(" kernel:%x ", sha256.Sum256([]byte("kernel-a"))))
	require.True(t, strings.HasSuffix(before.Version, ";fuse-no-ioctl-v1;clip-stateless-dir-v1"))
	require.True(t, strings.HasPrefix(before.key(), types.CheckpointCompatibilityMicroVMMetadataPrefix))
	alternate := filepath.Join(dir, "alternate-vmlinux")
	require.NoError(t, os.WriteFile(alternate, []byte("kernel-a"), 0644))
	same, err := worker.readCheckpointHostProfileWithGuestFiles(initPath, alternate)
	require.NoError(t, err)
	require.Equal(t, before.key(), same.key(), "identical bytes at another configured path retain the same ABI")
	require.NoError(t, os.WriteFile(alternate, []byte("kernel-b"), 0644))
	changedPath, err := worker.readCheckpointHostProfileWithGuestFiles(initPath, alternate)
	require.NoError(t, err)
	require.NotEqual(t, before.key(), changedPath.key(), "profile must hash the selected path, not the default kernel")
	require.NoError(t, os.WriteFile(kernelPath, []byte("kernel-c"), 0644))
	changedBytes, err := worker.readCheckpointHostProfileWithGuestFiles(initPath, kernelPath)
	require.NoError(t, err)
	require.NotEqual(t, before.key(), changedBytes.key(), "same-path kernel replacement changes compatibility")
	_, err = worker.readCheckpointHostProfileWithGuestFiles(initPath, filepath.Join(dir, "missing"))
	require.ErrorContains(t, err, "read guest kernel for checkpoint compatibility")
	legacyDirectory := before
	legacyDirectory.Version = strings.TrimSuffix(before.Version, ";clip-stateless-dir-v1")
	require.NotEqual(t, before.key(), legacyDirectory.key(), "stateful-directory checkpoints must remain a distinct ABI")
	old := before
	old.Version = strings.Replace(old.Version, fmt.Sprintf(" kernel:%x", sha256.Sum256([]byte("kernel-a"))), "", 1)
	old.Version = strings.TrimSuffix(old.Version, ";fuse-no-ioctl-v1")
	require.NotEqual(t, before.key(), old.key(), "prior unbound v8 profile must never restore a new-kernel checkpoint")
	worker.checkpointCompatibilityKey = before.key()
	request := &types.ContainerRequest{CheckpointEnabled: true, Checkpoint: &types.Checkpoint{Status: string(types.CheckpointStatusAvailable), Runtime: "microvm", CompatibilityKey: old.key()}}
	rt := NewMockRuntime(types.ContainerRuntimeMicroVM.String(), runtime.Capabilities{CheckpointRestore: true})
	err = worker.validateMicroVMCheckpointCompatibility(request, rt)
	var incompatible *ErrCheckpointHostIncompatible
	require.ErrorAs(t, err, &incompatible, "old kernel ABI must be rejected before restore preparation")
	request.Checkpoint.CompatibilityKey = before.key()
	require.NoError(t, worker.validateMicroVMCheckpointCompatibility(request, rt), "matching new profile passes the actual compatibility gate")
}
