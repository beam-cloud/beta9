package vm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

// snapshotlessBackend holds no snapshot of any disk, as when a worker
// uploaded none.
type snapshotlessBackend struct{ *vmBackend }

func (b *snapshotlessBackend) GetLatestDiskSnapshot(context.Context, uint, string) (*types.DiskSnapshot, error) {
	return nil, &types.ErrDiskSnapshotNotFound{SnapshotId: "latest"}
}

// uncapturedRootFixture is a running VM whose root is not captured, the way
// a GitHub runner's is, on a backend with no snapshot of it.
func uncapturedRootFixture() (*Service, *types.VM, *auth.AuthInfo, *vmRuntime, *vmGateway) {
	s, v, info, runtime, gateway := fixture()
	captureExitSnapshot := false
	v.Spec.CaptureExitSnapshot = &captureExitSnapshot
	v.TokenID = info.Token.ExternalId
	s.backend = &snapshotlessBackend{s.backend.(*vmBackend)}
	return s, v, info, runtime, gateway
}

// Nothing would be left to start such a VM from, so what stops it to start
// it again later, or takes a copy of its root, is refused and says why.
func TestUncapturedRootRefusesStopPauseAndCopies(t *testing.T) {
	for _, action := range []string{"stop", "pause", "snapshot", "template", "fork"} {
		t.Run(action, func(t *testing.T) {
			s, v, info, runtime, gateway := uncapturedRootFixture()
			runtime.memoryResponse = &pb.PodSandboxSnapshotMemoryResponse{Ok: true, Runtime: "microvm", CheckpointId: "memory"}
			container := v.ContainerID

			response := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId+"/"+v.ID+"/"+action, `{}`)
			require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
			require.Contains(t, response.Body.String(), "not kept when it exits")
			require.Contains(t, response.Body.String(), "cannot be stopped, paused, snapshotted or forked, only deleted")

			stored, err := s.repo.GetVM(context.Background(), v.WorkspaceID, v.ID)
			require.NoError(t, err)
			require.Equal(t, "running", stored.DesiredState)
			require.Equal(t, "running", stored.Status)
			require.Empty(t, stored.Error, "a refusal is not a failure of the VM")
			require.Equal(t, container, stored.ContainerID)
			require.Empty(t, stored.MemoryCheckpointID)
			require.Zero(t, gateway.stops)
			require.Zero(t, runtime.snapshots)
			require.Len(t, s.repo.(*vmStore).rows, 1)
			require.Empty(t, s.repo.(*vmStore).artifacts)
		})
	}

	// The same requests of a VM without the setting are served as before.
	s, v, info, runtime, gateway := fixture()
	response := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId+"/"+v.ID+"/stop", `{"no_snapshot":true}`)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Equal(t, 1, runtime.snapshots)
	require.Equal(t, 1, gateway.stops)
	require.Equal(t, "stopped", s.repo.(*vmStore).rows[v.ID].Status)
}

func TestUncapturedRootIsDeletedWithoutASnapshot(t *testing.T) {
	s, v, info, runtime, gateway := uncapturedRootFixture()
	c, rec := actionContext(info, v.Name, "", "")
	require.NoError(t, s.remove(c))
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Zero(t, runtime.snapshots)
	require.Equal(t, 1, gateway.stops)
	stored := s.repo.(*vmStore).rows[v.ID]
	require.Equal(t, types.VMStateDeleted, stored.Status)
	require.Empty(t, stored.ContainerID)
	require.Empty(t, stored.RootSnapshotID)
	require.Empty(t, stored.Error)
	require.Equal(t, 1, s.backend.(*snapshotlessBackend).deletedDisks)

	// A root that is captured must still have its final snapshot.
	s, v, info, _, _ = uncapturedRootFixture()
	v.Spec.CaptureExitSnapshot = nil
	c, _ = actionContext(info, v.Name, "", "")
	require.ErrorContains(t, s.remove(c), "unable to confirm final root snapshot")
	require.NotEqual(t, types.VMStateDeleted, s.repo.(*vmStore).rows[v.ID].Status)
	require.Zero(t, s.backend.(*snapshotlessBackend).deletedDisks)
}

// A VM stored before the setting existed has no such field. It commits its
// root before it stops, and stops on the snapshot its worker published.
func TestStoredVMWithoutTheSettingStopsOnItsSnapshot(t *testing.T) {
	s, v, info, runtime, gateway := fixture()
	stored, err := json.Marshal(v)
	require.NoError(t, err)
	require.NotContains(t, string(stored), "capture_exit_snapshot")
	loaded := &types.VM{WorkspaceID: v.WorkspaceID}
	require.NoError(t, json.Unmarshal(stored, loaded))
	require.Nil(t, loaded.Spec.CaptureExitSnapshot)

	loaded.DesiredState = types.VMStateStopped
	require.NoError(t, s.stop(auth.ContextWithAuthInfo(context.Background(), info), loaded, false))
	require.Equal(t, 1, runtime.snapshots)
	require.Equal(t, 1, gateway.stops)
	require.Equal(t, "root-after-shutdown", loaded.RootSnapshotID)
	require.Equal(t, types.VMStateStopped, loaded.Status)
	require.Empty(t, loaded.ContainerID)
}

// A VM whose owner's token is revoked gives its compute back. Stopped, this
// one would have nothing to start from, so it is removed.
func TestRevocationRemovesAnUncapturedRoot(t *testing.T) {
	s, v, _, runtime, gateway := uncapturedRootFixture()
	backend := s.backend.(*snapshotlessBackend)
	backend.token = &types.Token{Active: false}
	require.NoError(t, s.reconcileVM(context.Background(), v))
	require.Equal(t, types.VMStateDeleted, v.DesiredState)
	require.Equal(t, types.VMStateDeleted, v.Status)
	require.Empty(t, v.ContainerID)
	require.Equal(t, 1, gateway.stops)
	require.Equal(t, 1, backend.deletedDisks)
	require.Zero(t, runtime.snapshots)
	require.Empty(t, runtime.requests)
}

func TestUncapturedRootIsNeitherSnapshottedNorIdled(t *testing.T) {
	s, v, _, runtime, gateway := uncapturedRootFixture()
	// Validation refuses an idle timeout for such a VM; one that is stored
	// anyway must still not stop it.
	v.Spec.IdleTimeout = 30
	v.LastActiveAt = time.Now().Add(-time.Hour)
	v.UpdatedAt = time.Now().Add(-time.Hour)
	require.NoError(t, s.reconcileVM(context.Background(), v))
	require.Equal(t, types.VMStateRunning, v.DesiredState)
	require.Equal(t, types.VMStateRunning, v.Status)
	require.NotEmpty(t, v.ContainerID)
	require.Zero(t, gateway.stops)
	require.Zero(t, runtime.snapshots)
}

func TestUncapturedRootHasNothingThatStopsItToStartAgain(t *testing.T) {
	captureExitSnapshot := false
	for name, spec := range map[string]types.VMSpec{
		"idle timeout": {IdleTimeout: 30},
		"idle action":  {IdleAction: "pause"},
		"auto-resume":  {AutoResume: true},
		"Spot":         {CPUTier: types.CPUTierSpot},
	} {
		t.Run(name, func(t *testing.T) {
			spec.ImageID = "base"
			require.NoError(t, validate(&spec), "a VM that keeps its disk may have it")
			spec.CaptureExitSnapshot = &captureExitSnapshot
			require.ErrorContains(t, validate(&spec), "no idle timeout, idle action, auto-resume or Spot tier")
		})
	}
	require.NoError(t, validate(&types.VMSpec{ImageID: "base", CaptureExitSnapshot: &captureExitSnapshot}))

	s, v, info, _, _ := uncapturedRootFixture()
	for _, update := range []string{`{"idle_timeout":30}`, `{"idle_action":"stop"}`, `{"auto_resume":true}`} {
		response := vmRequest(managementAPI(s, info), "PATCH", "/"+info.Workspace.ExternalId+"/"+v.ID, update)
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	}
	response := vmRequest(managementAPI(s, info), "PATCH", "/"+info.Workspace.ExternalId+"/"+v.ID, `{"metadata":{"job":"build"}}`)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
}

// Only the service turns the setting off, for GitHub runners. A request can
// set it neither way, on the VM or on a disk of it.
func TestCaptureExitSnapshotIsNotAnAPIOption(t *testing.T) {
	for _, spec := range []string{
		`{"image_id":"base","capture_exit_snapshot":false}`,
		`{"image_id":"base","capture_exit_snapshot":true}`,
		`{"image_id":"base","disks":[{"name":"data","size":"1Gi","mount_path":"/data","capture_exit_snapshot":false}]}`,
	} {
		s, _, info, runtime, gateway := fixture()
		rec := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId, fmt.Sprintf(`{"name":"scratch","spec":%s}`, spec))
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "capture_exit_snapshot is not a")
		require.Empty(t, runtime.requests)
		require.Nil(t, gateway.stub)
	}

	s, _, info, _, gateway := fixture()
	rec := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId, `{"name":"ordinary","spec":{"image_id":"base","disks":[{"name":"data","size":"1Gi","mount_path":"/data"}]}}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), "capture_exit_snapshot")
	for _, disk := range gateway.stub.Disks {
		require.Nil(t, disk.CaptureExitSnapshot, "the stub of an ordinary VM is the one it has today")
	}
}
