package vm

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type tierVMGateway struct {
	*vmGateway
	defaultTier types.CPUTier
	err         error
}

func (g *tierVMGateway) IsHostedCPUPool(name string) bool { return name == "old-public-pool" }
func (g *tierVMGateway) ResolveCPUTier(ctx context.Context, workspace *types.Workspace, requested types.CPUTier, pool string, eligible bool) (types.CPUTier, error) {
	if g.err != nil {
		return "", g.err
	}
	if eligible && requested == "" {
		return g.defaultTier, nil
	}
	return g.vmGateway.ResolveCPUTier(ctx, workspace, requested, pool, eligible)
}

func TestTierChangeRequiresStoppedVMAndNewStub(t *testing.T) {
	s, v, info, _, gateway := fixture()
	s.gateway = &tierVMGateway{vmGateway: gateway}
	v.Spec.Pool = "old-public-pool"
	handle, disk := v.Handle, rootDisk(v)
	api := managementAPI(s, info)
	route := "/" + info.Workspace.ExternalId + "/" + v.Name
	require.Equal(t, http.StatusConflict, vmRequest(api, "PATCH", route, `{"cpu_tier":"performance"}`).Code)
	v.DesiredState, v.Status, v.ContainerID = types.VMStateStopped, types.VMStateStopped, ""
	require.Equal(t, http.StatusOK, vmRequest(api, "PATCH", route, `{"cpu_tier":"performance","block_network":true}`).Code)
	saved, err := s.repo.GetVM(context.Background(), v.WorkspaceID, v.ID)
	require.NoError(t, err)
	require.Empty(t, saved.StubID)
	require.Equal(t, types.CPUTierPerformance, saved.Spec.CPUTier)
	require.Empty(t, saved.Spec.Pool)
	require.Equal(t, handle, saved.Handle)
	require.Equal(t, disk, rootDisk(saved))

	// The next start creates the new tier's stub from the whole update.
	require.NoError(t, s.start(auth.ContextWithAuthInfo(context.Background(), info), info, saved))
	require.Equal(t, string(types.CPUTierPerformance), gateway.stub.CpuTier)
	require.True(t, gateway.stub.BlockNetwork)
}

func TestTierUpdateToTheCurrentDefaultIsANoOp(t *testing.T) {
	s, v, info, _, gateway := fixture()
	s.gateway = &tierVMGateway{vmGateway: gateway, defaultTier: types.CPUTierShared}
	v.Spec.CPUTier = types.CPUTierShared
	stub := v.StubID
	rec := vmRequest(managementAPI(s, info), "PATCH", "/"+info.Workspace.ExternalId+"/"+v.Name, `{"cpu_tier":""}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	saved, err := s.repo.GetVM(context.Background(), v.WorkspaceID, v.ID)
	require.NoError(t, err)
	require.Equal(t, stub, saved.StubID)
	require.Equal(t, types.CPUTierShared, saved.Spec.CPUTier)
}

func TestRequestReplayDoesNotResolveTheTierAgain(t *testing.T) {
	s, _, info, _, gateway := fixture()
	tiers := &tierVMGateway{vmGateway: gateway, defaultTier: types.CPUTierShared}
	s.gateway = tiers
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	opts := types.VMCreateOptions{Name: "onboarding", RequestID: uuid.NewString(), Spec: types.VMSpec{ImageID: "image"}}
	created, err := s.createVM(ctx, info, opts)
	require.NoError(t, err)
	tiers.err = errors.New("billing unavailable")
	replayed, err := s.createVM(ctx, info, opts)
	require.NoError(t, err)
	require.Equal(t, created.ID, replayed.ID)
	require.Equal(t, types.CPUTierShared, replayed.Spec.CPUTier)
}

func TestTierReplacesOnlyHostedPools(t *testing.T) {
	s, v, info, _, gateway := fixture()
	tiers := &tierVMGateway{vmGateway: gateway, defaultTier: types.CPUTierPerformance}
	s.gateway = tiers
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	child, err := s.createVM(ctx, info, types.VMCreateOptions{Name: "from-template", Spec: types.VMSpec{ImageID: "image", Pool: "old-public-pool", CPUTier: types.CPUTierShared}})
	require.NoError(t, err)
	require.Empty(t, child.Error)
	require.Empty(t, child.Spec.Pool)
	require.Nil(t, gateway.stub.Pool)
	require.Equal(t, types.CPUTierShared, child.Spec.CPUTier)
	// The default tier replaces a hosted pool too; only untiered VMs keep it.
	defaulted, err := s.createVM(ctx, info, types.VMCreateOptions{Name: "defaulted", Spec: types.VMSpec{ImageID: "image", Pool: "old-public-pool"}})
	require.NoError(t, err)
	require.Equal(t, types.CPUTierPerformance, defaulted.Spec.CPUTier)
	require.Empty(t, defaulted.Spec.Pool)
	tiers.defaultTier = ""
	untiered, err := s.createVM(ctx, info, types.VMCreateOptions{Name: "untiered", Spec: types.VMSpec{ImageID: "image", Pool: "old-public-pool"}})
	require.NoError(t, err)
	require.Empty(t, untiered.Spec.CPUTier)
	require.Equal(t, "old-public-pool", untiered.Spec.Pool)
	_, err = s.createVM(ctx, info, types.VMCreateOptions{Name: "private", Spec: types.VMSpec{ImageID: "image", Pool: "private-pool", CPUTier: types.CPUTierShared}})
	require.ErrorContains(t, err, "hosted CPU")

	v.Spec.Pool = "private-pool"
	v.DesiredState, v.Status, v.ContainerID = types.VMStateStopped, types.VMStateStopped, ""
	rec := vmRequest(managementAPI(s, info), "PATCH", "/"+info.Workspace.ExternalId+"/"+v.Name, `{"cpu_tier":"shared"}`)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "hosted CPU")
	saved, err := s.repo.GetVM(context.Background(), v.WorkspaceID, v.ID)
	require.NoError(t, err)
	require.Equal(t, "private-pool", saved.Spec.Pool)
	require.Empty(t, saved.Spec.CPUTier)
}

func TestDefaultTierMustFitTheVMBeforeItIsSaved(t *testing.T) {
	s, _, info, _, gateway := fixture()
	s.gateway = &tierVMGateway{vmGateway: gateway, defaultTier: types.CPUTierShared}
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	child, err := s.createVM(ctx, info, types.VMCreateOptions{Name: "default", Spec: types.VMSpec{ImageID: "image"}})
	require.NoError(t, err)
	require.Equal(t, types.CPUTierShared, child.Spec.CPUTier)
	saved := len(s.repo.(*vmStore).rows)
	_, err = s.createVM(ctx, info, types.VMCreateOptions{Name: "large", Spec: types.VMSpec{ImageID: "image", CPU: 16000, Memory: 32768}})
	require.ErrorContains(t, err, "The shared tier supports at most 8 vCPU")
	require.Len(t, s.repo.(*vmStore).rows, saved)
}

// stoppingContainers is the container repository while a worker stops a VM:
// after the first read, the VM's container is gone.
type stoppingContainers struct {
	*vmContainers
	reads int
}

func (c *stoppingContainers) GetContainerState(id string) (*types.ContainerState, error) {
	if c.reads++; c.reads > 1 {
		delete(c.states, id)
	}
	return c.vmContainers.GetContainerState(id)
}

func TestPreemptedVMStopsWithoutALiveSnapshotAndBlocksTrafficResume(t *testing.T) {
	// Its worker stops a preempted VM. One evicted for a request is STOPPING
	// by then, but one the worker reclaims on its own may still be running
	// here and is stopped again. Neither is snapshotted while it holds the
	// capacity: its final sync commits the root.
	for status, stops := range map[types.ContainerStatus]int{types.ContainerStatusStopping: 0, types.ContainerStatusRunning: 1} {
		t.Run(string(status), func(t *testing.T) {
			s, v, info, runtime, gateway := fixture()
			v.Spec.CPUTier, v.Spec.AutoResume = types.CPUTierSpot, true
			// As PreemptVM leaves the VM.
			v.Status, v.StopReason, v.DesiredState = types.VMStateStopping, types.VMStopReasonPreempted, types.VMStateStopped
			gateway.containers.states[v.ContainerID].Status = status
			gateway.containers.exitCode = int(types.ContainerExitCodeEvicted)
			s.containers = &stoppingContainers{vmContainers: gateway.containers}
			ctx, cancel := context.WithTimeout(auth.ContextWithAuthInfo(context.Background(), info), 5*time.Second)
			defer cancel()
			require.NoError(t, s.stop(ctx, v, false))
			require.Equal(t, types.VMStateStopped, v.Status)
			require.Equal(t, types.VMStopReasonPreempted, v.StopReason)
			require.Equal(t, "root-after-shutdown", v.RootSnapshotID)
			require.Zero(t, runtime.snapshots)
			require.Equal(t, stops, gateway.stops)
			rec := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId+"/"+v.Name+"/wake", "{}")
			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), "call start()")
			_, _, err := s.resumeAccessAttempt(ctx, v, 8080)
			require.ErrorIs(t, err, errVMPreempted)
			require.Empty(t, runtime.requests)
		})
	}
}
