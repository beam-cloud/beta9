package vm

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type tierVMGateway struct{ *vmGateway }

func (g *tierVMGateway) IsHostedCPUPool(name string) bool { return name == "old-public-pool" }
func (g *tierVMGateway) GetOrCreateStub(ctx context.Context, req *pb.GetOrCreateStubRequest) (*pb.GetOrCreateStubResponse, error) {
	response, err := g.vmGateway.GetOrCreateStub(ctx, req)
	if err != nil {
		return nil, err
	}
	binding, _ := json.Marshal(types.CPUPriceBinding{Tier: types.CPUTier(req.CpuTier), Version: "current", CPUPriceID: "price_cpu", MemoryPriceID: "price_ram", CPUCentsPerHour: "5", MemoryCentsPerGiBHour: "1"})
	response.CpuPricing = string(binding)
	return response, nil
}

func TestTierChangeRequiresStoppedVMAndNewStub(t *testing.T) {
	s, v, info, _, gateway := fixture()
	s.gateway = &tierVMGateway{gateway}
	v.Spec.Pool = "old-public-pool"
	oldStub, handle, disk := v.StubID, v.Handle, rootDisk(v)
	api := managementAPI(s, info)
	route := "/" + info.Workspace.ExternalId + "/" + v.Name
	require.Equal(t, http.StatusConflict, vmRequest(api, "PATCH", route, `{"cpu_tier":"performance"}`).Code)
	v.DesiredState, v.Status, v.ContainerID = types.VMStateStopped, types.VMStateStopped, ""
	require.Equal(t, http.StatusOK, vmRequest(api, "PATCH", route, `{"cpu_tier":"performance"}`).Code)
	saved, err := s.repo.GetVM(context.Background(), v.WorkspaceID, v.ID)
	require.NoError(t, err)
	require.NotEqual(t, oldStub, saved.StubID)
	require.Equal(t, types.CPUTierPerformance, saved.Spec.CPUTier)
	require.Empty(t, saved.Spec.Pool)
	require.Equal(t, "current", saved.CPUPricing.Version)
	require.Equal(t, handle, saved.Handle)
	require.Equal(t, disk, rootDisk(saved))
}

func TestTierCannotReplacePrivatePool(t *testing.T) {
	s, v, _, _, gateway := fixture()
	s.gateway = &tierVMGateway{gateway}
	v.Spec.Pool = "private-pool"
	require.ErrorContains(t, s.changeCPUTier(&v.Spec, types.CPUTierShared), "hosted CPU")
	require.Equal(t, "private-pool", v.Spec.Pool)
	require.Empty(t, v.Spec.CPUTier)
}

func TestPreemptionReasonSurvivesDurableStopAndBlocksTrafficResume(t *testing.T) {
	s, v, info, runtime, gateway := fixture()
	v.Spec.CPUTier, v.Spec.AutoResume = types.CPUTierSpot, true
	v.StopReason, v.DesiredState, v.Status = types.VMStopReasonPreempted, types.VMStateStopped, types.VMStateStopping
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	require.NoError(t, s.stop(ctx, v, false))
	require.Equal(t, types.VMStateStopped, v.Status)
	require.Equal(t, types.VMStopReasonPreempted, v.StopReason)
	require.Equal(t, "root-after-shutdown", v.RootSnapshotID)
	require.Equal(t, 1, runtime.snapshots)
	require.Equal(t, 1, gateway.stops)
	rec := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId+"/"+v.Name+"/wake", "{}")
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	require.Empty(t, runtime.requests)
}
