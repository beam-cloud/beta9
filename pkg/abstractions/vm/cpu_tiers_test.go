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

type tierVMGateway struct {
	*vmGateway
	defaultTier types.CPUTier
}

func (g *tierVMGateway) IsHostedCPUPool(name string) bool { return name == "old-public-pool" }
func (g *tierVMGateway) GetCPUTierCatalog(context.Context, *pb.GetCPUTierCatalogRequest) (*pb.GetCPUTierCatalogResponse, error) {
	return &pb.GetCPUTierCatalogResponse{Catalog: `{"default_cpu_tier":"` + string(g.defaultTier) + `"}`}, nil
}
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
	s.gateway = &tierVMGateway{vmGateway: gateway}
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

func TestTierReplacesOnlyHostedPools(t *testing.T) {
	s, v, info, _, gateway := fixture()
	s.gateway = &tierVMGateway{vmGateway: gateway}
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	child, err := s.createVM(ctx, info, types.VMCreateOptions{Name: "from-template", Spec: types.VMSpec{ImageID: "image", Pool: "old-public-pool", CPUTier: types.CPUTierShared}})
	require.NoError(t, err)
	require.Empty(t, child.Error)
	require.Empty(t, child.Spec.Pool)
	require.Nil(t, gateway.stub.Pool)
	require.Equal(t, types.CPUTierShared, child.Spec.CPUTier)
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
	require.ErrorContains(t, err, "Shared supports at most 8 vCPU")
	require.Len(t, s.repo.(*vmStore).rows, saved)
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
