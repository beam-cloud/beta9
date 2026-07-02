package clients

import (
	"context"
	"math"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	usageKindOnDemand   = "on_demand"
	usageKindServerless = "serverless"
)

var serverlessStubKinds = map[string]struct{}{
	types.StubTypeFunction:     {},
	types.StubTypeEndpoint:     {},
	types.StubTypeTaskQueue:    {},
	types.StubTypeBot:          {},
	types.StubTypeScheduledJob: {},
}

// WorkerIdentity describes the worker a usage recorder reports for.
type WorkerIdentity struct {
	WorkerID  string
	PoolName  string
	MachineID string
	Runtime   string
}

// ManagedComputeUsageRecorder bills buyer usage on managed-compute machines.
// All marketplace vocabulary (listing, seller, payout) lives here so the
// worker stays marketplace-agnostic: it hands over the container and the
// interval, and this adapter attributes it from provisioned config.
type ManagedComputeUsageRecorder struct {
	client     *MarketplaceUsageClient
	config     types.ManagedComputeConfig
	worker     WorkerIdentity
	payoutRate float64
}

// NewManagedComputeUsageRecorder returns nil unless the managed-compute
// config carries a billing endpoint and a full marketplace identity — i.e.
// only for workers the agent provisioned on seller machines.
func NewManagedComputeUsageRecorder(config types.ManagedComputeConfig, worker WorkerIdentity) *ManagedComputeUsageRecorder {
	client := NewMarketplaceUsageClient(config.Billing)
	if client == nil || config.MarketplaceListingID == "" || config.SellerWorkspaceID == "" || worker.MachineID == "" {
		return nil
	}
	return &ManagedComputeUsageRecorder{
		client:     client,
		config:     config,
		worker:     worker,
		payoutRate: math.Max(0, 1-config.BillableMarginPctOrDefault()),
	}
}

func (r *ManagedComputeUsageRecorder) RecordContainerUsage(ctx context.Context, request *types.ContainerRequest, start, end time.Time, costCents float64) error {
	return r.client.Record(ctx, MarketplaceUsageRequest{
		BuyerWorkspaceID:          request.WorkspaceId,
		SellerWorkspaceID:         r.config.SellerWorkspaceID,
		ListingID:                 r.config.MarketplaceListingID,
		MachineID:                 r.worker.MachineID,
		ContainerID:               request.ContainerId,
		StubID:                    request.StubId,
		AppID:                     request.AppId,
		UsageKind:                 usageKind(request),
		Runtime:                   r.worker.Runtime,
		GPU:                       request.Gpu,
		GPUCount:                  request.GpuCount,
		DurationSeconds:           end.Sub(start).Seconds(),
		BuyerCostCents:            costCents,
		SellerPayoutEstimateCents: math.Max(0, costCents*r.payoutRate),
		StartAt:                   start.UTC(),
		EndAt:                     end.UTC(),
		ContainerType:             request.Stub.Type.Kind(),
		RuntimeMetadata: map[string]interface{}{
			"worker_id": r.worker.WorkerID,
			"pool_name": r.worker.PoolName,
		},
	})
}

func usageKind(request *types.ContainerRequest) string {
	if _, ok := serverlessStubKinds[request.Stub.Type.Kind()]; ok {
		return usageKindServerless
	}
	return usageKindOnDemand
}
