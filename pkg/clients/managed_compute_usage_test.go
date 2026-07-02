package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestNewManagedComputeUsageRecorderRequiresFullIdentity(t *testing.T) {
	config := types.ManagedComputeConfig{
		Billing:              types.ManagedComputeBillingConfig{Endpoint: "https://api.example.com/v2/payment/"},
		MarketplaceListingID: "listing-1",
		SellerWorkspaceID:    "seller-1",
	}
	worker := WorkerIdentity{WorkerID: "worker-1", MachineID: "machine-1"}

	if NewManagedComputeUsageRecorder(config, worker) == nil {
		t.Fatal("recorder = nil, want configured recorder")
	}

	for name, breakIt := range map[string]func(*types.ManagedComputeConfig, *WorkerIdentity){
		"no endpoint": func(c *types.ManagedComputeConfig, _ *WorkerIdentity) { c.Billing.Endpoint = "" },
		"no listing":  func(c *types.ManagedComputeConfig, _ *WorkerIdentity) { c.MarketplaceListingID = "" },
		"no seller":   func(c *types.ManagedComputeConfig, _ *WorkerIdentity) { c.SellerWorkspaceID = "" },
		"no machine":  func(_ *types.ManagedComputeConfig, w *WorkerIdentity) { w.MachineID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			brokenConfig, brokenWorker := config, worker
			breakIt(&brokenConfig, &brokenWorker)
			if NewManagedComputeUsageRecorder(brokenConfig, brokenWorker) != nil {
				t.Fatal("recorder != nil, want nil when identity is incomplete")
			}
		})
	}
}

func TestManagedComputeUsageRecorderAttributesFromConfig(t *testing.T) {
	var got MarketplaceUsageRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode usage request: %v", err)
		}
		w.Write([]byte(`{"ok": true}`))
	}))
	t.Cleanup(server.Close)

	margin := 0.1
	recorder := NewManagedComputeUsageRecorder(types.ManagedComputeConfig{
		BillableMarginPct:    &margin,
		Billing:              types.ManagedComputeBillingConfig{Endpoint: server.URL},
		MarketplaceListingID: "listing-1",
		SellerWorkspaceID:    "seller-1",
	}, WorkerIdentity{
		WorkerID:  "worker-1",
		PoolName:  "marketplace-a100",
		MachineID: "machine-1",
		Runtime:   "runsc",
	})
	if recorder == nil {
		t.Fatal("recorder = nil, want configured recorder")
	}

	start := time.Now().Add(-30 * time.Second)
	end := time.Now()
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		WorkspaceId: "buyer-1",
		StubId:      "stub-1",
		Gpu:         "A100-40",
		GpuCount:    1,
		Stub:        types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeFunction)}},
	}
	if err := recorder.RecordContainerUsage(context.Background(), request, start, end, 100); err != nil {
		t.Fatalf("RecordContainerUsage() error = %v", err)
	}

	if got.ListingID != "listing-1" || got.SellerWorkspaceID != "seller-1" || got.MachineID != "machine-1" {
		t.Fatalf("attribution = %q/%q/%q, want config-provided identity", got.ListingID, got.SellerWorkspaceID, got.MachineID)
	}
	if got.BuyerWorkspaceID != "buyer-1" || got.UsageKind != usageKindServerless || got.Runtime != "runsc" {
		t.Fatalf("usage = %+v, want buyer, serverless kind, and worker runtime", got)
	}
	if got.BuyerCostCents != 100 || got.SellerPayoutEstimateCents != 90 {
		t.Fatalf("cost/payout = %v/%v, want 100/90 with 10%% margin", got.BuyerCostCents, got.SellerPayoutEstimateCents)
	}
}
