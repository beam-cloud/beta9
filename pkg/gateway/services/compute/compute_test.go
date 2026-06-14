package compute

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func TestPrivatePoolCreatedByAuthRequiresCreatorToken(t *testing.T) {
	authInfo := &auth.AuthInfo{
		Token: &types.Token{ExternalId: "token-owner"},
	}

	tests := []struct {
		name  string
		state *model.PoolState
		want  bool
	}{
		{
			name:  "matching creator",
			state: &model.PoolState{CreatedByTokenID: "token-owner"},
			want:  true,
		},
		{
			name:  "different creator",
			state: &model.PoolState{CreatedByTokenID: "other-token"},
			want:  false,
		},
		{
			name:  "missing creator",
			state: &model.PoolState{},
			want:  false,
		},
		{
			name:  "nil state",
			state: nil,
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := computePoolCreatedByAuth(tt.state, authInfo); got != tt.want {
				t.Fatalf("computePoolCreatedByAuth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPrivatePoolReadsAreWorkspaceScoped(t *testing.T) {
	ctx := testAuthContext("workspace-1", "viewer-token")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{Name: "owned", CreatedByTokenID: "owner-token", Status: "active"},
				{Name: "other", CreatedByTokenID: "other-token", Status: "active"},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "other"): {
				{WorkspaceID: "workspace-1", PoolName: "other", MachineID: "machine-1"},
			},
		},
	}
	service := &Service{computeRepo: repo}

	pools, err := service.ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil {
		t.Fatalf("ListPrivatePools() error = %v", err)
	}
	if !pools.Ok {
		t.Fatalf("ListPrivatePools() not ok: %s", pools.ErrMsg)
	}
	if got, want := poolNames(pools.Pools), []string{"owned", "other"}; !sameStrings(got, want) {
		t.Fatalf("ListPrivatePools() names = %v, want %v", got, want)
	}

	machines, err := service.ListPoolMachines(ctx, &pb.ListPoolMachinesRequest{PoolName: "other"})
	if err != nil {
		t.Fatalf("ListPoolMachines() error = %v", err)
	}
	if !machines.Ok {
		t.Fatalf("ListPoolMachines() not ok: %s", machines.ErrMsg)
	}
	if got, want := len(machines.Machines), 1; got != want {
		t.Fatalf("ListPoolMachines() count = %d, want %d", got, want)
	}
}

func TestListPrivatePoolsReadyMachineCountUsesAgentConnection(t *testing.T) {
	now := time.Now().UTC()
	ctx := testAuthContext("workspace-1", "viewer-token")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{Name: "private-pool", Status: "active"},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "private-pool"): {
				{
					WorkspaceID:     "workspace-1",
					PoolName:        "private-pool",
					MachineID:       "machine-1",
					Schedulable:     true,
					LastJoinAt:      now.Add(-time.Minute),
					LastHeartbeatAt: now,
				},
			},
		},
	}
	service := &Service{computeRepo: repo}

	pools, err := service.ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil {
		t.Fatalf("ListPrivatePools() error = %v", err)
	}
	if !pools.Ok {
		t.Fatalf("ListPrivatePools() not ok: %s", pools.ErrMsg)
	}
	if got, want := len(pools.Pools), 1; got != want {
		t.Fatalf("pool count = %d, want %d", got, want)
	}
	if got, want := pools.Pools[0].ReadyMachineCount, uint32(1); got != want {
		t.Fatalf("ready machine count = %d, want %d", got, want)
	}
}

func TestCreatePoolConflictsWithWorkspacePoolOwnedByAnotherToken(t *testing.T) {
	ctx := testAuthContext("workspace-1", "viewer-token")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{Name: "existing", CreatedByTokenID: "owner-token", Status: "active"},
			},
		},
	}
	service := &Service{computeRepo: repo}

	res, err := service.CreatePool(ctx, &pb.CreatePoolRequest{
		Pool: &pb.PoolConfig{Name: "existing"},
	})
	if err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if res.Ok {
		t.Fatal("CreatePool() unexpectedly succeeded")
	}
	if got, want := res.ErrMsg, "pool already exists in this workspace"; got != want {
		t.Fatalf("CreatePool() error = %q, want %q", got, want)
	}
	if repo.savedPool {
		t.Fatal("CreatePool() saved over a pool owned by another token")
	}
}

func TestCreatePoolRejectsMultipleGPUTypes(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	service := &Service{computeRepo: repo}

	res, err := service.CreatePool(ctx, &pb.CreatePoolRequest{
		Pool: &pb.PoolConfig{Name: "gpu-pool", Gpu: []string{"A4000", "H100"}},
	})
	if err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if res.Ok {
		t.Fatal("CreatePool() unexpectedly accepted mixed GPU types")
	}
	if !strings.Contains(res.ErrMsg, "private pools require one GPU type") {
		t.Fatalf("CreatePool() error = %q, want single GPU type error", res.ErrMsg)
	}
}

func TestCreatePoolStoresCanonicalGPUType(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	service := &Service{computeRepo: repo}

	res, err := service.CreatePool(ctx, &pb.CreatePoolRequest{
		Pool: &pb.PoolConfig{Name: "gpu-pool", Gpu: []string{"NVIDIA RTX A4000"}},
	})
	if err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("CreatePool() not ok: %s", res.ErrMsg)
	}
	if got, want := repo.pools["workspace-1"][0].Config.Gpu, []string{"A4000"}; !sameStrings(got, want) {
		t.Fatalf("stored pool GPU = %v, want %v", got, want)
	}
}

func TestValidatePrivatePoolGPURequestAllowsWorkloadPreferenceList(t *testing.T) {
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{Name: "gpu-pool", Config: &pb.PoolConfig{Name: "gpu-pool", Gpu: []string{"A4000"}}},
			},
		},
	}
	service := &Service{computeRepo: repo}

	err := service.ValidatePrivatePoolGPURequest(
		context.Background(),
		"workspace-1",
		"gpu-pool",
		[]types.GpuType{types.GPU_H100, types.GPU_A4000},
	)
	if err != nil {
		t.Fatalf("ValidatePrivatePoolGPURequest() error = %v", err)
	}
}

func TestJoinAgentLocksPoolGPUTypeFromFirstMachine(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					WorkspaceID:      "workspace-1",
					Name:             "gpu-pool",
					Config:           &pb.PoolConfig{Name: "gpu-pool"},
					CreatedByTokenID: "owner-token",
					CreatedAt:        now,
					UpdatedAt:        now,
				},
			},
		},
		joinTokens: map[string]*model.JoinTokenState{
			hashComputeToken("join-token"): {
				TokenHash:        hashComputeToken("join-token"),
				WorkspaceID:      "workspace-1",
				PoolName:         "gpu-pool",
				CreatedByTokenID: "owner-token",
				ExpiresAt:        now.Add(time.Hour),
			},
		},
	}
	service := &Service{computeRepo: repo}

	res, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken:          "join-token",
		MachineFingerprint: "fingerprint-1",
		Gpu:                []string{"A4000"},
		GpuCount:           1,
		CpuCount:           4,
		MemoryMb:           1024,
		Schedulable:        true,
	})
	if err != nil {
		t.Fatalf("JoinAgent() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("JoinAgent() not ok: %s", res.ErrMsg)
	}
	pool := repo.pools["workspace-1"][0]
	if got, want := pool.Config.Gpu, []string{"A4000"}; !sameStrings(got, want) {
		t.Fatalf("pool GPU config = %v, want %v", got, want)
	}
}

func TestJoinAgentRejectsPoolGPUMismatch(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					WorkspaceID:      "workspace-1",
					Name:             "gpu-pool",
					Config:           &pb.PoolConfig{Name: "gpu-pool", Gpu: []string{"A4000"}},
					CreatedByTokenID: "owner-token",
				},
			},
		},
		joinTokens: map[string]*model.JoinTokenState{
			hashComputeToken("join-token"): {
				TokenHash:        hashComputeToken("join-token"),
				WorkspaceID:      "workspace-1",
				PoolName:         "gpu-pool",
				CreatedByTokenID: "owner-token",
				ExpiresAt:        now.Add(time.Hour),
			},
		},
	}
	service := &Service{computeRepo: repo}

	res, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken:          "join-token",
		MachineFingerprint: "fingerprint-1",
		Gpu:                []string{"H100"},
		GpuCount:           1,
		CpuCount:           4,
		MemoryMb:           1024,
		Schedulable:        true,
	})
	if err != nil {
		t.Fatalf("JoinAgent() error = %v", err)
	}
	if res.Ok {
		t.Fatal("JoinAgent() unexpectedly accepted a mismatched GPU")
	}
	if !strings.Contains(res.ErrMsg, `requires GPU type "A4000"`) {
		t.Fatalf("JoinAgent() error = %q, want pool GPU mismatch", res.ErrMsg)
	}
}

func TestJoinAgentRejectsGPUAfterCPUOnlyPoolInitialized(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					WorkspaceID:      "workspace-1",
					Name:             "cpu-pool",
					Config:           &pb.PoolConfig{Name: "cpu-pool"},
					CreatedByTokenID: "owner-token",
				},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "cpu-pool"): {
				{WorkspaceID: "workspace-1", PoolName: "cpu-pool", MachineID: "cpu-machine"},
			},
		},
		joinTokens: map[string]*model.JoinTokenState{
			hashComputeToken("join-token"): {
				TokenHash:        hashComputeToken("join-token"),
				WorkspaceID:      "workspace-1",
				PoolName:         "cpu-pool",
				CreatedByTokenID: "owner-token",
				ExpiresAt:        now.Add(time.Hour),
			},
		},
	}
	service := &Service{computeRepo: repo}

	res, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken:          "join-token",
		MachineFingerprint: "fingerprint-1",
		Gpu:                []string{"A4000"},
		GpuCount:           1,
		CpuCount:           4,
		MemoryMb:           1024,
		Schedulable:        true,
	})
	if err != nil {
		t.Fatalf("JoinAgent() error = %v", err)
	}
	if res.Ok {
		t.Fatal("JoinAgent() unexpectedly accepted a GPU machine after CPU-only initialization")
	}
	if !strings.Contains(res.ErrMsg, "initialized without GPUs") {
		t.Fatalf("JoinAgent() error = %q, want CPU-only pool error", res.ErrMsg)
	}
}

func TestJoinAgentRejectsMixedMachineGPUTypes(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					WorkspaceID:      "workspace-1",
					Name:             "gpu-pool",
					Config:           &pb.PoolConfig{Name: "gpu-pool"},
					CreatedByTokenID: "owner-token",
				},
			},
		},
		joinTokens: map[string]*model.JoinTokenState{
			hashComputeToken("join-token"): {
				TokenHash:        hashComputeToken("join-token"),
				WorkspaceID:      "workspace-1",
				PoolName:         "gpu-pool",
				CreatedByTokenID: "owner-token",
				ExpiresAt:        now.Add(time.Hour),
			},
		},
	}
	service := &Service{computeRepo: repo}

	res, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken:          "join-token",
		MachineFingerprint: "fingerprint-1",
		Gpu:                []string{"A4000", "H100"},
		GpuCount:           2,
		CpuCount:           4,
		MemoryMb:           1024,
		Schedulable:        true,
	})
	if err != nil {
		t.Fatalf("JoinAgent() error = %v", err)
	}
	if res.Ok {
		t.Fatal("JoinAgent() unexpectedly accepted mixed GPU types")
	}
	if !strings.Contains(res.ErrMsg, "mixed GPU types") {
		t.Fatalf("JoinAgent() error = %q, want mixed GPU type error", res.ErrMsg)
	}
}

func TestMissingPrivatePoolGPUWarning(t *testing.T) {
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{Name: "gpu-pool", Config: &pb.PoolConfig{Name: "gpu-pool", Gpu: []string{"A4000"}}},
			},
		},
	}
	service := &Service{computeRepo: repo}

	warning, err := service.MissingPrivatePoolGPUWarning(context.Background(), "workspace-1", []types.GpuType{types.GPU_H100})
	if err != nil {
		t.Fatalf("MissingPrivatePoolGPUWarning() error = %v", err)
	}
	if !strings.Contains(warning, "GPU type H100 is not available") || !strings.Contains(warning, "A4000") {
		t.Fatalf("warning = %q, want requested and configured GPU types", warning)
	}
}

func TestIsLocalGatewayURL(t *testing.T) {
	tests := []struct {
		rawURL string
		want   bool
	}{
		{rawURL: "http://localhost:1994", want: true},
		{rawURL: "http://127.0.0.1:1994", want: true},
		{rawURL: "http://[::1]:1994", want: true},
		{rawURL: "https://gateway.beam.cloud", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.rawURL, func(t *testing.T) {
			if got := isLocalGatewayURL(tt.rawURL); got != tt.want {
				t.Fatalf("isLocalGatewayURL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateAgentTransportConfig(t *testing.T) {
	s := &Service{
		appConfig: types.AppConfig{
			Tailscale: types.TailscaleConfig{
				Enabled:      true,
				AuthKey:      "tskey-auth-gateway",
				AgentAuthKey: "tskey-auth-worker",
			},
		},
	}

	if err := s.validateAgentTransportConfig(types.BackendRouteTransportTSNet); err != nil {
		t.Fatalf("expected configured tsnet transport to pass, got %v", err)
	}

	s.appConfig.Tailscale.AgentAuthKey = ""
	if err := s.validateAgentTransportConfig(types.BackendRouteTransportTSNet); err == nil {
		t.Fatal("expected missing agent auth key to fail")
	}
}

func TestAgentInstallCommandDoesNotUseSudoOnDarwin(t *testing.T) {
	command := agentInstallCommand("https://app.stage.beam.cloud", "join-token", false)

	if !strings.Contains(command, `uname -s`) || !strings.Contains(command, `Darwin`) {
		t.Fatalf("expected command to branch on Darwin, got %s", command)
	}
	if !strings.Contains(command, `then curl -fsSL 'https://app.stage.beam.cloud/install/agent' | sh -s -- --gateway 'https://app.stage.beam.cloud' --join-token 'join-token'`) {
		t.Fatalf("expected Darwin/root path to run without sudo, got %s", command)
	}
	if !strings.Contains(command, `else curl -fsSL 'https://app.stage.beam.cloud/install/agent' | sudo sh -s -- --gateway 'https://app.stage.beam.cloud' --join-token 'join-token'`) {
		t.Fatalf("expected non-root Linux path to use sudo, got %s", command)
	}
}

func TestAgentInstallCommandDevModeRunsWithoutSudo(t *testing.T) {
	command := agentInstallCommand("http://localhost:1994", "join-token", true)

	if strings.Contains(command, "sudo") {
		t.Fatalf("dev command should not use sudo: %s", command)
	}
	if !strings.Contains(command, "--dev") {
		t.Fatalf("dev command should include --dev: %s", command)
	}
}

func TestManagedComputeBillableMicros(t *testing.T) {
	tests := []struct {
		name       string
		provider   int64
		margin     float64
		want       int64
		wantBudget int64
	}{
		{name: "default margin", provider: 1_500_000, margin: 0.10, want: 1_650_000, wantBudget: 1_363_636},
		{name: "explicit zero margin", provider: 1_500_000, margin: 0, want: 1_500_000, wantBudget: 1_500_000},
		{name: "negative margin clamps", provider: 1_500_000, margin: -0.5, want: 1_500_000, wantBudget: 1_500_000},
		{name: "keeps positive budgets positive", provider: 1, margin: 0.10, want: 2, wantBudget: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := billableMicros(tt.provider, tt.margin); got != tt.want {
				t.Fatalf("billableMicros() = %d, want %d", got, tt.want)
			}
			if got := providerBudgetMicros(tt.provider, tt.margin); got != tt.wantBudget {
				t.Fatalf("providerBudgetMicros() = %d, want %d", got, tt.wantBudget)
			}
		})
	}
}

func TestCheckManagedLaunchCreditBuildsBillingRequest(t *testing.T) {
	billing := &fakeManagedBilling{
		launchDecision: billingDecision{OK: true, AvailableCents: 3000, RequiredCents: 2500},
	}
	service := &Service{
		billing: billing,
		appConfig: types.AppConfig{
			ManagedCompute: types.ManagedComputeConfig{
				Billing: types.ManagedComputeBillingConfig{MinimumCreditCents: 2500},
			},
		},
	}

	plan := model.SolvePlan{
		Actions: []model.SolveAction{
			{
				Type:  model.ActionCreate,
				Count: 2,
				Offer: model.Offer{HourlyCostMicros: 1_500_000},
			},
			{
				Type:  model.ActionKeep,
				Count: 1,
				Offer: model.Offer{HourlyCostMicros: 9_000_000},
			},
		},
		CommittedCostMicros: 42_000_000,
	}

	decision, err := service.checkManagedLaunchCredit(context.Background(), "workspace-1", "pool-1", plan, nil)
	if err != nil {
		t.Fatalf("checkManagedLaunchCredit() error = %v", err)
	}
	if !decision.OK {
		t.Fatal("checkManagedLaunchCredit() returned non-ok decision")
	}
	if billing.launchCalls != 1 {
		t.Fatalf("CheckLaunchCredit calls = %d, want 1", billing.launchCalls)
	}
	req := billing.launchRequest
	if req.WorkspaceID != "workspace-1" || req.PoolName != "pool-1" {
		t.Fatalf("billing request identity = %s/%s, want workspace-1/pool-1", req.WorkspaceID, req.PoolName)
	}
	if req.RequiredCents != 2500 {
		t.Fatalf("billing request required cents = %d, want 2500", req.RequiredCents)
	}
	if req.Quantity != 2 {
		t.Fatalf("billing request quantity = %d, want 2", req.Quantity)
	}
	if req.EstimatedHourlyCostMicros != 3_300_000 {
		t.Fatalf("billing request hourly micros = %d, want 3300000", req.EstimatedHourlyCostMicros)
	}
	if req.EstimatedCommittedMicros != 46_200_000 {
		t.Fatalf("billing request committed micros = %d, want 46200000", req.EstimatedCommittedMicros)
	}
}

func TestListPoolOffersReturnsHetznerCPUNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/locations":
			_, _ = w.Write([]byte(`{
				"locations": [{"id":1,"name":"ash","description":"Ashburn, VA","country":"US","city":"Ashburn","latitude":39.0438,"longitude":-77.4874,"network_zone":"us-east"}],
				"meta":{"pagination":{"page":1,"per_page":50,"previous_page":null,"next_page":null,"last_page":1,"total_entries":1}}
			}`))
		case "/server_types":
			_, _ = w.Write([]byte(`{
				"server_types": [{
					"id":45,
					"name":"cpx31",
					"description":"CPX31",
					"cores":4,
					"memory":8,
					"disk":160,
					"deprecated":false,
					"category":"Shared vCPU",
					"cpu_type":"shared",
					"storage_type":"local",
					"architecture":"x86",
					"locations":[{"id":1,"name":"ash","available":true,"deprecation":null}],
					"prices":[{"location":"ash","price_hourly":{"net":"0.0312","gross":"0.0248"}}]
				}],
				"meta":{"pagination":{"page":1,"per_page":50,"previous_page":null,"next_page":null,"last_page":1,"total_entries":1}}
			}`))
		default:
			t.Fatalf("unexpected Hetzner path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	service := &Service{
		appConfig: types.AppConfig{
			Providers: types.ProviderConfig{
				Hetzner: types.HetznerProviderConfig{
					ApiToken:             "hetzner-token",
					BaseURL:              server.URL,
					ServerTypeCategories: map[string]string{"cpx31": "shared"},
					RegionMetadata: map[string]types.HetznerRegionConfig{
						"ash": {DisplayName: "Ashburn", Latitude: 39.0438, Longitude: -77.4874},
					},
				},
			},
		},
	}

	res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{
		Pool: &pb.PoolConfig{Nodes: 1},
	})
	if err != nil {
		t.Fatalf("ListPoolOffers() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("ListPoolOffers() not ok: %s", res.ErrMsg)
	}
	if got, want := len(res.Offers), 1; got != want {
		t.Fatalf("offers = %d, want %d", got, want)
	}

	offer := res.Offers[0]
	if offer.Provider != "hetzner" || offer.Cloud != "hetzner" {
		t.Fatalf("offer provider/cloud = %s/%s, want hetzner/hetzner", offer.Provider, offer.Cloud)
	}
	if offer.NodeCount != 1 || offer.GpuCount != 0 {
		t.Fatalf("offer node/gpu count = %d/%d, want 1/0", offer.NodeCount, offer.GpuCount)
	}
	if offer.CpuMillicores != 4000 || offer.MemoryMb != 8192 || offer.StorageMb != 163840 {
		t.Fatalf("offer resources = %d/%d/%d", offer.CpuMillicores, offer.MemoryMb, offer.StorageMb)
	}
	if offer.DisplayName != "CPX31" || offer.Category != "shared" || offer.RegionDisplayName != "Ashburn" {
		t.Fatalf("offer display fields = %q/%q/%q", offer.DisplayName, offer.Category, offer.RegionDisplayName)
	}
	if offer.Latitude != 39.0438 || offer.Longitude != -77.4874 {
		t.Fatalf("offer coordinates = %f/%f", offer.Latitude, offer.Longitude)
	}
}

func TestLaunchPoolCapacityCreatesProviderReservation(t *testing.T) {
	var createCalls int
	var createBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/instances/types":
			_, _ = w.Write([]byte(`{"instance_types":[{"id":"sf-a10g-1","cloud":"lambda","shade_instance_type":"A10Gx1","hourly_price":150,"deployment_type":"vm","configuration":{"gpu_type":"A10G","num_gpus":1,"vcpus":4,"memory_in_gb":16,"storage_in_gb":128},"availability":[{"region":"us-east","available":true}]}]}`))
		case "/instances/create":
			createCalls++
			if err := json.NewDecoder(r.Body).Decode(&createBody); err != nil {
				t.Fatalf("decode create body: %v", err)
			}
			_, _ = w.Write([]byte(`{"id":"reservation-1"}`))
		default:
			t.Fatalf("unexpected shadeform path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	repo := &fakeComputeRepo{}
	service := &Service{
		computeRepo: repo,
		billing: &fakeManagedBilling{
			launchDecision: billingDecision{OK: true, AvailableCents: 5000, RequiredCents: 2500},
		},
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				HTTP: types.HTTPConfig{ExternalHost: "app.beam.test", ExternalPort: 443, TLS: true},
			},
			Tailscale: types.TailscaleConfig{Enabled: true, AuthKey: "gateway-key", AgentAuthKey: "agent-key"},
			Providers: types.ProviderConfig{
				Shadeform: types.ShadeformProviderConfig{ApiKey: "shadeform-key", BaseURL: server.URL},
			},
			ManagedCompute: types.ManagedComputeConfig{
				Billing: types.ManagedComputeBillingConfig{MinimumCreditCents: 2500},
			},
		},
	}

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{
			Name:      "pool-1",
			Gpu:       []string{"A10G"},
			Gpus:      1,
			OfferId:   "sf-a10g-1",
			Ttl:       "1h",
			MaxSpend:  2,
			Providers: []string{"shadeform"},
			Regions:   []string{"us-east"},
		},
	})
	if err != nil {
		t.Fatalf("LaunchPoolCapacity() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("LaunchPoolCapacity() not ok: code=%s msg=%s", res.ErrorCode, res.ErrMsg)
	}
	if createCalls != 1 {
		t.Fatalf("shadeform create calls = %d, want 1", createCalls)
	}
	state := repo.pools["workspace-1"][0]
	if got, want := len(state.Reservations), 1; got != want {
		t.Fatalf("reservation count = %d, want %d", got, want)
	}
	reservation := state.Reservations[0]
	if got, want := reservation.ID, "reservation-1"; got != want {
		t.Fatalf("reservation id = %q, want %q", got, want)
	}
	if reservation.MachineID == "" {
		t.Fatal("reservation missing managed machine id")
	}
	if !strings.Contains(createBody["name"].(string), reservation.MachineID) {
		t.Fatalf("provider name %q does not include machine id %q", createBody["name"], reservation.MachineID)
	}
}

func TestLaunchPoolCapacityCreatesHetznerCPUNodeReservation(t *testing.T) {
	var createCalls int
	var createBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/locations":
			_, _ = w.Write([]byte(`{
				"locations": [{"id":1,"name":"ash","description":"Ashburn","network_zone":"us-east"}],
				"meta":{"pagination":{"page":1,"per_page":50,"previous_page":null,"next_page":null,"last_page":1,"total_entries":1}}
			}`))
		case r.Method == http.MethodGet && r.URL.Path == "/server_types":
			_, _ = w.Write([]byte(`{
				"server_types": [{
					"id":45,
					"name":"cpx31",
					"description":"CPX31",
					"cores":4,
					"memory":8,
					"disk":160,
					"deprecated":false,
					"category":"Shared vCPU",
					"cpu_type":"shared",
					"storage_type":"local",
					"architecture":"x86",
					"locations":[{"id":1,"name":"ash","available":true,"deprecation":null}],
					"prices":[{"location":"ash","price_hourly":{"net":"0.0312","gross":"0.0248"}}]
				}],
				"meta":{"pagination":{"page":1,"per_page":50,"previous_page":null,"next_page":null,"last_page":1,"total_entries":1}}
			}`))
		case r.Method == http.MethodGet && r.URL.Path == "/networks":
			_, _ = w.Write([]byte(`{
				"networks": [{"id":456,"name":"beam-workers","subnets":[{"type":"cloud","ip_range":"10.42.0.0/24","network_zone":"us-east"}]}],
				"meta":{"pagination":{"page":1,"per_page":50,"previous_page":null,"next_page":null,"last_page":1,"total_entries":1}}
			}`))
		case r.Method == http.MethodPost && r.URL.Path == "/servers":
			createCalls++
			if err := json.NewDecoder(r.Body).Decode(&createBody); err != nil {
				t.Fatalf("decode create body: %v", err)
			}
			_, _ = w.Write([]byte(`{"server":{"id":42,"status":"initializing","location":{"name":"ash"},"server_type":{"id":45,"name":"cpx31","cores":4,"memory":8,"disk":160}}}`))
		default:
			t.Fatalf("unexpected Hetzner request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	repo := &fakeComputeRepo{}
	service := &Service{
		computeRepo: repo,
		billing: &fakeManagedBilling{
			launchDecision: billingDecision{OK: true, AvailableCents: 5000, RequiredCents: 2500},
		},
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				HTTP: types.HTTPConfig{ExternalHost: "app.beam.test", ExternalPort: 443, TLS: true},
			},
			Tailscale: types.TailscaleConfig{Enabled: true, AuthKey: "gateway-key", AgentAuthKey: "agent-key"},
			Providers: types.ProviderConfig{
				Hetzner: types.HetznerProviderConfig{
					ApiToken: "hetzner-token",
					BaseURL:  server.URL,
					Image:    "ubuntu-24.04",
					PrivateNetwork: types.HetznerPrivateNetworkConfig{
						Name: "beam-workers",
					},
				},
			},
			ManagedCompute: types.ManagedComputeConfig{
				Billing: types.ManagedComputeBillingConfig{MinimumCreditCents: 2500},
			},
		},
	}

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{
			Name:      "cpu-pool",
			Nodes:     1,
			OfferId:   "cpx31",
			Ttl:       "1h",
			MaxSpend:  1,
			Providers: []string{"hetzner"},
			Regions:   []string{"ash"},
		},
	})
	if err != nil {
		t.Fatalf("LaunchPoolCapacity() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("LaunchPoolCapacity() not ok: code=%s msg=%s", res.ErrorCode, res.ErrMsg)
	}
	if createCalls != 1 {
		t.Fatalf("Hetzner create calls = %d, want 1", createCalls)
	}
	if createBody["server_type"] != "cpx31" || createBody["image"] != "ubuntu-24.04" || createBody["location"] != "ash" {
		t.Fatalf("unexpected Hetzner create body: %#v", createBody)
	}
	if got := createBody["networks"]; fmt.Sprint(got) != "[456]" {
		t.Fatalf("Hetzner networks = %#v, want [456]", got)
	}

	state := repo.pools["workspace-1"][0]
	if got, want := state.ReservedNodes, uint32(1); got != want {
		t.Fatalf("reserved nodes = %d, want %d", got, want)
	}
	if got, want := state.ReservedGPUs, uint32(0); got != want {
		t.Fatalf("reserved gpus = %d, want %d", got, want)
	}
	if got, want := len(state.Reservations), 1; got != want {
		t.Fatalf("reservation count = %d, want %d", got, want)
	}
	reservation := state.Reservations[0]
	if reservation.Provider != "hetzner" || reservation.NodeCount != 1 || reservation.GPUCount != 0 {
		t.Fatalf("reservation provider/node/gpu = %s/%d/%d", reservation.Provider, reservation.NodeCount, reservation.GPUCount)
	}
	if state.Config.Nodes != 1 || state.Config.Gpus != 0 {
		t.Fatalf("pool config nodes/gpus = %d/%d, want 1/0", state.Config.Nodes, state.Config.Gpus)
	}
}

func TestLaunchPoolCapacityAddsReservationToExistingPool(t *testing.T) {
	var createCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/instances/types":
			_, _ = w.Write([]byte(`{"instance_types":[{"id":"sf-a10g-1","cloud":"lambda","shade_instance_type":"A10Gx1","hourly_price":150,"deployment_type":"vm","configuration":{"gpu_type":"A10G","num_gpus":1,"vcpus":4,"memory_in_gb":16,"storage_in_gb":128},"availability":[{"region":"us-east","available":true}]}]}`))
		case "/instances/create":
			createCalls++
			_, _ = w.Write([]byte(`{"id":"reservation-2"}`))
		default:
			t.Fatalf("unexpected shadeform path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	now := time.Now().UTC()
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:                 "pool-1",
					Selector:             "pool-1",
					CreatedByTokenID:     "token-1",
					ReservedGPUs:         1,
					CommittedSpendMicros: 1_650_000,
					Reservations: []model.Reservation{
						{
							ID:               "reservation-1",
							PoolName:         "pool-1",
							Selector:         "pool-1",
							Provider:         "shadeform",
							OfferID:          "sf-a10g-1",
							MachineID:        "machine-1",
							GPU:              "A10G",
							GPUCount:         1,
							HourlyCostMicros: 1_500_000,
							Source:           model.SourceCLIReservation,
							Status:           model.ReservationActive,
							CreatedAt:        now.Add(-time.Minute),
							ExpiresAt:        now.Add(time.Hour),
						},
					},
				},
			},
		},
	}
	service := &Service{
		computeRepo: repo,
		billing: &fakeManagedBilling{
			launchDecision: billingDecision{OK: true, AvailableCents: 5000, RequiredCents: 2500},
		},
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				HTTP: types.HTTPConfig{ExternalHost: "app.beam.test", ExternalPort: 443, TLS: true},
			},
			Tailscale: types.TailscaleConfig{Enabled: true, AuthKey: "gateway-key", AgentAuthKey: "agent-key"},
			Providers: types.ProviderConfig{
				Shadeform: types.ShadeformProviderConfig{ApiKey: "shadeform-key", BaseURL: server.URL},
			},
			ManagedCompute: types.ManagedComputeConfig{
				Billing: types.ManagedComputeBillingConfig{MinimumCreditCents: 2500},
			},
		},
	}

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{
			Name:      "pool-1",
			Gpu:       []string{"A10G"},
			Gpus:      1,
			OfferId:   "sf-a10g-1",
			Ttl:       "1h",
			MaxSpend:  2,
			Providers: []string{"shadeform"},
			Regions:   []string{"us-east"},
		},
	})
	if err != nil {
		t.Fatalf("LaunchPoolCapacity() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("LaunchPoolCapacity() not ok: code=%s msg=%s", res.ErrorCode, res.ErrMsg)
	}
	if createCalls != 1 {
		t.Fatalf("shadeform create calls = %d, want 1", createCalls)
	}
	state := repo.pools["workspace-1"][0]
	if got, want := len(state.Reservations), 2; got != want {
		t.Fatalf("reservation count = %d, want %d", got, want)
	}
	if got, want := state.Reservations[1].ID, "reservation-2"; got != want {
		t.Fatalf("new reservation id = %q, want %q", got, want)
	}
	if got, want := state.ReservedGPUs, uint32(2); got != want {
		t.Fatalf("reserved gpus = %d, want %d", got, want)
	}
	if got, want := state.CommittedSpendMicros, int64(3_300_000); got != want {
		t.Fatalf("committed spend micros = %d, want %d", got, want)
	}
}

func TestRecordManagedUsageEmitsOpenMeterMetrics(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		Name: "pool-1",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Provider:         "shadeform",
				InstanceID:       "instance-1",
				MachineID:        "machine-1",
				GPU:              "A10G",
				GPUCount:         1,
				CPUMillicores:    4000,
				MemoryMB:         16384,
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationActive,
				CreatedAt:        now.Add(-time.Hour),
				BillingCursorAt:  now.Add(-time.Minute),
				ExpiresAt:        now.Add(time.Hour),
				HourlyCostMicros: 1_500_000,
			},
		},
	}
	billing := &fakeManagedBilling{}
	usageRepo := &fakeUsageMetricsRepo{}
	service := &Service{billing: billing, usageMetricsRepo: usageRepo}

	if !service.recordManagedUsage(context.Background(), "workspace-1", state, now, false) {
		t.Fatal("recordManagedUsage() did not report a state change")
	}

	if got, want := len(usageRepo.counters), 2; got != want {
		t.Fatalf("usage counter count = %d, want %d", got, want)
	}
	if got, want := usageRepo.counters[0].name, types.UsageMetricsManagedComputeReservationSeconds; got != want {
		t.Fatalf("usage counter[0] = %q, want %q", got, want)
	}
	if got, want := usageRepo.counters[0].value, float64(60); got < want-0.1 || got > want+0.1 {
		t.Fatalf("seconds value = %f, want about %f", got, want)
	}
	if got, want := usageRepo.counters[1].name, types.UsageMetricsManagedComputeReservationCost; got != want {
		t.Fatalf("usage counter[1] = %q, want %q", got, want)
	}
	if usageRepo.counters[1].value <= 0 {
		t.Fatalf("cost value = %f, want positive", usageRepo.counters[1].value)
	}
	if got, want := usageRepo.counters[0].metadata["workspace_id"], "workspace-1"; got != want {
		t.Fatalf("workspace metadata = %v, want %s", got, want)
	}
	if got, want := len(billing.usage), 1; got != want {
		t.Fatalf("billing usage count = %d, want %d", got, want)
	}
	if got, want := billing.usage[0].HourlyCostMicros, int64(1_650_000); got != want {
		t.Fatalf("billable hourly cost = %d, want %d", got, want)
	}
	if got, want := billing.usage[0].CostCents, 2.75; got < want-0.001 || got > want+0.001 {
		t.Fatalf("billable cost cents = %f, want about %f", got, want)
	}
	if !state.Reservations[0].BillingCursorAt.Equal(now) {
		t.Fatalf("billing cursor = %s, want %s", state.Reservations[0].BillingCursorAt, now)
	}
}

func TestMergePoolConfigForLaunchPreservesPoolIdentity(t *testing.T) {
	existing := &pb.PoolConfig{
		Name:     "A4000-pool",
		Gpu:      []string{"A4000"},
		Ttl:      "1h",
		MaxSpend: 0.18,
		Regions:  []string{"oslo-norway-1"},
		OfferId:  "A4000",
	}
	request := &pb.PoolConfig{
		Name:     "A4000-pool",
		Gpu:      []string{"A4000"},
		Ttl:      "1h",
		MaxSpend: 0.79,
		Regions:  []string{"newyork-usa-1"},
		OfferId:  "A4000",
	}

	merged := mergePoolConfigForLaunch(existing, request)

	if got, want := merged.Regions, []string{"oslo-norway-1", "newyork-usa-1"}; !sameStrings(got, want) {
		t.Fatalf("merged regions = %v, want %v", got, want)
	}
	if got, want := merged.MaxSpend, 0.97; got < want-0.001 || got > want+0.001 {
		t.Fatalf("merged max spend = %f, want %f", got, want)
	}
	if got, want := merged.Gpu, []string{"A4000"}; !sameStrings(got, want) {
		t.Fatalf("merged gpu = %v, want %v", got, want)
	}
	if merged.Ttl != "1h" {
		t.Fatalf("merged ttl = %q, want 1h", merged.Ttl)
	}
}

func TestValidatePoolLaunchCompatibleRejectsGPUMismatch(t *testing.T) {
	existing := &model.PoolState{
		Name:   "A4000-pool",
		Config: &pb.PoolConfig{Name: "A4000-pool", Gpu: []string{"A4000"}},
	}

	if err := validatePoolLaunchCompatible(existing, model.Pool{GPUs: []string{"A4000"}}); err != nil {
		t.Fatalf("same GPU should be compatible, got %v", err)
	}
	if err := validatePoolLaunchCompatible(existing, model.Pool{GPUs: []string{"A6000"}}); err == nil {
		t.Fatal("different GPU should be rejected")
	}
}

type fakeVendor struct {
	extended map[string]time.Time
}

func (v *fakeVendor) Name() string { return "shadeform" }
func (v *fakeVendor) ListOffers(context.Context, model.OfferRequest) ([]model.Offer, error) {
	return nil, nil
}
func (v *fakeVendor) CreateReservation(context.Context, model.ReservationRequest) (*model.Reservation, error) {
	return nil, nil
}
func (v *fakeVendor) GetReservation(context.Context, string) (*model.Reservation, error) {
	return nil, nil
}
func (v *fakeVendor) ExtendReservation(_ context.Context, id string, expiresAt time.Time) error {
	if v.extended == nil {
		v.extended = map[string]time.Time{}
	}
	v.extended[id] = expiresAt
	return nil
}
func (v *fakeVendor) DeleteReservation(context.Context, string) error { return nil }

func TestRenewManagedReservationsExtendsExpiringReservation(t *testing.T) {
	now := time.Now().UTC()
	expiresAt := now.Add(time.Minute)
	state := &model.PoolState{
		Name: "pool-1",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Provider:         "shadeform",
				InstanceID:       "instance-1",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationActive,
				CreatedAt:        now.Add(-time.Hour),
				ExpiresAt:        expiresAt,
				HourlyCostMicros: 1_500_000,
				CommittedMicros:  1_500_000,
			},
		},
	}
	vendor := &fakeVendor{}
	service := &Service{}

	// Plenty of balance: renew for another hour
	decision := billingDecision{OK: true, AvailableCents: 10_000}
	if !service.renewManagedReservations(context.Background(), "workspace-1", state, decision, 5, now, map[string]model.Vendor{"shadeform": vendor}) {
		t.Fatal("renewManagedReservations() did not report a state change")
	}
	if got, want := state.Reservations[0].ExpiresAt, expiresAt.Add(time.Hour); !got.Equal(want) {
		t.Fatalf("expires at = %s, want %s", got, want)
	}
	if got, want := state.Reservations[0].CommittedMicros, int64(3_000_000); got != want {
		t.Fatalf("committed micros = %d, want %d", got, want)
	}
	if got, want := state.CommittedSpendMicros, int64(1_650_000); got != want {
		t.Fatalf("pool committed spend = %d, want %d", got, want)
	}
	if got, want := vendor.extended["instance-1"], expiresAt.Add(time.Hour); !got.Equal(want) {
		t.Fatalf("vendor extended to %s, want %s", got, want)
	}
}

func TestRenewManagedReservationsSkipsWhenBalanceInsufficient(t *testing.T) {
	now := time.Now().UTC()
	expiresAt := now.Add(time.Minute)
	state := &model.PoolState{
		Name: "pool-1",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Provider:         "shadeform",
				InstanceID:       "instance-1",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationActive,
				CreatedAt:        now.Add(-time.Hour),
				ExpiresAt:        expiresAt,
				HourlyCostMicros: 1_500_000,
			},
		},
	}
	vendor := &fakeVendor{}
	service := &Service{}

	// 16.5 cents/hr billable but only 10 cents headroom: do not renew
	decision := billingDecision{OK: true, AvailableCents: 15}
	if service.renewManagedReservations(context.Background(), "workspace-1", state, decision, 5, now, map[string]model.Vendor{"shadeform": vendor}) {
		t.Fatal("renewManagedReservations() renewed without sufficient balance")
	}
	if !state.Reservations[0].ExpiresAt.Equal(expiresAt) {
		t.Fatal("reservation expiry should be unchanged")
	}
	if len(vendor.extended) != 0 {
		t.Fatal("vendor should not have been called")
	}
}

func TestRenewManagedReservationsIgnoresDistantExpiry(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		Name: "pool-1",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Provider:         "shadeform",
				InstanceID:       "instance-1",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationActive,
				CreatedAt:        now,
				ExpiresAt:        now.Add(time.Hour),
				HourlyCostMicros: 1_500_000,
			},
		},
	}
	vendor := &fakeVendor{}
	service := &Service{}

	decision := billingDecision{OK: true, AvailableCents: 10_000}
	if service.renewManagedReservations(context.Background(), "workspace-1", state, decision, 0, now, map[string]model.Vendor{"shadeform": vendor}) {
		t.Fatal("renewManagedReservations() should not renew a reservation far from expiry")
	}
}

func TestRecordAgentMetricsEmitsNodeUsage(t *testing.T) {
	now := time.Now().UTC()
	previous := now.Add(-5 * time.Second)
	machine := &model.AgentTokenState{
		TokenHash:       "agent-token-hash",
		WorkspaceID:     "workspace-1",
		PoolName:        "pool-1",
		MachineID:       "machine-1",
		Hostname:        "node-1",
		OS:              "linux",
		Arch:            "amd64",
		CPUCount:        4,
		CPUMillicores:   4000,
		MemoryMB:        8192,
		GPUs:            []string{"A10G"},
		GPUIDs:          []string{"0"},
		GPUCount:        1,
		Executor:        types.DefaultAgentWorkerContainerMode,
		Schedulable:     true,
		LastJoinAt:      now.Add(-time.Minute),
		LastHeartbeatAt: previous,
	}
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					WorkspaceID: "workspace-1",
					Name:        "pool-1",
					Source:      model.SourceCLIReservation,
					Mode:        string(types.PoolModePrivate),
					Transport:   string(types.BackendRouteTransportTSNet),
				},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "pool-1"): {machine},
		},
	}
	usageRepo := &fakeUsageMetricsRepo{}
	service := &Service{computeRepo: repo, usageMetricsRepo: usageRepo}

	err := service.recordAgentMetrics(context.Background(), machine, &pb.AgentMetricSnapshot{
		TimestampUnixNano:    now.UnixNano(),
		CpuUtilizationPct:    25,
		MemoryUsedMb:         2048,
		MemoryTotalMb:        8192,
		MemoryUtilizationPct: 25,
		DiskUsedMb:           1024,
		DiskTotalMb:          4096,
		DiskUsagePct:         25,
		WorkerCount:          1,
		ContainerCount:       2,
		FreeGpuCount:         1,
	})
	if err != nil {
		t.Fatalf("recordAgentMetrics() error = %v", err)
	}

	if got, want := len(usageRepo.counters), 1; got != want {
		t.Fatalf("usage counter count = %d, want %d", got, want)
	}
	counter := usageRepo.counters[0]
	if got, want := counter.name, types.UsageMetricsNodeUsage; got != want {
		t.Fatalf("usage counter = %q, want %q", got, want)
	}
	// Usage seconds are measured on the gateway clock, so allow a small delta
	// between the test's reference time and the call's time.Now().
	if got, want := counter.value, float64(5); got < want || got > want+1 {
		t.Fatalf("node usage value = %f, want ~%f", got, want)
	}
	if got, want := counter.metadata["workspace_id"], "workspace-1"; got != want {
		t.Fatalf("workspace metadata = %v, want %s", got, want)
	}
	if got, want := counter.metadata["node_type"], "managed"; got != want {
		t.Fatalf("node type metadata = %v, want %s", got, want)
	}
	if got, want := counter.metadata["capacity_source"], string(model.SourceCLIReservation); got != want {
		t.Fatalf("capacity source metadata = %v, want %s", got, want)
	}
	if got, want := counter.metadata["worker_count"], int32(1); got != want {
		t.Fatalf("worker count metadata = %v, want %d", got, want)
	}
	if got, want := counter.metadata["container_count"], int32(2); got != want {
		t.Fatalf("container count metadata = %v, want %d", got, want)
	}
}

func TestRecordAgentMetricsKeepsAgentAliveWhenNodeUsageMetricsFail(t *testing.T) {
	now := time.Now().UTC()
	machine := &model.AgentTokenState{
		TokenHash:       "agent-token-hash",
		WorkspaceID:     "workspace-1",
		PoolName:        "pool-1",
		MachineID:       "machine-1",
		CPUCount:        4,
		CPUMillicores:   4000,
		MemoryMB:        8192,
		Executor:        types.DefaultAgentWorkerContainerMode,
		LastJoinAt:      now.Add(-time.Minute),
		LastHeartbeatAt: now.Add(-5 * time.Second),
	}
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{WorkspaceID: "workspace-1", Name: "pool-1", Source: model.SourceCLIReservation},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "pool-1"): {machine},
		},
	}
	service := &Service{
		computeRepo:      repo,
		usageMetricsRepo: &fakeUsageMetricsRepo{err: errors.New("openmeter unavailable")},
	}

	err := service.recordAgentMetrics(context.Background(), machine, &pb.AgentMetricSnapshot{
		TimestampUnixNano: now.UnixNano(),
		MemoryTotalMb:     8192,
	})
	if err != nil {
		t.Fatalf("recordAgentMetrics() error = %v", err)
	}
}

func TestRecordAgentDisconnectIgnoresFreshHeartbeat(t *testing.T) {
	now := time.Now().UTC()
	machine := &model.AgentTokenState{
		TokenHash:       "agent-token-hash",
		WorkspaceID:     "workspace-1",
		PoolName:        "pool-1",
		MachineID:       "machine-1",
		Schedulable:     true,
		LastJoinAt:      now.Add(-time.Minute),
		LastHeartbeatAt: now.Add(-5 * time.Second),
	}
	workerID := model.AgentMachineWorkerID(machine.MachineID)
	workerRepo := &fakeWorkerRepo{
		worker: &types.Worker{Id: workerID, MachineId: machine.MachineID, PoolName: machine.PoolName},
	}
	service := &Service{
		computeRepo: &fakeComputeRepo{
			machines: map[string][]*model.AgentTokenState{
				fakeComputeKey("workspace-1", "pool-1"): {machine},
			},
		},
		workerRepo: workerRepo,
	}

	service.recordAgentDisconnect(context.Background(), machine)

	if !machine.LastDisconnectAt.IsZero() {
		t.Fatalf("last disconnect = %s, want zero", machine.LastDisconnectAt)
	}
	if got := workerRepo.worker.Status; got == types.WorkerStatusDisabled {
		t.Fatalf("worker status = %q, want not disabled", got)
	}
}

func TestRecordAgentDisconnectDisablesStaleHeartbeat(t *testing.T) {
	now := time.Now().UTC()
	machine := &model.AgentTokenState{
		TokenHash:       "agent-token-hash",
		WorkspaceID:     "workspace-1",
		PoolName:        "pool-1",
		MachineID:       "machine-1",
		Schedulable:     true,
		LastJoinAt:      now.Add(-model.AgentHeartbeatTimeout - 2*time.Second),
		LastHeartbeatAt: now.Add(-model.AgentHeartbeatTimeout - time.Second),
	}
	workerID := model.AgentMachineWorkerID(machine.MachineID)
	workerRepo := &fakeWorkerRepo{
		worker: &types.Worker{Id: workerID, MachineId: machine.MachineID, PoolName: machine.PoolName},
	}
	service := &Service{
		computeRepo: &fakeComputeRepo{
			machines: map[string][]*model.AgentTokenState{
				fakeComputeKey("workspace-1", "pool-1"): {machine},
			},
		},
		workerRepo: workerRepo,
	}

	service.recordAgentDisconnect(context.Background(), machine)

	if machine.LastDisconnectAt.IsZero() {
		t.Fatal("last disconnect is zero, want timestamp")
	}
	if got, want := workerRepo.status, types.WorkerStatusDisabled; got != want {
		t.Fatalf("worker status = %q, want %q", got, want)
	}
}

func TestDisableMachineWorkerStopsActiveContainers(t *testing.T) {
	machine := &model.AgentTokenState{
		WorkspaceID: "workspace-1",
		PoolName:    "pool-1",
		MachineID:   "machine-1",
	}
	workerID := model.AgentMachineWorkerID(machine.MachineID)
	workerRepo := &fakeWorkerRepo{
		worker: &types.Worker{Id: workerID, MachineId: machine.MachineID, PoolName: machine.PoolName},
	}
	containerRepo := &fakeContainerRepo{
		containers: []types.ContainerState{
			{ContainerId: "container-running", Status: types.ContainerStatusRunning},
			{ContainerId: "container-stopping", Status: types.ContainerStatusStopping},
		},
	}
	service := &Service{workerRepo: workerRepo, containerRepo: containerRepo}

	if !service.disableMachineWorker(context.Background(), machine, reconcileReasonCreditExhausted) {
		t.Fatal("disableMachineWorker() = false, want true")
	}
	if got, want := workerRepo.status, types.WorkerStatusDisabled; got != want {
		t.Fatalf("worker status = %q, want %q", got, want)
	}
	if got, want := containerRepo.stopped, []string{"container-running"}; !sameStrings(got, want) {
		t.Fatalf("stopped containers = %v, want %v", got, want)
	}
}

func TestDisableMachineWorkerKeepsActiveContainersOnDisconnect(t *testing.T) {
	machine := &model.AgentTokenState{
		WorkspaceID: "workspace-1",
		PoolName:    "pool-1",
		MachineID:   "machine-1",
	}
	workerID := model.AgentMachineWorkerID(machine.MachineID)
	workerRepo := &fakeWorkerRepo{
		worker: &types.Worker{Id: workerID, MachineId: machine.MachineID, PoolName: machine.PoolName},
	}
	containerRepo := &fakeContainerRepo{
		containers: []types.ContainerState{
			{ContainerId: "container-running", Status: types.ContainerStatusRunning},
		},
	}
	service := &Service{workerRepo: workerRepo, containerRepo: containerRepo}

	if !service.disableMachineWorker(context.Background(), machine, reconcileReasonAgentDisconnected) {
		t.Fatal("disableMachineWorker() = false, want true")
	}
	if got, want := workerRepo.status, types.WorkerStatusDisabled; got != want {
		t.Fatalf("worker status = %q, want %q", got, want)
	}
	if len(containerRepo.stopped) != 0 {
		t.Fatalf("stopped containers = %v, want none", containerRepo.stopped)
	}
}

func TestDisableMachineWorkerStopsContainersWhenAlreadyDisabledForHardTeardown(t *testing.T) {
	machine := &model.AgentTokenState{
		WorkspaceID: "workspace-1",
		PoolName:    "pool-1",
		MachineID:   "machine-1",
	}
	workerID := model.AgentMachineWorkerID(machine.MachineID)
	workerRepo := &fakeWorkerRepo{
		worker: &types.Worker{
			Id:        workerID,
			MachineId: machine.MachineID,
			PoolName:  machine.PoolName,
			Status:    types.WorkerStatusDisabled,
		},
	}
	containerRepo := &fakeContainerRepo{
		containers: []types.ContainerState{
			{ContainerId: "container-running", Status: types.ContainerStatusRunning},
		},
	}
	service := &Service{workerRepo: workerRepo, containerRepo: containerRepo}

	if service.disableMachineWorker(context.Background(), machine, reconcileReasonCreditExhausted) {
		t.Fatal("disableMachineWorker() = true, want false for already-disabled worker")
	}
	if got, want := containerRepo.stopped, []string{"container-running"}; !sameStrings(got, want) {
		t.Fatalf("stopped containers = %v, want %v", got, want)
	}
}

func TestNodeUsageSecondsCapsStaleGap(t *testing.T) {
	now := time.Now().UTC()
	if got, want := nodeUsageSeconds(now.Add(-model.AgentHeartbeatTimeout-time.Minute), now), model.AgentHeartbeatTimeout.Seconds(); got != want {
		t.Fatalf("nodeUsageSeconds() = %f for stale gap, want capped %f", got, want)
	}
	if got := nodeUsageSeconds(now.Add(-5*time.Second), now); got != 5 {
		t.Fatalf("nodeUsageSeconds() = %f, want 5", got)
	}
	if got := nodeUsageSeconds(time.Time{}, now); got != 0 {
		t.Fatalf("nodeUsageSeconds() = %f for zero previous, want 0", got)
	}
}

func TestAgentNodeUsageMetadataMarksAttachedNodesAsBYO(t *testing.T) {
	metadata := agentNodeUsageMetadata(
		&model.AgentTokenState{WorkspaceID: "workspace-1", PoolName: "pool-1", MachineID: "machine-1"},
		&model.PoolState{Source: model.SourceAttached},
		nil,
		5,
	)
	if got, want := metadata["node_type"], "byo"; got != want {
		t.Fatalf("node type metadata = %v, want %s", got, want)
	}
	if got, want := metadata["capacity_source"], string(model.SourceAttached); got != want {
		t.Fatalf("capacity source metadata = %v, want %s", got, want)
	}
}

func TestRecordManagedUsageAdvancesCursorWhenMetricsFailAfterBilling(t *testing.T) {
	now := time.Now().UTC()
	cursor := now.Add(-time.Minute)
	state := managedUsageTestState(now, cursor, now.Add(time.Hour), 1_500_000)
	billing := &fakeManagedBilling{}
	service := &Service{
		billing:          billing,
		usageMetricsRepo: &fakeUsageMetricsRepo{err: errors.New("openmeter unavailable")},
	}

	if !service.recordManagedUsage(context.Background(), "workspace-1", state, now, false) {
		t.Fatal("recordManagedUsage() did not report a state change")
	}
	if got, want := len(billing.usage), 1; got != want {
		t.Fatalf("billing usage count = %d, want %d", got, want)
	}
	if !state.Reservations[0].BillingCursorAt.Equal(now) {
		t.Fatalf("billing cursor = %s, want %s", state.Reservations[0].BillingCursorAt, now)
	}
	if !strings.Contains(state.Reservations[0].LastError, "openmeter unavailable") {
		t.Fatalf("last error = %q, want openmeter error", state.Reservations[0].LastError)
	}
}

func TestRecordManagedUsageDoesNotEmitMetricsOrAdvanceCursorWhenBillingFails(t *testing.T) {
	now := time.Now().UTC()
	cursor := now.Add(-time.Minute)
	state := managedUsageTestState(now, cursor, now.Add(time.Hour), 1_500_000)
	billing := &fakeManagedBilling{usageErr: errors.New("billing callback unavailable")}
	usageRepo := &fakeUsageMetricsRepo{}
	service := &Service{
		billing:          billing,
		usageMetricsRepo: usageRepo,
	}

	if !service.recordManagedUsage(context.Background(), "workspace-1", state, now, false) {
		t.Fatal("recordManagedUsage() did not report a state change")
	}
	if got, want := len(usageRepo.counters), 0; got != want {
		t.Fatalf("usage counter count = %d, want %d", got, want)
	}
	if !state.Reservations[0].BillingCursorAt.Equal(cursor) {
		t.Fatalf("billing cursor advanced to %s, want %s", state.Reservations[0].BillingCursorAt, cursor)
	}
	if !strings.Contains(state.Reservations[0].LastError, "billing callback unavailable") {
		t.Fatalf("last error = %q, want billing callback error", state.Reservations[0].LastError)
	}
}

func TestRecordManagedUsageSkipsActiveSubCentWindow(t *testing.T) {
	now := time.Now().UTC()
	cursor := now.Add(-5 * time.Second)
	state := managedUsageTestState(now, cursor, now.Add(time.Hour), 1_000_000)
	billing := &fakeManagedBilling{}
	service := &Service{billing: billing}

	if service.recordManagedUsage(context.Background(), "workspace-1", state, now, false) {
		t.Fatal("recordManagedUsage() reported a change for an active sub-cent window")
	}
	if got := len(billing.usage); got != 0 {
		t.Fatalf("billing usage count = %d, want 0", got)
	}
	if !state.Reservations[0].BillingCursorAt.Equal(cursor) {
		t.Fatalf("billing cursor advanced to %s, want %s", state.Reservations[0].BillingCursorAt, cursor)
	}
}

func TestRecordManagedUsageBillsClosedSubCentWindow(t *testing.T) {
	now := time.Now().UTC()
	cursor := now.Add(-5 * time.Second)
	state := managedUsageTestState(now, cursor, now, 1_000_000)
	billing := &fakeManagedBilling{}
	service := &Service{billing: billing}

	if !service.recordManagedUsage(context.Background(), "workspace-1", state, now, false) {
		t.Fatal("recordManagedUsage() did not bill a closed sub-cent window")
	}
	if got := len(billing.usage); got != 1 {
		t.Fatalf("billing usage count = %d, want 1", got)
	}
	if !state.Reservations[0].BillingCursorAt.Equal(now) {
		t.Fatalf("billing cursor = %s, want %s", state.Reservations[0].BillingCursorAt, now)
	}
}

func TestReconcileManagedComputeTerminatesReservationsWhenCreditsAreExhausted(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Provider:         "shadeform",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationActive,
				CreatedAt:        now.Add(-time.Hour),
				ExpiresAt:        now.Add(time.Hour),
				HourlyCostMicros: 1_000_000,
			},
		},
	}
	machine := &model.AgentTokenState{
		WorkspaceID:         "workspace-1",
		PoolName:            "pool-1",
		MachineID:           "machine-1",
		Schedulable:         true,
		LastHeartbeatAt:     now,
		LastJoinAt:          now,
		Executor:            types.DefaultAgentWorkerContainerMode,
		CPUCount:            4,
		CPUMillicores:       4000,
		MemoryMB:            8192,
		NetworkSlotPoolSize: 16,
	}
	repo := &fakeComputeRepo{
		pools:    map[string][]*model.PoolState{"workspace-1": {state}},
		machines: map[string][]*model.AgentTokenState{fakeComputeKey("workspace-1", "pool-1"): {machine}},
	}
	service := &Service{
		computeRepo: repo,
		billing: &fakeManagedBilling{
			balanceDecision: billingDecision{
				OK:             false,
				ErrorCode:      launchErrorInsufficientCredit,
				Message:        "credits exhausted",
				AvailableCents: 0,
				RequiredCents:  2500,
			},
		},
	}

	if err := service.ReconcileManagedCompute(context.Background()); err != nil {
		t.Fatalf("ReconcileManagedCompute() error = %v", err)
	}

	saved := repo.pools["workspace-1"][0]
	if !repo.savedPool {
		t.Fatal("ReconcileManagedCompute() did not persist the pool transition")
	}
	if got, want := saved.Reservations[0].Status, model.ReservationTerminating; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
	if got, want := saved.Reservations[0].TerminatingReason, "credit_exhausted"; got != want {
		t.Fatalf("terminating reason = %q, want %q", got, want)
	}
	if machine.Schedulable {
		t.Fatal("credit exhaustion did not mark the machine unschedulable")
	}
}

func TestReconcileManagedComputeRecordsUsageBeforeBalanceCheck(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationActive,
				CreatedAt:        now.Add(-time.Hour),
				BillingCursorAt:  now.Add(-time.Minute),
				ExpiresAt:        now.Add(time.Hour),
				HourlyCostMicros: 1_000_000,
			},
		},
	}
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {state}}}
	billing := &fakeManagedBilling{balanceDecision: billingDecision{OK: true, RequiredCents: 1, AvailableCents: 1000}}
	service := &Service{computeRepo: repo, billing: billing}

	if err := service.ReconcileManagedCompute(context.Background()); err != nil {
		t.Fatalf("ReconcileManagedCompute() error = %v", err)
	}

	if got, want := len(billing.usage), 1; got != want {
		t.Fatalf("managed usage records = %d, want %d", got, want)
	}
	if got, want := billing.balanceSawUsageCount, 1; got != want {
		t.Fatalf("balance saw usage count = %d, want %d", got, want)
	}
	if !state.Reservations[0].BillingCursorAt.After(now.Add(-time.Second)) {
		t.Fatalf("billing cursor was not advanced: %s", state.Reservations[0].BillingCursorAt)
	}
}

func TestReconcileManagedComputeChecksBalanceAfterFreshUsageTick(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Reservations: []model.Reservation{
			{
				ID:                 "reservation-1",
				Source:             model.SourceCLIReservation,
				Status:             model.ReservationActive,
				CreatedAt:          now.Add(-time.Hour),
				BillingCursorAt:    now.Add(-time.Minute),
				LastBillingCheckAt: now.Add(-time.Second),
				ExpiresAt:          now.Add(time.Hour),
				HourlyCostMicros:   1_000_000,
			},
		},
	}
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {state}}}
	billing := &fakeManagedBilling{balanceDecision: billingDecision{OK: true, RequiredCents: 1, AvailableCents: 1000}}
	service := &Service{computeRepo: repo, billing: billing}

	if err := service.ReconcileManagedCompute(context.Background()); err != nil {
		t.Fatalf("ReconcileManagedCompute() error = %v", err)
	}
	if got, want := billing.balanceCalls, 1; got != want {
		t.Fatalf("balance calls = %d, want %d", got, want)
	}
}

func TestReconcileManagedComputeChecksBalanceWhenUsageCursorIsFresh(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Reservations: []model.Reservation{
			{
				ID:                 "reservation-1",
				Source:             model.SourceCLIReservation,
				Status:             model.ReservationActive,
				CreatedAt:          now.Add(-time.Hour),
				BillingCursorAt:    now,
				LastBillingCheckAt: now,
				ExpiresAt:          now.Add(time.Hour),
				HourlyCostMicros:   1_000_000,
			},
		},
	}
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {state}}}
	billing := &fakeManagedBilling{balanceDecision: billingDecision{OK: true, RequiredCents: 1, AvailableCents: 1000}}
	service := &Service{computeRepo: repo, billing: billing}

	if err := service.ReconcileManagedCompute(context.Background()); err != nil {
		t.Fatalf("ReconcileManagedCompute() error = %v", err)
	}
	if got, want := len(billing.usage), 0; got != want {
		t.Fatalf("managed usage records = %d, want %d", got, want)
	}
	if got, want := billing.balanceCalls, 1; got != want {
		t.Fatalf("balance calls = %d, want %d", got, want)
	}
}

func TestReconcileManagedComputeBillsExpiredReservationBeforeTerminating(t *testing.T) {
	now := time.Now().UTC()
	cursor := now.Add(-10 * time.Minute)
	expiresAt := now.Add(-time.Minute)
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Provider:         "shadeform",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationActive,
				CreatedAt:        now.Add(-time.Hour),
				BillingCursorAt:  cursor,
				ExpiresAt:        expiresAt,
				HourlyCostMicros: 1_000_000,
			},
		},
	}
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {state}}}
	billing := &fakeManagedBilling{balanceDecision: billingDecision{OK: true, RequiredCents: 1, AvailableCents: 1000}}
	service := &Service{computeRepo: repo, billing: billing}

	if err := service.ReconcileManagedCompute(context.Background()); err != nil {
		t.Fatalf("ReconcileManagedCompute() error = %v", err)
	}

	if got, want := len(billing.usage), 1; got != want {
		t.Fatalf("managed usage records = %d, want %d", got, want)
	}
	if got, want := billing.usage[0].DurationSeconds, float64(9*time.Minute/time.Second); got != want {
		t.Fatalf("managed usage duration = %f, want %f", got, want)
	}
	if !state.Reservations[0].BillingCursorAt.Equal(expiresAt) {
		t.Fatalf("billing cursor = %s, want %s", state.Reservations[0].BillingCursorAt, expiresAt)
	}
	if got, want := state.Reservations[0].Status, model.ReservationTerminating; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
}

func TestReservationBillingWindowSkipsTerminatingReservation(t *testing.T) {
	now := time.Now().UTC()
	reservation := &model.Reservation{
		ID:              "reservation-1",
		Source:          model.SourceCLIReservation,
		Status:          model.ReservationTerminating,
		CreatedAt:       now.Add(-time.Hour),
		BillingCursorAt: now.Add(-time.Minute),
		ExpiresAt:       now.Add(time.Hour),
	}

	if _, _, ok := reservationBillingWindow(reservation, now); ok {
		t.Fatal("terminating reservation should not have a billing window")
	}
}

func TestReconcileManagedComputeDoesNotTerminateOnBillingError(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationActive,
				CreatedAt:        now.Add(-time.Hour),
				ExpiresAt:        now.Add(time.Hour),
				HourlyCostMicros: 1_000_000,
			},
		},
	}
	machine := &model.AgentTokenState{
		WorkspaceID:     "workspace-1",
		PoolName:        "pool-1",
		MachineID:       "machine-1",
		Schedulable:     true,
		LastHeartbeatAt: now,
	}
	repo := &fakeComputeRepo{
		pools:    map[string][]*model.PoolState{"workspace-1": {state}},
		machines: map[string][]*model.AgentTokenState{fakeComputeKey("workspace-1", "pool-1"): {machine}},
	}
	service := &Service{
		computeRepo: repo,
		billing:     &fakeManagedBilling{balanceErr: errors.New("billing unavailable")},
	}

	if err := service.ReconcileManagedCompute(context.Background()); err != nil {
		t.Fatalf("ReconcileManagedCompute() error = %v", err)
	}

	if got, want := state.Reservations[0].Status, model.ReservationActive; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
	if !machine.Schedulable {
		t.Fatal("billing error marked the machine unschedulable")
	}
	if state.Reservations[0].TerminatingReason != "" {
		t.Fatalf("billing error set terminating reason = %q", state.Reservations[0].TerminatingReason)
	}
}

func TestReconcileManagedComputeKeepsSameSecondHeartbeatConnected(t *testing.T) {
	reconcileAt := time.Date(2026, 6, 13, 17, 19, 47, 700*int(time.Millisecond), time.UTC)
	heartbeatAt := reconcileAt.Truncate(time.Second).Add(650 * time.Millisecond)
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
	}
	machine := &model.AgentTokenState{
		WorkspaceID:     "workspace-1",
		PoolName:        "pool-1",
		MachineID:       "machine-1",
		Executor:        types.DefaultAgentWorkerContainerMode,
		Schedulable:     true,
		LastJoinAt:      reconcileAt.Add(-time.Minute),
		LastHeartbeatAt: heartbeatAt,
	}
	workerID := model.AgentMachineWorkerID(machine.MachineID)
	workerRepo := &fakeWorkerRepo{
		worker: &types.Worker{
			Id:        workerID,
			Status:    types.WorkerStatusAvailable,
			MachineId: machine.MachineID,
			PoolName:  machine.PoolName,
		},
	}
	service := &Service{
		computeRepo: &fakeComputeRepo{
			pools: map[string][]*model.PoolState{"workspace-1": {state}},
			machines: map[string][]*model.AgentTokenState{
				fakeComputeKey("workspace-1", "pool-1"): {machine},
			},
		},
		workerRepo: workerRepo,
	}

	if err := service.reconcileManagedComputeAt(context.Background(), reconcileAt); err != nil {
		t.Fatalf("reconcileManagedComputeAt() error = %v", err)
	}
	if got, want := workerRepo.worker.Status, types.WorkerStatusAvailable; got != want {
		t.Fatalf("worker status = %q, want %q", got, want)
	}
	if got := workerRepo.status; got != "" {
		t.Fatalf("worker status update = %q, want none", got)
	}
}

func TestReconcileManagedComputeUsesFreshTimePerPool(t *testing.T) {
	firstPoolTime := time.Date(2026, 6, 13, 17, 19, 47, 0, time.UTC)
	secondPoolTime := firstPoolTime.Add(70 * time.Second)
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "live-pool",
	}
	machine := &model.AgentTokenState{
		WorkspaceID:     "workspace-1",
		PoolName:        "live-pool",
		MachineID:       "machine-1",
		Executor:        types.DefaultAgentWorkerContainerMode,
		Schedulable:     true,
		LastJoinAt:      firstPoolTime.Add(-time.Minute),
		LastHeartbeatAt: secondPoolTime.Add(-5 * time.Second),
	}
	workerID := model.AgentMachineWorkerID(machine.MachineID)
	workerRepo := &fakeWorkerRepo{
		worker: &types.Worker{
			Id:        workerID,
			Status:    types.WorkerStatusAvailable,
			MachineId: machine.MachineID,
			PoolName:  machine.PoolName,
		},
	}
	service := &Service{
		computeRepo: &fakeComputeRepo{
			pools: map[string][]*model.PoolState{
				"workspace-1": {
					{WorkspaceID: "workspace-1", Name: "slow-pool"},
					state,
				},
			},
			machines: map[string][]*model.AgentTokenState{
				fakeComputeKey("workspace-1", "live-pool"): {machine},
			},
		},
		workerRepo: workerRepo,
	}
	clockCalls := 0

	if err := service.reconcileManagedComputeWithClock(context.Background(), func() time.Time {
		clockCalls++
		if clockCalls == 1 {
			return firstPoolTime
		}
		return secondPoolTime
	}); err != nil {
		t.Fatalf("reconcileManagedComputeWithClock() error = %v", err)
	}
	if got, want := workerRepo.worker.Status, types.WorkerStatusAvailable; got != want {
		t.Fatalf("worker status = %q, want %q", got, want)
	}
	if got := workerRepo.status; got != "" {
		t.Fatalf("worker status update = %q, want none", got)
	}
	if got, want := clockCalls, 2; got != want {
		t.Fatalf("clock calls = %d, want %d", got, want)
	}
}

func TestReconcileManagedComputeRemovesMachineForDeletedReservation(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Reservations: []model.Reservation{
			{
				ID:        "reservation-1",
				Provider:  "shadeform",
				Source:    model.SourceCLIReservation,
				Status:    model.ReservationDeleted,
				MachineID: "machine-1",
			},
		},
	}
	machine := &model.AgentTokenState{
		WorkspaceID:     "workspace-1",
		PoolName:        "pool-1",
		MachineID:       "machine-1",
		Schedulable:     true,
		LastHeartbeatAt: now,
	}
	repo := &fakeComputeRepo{
		pools:    map[string][]*model.PoolState{"workspace-1": {state}},
		machines: map[string][]*model.AgentTokenState{fakeComputeKey("workspace-1", "pool-1"): {machine}},
	}
	service := &Service{computeRepo: repo}

	if err := service.ReconcileManagedCompute(context.Background()); err != nil {
		t.Fatalf("ReconcileManagedCompute() error = %v", err)
	}
	if got := len(repo.machines[fakeComputeKey("workspace-1", "pool-1")]); got != 0 {
		t.Fatalf("machine count after reconcile = %d, want 0", got)
	}
	if !repo.savedPool {
		t.Fatal("ReconcileManagedCompute() did not persist closed reservation cleanup")
	}
}

func TestReleasePrivateMachineTransitionsManagedReservation(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Reservations: []model.Reservation{
			{
				ID:                    "reservation-1",
				Provider:              "shadeform",
				InstanceID:            "instance-1",
				MachineID:             "machine-1",
				Source:                model.SourceCLIReservation,
				Status:                model.ReservationActive,
				CreatedAt:             now.Add(-time.Hour),
				ExpiresAt:             now.Add(time.Hour),
				HourlyCostMicros:      1_000_000,
				RegistrationTokenHash: "join-token-hash",
			},
		},
	}
	machine := &model.AgentTokenState{
		WorkspaceID:     "workspace-1",
		PoolName:        "pool-1",
		MachineID:       "machine-1",
		Schedulable:     true,
		LastHeartbeatAt: now,
	}
	repo := &fakeComputeRepo{
		pools:      map[string][]*model.PoolState{"workspace-1": {state}},
		machines:   map[string][]*model.AgentTokenState{fakeComputeKey("workspace-1", "pool-1"): {machine}},
		joinTokens: map[string]*model.JoinTokenState{"join-token-hash": {TokenHash: "join-token-hash", ExpiresAt: now.Add(time.Hour)}},
	}
	billing := &fakeManagedBilling{}
	service := &Service{computeRepo: repo, billing: billing}

	if err := service.releasePrivateMachine(context.Background(), machine); err != nil {
		t.Fatalf("releasePrivateMachine() error = %v", err)
	}

	saved := repo.pools["workspace-1"][0]
	if !repo.savedPool {
		t.Fatal("releasePrivateMachine() did not persist the reservation transition")
	}
	if got, want := saved.Reservations[0].Status, model.ReservationTerminating; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
	if got, want := saved.Reservations[0].TerminatingReason, reconcileReasonMachineReleased; got != want {
		t.Fatalf("terminating reason = %q, want %q", got, want)
	}
	if got, want := saved.Reservations[0].MachineID, "machine-1"; got != want {
		t.Fatalf("reservation machine id = %q, want %q", got, want)
	}
	if !strings.Contains(saved.Reservations[0].LastError, "vendor \"shadeform\" is not configured") {
		t.Fatalf("reservation last error = %q, want missing vendor error", saved.Reservations[0].LastError)
	}
	if machine.Schedulable {
		t.Fatal("releasePrivateMachine() did not mark the machine unschedulable before deleting it")
	}
	if got := len(repo.machines[fakeComputeKey("workspace-1", "pool-1")]); got != 0 {
		t.Fatalf("machine count after release = %d, want 0", got)
	}
	if got, want := len(billing.usage), 1; got != want {
		t.Fatalf("managed usage records = %d, want %d", got, want)
	}
	if token := repo.joinTokens["join-token-hash"]; token == nil || !token.Revoked {
		t.Fatal("releasePrivateMachine() did not revoke the reservation join token")
	}
}

func TestReleasePrivateMachineLinksSingleUnassignedManagedReservation(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Reservations: []model.Reservation{
			{
				ID:         "reservation-1",
				Provider:   "shadeform",
				InstanceID: "instance-1",
				Source:     model.SourceCLIReservation,
				Status:     model.ReservationActive,
				CreatedAt:  now.Add(-time.Hour),
				ExpiresAt:  now.Add(time.Hour),
			},
		},
	}
	machine := &model.AgentTokenState{
		WorkspaceID:     "workspace-1",
		PoolName:        "pool-1",
		MachineID:       "machine-1",
		Schedulable:     true,
		LastHeartbeatAt: now,
	}
	repo := &fakeComputeRepo{
		pools:    map[string][]*model.PoolState{"workspace-1": {state}},
		machines: map[string][]*model.AgentTokenState{fakeComputeKey("workspace-1", "pool-1"): {machine}},
	}
	service := &Service{computeRepo: repo}

	if err := service.releasePrivateMachine(context.Background(), machine); err != nil {
		t.Fatalf("releasePrivateMachine() error = %v", err)
	}
	if got, want := state.Reservations[0].Status, model.ReservationTerminating; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
	if got, want := state.Reservations[0].MachineID, "machine-1"; got != want {
		t.Fatalf("reservation machine id = %q, want %q", got, want)
	}
	if machine.Schedulable {
		t.Fatal("releasePrivateMachine() did not mark the machine unschedulable")
	}
	if got := len(repo.machines[fakeComputeKey("workspace-1", "pool-1")]); got != 0 {
		t.Fatalf("machine count after release = %d, want 0", got)
	}
}

func TestReleasePrivateMachineRejectsAmbiguousUnassignedManagedReservations(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Reservations: []model.Reservation{
			{
				ID:         "reservation-1",
				Provider:   "shadeform",
				InstanceID: "instance-1",
				Source:     model.SourceCLIReservation,
				Status:     model.ReservationActive,
				CreatedAt:  now.Add(-time.Hour),
				ExpiresAt:  now.Add(time.Hour),
			},
			{
				ID:         "reservation-2",
				Provider:   "shadeform",
				InstanceID: "instance-2",
				Source:     model.SourceCLIReservation,
				Status:     model.ReservationActive,
				CreatedAt:  now.Add(-time.Hour),
				ExpiresAt:  now.Add(time.Hour),
			},
		},
	}
	machine := &model.AgentTokenState{
		WorkspaceID:     "workspace-1",
		PoolName:        "pool-1",
		MachineID:       "machine-1",
		Schedulable:     true,
		LastHeartbeatAt: now,
	}
	repo := &fakeComputeRepo{
		pools:    map[string][]*model.PoolState{"workspace-1": {state}},
		machines: map[string][]*model.AgentTokenState{fakeComputeKey("workspace-1", "pool-1"): {machine}},
	}
	service := &Service{computeRepo: repo}

	err := service.releasePrivateMachine(context.Background(), machine)
	if err == nil || !strings.Contains(err.Error(), "managed reservation for machine \"machine-1\" is ambiguous") {
		t.Fatalf("releasePrivateMachine() error = %v, want ambiguous reservation error", err)
	}
	if got, want := state.Reservations[0].Status, model.ReservationActive; got != want {
		t.Fatalf("reservation[0] status = %q, want %q", got, want)
	}
	if got, want := state.Reservations[1].Status, model.ReservationActive; got != want {
		t.Fatalf("reservation[1] status = %q, want %q", got, want)
	}
	if !machine.Schedulable {
		t.Fatal("failed release should not mark the machine unschedulable")
	}
	if got := len(repo.machines[fakeComputeKey("workspace-1", "pool-1")]); got != 1 {
		t.Fatalf("machine count after failed release = %d, want 1", got)
	}
}

func TestDeletePoolMachineReleasesManagedReservationID(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID:      "workspace-1",
		Name:             "pool-1",
		CreatedByTokenID: "token-owner",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Provider:         "shadeform",
				InstanceID:       "instance-1",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationPending,
				CreatedAt:        now.Add(-time.Minute),
				ExpiresAt:        now.Add(time.Hour),
				HourlyCostMicros: 1_000_000,
			},
		},
	}
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{"workspace-1": {state}},
	}
	service := &Service{computeRepo: repo}

	_, res, err := service.DeletePoolMachine(
		testAuthContext("workspace-1", "token-owner"),
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: "reservation-1"},
	)
	if err != nil {
		t.Fatalf("DeletePoolMachine() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePoolMachine() response = %#v", res)
	}
	if !repo.savedPool {
		t.Fatal("DeletePoolMachine() did not persist the reservation transition")
	}
	if got, want := state.Reservations[0].Status, model.ReservationTerminating; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
	if got, want := state.Reservations[0].TerminatingReason, reconcileReasonMachineReleased; got != want {
		t.Fatalf("terminating reason = %q, want %q", got, want)
	}
	if !strings.Contains(state.Reservations[0].LastError, "vendor \"shadeform\" is not configured") {
		t.Fatalf("reservation last error = %q, want missing vendor error", state.Reservations[0].LastError)
	}
}

func TestDeletePoolMachineReleasesManagedMachineID(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID:      "workspace-1",
		Name:             "pool-1",
		CreatedByTokenID: "token-owner",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Provider:         "shadeform",
				InstanceID:       "instance-1",
				MachineID:        "machine-1",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationPending,
				CreatedAt:        now.Add(-time.Minute),
				ExpiresAt:        now.Add(time.Hour),
				HourlyCostMicros: 1_000_000,
			},
		},
	}
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{"workspace-1": {state}},
	}
	service := &Service{computeRepo: repo}

	_, res, err := service.DeletePoolMachine(
		testAuthContext("workspace-1", "token-owner"),
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: "machine-1"},
	)
	if err != nil {
		t.Fatalf("DeletePoolMachine() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePoolMachine() response = %#v", res)
	}
	if got, want := state.Reservations[0].Status, model.ReservationTerminating; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
}

func TestDeletePoolMachineReleasesManagedProviderInstanceID(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID:      "workspace-1",
		Name:             "pool-1",
		CreatedByTokenID: "token-owner",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Provider:         "shadeform",
				InstanceID:       "instance-1",
				MachineID:        "machine-1",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationPending,
				CreatedAt:        now.Add(-time.Minute),
				ExpiresAt:        now.Add(time.Hour),
				HourlyCostMicros: 1_000_000,
			},
		},
	}
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{"workspace-1": {state}},
	}
	service := &Service{computeRepo: repo}

	_, res, err := service.DeletePoolMachine(
		testAuthContext("workspace-1", "token-owner"),
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: "instance-1"},
	)
	if err != nil {
		t.Fatalf("DeletePoolMachine() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePoolMachine() response = %#v", res)
	}
	if got, want := state.Reservations[0].Status, model.ReservationTerminating; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
}

func TestDeletePoolMachineByReservationIDCleansLinkedMachine(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID:      "workspace-1",
		Name:             "pool-1",
		CreatedByTokenID: "token-owner",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Provider:         "shadeform",
				InstanceID:       "instance-1",
				MachineID:        "machine-1",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationActive,
				CreatedAt:        now.Add(-time.Hour),
				ExpiresAt:        now.Add(time.Hour),
				HourlyCostMicros: 1_000_000,
			},
		},
	}
	machine := &model.AgentTokenState{
		WorkspaceID:     "workspace-1",
		PoolName:        "pool-1",
		MachineID:       "machine-1",
		Schedulable:     true,
		LastHeartbeatAt: now,
	}
	repo := &fakeComputeRepo{
		pools:    map[string][]*model.PoolState{"workspace-1": {state}},
		machines: map[string][]*model.AgentTokenState{fakeComputeKey("workspace-1", "pool-1"): {machine}},
	}
	service := &Service{computeRepo: repo}

	_, res, err := service.DeletePoolMachine(
		testAuthContext("workspace-1", "token-owner"),
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: "reservation-1"},
	)
	if err != nil {
		t.Fatalf("DeletePoolMachine() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePoolMachine() response = %#v", res)
	}
	if got, want := state.Reservations[0].Status, model.ReservationTerminating; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
	if machine.Schedulable {
		t.Fatal("DeletePoolMachine() did not mark linked machine unschedulable")
	}
	if got := len(repo.machines[fakeComputeKey("workspace-1", "pool-1")]); got != 0 {
		t.Fatalf("machine count after release = %d, want 0", got)
	}
}

func TestAssignManagedReservationToMachineUsesJoinTokenHash(t *testing.T) {
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Reservations: []model.Reservation{
			{
				ID:                    "reservation-1",
				Provider:              "shadeform",
				Source:                model.SourceCLIReservation,
				Status:                model.ReservationPending,
				RegistrationTokenHash: "token-hash",
			},
		},
	}
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{"workspace-1": {state}},
	}
	service := &Service{computeRepo: repo}

	err := service.assignManagedReservationToMachine(
		context.Background(),
		state,
		&model.JoinTokenState{TokenHash: "token-hash"},
		&model.AgentTokenState{WorkspaceID: "workspace-1", PoolName: "pool-1", MachineID: "machine-1"},
	)
	if err != nil {
		t.Fatalf("assignManagedReservationToMachine() error = %v", err)
	}

	if !repo.savedPool {
		t.Fatal("assignManagedReservationToMachine() did not persist the pool state")
	}
	if got, want := state.Reservations[0].MachineID, "machine-1"; got != want {
		t.Fatalf("reservation machine id = %q, want %q", got, want)
	}
}

func TestAssignManagedReservationToMachineRejectsClosedManagedJoinToken(t *testing.T) {
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Reservations: []model.Reservation{
			{
				ID:                    "reservation-1",
				Provider:              "shadeform",
				Source:                model.SourceCLIReservation,
				Status:                model.ReservationDeleted,
				MachineID:             "machine-1",
				RegistrationTokenHash: "join-token-hash",
			},
		},
	}
	token := &model.JoinTokenState{
		TokenHash: "join-token-hash",
		MachineID: "machine-1",
	}
	machine := &model.AgentTokenState{MachineID: "machine-1"}

	err := (&Service{}).assignManagedReservationToMachine(context.Background(), state, token, machine)
	if err == nil {
		t.Fatal("assignManagedReservationToMachine() accepted a closed managed reservation")
	}
}

func testAuthContext(workspaceID, tokenID string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: workspaceID},
		Token:     &types.Token{ExternalId: tokenID},
	})
}

func poolNames(pools []*pb.PrivatePool) []string {
	names := make([]string, 0, len(pools))
	for _, pool := range pools {
		if pool != nil {
			names = append(names, pool.Name)
		}
	}
	return names
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := map[string]int{}
	for _, value := range a {
		counts[value]++
	}
	for _, value := range b {
		counts[value]--
		if counts[value] < 0 {
			return false
		}
	}
	return true
}

func managedUsageTestState(now, cursor, expiresAt time.Time, hourlyCostMicros int64) *model.PoolState {
	return &model.PoolState{
		Name: "pool-1",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Provider:         "shadeform",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationActive,
				CreatedAt:        now.Add(-time.Hour),
				BillingCursorAt:  cursor,
				ExpiresAt:        expiresAt,
				HourlyCostMicros: hourlyCostMicros,
			},
		},
	}
}

type fakeComputeRepo struct {
	pools      map[string][]*model.PoolState
	machines   map[string][]*model.AgentTokenState
	joinTokens map[string]*model.JoinTokenState
	savedPool  bool
}

func (r *fakeComputeRepo) LockPoolState(ctx context.Context, workspaceID, name string) error {
	return nil
}

func (r *fakeComputeRepo) UnlockPoolState(ctx context.Context, workspaceID, name string) error {
	return nil
}

func (r *fakeComputeRepo) PruneAgentMachineIndex(ctx context.Context, workspaceID, poolName string) error {
	return nil
}

func (r *fakeComputeRepo) SavePoolState(ctx context.Context, workspaceID string, state *model.PoolState) error {
	r.savedPool = true
	if state == nil {
		return nil
	}
	if r.pools == nil {
		r.pools = map[string][]*model.PoolState{}
	}
	state.WorkspaceID = workspaceID
	for i, pool := range r.pools[workspaceID] {
		if pool != nil && pool.Name == state.Name {
			r.pools[workspaceID][i] = state
			return nil
		}
	}
	r.pools[workspaceID] = append(r.pools[workspaceID], state)
	return nil
}

func (r *fakeComputeRepo) GetPoolState(ctx context.Context, workspaceID, name string) (*model.PoolState, error) {
	for _, pool := range r.pools[workspaceID] {
		if pool != nil && pool.Name == name {
			return pool, nil
		}
	}
	return nil, nil
}

func (r *fakeComputeRepo) ListPoolStates(ctx context.Context, workspaceID string, limit int) ([]*model.PoolState, error) {
	pools := append([]*model.PoolState(nil), r.pools[workspaceID]...)
	if limit > 0 && len(pools) > limit {
		pools = pools[:limit]
	}
	return pools, nil
}

func (r *fakeComputeRepo) ListAllPoolStates(ctx context.Context, limit int) ([]*model.PoolState, error) {
	pools := []*model.PoolState{}
	for workspaceID, states := range r.pools {
		for _, state := range states {
			if state != nil && state.WorkspaceID == "" {
				state.WorkspaceID = workspaceID
			}
			pools = append(pools, state)
			if limit > 0 && len(pools) >= limit {
				return pools, nil
			}
		}
	}
	return pools, nil
}

func (r *fakeComputeRepo) DeletePoolState(ctx context.Context, workspaceID, name string) error {
	return nil
}

func (r *fakeComputeRepo) SaveJoinTokenState(ctx context.Context, state *model.JoinTokenState, ttl time.Duration) error {
	if state == nil {
		return nil
	}
	if r.joinTokens == nil {
		r.joinTokens = map[string]*model.JoinTokenState{}
	}
	r.joinTokens[state.TokenHash] = state
	return nil
}

func (r *fakeComputeRepo) GetJoinTokenState(ctx context.Context, tokenHash string) (*model.JoinTokenState, error) {
	return r.joinTokens[tokenHash], nil
}

func (r *fakeComputeRepo) SaveAgentTokenState(ctx context.Context, state *model.AgentTokenState, ttl time.Duration) error {
	if state == nil {
		return nil
	}
	if r.machines == nil {
		r.machines = map[string][]*model.AgentTokenState{}
	}
	key := fakeComputeKey(state.WorkspaceID, state.PoolName)
	for i, machine := range r.machines[key] {
		if machine != nil && machine.MachineID == state.MachineID {
			r.machines[key][i] = state
			return nil
		}
	}
	r.machines[key] = append(r.machines[key], state)
	return nil
}

func (r *fakeComputeRepo) GetAgentTokenState(ctx context.Context, tokenHash string) (*model.AgentTokenState, error) {
	return nil, nil
}

func (r *fakeComputeRepo) GetAgentMachineState(ctx context.Context, workspaceID, poolName, machineID string) (*model.AgentTokenState, error) {
	for _, machine := range r.machines[fakeComputeKey(workspaceID, poolName)] {
		if machine != nil && machine.MachineID == machineID {
			return machine, nil
		}
	}
	return nil, nil
}

func (r *fakeComputeRepo) GetAgentMachineStateForWorkspace(ctx context.Context, workspaceID, machineID string) (*model.AgentTokenState, error) {
	for key, machines := range r.machines {
		if !strings.HasPrefix(key, workspaceID+"\x00") {
			continue
		}
		for _, machine := range machines {
			if machine != nil && machine.MachineID == machineID {
				return machine, nil
			}
		}
	}
	return nil, nil
}

func (r *fakeComputeRepo) ListAgentTokenStates(ctx context.Context, workspaceID, poolName string) ([]*model.AgentTokenState, error) {
	return append([]*model.AgentTokenState(nil), r.machines[fakeComputeKey(workspaceID, poolName)]...), nil
}

func (r *fakeComputeRepo) DeleteAgentMachineState(ctx context.Context, workspaceID, poolName, machineID string) error {
	key := fakeComputeKey(workspaceID, poolName)
	machines := r.machines[key]
	kept := machines[:0]
	for _, machine := range machines {
		if machine == nil || machine.MachineID != machineID {
			kept = append(kept, machine)
		}
	}
	if len(kept) == 0 {
		delete(r.machines, key)
		return nil
	}
	r.machines[key] = kept
	return nil
}

func (r *fakeComputeRepo) SaveAgentWorkerSlotState(ctx context.Context, state *model.AgentWorkerSlotState) error {
	return nil
}

func (r *fakeComputeRepo) ListAgentWorkerSlotStates(ctx context.Context, workspaceID, poolName, machineID string) ([]*model.AgentWorkerSlotState, error) {
	return nil, nil
}

func (r *fakeComputeRepo) DeleteAgentWorkerSlotState(ctx context.Context, workspaceID, poolName, machineID, workerID string) error {
	return nil
}

func fakeComputeKey(workspaceID, poolName string) string {
	return workspaceID + "\x00" + poolName
}

func TestManagedBillingURLMatchesInternalAPIActionRoutes(t *testing.T) {
	base := "https://api.stage.beam.cloud/v2/payment/managed-compute/"
	tests := map[string]string{
		"launch-check/": "https://api.stage.beam.cloud/v2/payment/managed-compute/launch-check/",
		"balance/":      "https://api.stage.beam.cloud/v2/payment/managed-compute/balance/",
		"usage/":        "https://api.stage.beam.cloud/v2/payment/managed-compute/usage/",
	}

	for path, want := range tests {
		if got := joinBillingURL(base, path); got != want {
			t.Fatalf("joinBillingURL(%q, %q) = %q, want %q", base, path, got, want)
		}
	}
}

type fakeManagedBilling struct {
	launchDecision       billingDecision
	launchErr            error
	launchCalls          int
	launchRequest        billingCreditRequest
	balanceDecision      billingDecision
	balanceErr           error
	balanceCalls         int
	balanceSawUsageCount int
	usage                []managedUsage
	usageErr             error
}

func (b *fakeManagedBilling) CheckLaunchCredit(_ context.Context, req billingCreditRequest) (billingDecision, error) {
	b.launchCalls++
	b.launchRequest = req
	return b.launchDecision, b.launchErr
}

func (b *fakeManagedBilling) CheckBalance(context.Context, string) (billingDecision, error) {
	b.balanceCalls++
	b.balanceSawUsageCount = len(b.usage)
	return b.balanceDecision, b.balanceErr
}

func (b *fakeManagedBilling) RecordManagedUsage(_ context.Context, usage managedUsage) error {
	b.usage = append(b.usage, usage)
	return b.usageErr
}

type fakeUsageMetricsRepo struct {
	counters []fakeUsageCounter
	err      error
}

type fakeUsageCounter struct {
	name     string
	metadata map[string]interface{}
	value    float64
}

func (r *fakeUsageMetricsRepo) Init(string) error {
	return nil
}

func (r *fakeUsageMetricsRepo) IncrementCounter(name string, metadata map[string]interface{}, value float64) error {
	if r.err != nil {
		return r.err
	}
	r.counters = append(r.counters, fakeUsageCounter{name: name, metadata: metadata, value: value})
	return nil
}

func (r *fakeUsageMetricsRepo) SetGauge(name string, metadata map[string]interface{}, value float64) error {
	if r.err != nil {
		return r.err
	}
	return nil
}

type fakeWorkerRepo struct {
	repository.WorkerRepository
	worker *types.Worker
	status types.WorkerStatus
}

func (r *fakeWorkerRepo) GetWorkerById(workerID string) (*types.Worker, error) {
	if r.worker == nil || r.worker.Id != workerID {
		return nil, &types.ErrWorkerNotFound{WorkerId: workerID}
	}
	return r.worker, nil
}

func (r *fakeWorkerRepo) UpdateWorkerStatus(workerID string, status types.WorkerStatus) error {
	if r.worker == nil || r.worker.Id != workerID {
		return &types.ErrWorkerNotFound{WorkerId: workerID}
	}
	r.worker.Status = status
	r.status = status
	return nil
}

type fakeContainerRepo struct {
	repository.ContainerRepository
	containers []types.ContainerState
	stopped    []string
}

func (r *fakeContainerRepo) GetActiveContainersByWorkerId(string) ([]types.ContainerState, error) {
	return append([]types.ContainerState(nil), r.containers...), nil
}

func (r *fakeContainerRepo) UpdateContainerStatus(containerID string, status types.ContainerStatus, _ int64) error {
	if status == types.ContainerStatusStopping {
		r.stopped = append(r.stopped, containerID)
	}
	return nil
}
