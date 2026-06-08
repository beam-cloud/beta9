package compute

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
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

	decision, err := service.checkManagedLaunchCredit(context.Background(), "workspace-1", "pool-1", plan)
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
	if req.EstimatedHourlyCostMicros != 3_000_000 {
		t.Fatalf("billing request hourly micros = %d, want 3000000", req.EstimatedHourlyCostMicros)
	}
	if req.EstimatedCommittedMicros != 42_000_000 {
		t.Fatalf("billing request committed micros = %d, want 42000000", req.EstimatedCommittedMicros)
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

type fakeComputeRepo struct {
	pools     map[string][]*model.PoolState
	machines  map[string][]*model.AgentTokenState
	savedPool bool
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
	return nil
}

func (r *fakeComputeRepo) GetJoinTokenState(ctx context.Context, tokenHash string) (*model.JoinTokenState, error) {
	return nil, nil
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

type fakeManagedBilling struct {
	launchDecision  billingDecision
	launchErr       error
	launchCalls     int
	launchRequest   billingCreditRequest
	balanceDecision billingDecision
	balanceErr      error
	usage           []managedUsage
	usageErr        error
}

func (b *fakeManagedBilling) CheckLaunchCredit(_ context.Context, req billingCreditRequest) (billingDecision, error) {
	b.launchCalls++
	b.launchRequest = req
	return b.launchDecision, b.launchErr
}

func (b *fakeManagedBilling) CheckBalance(context.Context, string) (billingDecision, error) {
	return b.balanceDecision, b.balanceErr
}

func (b *fakeManagedBilling) RecordManagedUsage(_ context.Context, usage managedUsage) error {
	b.usage = append(b.usage, usage)
	return b.usageErr
}
