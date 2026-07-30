package compute

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
)

func onDemandTestService(config types.FailoverConfig) (*Service, *fakeComputeRepo) {
	managedStates := &fakeComputeRepo{}
	return &Service{
		appConfig: types.AppConfig{
			Scheduling: types.SchedulingConfig{Failover: config},
		},
		backendRepo:     &fakeManagedPoolBackendRepo{},
		computeRepo:     &fakeComputeRepo{},
		managedPoolRepo: &fakeManagedPoolRepo{repo: managedStates},
	}, managedStates
}

type failFinalSaveManagedPoolRepo struct {
	*fakeManagedPoolRepo
	saveCalls int
}

func (r *failFinalSaveManagedPoolRepo) SaveManagedPoolState(ctx context.Context, workspaceID string, state *model.PoolState) error {
	r.saveCalls++
	if r.saveCalls == 3 {
		return errors.New("redis unavailable")
	}
	// Model the serialization boundary of the real Redis repository. The
	// reconciler mutates its in-memory state after the first save, but those
	// mutations are not durable unless the second save succeeds.
	snapshot := *state
	snapshot.Reservations = append([]model.Reservation(nil), state.Reservations...)
	return r.fakeManagedPoolRepo.SaveManagedPoolState(ctx, workspaceID, &snapshot)
}

func testOnDemandConfig() types.FailoverConfig {
	return types.FailoverConfig{
		Enabled: true,
		Chains: map[string]types.FailoverChain{
			"A10G": {
				OnDemand: &types.FailoverOnDemandStep{
					GPUs:     []types.GpuType{types.GPU_A10G, types.GPU_RTX4090},
					MaxNodes: 2,
				},
			},
		},
		OnDemand: types.OnDemandConfig{
			Budget:             types.OnDemandBudgetConfig{MaxHourlyCents: 300, MaxDailyCents: 2000},
			ScaleDownAfterIdle: 5 * time.Minute,
		},
	}
}

func TestOnDemandPoolStateIsCreatedOnlyForDemand(t *testing.T) {
	service, states := onDemandTestService(testOnDemandConfig())
	now := time.Now().UTC()

	state, err := service.onDemandPoolState(context.Background(), "admin-workspace", "A10G", "ondemand-a10g", false, now)
	if err != nil || state != nil {
		t.Fatalf("onDemandPoolState(create=false) = %#v, %v", state, err)
	}
	if got := len(states.pools["admin-workspace"]); got != 0 {
		t.Fatalf("managed pools without demand = %d, want 0", got)
	}

	state, err = service.onDemandPoolState(context.Background(), "admin-workspace", "A10G", "ondemand-a10g", true, now)
	if err != nil {
		t.Fatalf("onDemandPoolState(create=true) error = %v", err)
	}
	if state == nil || state.CreatedByTokenID != onDemandPoolCreator || state.ManagedInstanceID == "" {
		t.Fatalf("unexpected on-demand pool state: %#v", state)
	}
	if state.WorkerConfig == nil || !state.WorkerConfig.RequiresPoolSelector {
		t.Fatalf("on-demand failover pool must not advertise native serverless capacity: %#v", state.WorkerConfig)
	}
	active, err := service.activeManagedPoolState(state)
	if err != nil {
		t.Fatalf("activeManagedPoolState() error = %v", err)
	}
	if active == nil || active.WorkerConfig == nil || !active.WorkerConfig.RequiresPoolSelector {
		t.Fatalf("active on-demand pool must remain selector-bound: %#v", active)
	}
}

func TestOnDemandPoolStateDoesNotAdoptOperatorPool(t *testing.T) {
	service, states := onDemandTestService(testOnDemandConfig())
	existing := newManagedPoolState(
		"admin-workspace",
		"ondemand-a10g",
		types.WorkerPoolManagementSourceAPI,
		"operator-token",
		types.WorkerPoolConfig{GPUType: "A10G", Mode: types.PoolModeExternal},
		time.Now().UTC(),
	)
	if err := states.SavePoolState(context.Background(), "admin-workspace", existing); err != nil {
		t.Fatal(err)
	}

	if _, err := service.onDemandPoolState(context.Background(), "admin-workspace", "A10G", "ondemand-a10g", true, time.Now().UTC()); err == nil {
		t.Fatal("operator-created pool was adopted by failover reconciler")
	}
}

func TestOnDemandHourlyHeadroomUsesTotalTTLSpendCeiling(t *testing.T) {
	if got, want := onDemandMaxSpendMicros(300, 24*time.Hour), types.CentsToMicros(300)*24; got != want {
		t.Fatalf("onDemandMaxSpendMicros() = %d, want %d", got, want)
	}
	if got := onDemandMaxSpendMicros(0, 24*time.Hour); got != 0 {
		t.Fatalf("unlimited onDemandMaxSpendMicros() = %d, want 0", got)
	}
}

func TestOnDemandBudgetHeadroomCountsAllOwnedPools(t *testing.T) {
	config := testOnDemandConfig()
	service, states := onDemandTestService(config)
	state := newManagedPoolState(
		"admin-workspace",
		"ondemand-a10g",
		types.WorkerPoolManagementSourceAPI,
		onDemandPoolCreator,
		types.WorkerPoolConfig{GPUType: "A10G", Mode: types.PoolModeExternal},
		time.Now().UTC(),
	)
	state.Reservations = []model.Reservation{{
		Provider:         "shadeform",
		Source:           model.SourceCLIReservation,
		Status:           model.ReservationPending,
		HourlyCostMicros: types.CentsToMicros(120),
		CreatedAt:        time.Now().UTC(),
		ExpiresAt:        time.Now().UTC().Add(time.Hour),
	}}
	if err := states.SavePoolState(context.Background(), "admin-workspace", state); err != nil {
		t.Fatal(err)
	}

	headroom, ok, err := service.onDemandBudgetHeadroom(context.Background(), state, config, time.Now().UTC())
	if err != nil || !ok || headroom != 180 {
		t.Fatalf("onDemandBudgetHeadroom() = %d, %v, %v; want 180, true, nil", headroom, ok, err)
	}
}

func TestReconcileOnDemandFailoverLaunchesAlternateGPUWithinHourlyBudget(t *testing.T) {
	var createCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/instances/types":
			_, _ = w.Write([]byte(`{"instance_types":[{
				"id":"sf-rtx4090-1",
				"cloud":"test-cloud",
				"shade_instance_type":"RTX4090",
				"hourly_price":66,
				"deployment_type":"vm",
				"configuration":{"gpu_type":"RTX4090","num_gpus":1,"vcpus":12,"memory_in_gb":70,"storage_in_gb":128},
				"availability":[{"region":"test-region","available":true}]
			}]}`))
		case "/instances/create":
			createCalls++
			_, _ = w.Write([]byte(`{"id":"reservation-1"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	config := testOnDemandConfig()
	computeRepo := &fakeComputeRepo{demand: map[string]*model.FailoverDemand{
		"A10G": {GPU: "A10G", GPUCount: 1, CreatedAt: time.Now().UTC()},
	}}
	managedStates := &fakeComputeRepo{}
	service := &Service{
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				HTTP: types.HTTPConfig{ExternalHost: "app.beam.test", ExternalPort: 443, TLS: true},
			},
			Providers: types.ProviderConfig{
				Shadeform: types.ShadeformProviderConfig{ApiKey: "test-key", BaseURL: server.URL},
			},
			Scheduling: types.SchedulingConfig{Failover: config},
		},
		backendRepo:     &fakeManagedPoolBackendRepo{},
		computeRepo:     computeRepo,
		managedPoolRepo: &fakeManagedPoolRepo{repo: managedStates},
	}

	if err := service.reconcileOnDemandFailover(context.Background(), time.Now().UTC()); err != nil {
		t.Fatalf("reconcileOnDemandFailover() error = %v", err)
	}
	if createCalls != 1 {
		t.Fatalf("Shadeform create calls = %d, want 1", createCalls)
	}
	state, err := managedStates.GetPoolState(context.Background(), "admin-workspace", "ondemand-a10g")
	if err != nil || state == nil {
		t.Fatalf("managed on-demand state = %#v, %v", state, err)
	}
	if len(state.Reservations) != 1 || state.Reservations[0].GPU != "RTX4090" {
		t.Fatalf("unexpected reservations: %#v", state.Reservations)
	}
	if state.WorkerConfig == nil || state.WorkerConfig.GPUType != "RTX4090" {
		t.Fatalf("managed pool hardware profile = %#v, want RTX4090", state.WorkerConfig)
	}
	if state.Config == nil || len(state.Config.Gpu) != 1 || state.Config.Gpu[0] != "RTX4090" {
		t.Fatalf("managed pool config GPUs = %#v, want RTX4090", state.Config)
	}
	if _, exists := computeRepo.demand["A10G"]; exists {
		t.Fatal("demand was not cleared after durable reservation state")
	}
}

func TestReconcileOnDemandFailoverReportsOfferOutsideHourlyHeadroom(t *testing.T) {
	var createCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/instances/types":
			_, _ = w.Write([]byte(`{"instance_types":[{
				"id":"sf-rtx4090-1",
				"cloud":"test-cloud",
				"shade_instance_type":"RTX4090",
				"hourly_price":60,
				"deployment_type":"vm",
				"configuration":{"gpu_type":"RTX4090","num_gpus":1,"vcpus":12,"memory_in_gb":70,"storage_in_gb":128},
				"availability":[{"region":"test-region","available":true}]
			}]}`))
		case "/instances/create":
			createCalls++
			_, _ = w.Write([]byte(`{"id":"reservation-2"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	now := time.Now().UTC()
	config := testOnDemandConfig()
	config.OnDemand.Budget.MaxHourlyCents = 100
	computeRepo := &fakeComputeRepo{demand: map[string]*model.FailoverDemand{
		"A10G": {GPU: "A10G", GPUCount: 1, CreatedAt: now},
	}}
	managedStates := &fakeComputeRepo{}
	state := newManagedPoolState(
		"admin-workspace",
		"ondemand-a10g",
		types.WorkerPoolManagementSourceAPI,
		onDemandPoolCreator,
		types.WorkerPoolConfig{GPUType: "RTX4090", Mode: types.PoolModeExternal, RequiresPoolSelector: true},
		now.Add(-time.Minute),
	)
	state.Reservations = []model.Reservation{{
		Provider:         "shadeform",
		Source:           model.SourceCLIReservation,
		Status:           model.ReservationActive,
		GPU:              "RTX4090",
		GPUCount:         1,
		NodeCount:        1,
		HourlyCostMicros: types.CentsToMicros(60),
		CreatedAt:        now.Add(-time.Minute),
		ExpiresAt:        now.Add(time.Hour),
		BillingCursorAt:  now,
	}}
	if err := managedStates.SavePoolState(context.Background(), "admin-workspace", state); err != nil {
		t.Fatal(err)
	}
	service := &Service{
		appConfig: types.AppConfig{
			Providers: types.ProviderConfig{
				Shadeform: types.ShadeformProviderConfig{ApiKey: "test-key", BaseURL: server.URL},
			},
			Scheduling: types.SchedulingConfig{Failover: config},
		},
		backendRepo:     &fakeManagedPoolBackendRepo{},
		computeRepo:     computeRepo,
		managedPoolRepo: &fakeManagedPoolRepo{repo: managedStates},
	}

	if err := service.reconcileOnDemandFailover(context.Background(), now); err != nil {
		t.Fatalf("reconcileOnDemandFailover() error = %v", err)
	}
	if createCalls != 0 {
		t.Fatalf("Shadeform create calls = %d, want 0", createCalls)
	}
	saved, err := managedStates.GetPoolState(context.Background(), "admin-workspace", "ondemand-a10g")
	if err != nil || len(saved.Reservations) != 1 {
		t.Fatalf("managed reservations = %#v, %v; want one running node", saved, err)
	}
	if _, exists := computeRepo.demand["A10G"]; !exists {
		t.Fatal("budget-limited demand was cleared")
	}
}

func TestReconcileOnDemandFailoverCompensatesStateSaveFailure(t *testing.T) {
	var deleteCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/instances/types":
			_, _ = w.Write([]byte(`{"instance_types":[{
				"id":"sf-rtx4090-1",
				"cloud":"test-cloud",
				"shade_instance_type":"RTX4090",
				"hourly_price":66,
				"deployment_type":"vm",
				"configuration":{"gpu_type":"RTX4090","num_gpus":1,"vcpus":12,"memory_in_gb":70,"storage_in_gb":128},
				"availability":[{"region":"test-region","available":true}]
			}]}`))
		case "/instances/create":
			_, _ = w.Write([]byte(`{"id":"reservation-1"}`))
		case "/instances/reservation-1/delete":
			deleteCalls++
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	config := testOnDemandConfig()
	computeRepo := &fakeComputeRepo{demand: map[string]*model.FailoverDemand{
		"A10G": {GPU: "A10G", GPUCount: 1, CreatedAt: time.Now().UTC()},
	}}
	managedStates := &fakeComputeRepo{}
	managedRepo := &failFinalSaveManagedPoolRepo{
		fakeManagedPoolRepo: &fakeManagedPoolRepo{repo: managedStates},
	}
	service := &Service{
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				HTTP: types.HTTPConfig{ExternalHost: "app.beam.test", ExternalPort: 443, TLS: true},
			},
			Providers: types.ProviderConfig{
				Shadeform: types.ShadeformProviderConfig{ApiKey: "test-key", BaseURL: server.URL},
			},
			Scheduling: types.SchedulingConfig{Failover: config},
		},
		backendRepo:     &fakeManagedPoolBackendRepo{},
		computeRepo:     computeRepo,
		managedPoolRepo: managedRepo,
	}

	if err := service.reconcileOnDemandFailover(context.Background(), time.Now().UTC()); err == nil {
		t.Fatal("reconcile succeeded despite managed state save failure")
	}
	if deleteCalls != 1 {
		t.Fatalf("compensating provider deletes = %d, want 1", deleteCalls)
	}
	state, err := managedStates.GetPoolState(context.Background(), "admin-workspace", "ondemand-a10g")
	if err != nil || state == nil {
		t.Fatalf("managed pool after rollback = %#v, %v", state, err)
	}
	if len(state.Reservations) != 0 {
		t.Fatalf("failed reservation was persisted: %#v", state.Reservations)
	}
	if _, exists := computeRepo.demand["A10G"]; !exists {
		t.Fatal("demand was cleared before reservation state became durable")
	}
}

func TestOnDemandTerminationRetriesVendorDeletion(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{Name: "ondemand-a10g"}
	reservation := &model.Reservation{
		ID:       "reservation-1",
		Provider: "shadeform",
		Source:   model.SourceCLIReservation,
		Status:   model.ReservationPending,
	}
	vendor := &fakeVendor{deleteErr: errors.New("temporary provider failure")}
	service := &Service{}

	if !service.terminateOnDemandReservation(
		context.Background(), "admin-workspace", state, reservation,
		map[string]model.Vendor{"shadeform": vendor}, onDemandIdleReason, "idle", now,
	) {
		t.Fatal("failed deletion did not change reservation state")
	}
	if reservation.Status != model.ReservationTerminating {
		t.Fatalf("status after failed deletion = %q, want terminating", reservation.Status)
	}

	vendor.deleteErr = nil
	if !service.terminateOnDemandReservation(
		context.Background(), "admin-workspace", state, reservation,
		map[string]model.Vendor{"shadeform": vendor}, onDemandIdleReason, "idle", now.Add(time.Minute),
	) {
		t.Fatal("successful retry did not change reservation state")
	}
	if reservation.Status != model.ReservationDeleted {
		t.Fatalf("status after deletion retry = %q, want deleted", reservation.Status)
	}
	if len(vendor.deleted) != 2 {
		t.Fatalf("vendor delete calls = %d, want 2", len(vendor.deleted))
	}
}
