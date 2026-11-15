package compute

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

// faultyPools is a repository in which reading or saving a pool's record, or
// saving a join token, fails with the error set for it.
type faultyPools struct {
	*fakeComputeRepo
	getErr, saveErr, tokenErr error
}

func (r *faultyPools) GetPoolState(ctx context.Context, workspaceID, name string) (*model.PoolState, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.fakeComputeRepo.GetPoolState(ctx, workspaceID, name)
}

func (r *faultyPools) SavePoolState(ctx context.Context, workspaceID string, state *model.PoolState) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	return r.fakeComputeRepo.SavePoolState(ctx, workspaceID, state)
}

func (r *faultyPools) SaveJoinTokenState(ctx context.Context, state *model.JoinTokenState, ttl time.Duration) error {
	// Revoking a token saves it once more, and that still works.
	if r.tokenErr != nil && !state.Revoked {
		return r.tokenErr
	}
	return r.fakeComputeRepo.SaveJoinTokenState(ctx, state, ttl)
}

// a10gPool is the record of a pool of one A10G machine that was bought.
func a10gPool(now time.Time) *model.PoolState {
	return &model.PoolState{
		WorkspaceID:          "workspace-1",
		Name:                 "pool-1",
		Selector:             "pool-1",
		Mode:                 string(types.PoolModePrivate),
		Source:               model.SourceCLIReservation,
		Config:               &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 1, Ttl: "1h", MaxSpend: 100},
		CreatedAt:            now.Add(-time.Minute),
		ReservedNodes:        1,
		CommittedSpendMicros: 1_650_000,
		Reservations: []model.Reservation{{
			ID: "reservation-1", InstanceID: "reservation-1", Provider: "shadeform", MachineID: "machine-1",
			GPU: "A10G", GPUCount: 1, NodeCount: 1, HourlyCostMicros: 1_500_000,
			Source: model.SourceCLIReservation, Status: model.ReservationActive,
			CreatedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
		}},
	}
}

func TestLaunchPoolCapacitySaysWhyItBoughtNothing(t *testing.T) {
	limited := a10gOffer
	limited.Available = 1
	for name, tt := range map[string]struct {
		offer               providers.Offer
		existing            *model.PoolState
		billing             *fakeManagedBilling
		getErr              error
		in                  *pb.PoolConfig
		code, message       string
		required, available int64
	}{
		"the name belongs to a pool of another kind": {
			existing: &model.PoolState{Name: "pool-1", Mode: string(types.PoolModeProvider)},
			message:  "pool already exists in this workspace",
		},
		"the pool's record cannot be read": {
			getErr:  errors.New("redis is down"),
			message: "redis is down",
		},
		"the pool has machines of another GPU": {
			existing: &model.PoolState{Name: "pool-1", Mode: string(types.PoolModePrivate), Config: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"H100"}}},
			code:     "pool_gpu_mismatch",
			message:  `pool "pool-1" is configured for H100 node attributes; create a new pool for A10G`,
		},
		"no provider has as many machines": {
			offer:   limited,
			in:      &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 2, Ttl: "1h", MaxSpend: 100},
			code:    "no_compatible_capacity",
			message: "insufficient compatible capacity",
		},
		"the machines cost more than the pool may spend": {
			in:      &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 1, Ttl: "1h", MaxSpend: 1},
			code:    "no_compatible_capacity",
			message: "max spend would be exceeded",
		},
		"billing cannot be asked": {
			billing:  &fakeManagedBilling{launchErr: errors.New("billing is down"), launchDecision: billingDecision{RequiredCents: 2500}},
			code:     "billing_unavailable",
			message:  "billing is down",
			required: 2500,
		},
		"the workspace has too little credit": {
			billing:   &fakeManagedBilling{launchDecision: billingDecision{OK: false, RequiredCents: 2500, AvailableCents: 100}},
			code:      "insufficient_credits",
			message:   "not enough credits to launch managed compute",
			required:  2500,
			available: 100,
		},
		"billing says why it refuses": {
			billing: &fakeManagedBilling{launchDecision: billingDecision{OK: false, ErrorCode: "payment_method_required", Message: "add a card to launch machines"}},
			code:    "payment_method_required",
			message: "add a card to launch machines",
		},
	} {
		t.Run(name, func(t *testing.T) {
			offer := a10gOffer
			if tt.offer.ID != "" {
				offer = tt.offer
			}
			shadeform := fakeProvider("shadeform", offer)
			repo := &faultyPools{fakeComputeRepo: &fakeComputeRepo{}, getErr: tt.getErr}
			if tt.existing != nil {
				repo.pools = map[string][]*model.PoolState{"workspace-1": {tt.existing}}
			}
			service := launchService(repo.fakeComputeRepo, shadeform)
			service.computeRepo = repo
			if tt.billing != nil {
				service.billing = tt.billing
			}
			in := tt.in
			if in == nil {
				in = &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 1, Ttl: "1h", MaxSpend: 100}
			}

			res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{Pool: in})
			require.NoError(t, err)
			require.False(t, res.Ok)
			require.Equal(t, tt.code, res.ErrorCode)
			require.Equal(t, tt.message, res.ErrMsg)
			require.Equal(t, tt.required, res.RequiredCents)
			require.Equal(t, tt.available, res.AvailableCents)
			require.Nil(t, res.Pool)

			require.Empty(t, shadeform.Creates(), "nothing is bought")
			require.Empty(t, repo.joinTokens, "and nothing is given a token to join with")
			require.False(t, repo.savedPool, "and no record is written")
		})
	}
}

func TestLaunchPoolCapacityUndoesWhatItBoughtOnceWhenItCannotFinish(t *testing.T) {
	sshWithoutAKey := func(service *Service, _ *faultyPools) {
		service.appConfig.ManagedCompute.SSH.Enabled = true
	}
	selectorTaken := func(service *Service, repo *faultyPools) {
		service.scheduler = scheduler.NewSchedulerForCapacityChecks(nil, repo, scheduler.NewWorkerPoolManager())
		require.NoError(t, service.scheduler.EnsureAgentPool("workspace-1", &model.PoolState{
			Name: "selector-owner", Selector: "pool-1", Mode: string(types.PoolModePrivate),
		}))
	}
	for name, tt := range map[string]struct {
		arrange  func(*Service, *faultyPools)
		saveErr  error
		existing bool
		code     string
		message  string
	}{
		"its SSH key cannot be made": {
			arrange: sshWithoutAKey,
			code:    "ssh_bootstrap_failed",
			message: "database.postgres.encryptionKey is required for managed SSH",
		},
		"the pool's record cannot be saved": {
			saveErr: errors.New("redis is down"),
			code:    "store_failed",
			message: "redis is down",
		},
		"the record of the pool it adds to cannot be saved, or put back": {
			saveErr:  errors.New("redis is down"),
			existing: true,
			code:     "store_failed",
			message:  "redis is down; cleanup failed: restore pool state: redis is down",
		},
		"the scheduler will not have the pool": {
			arrange: selectorTaken,
			code:    "scheduler_failed",
			message: `pool selector "pool-1" is already owned by another pool`,
		},
		"the scheduler will not have the pool it adds to": {
			arrange:  selectorTaken,
			existing: true,
			code:     "scheduler_failed",
			message:  `pool selector "pool-1" is already owned by another pool`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			now := time.Now().UTC()
			shadeform := fakeProvider("shadeform", a10gOffer)
			repo := &faultyPools{fakeComputeRepo: &fakeComputeRepo{}}
			var before model.PoolState
			if tt.existing {
				existing := a10gPool(now)
				before = *existing
				repo.pools = map[string][]*model.PoolState{"workspace-1": {existing}}
			}
			service := launchService(repo.fakeComputeRepo, shadeform)
			service.computeRepo = repo
			if tt.arrange != nil {
				tt.arrange(service, repo)
			}
			repo.saveErr = tt.saveErr

			res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), service, 1)
			require.False(t, res.Ok)
			require.Equal(t, tt.code, res.ErrorCode)
			require.Contains(t, res.ErrMsg, tt.message)
			if tt.code != "scheduler_failed" {
				require.Equal(t, tt.message, res.ErrMsg)
			}
			require.Nil(t, res.Pool)

			require.Len(t, shadeform.Creates(), 1, "the machine was bought")
			require.Len(t, shadeform.Deletes(), 1, "and is given back once")
			left, err := shadeform.List(context.Background())
			require.NoError(t, err)
			require.Empty(t, left)
			requireEveryTokenRevoked(t, repo.fakeComputeRepo)
			require.Empty(t, repo.sshStates, "no SSH key is left for a machine that is gone")
			if tt.existing {
				require.Len(t, repo.pools["workspace-1"], 1)
				require.Equal(t, before, *repo.pools["workspace-1"][0], "the pool is as it was")
			} else {
				require.Empty(t, repo.pools["workspace-1"], "no pool is left behind")
			}
		})
	}
}

func TestLaunchPoolCapacityBuysNothingWhenAMachineCannotBeGivenAJoinCommand(t *testing.T) {
	shadeform := fakeProvider("shadeform", a10gOffer)
	repo := &faultyPools{fakeComputeRepo: &fakeComputeRepo{}, tokenErr: errors.New("redis is down")}
	service := launchService(repo.fakeComputeRepo, shadeform)
	service.computeRepo = repo

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), service, 1)
	require.False(t, res.Ok)
	require.Equal(t, "bootstrap_failed", res.ErrorCode)
	require.Equal(t, "redis is down", res.ErrMsg)
	require.Empty(t, shadeform.Creates())
	require.Empty(t, repo.pools["workspace-1"])
}

// resold is a provider whose offers name another provider as their seller.
type resold struct {
	*providers.Fake
	seller string
}

func (p resold) Offers(ctx context.Context, filter providers.OfferFilter) ([]providers.Offer, error) {
	offers, err := p.Fake.Offers(ctx, filter)
	for i := range offers {
		offers[i].Provider = p.seller
	}
	return offers, err
}

func TestLaunchPoolCapacityBuysNothingFromAProviderThatIsNotConfigured(t *testing.T) {
	shadeform := fakeProvider("shadeform", a10gOffer)
	repo := &fakeComputeRepo{}
	service := launchService(repo, resold{Fake: shadeform, seller: "elsewhere"})

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), service, 1)
	require.False(t, res.Ok)
	require.Equal(t, "provider_unavailable", res.ErrorCode)
	require.Equal(t, `provider "elsewhere" is not configured`, res.ErrMsg)
	require.Empty(t, shadeform.Creates())
	require.Empty(t, repo.joinTokens)
	require.Empty(t, repo.pools["workspace-1"])
}

func TestLaunchPoolCapacityAnswersWithThePoolItLaunchedInto(t *testing.T) {
	now := time.Now().UTC()
	shadeform := fakeProvider("shadeform", a10gOffer)
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {a10gPool(now)}}}
	events := &capturedEvents{}
	service := launchService(repo, shadeform)
	service.eventRepo = events

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), service, 2)
	require.True(t, res.Ok, res.ErrMsg)
	require.Empty(t, res.ErrorCode)
	require.Equal(t, "pool-1", res.Pool.Name)
	require.Len(t, res.Pool.Reservations, 3)
	require.Equal(t, uint32(3), res.Pool.ReservedNodes)

	stored := repo.pools["workspace-1"][0]
	require.Len(t, stored.Reservations, 3)
	require.Equal(t, "reservation-1", stored.Reservations[0].ID, "the machine the pool had comes first")
	require.Equal(t, uint32(3), stored.ReservedNodes)
	require.Equal(t, uint32(3), stored.Config.Nodes, "what the pool asks for grows by what was launched")
	require.Equal(t, float64(200), stored.Config.MaxSpend)
	// Two machines for an hour at $1.50, with the margin, on top of what the
	// pool had committed.
	require.Equal(t, int64(1_650_000+2*1_650_000), stored.CommittedSpendMicros)
	require.Equal(t, model.SourceCLIReservation, stored.Source)
	require.Equal(t, now.Add(-time.Minute), stored.CreatedAt, "the pool is as old as it was")
	require.WithinDuration(t, time.Now().Add(time.Hour), stored.ExpiresAt, time.Minute)
	for _, reservation := range stored.Reservations[1:] {
		token := repo.joinTokens[reservation.RegistrationTokenHash]
		require.NotNil(t, token)
		require.Equal(t, stored.CreatedAt, token.PoolCreatedAt, "a machine joins with a token tied to when the pool was made")
		require.Equal(t, reservation.MachineID, token.MachineID)
		require.True(t, token.ExpiresAt.IsZero(), "which does not lapse while the machine boots")
	}

	reserved := 0
	for _, event := range events.events {
		if event.Action == types.EventComputeActionPoolReserved {
			reserved++
		}
	}
	require.Equal(t, 1, reserved, "the launch is reported once")
}

// closedCatalogue is a provider whose offers cannot be listed.
type closedCatalogue struct {
	*providers.Fake
	err error
}

func (p closedCatalogue) Offers(context.Context, providers.OfferFilter) ([]providers.Offer, error) {
	return nil, p.err
}

func TestListPoolOffersSkipsAProviderThatCannotBeAsked(t *testing.T) {
	nebius := closedCatalogue{Fake: fakeProvider("nebius"), err: errors.New("nebius token expired")}
	service := &Service{providers: providers.RegistryOf(nebius, fakeProvider("hetzner", cpx31Offer))}

	res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{}})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Len(t, res.Offers, 1, "what the other providers sell is still listed")
	require.Equal(t, "hetzner", res.Offers[0].Provider)
}

func TestLaunchPoolCapacityBuysFromAProviderThatAnswersWhenAnotherDoesNot(t *testing.T) {
	nebius := closedCatalogue{Fake: fakeProvider("nebius"), err: errors.New("nebius token expired")}
	hetzner := fakeProvider("hetzner", cpx31Offer)
	repo := &fakeComputeRepo{}
	service := launchService(repo, nebius, hetzner)

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Nodes: 1,
		Pool:  &pb.PoolConfig{Name: "cpu-pool", Ttl: "1h", MaxSpend: 1},
	})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Len(t, hetzner.Creates(), 1)
}

func TestListPoolOffersFailsWhenNoProviderAnswers(t *testing.T) {
	service := &Service{providers: providers.RegistryOf(
		closedCatalogue{Fake: fakeProvider("nebius"), err: errors.New("nebius token expired")},
		closedCatalogue{Fake: fakeProvider("shadeform"), err: errors.New("shadeform is down")},
		fakeProvider("hetzner", cpx31Offer),
	)}

	// The pool names the two that are down, so nobody is left to answer.
	res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{
		Pool: &pb.PoolConfig{Providers: []string{"nebius", "shadeform"}},
	})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Contains(t, res.ErrMsg, "nebius: nebius token expired", "every failure is named")
	require.Contains(t, res.ErrMsg, "shadeform: shadeform is down")
}

func TestAnOfferWithoutAPriceIsNeitherListedNorBought(t *testing.T) {
	// The provider's catalogue has the same machine in a region it gave no
	// price for.
	unpriced := a10gOffer
	unpriced.Region, unpriced.HourlyCostMicros = "us-west", 0
	shadeform := fakeProvider("shadeform", unpriced, a10gOffer)
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform)

	listed, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{}})
	require.NoError(t, err)
	require.True(t, listed.Ok, listed.ErrMsg)
	require.Len(t, listed.Offers, 1)
	require.Equal(t, "us-east", listed.Offers[0].Region)

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 1, Ttl: "1h", MaxSpend: 2},
	})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	reservation := repo.pools["workspace-1"][0].Reservations[0]
	require.Equal(t, "us-east", reservation.Region, "no price is not the lowest price")
	require.Equal(t, a10gOffer.HourlyCostMicros, reservation.HourlyCostMicros)
}

func TestListPoolOffersListsNothingForAPoolWhoseProvidersAreNotConfigured(t *testing.T) {
	service := &Service{providers: providers.RegistryOf(fakeProvider("hetzner", cpx31Offer))}

	res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{
		Pool: &pb.PoolConfig{Providers: []string{"shadeform"}},
	})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Empty(t, res.Offers, "nobody was asked, so nothing failed")
}

// overTheWire is a provider whose calls fail once their context is done, as
// calls over a network do. It counts how often it is asked what it has.
type overTheWire struct {
	*providers.Fake
	// afterCreate, when set, runs once after the next create.
	afterCreate func()
	lists       int
}

func (p *overTheWire) Create(ctx context.Context, req providers.CreateRequest) (providers.Instance, error) {
	if err := ctx.Err(); err != nil {
		return providers.Instance{}, err
	}
	instance, err := p.Fake.Create(ctx, req)
	if p.afterCreate != nil {
		p.afterCreate()
		p.afterCreate = nil
	}
	return instance, err
}

func (p *overTheWire) List(ctx context.Context) ([]providers.Instance, error) {
	p.lists++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return p.Fake.List(ctx)
}

func (p *overTheWire) Delete(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.Fake.Delete(ctx, id)
}

func launchA10Gs(t *testing.T, ctx context.Context, service *Service, nodes uint32) *pb.LaunchPoolCapacityResponse {
	t.Helper()
	res, err := service.LaunchPoolCapacity(ctx, &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: nodes, Ttl: "1h", MaxSpend: 100},
	})
	require.NoError(t, err)
	return res
}

func requireEveryTokenRevoked(t *testing.T, repo *fakeComputeRepo) {
	t.Helper()
	require.NotEmpty(t, repo.joinTokens)
	for _, token := range repo.joinTokens {
		require.True(t, token.Revoked, "no join token outlives a launch that failed")
	}
}

func TestAFailedLaunchGivesBackItsMachinesAfterTheCallerHasGone(t *testing.T) {
	ctx, hangUp := context.WithCancel(testAuthContext("workspace-1", "token-1"))
	defer hangUp()
	// The caller goes away while the second machine is being bought.
	shadeform := &overTheWire{Fake: fakeProvider("shadeform", a10gOffer), afterCreate: hangUp}
	repo := &fakeComputeRepo{}

	res := launchA10Gs(t, ctx, launchService(repo, shadeform), 2)
	require.False(t, res.Ok)
	require.Equal(t, launchErrorProviderFailure, res.ErrorCode)
	require.NotContains(t, res.ErrMsg, "cleanup failed")

	require.Len(t, shadeform.Creates(), 1)
	left, err := shadeform.Fake.List(context.Background())
	require.NoError(t, err)
	require.Empty(t, left, "the machine that was bought is deleted although the caller has gone")
	require.Empty(t, repo.pools["workspace-1"], "no pool is left behind")
	requireEveryTokenRevoked(t, repo)
}

func TestAMachineAFailedLaunchCannotGiveBackIsKeptForTheReconciler(t *testing.T) {
	shadeform := fakeProvider("shadeform", a10gOffer)
	shadeform.RefuseCreatesAfter(1, fmt.Errorf("%w: none left in us-east", providers.ErrOutOfStock))
	shadeform.FailDeletes(errors.New("provider is down"))
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform)

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), service, 2)
	require.False(t, res.Ok)
	require.Contains(t, res.ErrMsg, "out of stock")
	require.Contains(t, res.ErrMsg, "cleanup failed")
	require.Contains(t, res.ErrMsg, "provider is down")

	// The pool's record is the only record of the machine. It is kept, with
	// the machine marked for the reconciler to go on deleting.
	bought, err := shadeform.List(context.Background())
	require.NoError(t, err)
	require.Len(t, bought, 1)
	require.Len(t, repo.pools["workspace-1"], 1)
	state := repo.pools["workspace-1"][0]
	require.Equal(t, "pool-1", state.Name)
	require.Equal(t, string(types.PoolModePrivate), state.Mode)
	require.Zero(t, state.ReservedNodes)
	require.Zero(t, state.CommittedSpendMicros, "nothing was reserved, so nothing is committed")
	require.Len(t, state.Reservations, 1)
	reservation := state.Reservations[0]
	require.Equal(t, bought[0].ID, reservation.ID)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Equal(t, "launch_failed", reservation.TerminatingReason)
	require.Equal(t, "provider is down", reservation.LastError)
	requireEveryTokenRevoked(t, repo)

	// It is not billed while it waits to be deleted.
	_, _, billed := reservationBillingWindow(&reservation, time.Now().Add(time.Hour))
	require.False(t, billed)

	shadeform.FailDeletes(nil)
	require.NoError(t, service.ReconcileManagedCompute(context.Background()))
	left, err := shadeform.List(context.Background())
	require.NoError(t, err)
	require.Empty(t, left, "the reconciler deleted the machine")
	require.Equal(t, model.ReservationDeleted, repo.pools["workspace-1"][0].Reservations[0].Status)
}

func TestAMachineAFailedLaunchCannotGiveBackIsAddedToThePoolAsItWas(t *testing.T) {
	shadeform := fakeProvider("shadeform", a10gOffer)
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform)
	require.True(t, launchA10Gs(t, testAuthContext("workspace-1", "token-1"), service, 1).Ok)
	before := *repo.pools["workspace-1"][0]

	shadeform.RefuseCreatesAfter(1, fmt.Errorf("%w: none left in us-east", providers.ErrOutOfStock))
	shadeform.FailDeletes(errors.New("provider is down"))
	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), service, 2)
	require.False(t, res.Ok)

	state := repo.pools["workspace-1"][0]
	require.Len(t, state.Reservations, 2)
	require.Equal(t, before.Reservations[0], state.Reservations[0], "the machine the pool had is as it was")
	require.Equal(t, model.ReservationTerminating, state.Reservations[1].Status)
	require.Equal(t, before.Config.Nodes, state.Config.Nodes, "the pool was not given what the launch asked for")
	require.Equal(t, before.CommittedSpendMicros, state.CommittedSpendMicros)
	require.Equal(t, before.ReservedNodes, state.ReservedNodes)
}

func TestAFailedLaunchDeletesAMachineWhoseCreateWentUnanswered(t *testing.T) {
	// The provider makes the machine, and the answer is lost on the way.
	shadeform := fakeProvider("shadeform", a10gOffer)
	shadeform.LoseAnswers(1)
	repo := &fakeComputeRepo{}

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), launchService(repo, shadeform), 1)
	require.False(t, res.Ok)
	require.Equal(t, launchErrorProviderFailure, res.ErrorCode)

	require.Len(t, shadeform.Deletes(), 1, "the machine is found by its tag and deleted")
	left, err := shadeform.List(context.Background())
	require.NoError(t, err)
	require.Empty(t, left)
	require.Empty(t, repo.pools["workspace-1"], "no pool is left behind")
	requireEveryTokenRevoked(t, repo)
}

func TestAnUnansweredMachineThatCannotBeDeletedIsKeptForTheReconciler(t *testing.T) {
	shadeform := fakeProvider("shadeform", a10gOffer)
	shadeform.LoseAnswers(1)
	shadeform.FailDeletes(errors.New("provider is down"))
	repo := &fakeComputeRepo{}

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), launchService(repo, shadeform), 1)
	require.False(t, res.Ok)

	bought, err := shadeform.List(context.Background())
	require.NoError(t, err)
	require.Len(t, bought, 1)
	require.Len(t, repo.pools["workspace-1"], 1)
	reservation := repo.pools["workspace-1"][0].Reservations[0]
	require.Equal(t, bought[0].ID, reservation.ID, "the record names the machine the provider made")
	require.Equal(t, bought[0].ID, reservation.InstanceID)
	require.Equal(t, bought[0].Tags["beam-machine"], reservation.MachineID)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
}

func TestAFailedLaunchDoesNotLookForAMachineTheProviderRefusedToSell(t *testing.T) {
	for name, refusal := range map[string]error{
		"out of stock": fmt.Errorf("%w: none left in us-east", providers.ErrOutOfStock),
		"over quota":   fmt.Errorf("%w: account limit reached", providers.ErrQuota),
	} {
		t.Run(name, func(t *testing.T) {
			shadeform := &overTheWire{Fake: fakeProvider("shadeform", a10gOffer)}
			shadeform.RefuseCreates(refusal)
			repo := &fakeComputeRepo{}

			res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), launchService(repo, shadeform), 1)
			require.False(t, res.Ok)
			require.Zero(t, shadeform.lists, "a clear refusal bought nothing")
			require.Empty(t, repo.pools["workspace-1"])
		})
	}
}

func TestAFailedLaunchLooksOnceForAMachineWhoseCreateFailedUnclearly(t *testing.T) {
	shadeform := &overTheWire{Fake: fakeProvider("shadeform", a10gOffer)}
	shadeform.RefuseCreates(errors.New("POST /instances/create: status 502: bad gateway"))
	repo := &fakeComputeRepo{}

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), launchService(repo, shadeform), 1)
	require.False(t, res.Ok)
	require.Equal(t, 1, shadeform.lists)
	require.Empty(t, shadeform.Deletes(), "the provider made nothing")
	require.Empty(t, repo.pools["workspace-1"])
}
