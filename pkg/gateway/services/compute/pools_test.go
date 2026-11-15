package compute

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

// byocPool is a pool whose machines run in the tenant's own AWS account. The
// stack it was set up with gave Beam a role to ask about it with.
func byocPool(now time.Time, bootstrapTokenHash string) *model.PoolState {
	stackURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	return &model.PoolState{
		Name:             "aws-cpu",
		Selector:         "aws-cpu",
		Mode:             string(types.PoolModePrivate),
		Status:           model.PoolStatusActive,
		CreatedByTokenID: "owner-token",
		Source:           model.SourceAWS,
		CreatedAt:        now.Add(-24 * time.Hour),
		Config:           &pb.PoolConfig{Name: "aws-cpu", Regions: []string{"us-east-1"}},
		BYOC: &model.BYOCProviderState{
			Provider:     "aws",
			AccountID:    "123456789012",
			Region:       "us-east-1",
			ResourceName: "beam-aws-cpu-test",
			ResourceURL:  stackURL,
			DestroyURL:   stackURL,
			Labels: map[string]string{
				byocBootstrapJoinTokenHashLabel:   bootstrapTokenHash,
				byocBootstrapJoinTokenHashesLabel: bootstrapTokenHash,
				"instance_type":                   "i4i.xlarge",
				"desired_nodes":                   "1",
				"max_nodes":                       "1",
				"target_sandboxes":                "20",
				"sandboxes_per_node":              "20",
				awsBYOCAutoScalingGroupNameLabel:  "beam-asg-aws-cpu-test",
				awsBYOCControlRoleArnLabel:        "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test",
				awsBYOCControlRoleExternalIDLabel: "beam-byoc-test-external-id",
			},
		},
	}
}

// byocService is a service that can set up pools in a tenant's AWS account.
func byocService(repo repository.ComputeRepository) *Service {
	return &Service{
		computeRepo: repo,
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				HTTP: types.HTTPConfig{ExternalHost: "app.beam.cloud", ExternalPort: 443, TLS: true},
			},
			ManagedCompute: types.ManagedComputeConfig{BYOC: types.ManagedComputeBYOCConfig{
				AWS: types.ManagedComputeBYOCAWSConfig{TemplateURL: "https://s3.us-east-1.amazonaws.com/beam-byoc-templates-test/aws/byoc-template.yaml"},
			}},
		},
	}
}

var awsCPUPoolRequest = &pb.CreateBYOCPoolRequest{
	Provider:     "aws",
	PoolName:     "aws-cpu",
	Region:       "us-east-1",
	InstanceType: "i4i.xlarge",
	DesiredNodes: 1,
	AccountId:    "123456789012",
}

func TestCreatePoolOverAPoolKeepsWhatItDoesNotSet(t *testing.T) {
	now := time.Now().UTC()
	existing := byocPool(now, hashComputeToken("byoc-token"))
	existing.CreditLowSince = now.Add(-time.Hour)
	existing.BillingDegradedSince = now.Add(-2 * time.Hour)
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {existing}}}
	service := &Service{computeRepo: repo}

	res, err := service.CreatePool(testAuthContext("workspace-1", "owner-token"), &pb.CreatePoolRequest{Pool: &pb.PoolConfig{Name: "aws-cpu"}})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)

	stored := repo.pools["workspace-1"][0]
	require.NotNil(t, stored.BYOC, "the pool's cloud resources, and the tokens its stack joins with")
	require.Equal(t, hashComputeToken("byoc-token"), stored.BYOC.Labels[byocBootstrapJoinTokenHashLabel])
	require.Equal(t, now.Add(-time.Hour), stored.CreditLowSince, "a grace period is not started again")
	require.Equal(t, now.Add(-2*time.Hour), stored.BillingDegradedSince)
	require.Equal(t, model.SourceAWS, stored.Source)
	require.Equal(t, now.Add(-24*time.Hour), stored.CreatedAt)
	require.True(t, service.poolNeedsManagedBilling(stored, now), "its machines are still billed")
}

func TestCreateBYOCPoolAgainKeepsABillingGracePeriod(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	service := byocService(repo)
	first, err := service.CreateBYOCPool(ctx, awsCPUPoolRequest)
	require.NoError(t, err)
	require.True(t, first.Ok, first.ErrMsg)

	since := time.Now().UTC().Add(-10 * time.Minute)
	repo.pools["workspace-1"][0].CreditLowSince = since
	repo.pools["workspace-1"][0].BillingDegradedSince = since

	// The tenant opens the setup page again.
	second, err := service.CreateBYOCPool(ctx, awsCPUPoolRequest)
	require.NoError(t, err)
	require.True(t, second.Ok, second.ErrMsg)
	stored := repo.pools["workspace-1"][0]
	require.Equal(t, since, stored.CreditLowSince)
	require.Equal(t, since, stored.BillingDegradedSince)
}

func TestLaunchIntoABYOCPoolIsRefused(t *testing.T) {
	now := time.Now().UTC()
	existing := byocPool(now, hashComputeToken("byoc-token"))
	existing.Config.Gpu = []string{"A10G"}
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {existing}}}
	shadeform := fakeProvider("shadeform", a10gOffer)
	service := launchService(repo, shadeform)

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "owner-token"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{Name: "aws-cpu", Gpu: []string{"A10G"}, Nodes: 1, Ttl: "1h", MaxSpend: 2},
	})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Contains(t, res.ErrMsg, `pool "aws-cpu" runs on machines from your own cloud account`)
	require.Empty(t, shadeform.Creates(), "nothing is bought")

	stored := repo.pools["workspace-1"][0]
	require.NotNil(t, stored.BYOC)
	require.Equal(t, model.SourceAWS, stored.Source)
	require.Empty(t, stored.Reservations)
}

// lockWatcher counts the saves of a pool's state, and those made while the
// pool's lock was not held.
type lockWatcher struct {
	*fakeComputeRepo
	locked        bool
	saves         int
	unlockedSaves int
}

func (r *lockWatcher) WithPoolStateLock(ctx context.Context, workspaceID, name string, fn func(context.Context) error) error {
	r.locked = true
	defer func() { r.locked = false }()
	return fn(ctx)
}

func (r *lockWatcher) SavePoolState(ctx context.Context, workspaceID string, state *model.PoolState) error {
	r.saves++
	if !r.locked {
		r.unlockedSaves++
	}
	return r.fakeComputeRepo.SavePoolState(ctx, workspaceID, state)
}

func TestTheBYOCBootstrapTokenIsSavedWithThePoolUnderItsLock(t *testing.T) {
	repo := &lockWatcher{fakeComputeRepo: &fakeComputeRepo{}}
	service := byocService(repo)

	res, err := service.CreateBYOCPool(testAuthContext("workspace-1", "owner-token"), awsCPUPoolRequest)
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)

	// A save made after the lock is released writes a record that the
	// reconciler may have changed in the meantime.
	require.Zero(t, repo.unlockedSaves)
	require.Equal(t, 1, repo.saves)
	stored := repo.pools["workspace-1"][0]
	require.Len(t, repo.joinTokens, 1)
	for tokenHash, token := range repo.joinTokens {
		require.Equal(t, tokenHash, stored.BYOC.Labels[byocBootstrapJoinTokenHashLabel])
		require.Equal(t, stored.CreatedAt, token.PoolCreatedAt)
		require.False(t, token.Revoked)
	}
}

func TestCreateBYOCPoolThatCannotBeRegisteredLeavesNoPoolAndNoToken(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	service := byocService(repo)
	// Another pool of the workspace answers to the selector already.
	service.scheduler = scheduler.NewSchedulerForCapacityChecks(nil, repo, scheduler.NewWorkerPoolManager())
	require.NoError(t, service.scheduler.EnsureAgentPool("workspace-1", &model.PoolState{
		Name: "selector-owner", Selector: "aws-cpu", Mode: string(types.PoolModePrivate),
	}))

	res, err := service.CreateBYOCPool(ctx, awsCPUPoolRequest)
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Contains(t, res.ErrMsg, "already owned by another pool")

	state, err := repo.GetPoolState(context.Background(), "workspace-1", "aws-cpu")
	require.NoError(t, err)
	require.Nil(t, state)
	for _, token := range repo.joinTokens {
		require.True(t, token.Revoked, "a token nobody was given admits nobody")
	}
}

// extendedPool is a pool with one running machine that is wanted for another
// six hours.
type extendedPool struct {
	service  *Service
	provider *providers.Fake
	repo     *fakeComputeRepo
	instance string
	expiry   time.Time
}

func newExtendedPool(t *testing.T) *extendedPool {
	t.Helper()
	now := time.Now().UTC()
	expiry := now.Add(6 * time.Hour)
	provider := fakeProvider("shadeform")
	instance := provider.Plant(testOwner, providers.StatusActive, nil)
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {{
		Name:      "pool-1",
		Mode:      string(types.PoolModePrivate),
		Config:    &pb.PoolConfig{Name: "pool-1", Ttl: "6h", MaxSpend: 10},
		ExpiresAt: expiry,
		Reservations: []model.Reservation{{
			ID:               instance,
			InstanceID:       instance,
			Provider:         "shadeform",
			Name:             "beam-pool-1-machine-1",
			MachineID:        "machine-1",
			Source:           model.SourceCLIReservation,
			Status:           model.ReservationActive,
			CreatedAt:        now.Add(-time.Hour),
			ExpiresAt:        expiry,
			HourlyCostMicros: 1_000_000,
			CommittedMicros:  7_000_000,
		}},
	}}}}
	return &extendedPool{
		service:  &Service{computeRepo: repo, providers: providers.RegistryOf(provider)},
		provider: provider,
		repo:     repo,
		instance: instance,
		expiry:   expiry,
	}
}

func (p *extendedPool) extend(t *testing.T, in *pb.ExtendPoolCapacityRequest) *pb.ExtendPoolCapacityResponse {
	t.Helper()
	p.repo.savedPool = false
	res, err := p.service.ExtendPoolCapacity(testAuthContext("workspace-1", "owner-token"), in)
	require.NoError(t, err)
	return res
}

func TestExtendPoolCapacityMovesThePoolAndItsMachinesToTheNewExpiry(t *testing.T) {
	p := newExtendedPool(t)
	started := time.Now()

	res := p.extend(t, &pb.ExtendPoolCapacityRequest{Name: "pool-1", Ttl: "48h", MaxSpend: 60})
	require.True(t, res.Ok, res.ErrMsg)
	require.Equal(t, "pool-1", res.Pool.Name)

	stored := p.repo.pools["workspace-1"][0]
	require.WithinDuration(t, started.Add(48*time.Hour), stored.ExpiresAt, time.Minute)
	require.Equal(t, "48h", stored.Config.Ttl)
	require.Equal(t, float64(60), stored.Config.MaxSpend)
	reservation := stored.Reservations[0]
	require.Equal(t, stored.ExpiresAt, reservation.ExpiresAt)
	require.Equal(t, stored.ExpiresAt.Add(reservationDeletionGrace), p.provider.Until(p.instance), "the provider is told too")
	require.Equal(t, int64(7_000_000+43_000_000), reservation.CommittedMicros, "42 hours and a moment more, in whole hours")
}

func TestExtendPoolCapacityRefusesATTLThatIsNotPositive(t *testing.T) {
	for _, ttl := range []string{"-5h", "0s", "0"} {
		t.Run(ttl, func(t *testing.T) {
			p := newExtendedPool(t)

			res := p.extend(t, &pb.ExtendPoolCapacityRequest{Name: "pool-1", Ttl: ttl})
			require.False(t, res.Ok)
			require.Equal(t, "ttl must be positive", res.ErrMsg)

			stored := p.repo.pools["workspace-1"][0]
			require.Equal(t, p.expiry, stored.ExpiresAt, "the pool does not end in the past")
			require.Equal(t, p.expiry, stored.Reservations[0].ExpiresAt)
			require.Equal(t, "6h", stored.Config.Ttl)
			require.False(t, p.repo.savedPool)
		})
	}
}

func TestExtendPoolCapacityRefusesWhatItCannotExtend(t *testing.T) {
	p := newExtendedPool(t)

	res := p.extend(t, &pb.ExtendPoolCapacityRequest{Name: "another-pool", Ttl: "48h"})
	require.False(t, res.Ok)
	require.Equal(t, "pool not found", res.ErrMsg)

	res = p.extend(t, &pb.ExtendPoolCapacityRequest{Name: "pool-1", Ttl: "two days"})
	require.False(t, res.Ok)
	require.Contains(t, res.ErrMsg, "two days")
	require.Equal(t, "6h", p.repo.pools["workspace-1"][0].Config.Ttl)
	require.False(t, p.repo.savedPool)
}

// twoPoolsOfMachines is a workspace with two pools of its own, one machine in
// the first and two in the second, and a pool it gives a machine to.
func twoPoolsOfMachines() *fakeComputeRepo {
	machine := func(pool, id string) *model.AgentTokenState {
		return &model.AgentTokenState{WorkspaceID: "workspace-1", PoolName: pool, MachineID: id}
	}
	return &fakeComputeRepo{
		pools: map[string][]*model.PoolState{"workspace-1": {
			{Name: "first", Mode: string(types.PoolModePrivate)},
			{Name: "given", Mode: string(types.PoolModeProvider)},
			{Name: "second", Mode: string(types.PoolModePrivate)},
		}},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "first"):  {machine("first", "first-1")},
			fakeComputeKey("workspace-1", "given"):  {machine("given", "given-1")},
			fakeComputeKey("workspace-1", "second"): {machine("second", "second-1"), machine("second", "second-2")},
		},
	}
}

func listPoolMachines(t *testing.T, ctx context.Context, service *Service, in *pb.ListPoolMachinesRequest) *pb.ListPoolMachinesResponse {
	t.Helper()
	res, err := service.ListPoolMachines(ctx, in)
	require.NoError(t, err)
	return res
}

func machineIDs(machines []*pb.Machine) []string {
	ids := []string{}
	for _, machine := range machines {
		ids = append(ids, machine.Id)
	}
	return ids
}

func TestListPoolMachinesListsTheMachinesOfTheWorkspacesOwnPools(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	service := &Service{computeRepo: twoPoolsOfMachines()}

	res := listPoolMachines(t, ctx, service, &pb.ListPoolMachinesRequest{})
	require.True(t, res.Ok, res.ErrMsg)
	require.Equal(t, []string{"first-1", "second-1", "second-2"}, machineIDs(res.Machines), "not those it gives to a provider pool")

	res = listPoolMachines(t, ctx, service, &pb.ListPoolMachinesRequest{Limit: 2})
	require.True(t, res.Ok, res.ErrMsg)
	require.Equal(t, []string{"first-1", "second-1"}, machineIDs(res.Machines), "the limit counts across pools")

	res = listPoolMachines(t, ctx, service, &pb.ListPoolMachinesRequest{PoolName: "second"})
	require.True(t, res.Ok, res.ErrMsg)
	require.Equal(t, []string{"second-1", "second-2"}, machineIDs(res.Machines))

	for _, name := range []string{"given", "no-such-pool"} {
		res = listPoolMachines(t, ctx, service, &pb.ListPoolMachinesRequest{PoolName: name})
		require.False(t, res.Ok)
		require.Equal(t, "pool not found", res.ErrMsg)
	}
}

func TestListPoolMachinesShowsSSHAccessOnlyToATokenThatMayUseIt(t *testing.T) {
	repo := twoPoolsOfMachines()
	repo.sshStates = map[string]*model.MachineSSHState{
		fakeComputeSSHKey("workspace-1", "first", "first-1"): {
			WorkspaceID: "workspace-1", PoolName: "first", MachineID: "first-1",
			Username: "beam", PublicIP: "203.0.113.7", Status: model.MachineSSHStatusReady,
		},
	}
	sshEnabled := types.AppConfig{ManagedCompute: types.ManagedComputeConfig{SSH: types.ManagedComputeSSHConfig{Enabled: true}}}
	owner := testAuthContext("workspace-1", "owner-token")
	workload := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: "workspace-1"},
		Token:     &types.Token{ExternalId: "workload-token", TokenType: types.TokenTypeWorkspaceRestricted},
	})

	res := listPoolMachines(t, owner, &Service{computeRepo: repo, appConfig: sshEnabled}, &pb.ListPoolMachinesRequest{})
	require.True(t, res.Ok, res.ErrMsg)
	require.Len(t, res.Machines, 3)
	require.NotNil(t, res.Machines[0].Ssh)
	require.True(t, res.Machines[0].Ssh.Supported)
	require.Equal(t, "203.0.113.7", res.Machines[0].Ssh.Host)
	require.Nil(t, res.Machines[1].Ssh, "a machine without an SSH record shows none")
	require.Nil(t, res.Machines[2].Ssh)

	for name, listed := range map[string]*pb.ListPoolMachinesResponse{
		"a token workloads run with": listPoolMachines(t, workload, &Service{computeRepo: repo, appConfig: sshEnabled}, &pb.ListPoolMachinesRequest{}),
		"SSH is not enabled":         listPoolMachines(t, owner, &Service{computeRepo: repo}, &pb.ListPoolMachinesRequest{}),
	} {
		require.True(t, listed.Ok, listed.ErrMsg)
		require.Len(t, listed.Machines, 3, name)
		for _, machine := range listed.Machines {
			require.Nil(t, machine.Ssh, name)
		}
	}
}

func TestListPoolMachinesLeavesOutAPoolWhoseStackIsGone(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	for name, in := range map[string]*pb.ListPoolMachinesRequest{
		"every pool":    {},
		"the pool only": {PoolName: "aws-cpu"},
	} {
		t.Run(name, func(t *testing.T) {
			repo, tokenHash := silentBYOCPool(time.Second)
			stackCheckAnswers(t, true, nil)

			res := listPoolMachines(t, ctx, &Service{computeRepo: repo, containerRepo: &fakeContainerRepo{}}, in)
			require.True(t, res.Ok, res.ErrMsg)
			require.Empty(t, res.Machines)
			require.Empty(t, repo.pools["workspace-1"], "what Beam held for the pool is cleaned up")
			require.Empty(t, repo.machines[fakeComputeKey("workspace-1", "aws-cpu")])
			require.True(t, repo.joinTokens[tokenHash].Revoked)
		})
	}
}

func TestListPoolMachinesListsTheMachinesOfABYOCPoolWhoseStackExists(t *testing.T) {
	repo, _ := silentBYOCPool(time.Second)
	stackCheckAnswers(t, false, nil)

	res := listPoolMachines(t, testAuthContext("workspace-1", "owner-token"),
		&Service{computeRepo: repo, containerRepo: &fakeContainerRepo{}}, &pb.ListPoolMachinesRequest{})
	require.True(t, res.Ok, res.ErrMsg)
	require.Equal(t, []string{"machine-1"}, machineIDs(res.Machines))
	require.Len(t, repo.pools["workspace-1"], 1)
}

func TestGetBYOCPoolDoesNotFindAPoolWhoseStackIsGone(t *testing.T) {
	repo, tokenHash := silentBYOCPool(time.Second)
	stackCheckAnswers(t, true, nil)

	res, err := (&Service{computeRepo: repo, containerRepo: &fakeContainerRepo{}}).GetBYOCPool(
		testAuthContext("workspace-1", "owner-token"), &pb.GetBYOCPoolRequest{PoolName: "aws-cpu"})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Equal(t, "pool not found", res.ErrMsg)
	require.Empty(t, repo.pools["workspace-1"], "what Beam held for the pool is cleaned up")
	require.True(t, repo.joinTokens[tokenHash].Revoked)
}

// leasedPools holds the pool lock the way the Redis repository does: what the
// work done under the lock returns comes back joined with the lock's own
// errors, of which a healthy lock has none. Its saves fail when saveErr is set.
type leasedPools struct {
	*fakeComputeRepo
	saveErr error
}

func (r *leasedPools) WithPoolStateLock(ctx context.Context, workspaceID, name string, fn func(context.Context) error) error {
	var leaseErr, releaseErr error
	return errors.Join(fn(ctx), leaseErr, releaseErr)
}

func (r *leasedPools) SavePoolState(ctx context.Context, workspaceID string, state *model.PoolState) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	return r.fakeComputeRepo.SavePoolState(ctx, workspaceID, state)
}

func TestScaleBYOCPoolSaysWhyItDidNotScale(t *testing.T) {
	for name, tt := range map[string]struct {
		pool    string
		nodes   uint32
		awsErr  error
		saveErr error
		want    string
	}{
		"a pool the workspace does not have":     {pool: "another-pool", nodes: 2, want: "pool not found"},
		"a pool that is not in the tenant cloud": {pool: "bought", nodes: 2, want: "missing BYOC provider metadata"},
		"more nodes than a pool may have":        {pool: "aws-cpu", nodes: 101, want: "desired_nodes must be <= 100"},
		"AWS refuses":                            {pool: "aws-cpu", nodes: 2, awsErr: errors.New("update AWS BYOC node group: AccessDenied"), want: "update AWS BYOC node group: AccessDenied"},
		"the record cannot be saved":             {pool: "aws-cpu", nodes: 2, saveErr: errors.New("redis is down"), want: "redis is down"},
	} {
		t.Run(name, func(t *testing.T) {
			previous := awsBYOCScaleAutoScalingGroup
			awsBYOCScaleAutoScalingGroup = func(context.Context, awsBYOCScaleAutoScalingGroupInput) error { return tt.awsErr }
			t.Cleanup(func() { awsBYOCScaleAutoScalingGroup = previous })
			repo := &leasedPools{saveErr: tt.saveErr, fakeComputeRepo: &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {
				byocPool(time.Now().UTC(), hashComputeToken("byoc-token")),
				{Name: "bought", Mode: string(types.PoolModePrivate), Source: model.SourceCLIReservation},
			}}}}

			res, err := (&Service{computeRepo: repo}).ScaleBYOCPool(testAuthContext("workspace-1", "owner-token"),
				&pb.ScaleBYOCPoolRequest{PoolName: tt.pool, DesiredNodes: tt.nodes})
			require.NoError(t, err)
			require.False(t, res.Ok)
			require.Equal(t, tt.want, res.ErrMsg)
			require.Nil(t, res.Pool)
		})
	}
}

func TestExtendPoolCapacitySaysWhyItDidNotExtend(t *testing.T) {
	for name, tt := range map[string]struct {
		in      *pb.ExtendPoolCapacityRequest
		saveErr error
		want    string
	}{
		"a pool the workspace does not have": {in: &pb.ExtendPoolCapacityRequest{Name: "another-pool", Ttl: "48h"}, want: "pool not found"},
		"a TTL that cannot be read":          {in: &pb.ExtendPoolCapacityRequest{Name: "pool-1", Ttl: "two days"}, want: `time: invalid duration "two days"`},
		"a TTL that is not ahead":            {in: &pb.ExtendPoolCapacityRequest{Name: "pool-1", Ttl: "-5h"}, want: "ttl must be positive"},
		"the record cannot be saved":         {in: &pb.ExtendPoolCapacityRequest{Name: "pool-1", Ttl: "48h"}, saveErr: errors.New("redis is down"), want: "redis is down"},
	} {
		t.Run(name, func(t *testing.T) {
			p := newExtendedPool(t)
			p.service.computeRepo = &leasedPools{fakeComputeRepo: p.repo, saveErr: tt.saveErr}

			res := p.extend(t, tt.in)
			require.False(t, res.Ok)
			require.Equal(t, tt.want, res.ErrMsg)
			require.Nil(t, res.Pool)
		})
	}
}

func TestScaleBYOCPoolAndExtendPoolCapacityAnswerUnderALeasedLock(t *testing.T) {
	previous := awsBYOCScaleAutoScalingGroup
	awsBYOCScaleAutoScalingGroup = func(context.Context, awsBYOCScaleAutoScalingGroupInput) error { return nil }
	t.Cleanup(func() { awsBYOCScaleAutoScalingGroup = previous })
	repo := &leasedPools{fakeComputeRepo: &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {
		byocPool(time.Now().UTC(), hashComputeToken("byoc-token")),
	}}}}
	scaled, err := (&Service{computeRepo: repo}).ScaleBYOCPool(testAuthContext("workspace-1", "owner-token"),
		&pb.ScaleBYOCPoolRequest{PoolName: "aws-cpu", DesiredNodes: 2})
	require.NoError(t, err)
	require.True(t, scaled.Ok, scaled.ErrMsg)
	require.Equal(t, uint32(2), scaled.Byoc.DesiredNodes)

	p := newExtendedPool(t)
	p.service.computeRepo = &leasedPools{fakeComputeRepo: p.repo}
	extended := p.extend(t, &pb.ExtendPoolCapacityRequest{Name: "pool-1", Ttl: "48h"})
	require.True(t, extended.Ok, extended.ErrMsg)
	require.Equal(t, "pool-1", extended.Pool.Name)
}
