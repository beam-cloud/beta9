package compute

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// anAWSCPUPool is a request for a pool of CPU machines on AWS, for a test to
// change before it sends it.
func anAWSCPUPool() *pb.CreateBYOCPoolRequest {
	return proto.Clone(awsCPUPoolRequest).(*pb.CreateBYOCPoolRequest)
}

// stackParameters reads what a setup link asks CloudFormation to create: the
// template, the stack's name and its parameters.
func stackParameters(t *testing.T, setupURL string) map[string]string {
	t.Helper()
	prefix, query, found := strings.Cut(setupURL, "#/stacks/create/review?")
	require.True(t, found, setupURL)
	require.Equal(t, "https://console.aws.amazon.com/cloudformation/home?region=us-west-2", prefix)
	values, err := url.ParseQuery(query)
	require.NoError(t, err)
	parameters := map[string]string{}
	for key, value := range values {
		require.Len(t, value, 1, key)
		parameters[key] = value[0]
	}
	return parameters
}

func TestCreateBYOCPoolAnswersWithTheStackTheTenantIsToCreate(t *testing.T) {
	const (
		templateURL = "https://s3.us-east-1.amazonaws.com/beam-byoc-templates-test/aws/byoc-template.yaml"
		principal   = "arn:aws:iam::999999999999:role/beam-gateway"
	)
	repo := &fakeComputeRepo{}
	events := &capturedEvents{}
	service := byocService(repo)
	service.eventRepo = events
	service.appConfig.ManagedCompute.BYOC.AWS.ControlRolePrincipalARN = principal
	service.appConfig.Worker = types.WorkerConfig{ImageRegistry: "public.ecr.aws/n4e0e1y0", ImageName: "beta9-worker", ImageTag: "worker-test"}

	res, err := service.CreateBYOCPool(testAuthContext("workspace-1", "owner-token"), &pb.CreateBYOCPoolRequest{
		Provider:     "aws",
		PoolName:     "aws-gpu",
		Region:       "us-west-2",
		InstanceType: "g6.xlarge",
		DesiredNodes: 2,
		MaxNodes:     4,
		AccountId:    "123456789012",
	})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)

	stack := awsCloudFormationStackName("workspace-1", "aws-gpu")
	require.Equal(t, stack, res.ResourceName)
	require.Equal(t, awsCloudFormationStackURL("us-west-2", stack), res.ResourceUrl)
	require.Equal(t, res.Pool.Byoc, res.Byoc)
	require.Equal(t, stack, res.Byoc.ResourceName)
	require.True(t, res.Byoc.DirectScaleEnabled)

	parameters := stackParameters(t, res.SetupUrl)
	joinToken := parameters["param_BeamJoinToken"]
	require.NotEmpty(t, joinToken)
	stored := repo.pools["workspace-1"][0]
	require.Equal(t, hashComputeToken(joinToken), stored.BYOC.Labels[byocBootstrapJoinTokenHashLabel], "the stack joins with the token saved with the pool")
	delete(parameters, "param_BeamJoinToken")
	require.Equal(t, map[string]string{
		"templateURL":                       templateURL,
		"stackName":                         stack,
		"param_BeamAutoScalingGroupName":    awsBYOCAutoScalingGroupName("workspace-1", "aws-gpu"),
		"param_BeamControlExternalId":       awsBYOCControlExternalID("workspace-1", "aws-gpu"),
		"param_BeamControlRoleName":         awsBYOCControlRoleName("workspace-1", "aws-gpu"),
		"param_BeamControlRolePrincipalArn": principal,
		"param_BeamGatewayURL":              "https://app.beam.cloud",
		"param_BeamPoolName":                "aws-gpu",
		"param_BeamWorkerImage":             "public.ecr.aws/n4e0e1y0/beta9-worker:worker-test",
		"param_ContainerStartConcurrency":   "16",
		"param_DesiredCapacity":             "2",
		"param_MaxSize":                     "4",
		"param_MinSize":                     "2",
		"param_NetworkSlots":                "128",
		"param_NodeGPUCount":                "1",
		"param_NodeInstanceType":            "g6.xlarge",
		"param_RootVolumeSizeGB":            "200",
		"param_TargetAWSAccountID":          "123456789012",
	}, parameters)

	require.Equal(t, &model.BYOCProviderState{
		Provider:     "aws",
		AccountID:    "123456789012",
		Region:       "us-west-2",
		ResourceName: stack,
		ResourceURL:  res.ResourceUrl,
		DestroyURL:   res.ResourceUrl,
		Labels: map[string]string{
			"instance_type":                   "g6.xlarge",
			"desired_nodes":                   "2",
			"max_nodes":                       "4",
			"target_sandboxes":                "40",
			"sandboxes_per_node":              "20",
			"hourly_cost_micros":              "110000",
			"total_hourly_cost_micros":        "220000",
			"capacity_unit":                   "gpu",
			"capacity_unit_label":             "GPUs",
			"gpu":                             "L4",
			"gpu_count":                       "1",
			awsBYOCAutoScalingGroupNameLabel:  awsBYOCAutoScalingGroupName("workspace-1", "aws-gpu"),
			awsBYOCControlRoleNameLabel:       awsBYOCControlRoleName("workspace-1", "aws-gpu"),
			awsBYOCControlRoleArnLabel:        awsBYOCControlRoleARN("us-west-2", "123456789012", awsBYOCControlRoleName("workspace-1", "aws-gpu")),
			awsBYOCControlRoleExternalIDLabel: awsBYOCControlExternalID("workspace-1", "aws-gpu"),
			byocBootstrapJoinTokenHashLabel:   hashComputeToken(joinToken),
			byocBootstrapJoinTokenHashesLabel: hashComputeToken(joinToken),
		},
	}, stored.BYOC)
	require.Equal(t, model.SourceAWS, stored.Source)
	require.Equal(t, []string{"us-west-2"}, stored.Config.Regions)
	require.Equal(t, []string{"L4"}, stored.Config.Gpu)

	configured := []types.EventComputeSchema{}
	for _, event := range events.events {
		if event.Status == "byoc_pool_configured" {
			configured = append(configured, event)
		}
	}
	require.Len(t, configured, 1)
	event := configured[0]
	event.Timestamp = time.Time{}
	require.Equal(t, types.EventComputeSchema{
		WorkspaceID: "workspace-1",
		PoolName:    "aws-gpu",
		Action:      types.EventComputeActionPoolCreated,
		Status:      "byoc_pool_configured",
		Source:      "aws",
		Transport:   stored.Transport,
		Fallback:    stored.Fallback,
		NodeCount:   2,
		Attrs: map[string]string{
			"provider":      "aws",
			"cloud":         "aws",
			"region":        "us-west-2",
			"instance_type": "g6.xlarge",
			"gpu":           "L4",
			"gpu_count":     "1",
		},
	}, event)
}

func TestCreateBYOCPoolTakesOnlyAProviderItKnows(t *testing.T) {
	for provider, want := range map[string]string{
		"":      "provider is required",
		"   ":   "provider is required",
		"gcp":   `unsupported BYOC provider "gcp"`,
		" GCP ": `unsupported BYOC provider " GCP "`,
	} {
		repo := &fakeComputeRepo{}
		request := anAWSCPUPool()
		request.Provider = provider

		res, err := byocService(repo).CreateBYOCPool(testAuthContext("workspace-1", "owner-token"), request)
		require.NoError(t, err)
		require.False(t, res.Ok)
		require.Equal(t, want, res.ErrMsg)
		require.Empty(t, repo.pools["workspace-1"])
		require.Empty(t, repo.joinTokens)
	}

	repo := &fakeComputeRepo{}
	request := anAWSCPUPool()
	request.Provider = " AWS "
	res, err := byocService(repo).CreateBYOCPool(testAuthContext("workspace-1", "owner-token"), request)
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Equal(t, "aws", repo.pools["workspace-1"][0].BYOC.Provider)
}

func TestCreateBYOCPoolSaysWhyItMadeNoPool(t *testing.T) {
	for name, tt := range map[string]struct {
		change   func(*pb.CreateBYOCPoolRequest)
		existing *model.PoolState
		want     string
	}{
		"no pool name":                    {change: func(in *pb.CreateBYOCPoolRequest) { in.PoolName = " " }, want: "pool name is required"},
		"a region AWS does not have":      {change: func(in *pb.CreateBYOCPoolRequest) { in.Region = "moon-1" }, want: `invalid AWS region "moon-1"`},
		"an instance type not on offer":   {change: func(in *pb.CreateBYOCPoolRequest) { in.InstanceType = "m5.large" }, want: `unsupported AWS instance type "m5.large"`},
		"a GPU the instance type lacks":   {change: func(in *pb.CreateBYOCPoolRequest) { in.Gpu = "L4" }, want: `AWS instance type "i4i.xlarge" does not provide GPUs`},
		"more nodes than the limit":       {change: func(in *pb.CreateBYOCPoolRequest) { in.DesiredNodes = 101 }, want: "desired_nodes must be <= 100"},
		"fewer nodes at most than wanted": {change: func(in *pb.CreateBYOCPoolRequest) { in.DesiredNodes, in.MaxNodes = 3, 2 }, want: "max_nodes must be >= desired_nodes"},
		"no account":                      {change: func(in *pb.CreateBYOCPoolRequest) { in.AccountId = "" }, want: "AWS account ID is required"},
		"an account that is not one":      {change: func(in *pb.CreateBYOCPoolRequest) { in.AccountId = "12345" }, want: "invalid AWS account ID"},
		"a pool of bought machines has the name": {
			existing: &model.PoolState{Name: "aws-cpu", Mode: string(types.PoolModePrivate), Source: model.SourceCLIReservation},
			want:     "pool already exists with source cli_reservation",
		},
		"a pool of another kind has the name": {
			existing: &model.PoolState{Name: "aws-cpu", Mode: string(types.PoolModeProvider)},
			want:     "pool already exists in this workspace",
		},
	} {
		t.Run(name, func(t *testing.T) {
			repo := &fakeComputeRepo{}
			if tt.existing != nil {
				repo.pools = map[string][]*model.PoolState{"workspace-1": {tt.existing}}
			}
			request := anAWSCPUPool()
			if tt.change != nil {
				tt.change(request)
			}

			res, err := byocService(repo).CreateBYOCPool(testAuthContext("workspace-1", "owner-token"), request)
			require.NoError(t, err)
			require.False(t, res.Ok)
			require.Equal(t, tt.want, res.ErrMsg)
			require.Empty(t, repo.joinTokens)
			require.False(t, repo.savedPool)
		})
	}
}

func TestABYOCPoolIsAskedAboutAndReleasedWithTheRoleItsStackGave(t *testing.T) {
	now := time.Now().UTC()
	pool := byocPool(now, hashComputeToken("byoc-token"))
	machine := &model.AgentTokenState{
		WorkspaceID: "workspace-1", PoolName: "aws-cpu", MachineID: "machine-1",
		Hostname: "ip-10-0-0-7.ec2.internal", Schedulable: true, LastJoinAt: now, LastHeartbeatAt: now,
	}
	repo := &fakeComputeRepo{
		pools:    map[string][]*model.PoolState{"workspace-1": {pool}},
		machines: map[string][]*model.AgentTokenState{fakeComputeKey("workspace-1", "aws-cpu"): {machine}},
	}

	var asked awsBYOCResourceDeletedInput
	previousCheck := awsBYOCResourceDeleted
	awsBYOCResourceDeleted = func(_ context.Context, input awsBYOCResourceDeletedInput) (bool, error) {
		asked = input
		return false, nil
	}
	var released awsBYOCReleaseMachineInput
	previousRelease := awsBYOCReleaseMachine
	awsBYOCReleaseMachine = func(_ context.Context, input awsBYOCReleaseMachineInput) error {
		released = input
		return nil
	}
	t.Cleanup(func() { awsBYOCResourceDeleted, awsBYOCReleaseMachine = previousCheck, previousRelease })
	service := &Service{computeRepo: repo, containerRepo: &fakeContainerRepo{}}
	ctx := testAuthContext("workspace-1", "owner-token")

	listed, err := service.ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	require.NoError(t, err)
	require.True(t, listed.Ok, listed.ErrMsg)
	require.Equal(t, awsBYOCResourceDeletedInput{
		Region:     "us-east-1",
		RoleARN:    "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test",
		ExternalID: "beam-byoc-test-external-id",
		StackName:  "beam-aws-cpu-test",
	}, asked)

	deleted, err := service.DeletePrivateMachine(ctx, &pb.DeleteMachineRequest{PoolName: "aws-cpu", MachineId: "machine-1"})
	require.NoError(t, err)
	require.True(t, deleted.Ok, deleted.ErrMsg)
	require.Equal(t, awsBYOCReleaseMachineInput{
		Region:               "us-east-1",
		RoleARN:              "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test",
		ExternalID:           "beam-byoc-test-external-id",
		AutoScalingGroupName: "beam-asg-aws-cpu-test",
		Machine:              machine,
	}, released)
}

func TestABYOCPoolWhoseStackGaveNoRoleIsNeitherAskedAboutNorScaledAtAWS(t *testing.T) {
	now := time.Now().UTC()
	pool := byocPool(now, hashComputeToken("byoc-token"))
	delete(pool.BYOC.Labels, awsBYOCControlRoleArnLabel)
	machine := &model.AgentTokenState{
		WorkspaceID: "workspace-1", PoolName: "aws-cpu", MachineID: "machine-1",
		Schedulable: true, LastJoinAt: now, LastHeartbeatAt: now,
	}
	repo := &fakeComputeRepo{
		pools:    map[string][]*model.PoolState{"workspace-1": {pool}},
		machines: map[string][]*model.AgentTokenState{fakeComputeKey("workspace-1", "aws-cpu"): {machine}},
	}
	calls := 0
	previousCheck, previousRelease, previousScale := awsBYOCResourceDeleted, awsBYOCReleaseMachine, awsBYOCScaleAutoScalingGroup
	awsBYOCResourceDeleted = func(context.Context, awsBYOCResourceDeletedInput) (bool, error) { calls++; return false, nil }
	awsBYOCReleaseMachine = func(context.Context, awsBYOCReleaseMachineInput) error { calls++; return nil }
	awsBYOCScaleAutoScalingGroup = func(context.Context, awsBYOCScaleAutoScalingGroupInput) error { calls++; return nil }
	t.Cleanup(func() {
		awsBYOCResourceDeleted, awsBYOCReleaseMachine, awsBYOCScaleAutoScalingGroup = previousCheck, previousRelease, previousScale
	})
	service := &Service{computeRepo: repo, containerRepo: &fakeContainerRepo{}}
	ctx := testAuthContext("workspace-1", "owner-token")

	listed, err := service.ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	require.NoError(t, err)
	require.True(t, listed.Ok, listed.ErrMsg)
	require.Len(t, listed.Pools, 1)
	require.False(t, listed.Pools[0].Byoc.DirectScaleEnabled)

	scaled, err := service.ScaleBYOCPool(ctx, &pb.ScaleBYOCPoolRequest{PoolName: "aws-cpu", DesiredNodes: 2})
	require.NoError(t, err)
	require.False(t, scaled.Ok)
	require.Equal(t, errBYOCDirectScaleUnavailable.Error(), scaled.ErrMsg)

	deleted, err := service.DeletePrivateMachine(ctx, &pb.DeleteMachineRequest{PoolName: "aws-cpu", MachineId: "machine-1"})
	require.NoError(t, err)
	require.False(t, deleted.Ok, "a connected machine is not dropped while its node cannot be terminated")
	require.Equal(t, errBYOCDirectScaleUnavailable.Error(), deleted.ErrMsg)
	require.Zero(t, calls, "AWS is not called without a role to call it with")
}

// silentBYOCPool is a BYOC pool whose only machine was last heard from the
// given time ago, with the token its stack joins with.
func silentBYOCPool(silentFor time.Duration) (*fakeComputeRepo, string) {
	now := time.Now().UTC()
	tokenHash := hashComputeToken("byoc-token")
	return &fakeComputeRepo{
		pools: map[string][]*model.PoolState{"workspace-1": {byocPool(now, tokenHash)}},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "aws-cpu"): {{
				WorkspaceID:     "workspace-1",
				PoolName:        "aws-cpu",
				MachineID:       "machine-1",
				TokenHash:       hashComputeToken("machine-token"),
				Schedulable:     true,
				LastJoinAt:      now.Add(-time.Hour),
				LastHeartbeatAt: now.Add(-silentFor),
			}},
		},
		joinTokens: map[string]*model.JoinTokenState{
			tokenHash: {TokenHash: tokenHash, WorkspaceID: "workspace-1", PoolName: "aws-cpu"},
		},
	}, tokenHash
}

// stackCheckAnswers makes the check of a pool's AWS stack answer as given,
// and returns how often it was asked.
func stackCheckAnswers(t *testing.T, deleted bool, err error) *int {
	t.Helper()
	asked := 0
	previous := awsBYOCResourceDeleted
	awsBYOCResourceDeleted = func(context.Context, awsBYOCResourceDeletedInput) (bool, error) {
		asked++
		return deleted, err
	}
	t.Cleanup(func() { awsBYOCResourceDeleted = previous })
	return &asked
}

func requireBYOCPoolKept(t *testing.T, repo *fakeComputeRepo, tokenHash string) {
	t.Helper()
	res, err := (&Service{computeRepo: repo, containerRepo: &fakeContainerRepo{}}).ListPrivatePools(
		testAuthContext("workspace-1", "owner-token"), &pb.ListPrivatePoolsRequest{})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Len(t, res.Pools, 1, "the pool is listed")
	require.Len(t, repo.pools["workspace-1"], 1, "its record is kept")
	require.Len(t, repo.machines[fakeComputeKey("workspace-1", "aws-cpu")], 1, "and so is its machine's")
	require.False(t, repo.joinTokens[tokenHash].Revoked, "the token in the tenant's stack still joins")
}

func TestABYOCPoolIsKeptWhileAWSSaysItsStackExists(t *testing.T) {
	// The pool's only node has been silent for longer than a deleted stack's
	// nodes would be: it is being replaced, or the gateway could not be reached.
	repo, tokenHash := silentBYOCPool(byocDeletedPoolRetention + time.Minute)
	asked := stackCheckAnswers(t, false, nil)

	requireBYOCPoolKept(t, repo, tokenHash)
	require.NotZero(t, *asked, "AWS was asked, and its answer decides")
}

func TestABYOCPoolIsKeptWhileAWSCannotSayWhetherItsStackExists(t *testing.T) {
	repo, tokenHash := silentBYOCPool(byocDeletedPoolRetention + time.Minute)
	stackCheckAnswers(t, false, errors.New("describe AWS BYOC stack: Throttling: Rate exceeded"))

	requireBYOCPoolKept(t, repo, tokenHash)
}

func TestABYOCPoolIsCleanedUpWhenAWSSaysItsStackIsGone(t *testing.T) {
	// Its machine was heard from a moment ago, and AWS knows better.
	repo, tokenHash := silentBYOCPool(time.Second)
	stackCheckAnswers(t, true, nil)

	res, err := (&Service{computeRepo: repo, containerRepo: &fakeContainerRepo{}}).ListPrivatePools(
		testAuthContext("workspace-1", "owner-token"), &pb.ListPrivatePoolsRequest{})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Empty(t, res.Pools)
	require.Empty(t, repo.pools["workspace-1"])
	require.True(t, repo.joinTokens[tokenHash].Revoked)
}

func TestABYOCPoolThatAWSCannotBeAskedAboutIsCleanedUpOnceItsMachinesStaySilent(t *testing.T) {
	// A stack from before stacks gave Beam a role to ask with.
	withoutRole := func(repo *fakeComputeRepo) {
		labels := repo.pools["workspace-1"][0].BYOC.Labels
		delete(labels, awsBYOCControlRoleArnLabel)
		delete(labels, awsBYOCControlRoleExternalIDLabel)
	}

	repo, tokenHash := silentBYOCPool(byocDeletedPoolRetention - time.Minute)
	withoutRole(repo)
	asked := stackCheckAnswers(t, false, nil)
	requireBYOCPoolKept(t, repo, tokenHash)

	repo, tokenHash = silentBYOCPool(byocDeletedPoolRetention + time.Minute)
	withoutRole(repo)
	res, err := (&Service{computeRepo: repo, containerRepo: &fakeContainerRepo{}}).ListPrivatePools(
		testAuthContext("workspace-1", "owner-token"), &pb.ListPrivatePoolsRequest{})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Empty(t, res.Pools)
	require.Empty(t, repo.pools["workspace-1"])
	require.True(t, repo.joinTokens[tokenHash].Revoked)
	require.Zero(t, *asked, "there is nobody to ask")
}
