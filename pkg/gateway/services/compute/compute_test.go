package compute

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/smithy-go"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/providers/watch"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestAgentBootstrapUsesConfiguredPublishingRegistry(t *testing.T) {
	for _, tt := range []struct {
		name, configured, fallback, want string
		insecure                         bool
	}{
		{"explicit insecure", "registry.example.com", "runner.example.com", "registry.example.com", true},
		{"explicit secure", "registry.example.com", "", "registry.example.com", false},
		{"runner fallback", "", "runner.example.com", "runner.example.com", true},
		{"localhost fallback", "", "", "localhost", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			service := &Service{appConfig: types.AppConfig{ImageService: types.ImageServiceConfig{
				BuildRegistry: tt.configured, BuildRepositoryName: "stage/vms", BuildRegistryInsecure: tt.insecure,
				Runner: types.RunnerConfig{BaseImageRegistry: tt.fallback},
			}}}
			bootstrap, err := service.agentBootstrapConfig(context.Background(), "workspace-one", &model.PoolState{
				Name: "vms", Config: &pb.PoolConfig{Name: "vms"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if bootstrap.ImageBuildRegistry != tt.want || bootstrap.ImageBuildRepositoryName != "stage/vms" || bootstrap.ImageBuildRegistryInsecure != tt.insecure {
				t.Fatal("bootstrap did not preserve resolved publishing settings")
			}
		})
	}
}

func TestPrivatePoolReadsAreWorkspaceScoped(t *testing.T) {
	ctx := testAuthContext("workspace-1", "viewer-token")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{Name: "owned", Mode: string(types.PoolModePrivate), CreatedByTokenID: "owner-token", Status: "active"},
				{Name: "other", Mode: string(types.PoolModePrivate), CreatedByTokenID: "other-token", Status: "active"},
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

func TestPrivatePoolJoinTokenIsWorkspaceOwned(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {{
				WorkspaceID:      "workspace-1",
				Name:             "pool-1",
				Mode:             string(types.PoolModePrivate),
				Config:           &pb.PoolConfig{Name: "pool-1", Mode: string(types.PoolModePrivate)},
				CreatedByTokenID: "creator-token",
				CreatedAt:        now,
			}},
		},
	}
	service := &Service{computeRepo: repo}

	created, err := service.CreatePoolJoinToken(
		testAuthContext("workspace-1", "current-token"),
		&pb.CreatePoolJoinTokenRequest{PoolName: "pool-1"},
	)
	if err != nil || !created.Ok {
		t.Fatalf("CreatePoolJoinToken() response = %+v, err = %v", created, err)
	}

	joined, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken:          created.Token,
		MachineFingerprint: "fingerprint-1",
		CpuCount:           4,
		MemoryMb:           8192,
		Schedulable:        true,
	})
	if err != nil || !joined.Ok {
		t.Fatalf("JoinAgent() response = %+v, err = %v", joined, err)
	}

	denied, err := service.RevokePoolJoinToken(
		testAuthContext("workspace-2", "other-token"),
		&pb.RevokePoolJoinTokenRequest{Token: created.Token},
	)
	if err != nil || denied.Ok || denied.ErrMsg != "join token not found" {
		t.Fatalf("cross-workspace RevokePoolJoinToken() response = %+v, err = %v", denied, err)
	}

	revoked, err := service.RevokePoolJoinToken(
		testAuthContext("workspace-1", "rotated-token"),
		&pb.RevokePoolJoinTokenRequest{Token: created.Token},
	)
	if err != nil || !revoked.Ok {
		t.Fatalf("same-workspace RevokePoolJoinToken() response = %+v, err = %v", revoked, err)
	}
	if state := repo.joinTokens[hashComputeToken(created.Token)]; state == nil || !state.Revoked {
		t.Fatalf("stored join token = %+v, want revoked", state)
	}
}

func TestPrivatePoolJoinTokenCannotCrossWorkspaces(t *testing.T) {
	service := &Service{computeRepo: &fakeComputeRepo{pools: map[string][]*model.PoolState{
		"workspace-1": {{Name: "pool-1", Mode: string(types.PoolModePrivate)}},
	}}}

	response, err := service.CreatePoolJoinToken(
		testAuthContext("workspace-2", "other-token"),
		&pb.CreatePoolJoinTokenRequest{PoolName: "pool-1"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if response.Ok || response.ErrMsg != "pool not found" {
		t.Fatalf("CreatePoolJoinToken() response = %+v", response)
	}
}

func TestPrivatePoolJoinTokenCannotJoinRecreatedPool(t *testing.T) {
	createdAt := time.Now().UTC()
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{
		"workspace-1": {{
			WorkspaceID: "workspace-1",
			Name:        "pool-1",
			Mode:        string(types.PoolModePrivate),
			Config:      &pb.PoolConfig{Name: "pool-1", Mode: string(types.PoolModePrivate)},
			CreatedAt:   createdAt,
		}},
	}}
	service := &Service{computeRepo: repo}
	created, err := service.CreatePoolJoinToken(
		testAuthContext("workspace-1", "workspace-token"),
		&pb.CreatePoolJoinTokenRequest{PoolName: "pool-1"},
	)
	if err != nil || !created.Ok {
		t.Fatalf("CreatePoolJoinToken() response = %+v, err = %v", created, err)
	}

	repo.pools["workspace-1"][0].CreatedAt = createdAt.Add(time.Second)
	joined, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken:          created.Token,
		MachineFingerprint: "fingerprint-1",
		CpuCount:           4,
		MemoryMb:           8192,
		Schedulable:        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if joined.Ok || joined.ErrMsg != "join token was issued for a previous instance of this pool" {
		t.Fatalf("JoinAgent() response = %+v", joined)
	}
}

// Legacy tokens predate PoolCreatedAt; rejecting their zero value stranded
// machines provisioned right before a deploy.
func TestPrivatePoolJoinTokenWithoutPoolCreatedAtStillJoins(t *testing.T) {
	createdAt := time.Now().UTC()
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{
		"workspace-1": {{
			WorkspaceID: "workspace-1",
			Name:        "pool-1",
			Mode:        string(types.PoolModePrivate),
			Config:      &pb.PoolConfig{Name: "pool-1", Mode: string(types.PoolModePrivate)},
			CreatedAt:   createdAt,
		}},
	}}
	repo.joinTokens = map[string]*model.JoinTokenState{
		hashComputeToken("legacy-token"): {
			TokenHash:   hashComputeToken("legacy-token"),
			WorkspaceID: "workspace-1",
			PoolName:    "pool-1",
			// No PoolCreatedAt: minted by a build that predates the field.
		},
	}
	service := &Service{computeRepo: repo}

	joined, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken:          "legacy-token",
		MachineFingerprint: "fingerprint-1",
		CpuCount:           4,
		MemoryMb:           8192,
		Schedulable:        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !joined.Ok {
		t.Fatalf("JoinAgent() rejected a legacy token: %+v", joined)
	}
}

func TestListPrivatePoolsReadyMachineCountUsesAgentConnection(t *testing.T) {
	now := time.Now().UTC()
	ctx := testAuthContext("workspace-1", "viewer-token")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{Name: "private-pool", Mode: string(types.PoolModePrivate), Status: "active"},
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

func TestConfiguredRuntimePoolReadyMachineCountRequiresMatchingWorker(t *testing.T) {
	now := time.Now().UTC()
	ctx := testAuthContext("workspace-1", "viewer-token")
	machine := &model.AgentTokenState{
		WorkspaceID:     "workspace-1",
		PoolName:        "restore-pool",
		MachineID:       "machine-1",
		Schedulable:     true,
		LastJoinAt:      now.Add(-time.Minute),
		LastHeartbeatAt: now,
	}
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {{
				Name:   "restore-pool",
				Mode:   string(types.PoolModePrivate),
				Status: "active",
				Config: &pb.PoolConfig{ContainerRuntime: types.ContainerRuntimeGvisor.String()},
			}},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "restore-pool"): {machine},
		},
	}
	worker := &types.Worker{
		Id:        model.AgentMachineWorkerID(machine.MachineID),
		PoolName:  machine.PoolName,
		MachineId: machine.MachineID,
		Status:    types.WorkerStatusAvailable,
		Runtime:   types.ContainerRuntimeRunc.String(),
	}
	service := &Service{computeRepo: repo, workerRepo: &fakeWorkerRepo{worker: worker}}

	pools, err := service.ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil || !pools.Ok {
		t.Fatalf("ListPrivatePools() = (%+v, %v)", pools, err)
	}
	if got := pools.Pools[0].ReadyMachineCount; got != 0 {
		t.Fatalf("runc restore pool ready machines = %d, want 0", got)
	}

	worker.Runtime = types.ContainerRuntimeGvisor.String()
	pools, err = service.ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil || !pools.Ok {
		t.Fatalf("ListPrivatePools() = (%+v, %v)", pools, err)
	}
	if got := pools.Pools[0].ReadyMachineCount; got != 1 {
		t.Fatalf("available gVisor restore pool ready machines = %d, want 1", got)
	}

	repo.pools["workspace-1"][0].Config.ContainerRuntime = types.ContainerRuntimeRunc.String()
	pools, err = service.ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil || !pools.Ok {
		t.Fatalf("ListPrivatePools() = (%+v, %v)", pools, err)
	}
	if got := pools.Pools[0].ReadyMachineCount; got != 0 {
		t.Fatalf("gVisor worker in runc pool ready machines = %d, want 0", got)
	}

	worker.Runtime = types.ContainerRuntimeRunc.String()
	pools, err = service.ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil || !pools.Ok {
		t.Fatalf("ListPrivatePools() = (%+v, %v)", pools, err)
	}
	if got := pools.Pools[0].ReadyMachineCount; got != 1 {
		t.Fatalf("available runc pool ready machines = %d, want 1", got)
	}
}

func TestFindReadyPrivatePoolForGPU(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{Name: "ondemand-a6000", Mode: string(types.PoolModePrivate), Status: "active", Config: &pb.PoolConfig{Gpu: []string{"A6000"}}},
				{Name: "empty-t4", Mode: string(types.PoolModePrivate), Status: "active", Config: &pb.PoolConfig{Gpu: []string{"T4"}}},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "ondemand-a6000"): {
				{
					WorkspaceID:     "workspace-1",
					PoolName:        "ondemand-a6000",
					MachineID:       "machine-1",
					Schedulable:     true,
					LastJoinAt:      now.Add(-time.Minute),
					LastHeartbeatAt: now,
				},
			},
		},
	}
	service := &Service{computeRepo: repo}
	ctx := context.Background()

	// Matching GPU with a connected machine.
	pool, err := service.FindReadyPrivatePoolForGPU(ctx, "workspace-1", []types.GpuType{types.GpuType("A6000")})
	if err != nil {
		t.Fatalf("FindReadyPrivatePoolForGPU() error = %v", err)
	}
	if pool != "ondemand-a6000" {
		t.Fatalf("pool = %q, want %q", pool, "ondemand-a6000")
	}

	// Matching GPU config but no connected machines: not ready.
	pool, err = service.FindReadyPrivatePoolForGPU(ctx, "workspace-1", []types.GpuType{types.GpuType("T4")})
	if err != nil {
		t.Fatalf("FindReadyPrivatePoolForGPU() error = %v", err)
	}
	if pool != "" {
		t.Fatalf("pool = %q, want empty", pool)
	}

	// No pool configured for the GPU at all.
	pool, err = service.FindReadyPrivatePoolForGPU(ctx, "workspace-1", []types.GpuType{types.GpuType("H100")})
	if err != nil {
		t.Fatalf("FindReadyPrivatePoolForGPU() error = %v", err)
	}
	if pool != "" {
		t.Fatalf("pool = %q, want empty", pool)
	}
}

func TestCreatePoolCanBeUpdatedByAnotherWorkspaceToken(t *testing.T) {
	ctx := testAuthContext("workspace-1", "viewer-token")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{Name: "existing", Mode: string(types.PoolModePrivate), CreatedByTokenID: "owner-token", Status: "active"},
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
	if !res.Ok {
		t.Fatalf("CreatePool() not ok: %s", res.ErrMsg)
	}
	if !repo.savedPool {
		t.Fatal("CreatePool() did not persist the workspace-owned update")
	}
	if got, want := repo.pools["workspace-1"][0].CreatedByTokenID, "owner-token"; got != want {
		t.Fatalf("creator audit field = %q, want %q", got, want)
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

func TestCreatePoolRejectsConfiguredWorkerPoolIdentity(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	for _, pool := range []*pb.PoolConfig{
		{Name: "configured"},
		{Name: "private", Selector: "configured"},
	} {
		repo := &fakeComputeRepo{}
		service := &Service{
			computeRepo: repo,
			appConfig: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
				"configured": {Mode: types.PoolModeLocal},
			}}},
		}

		res, err := service.CreatePool(ctx, &pb.CreatePoolRequest{Pool: pool})
		if err != nil || res.Ok || !strings.Contains(res.ErrMsg, "configured worker pool") {
			t.Fatalf("CreatePool(%+v) = %+v, %v; want config conflict", pool, res, err)
		}
		if repo.savedPool {
			t.Fatal("config conflict persisted private pool state")
		}
	}
}

func TestCreatePoolRollsBackFailedRegistration(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	manager := scheduler.NewWorkerPoolManager()
	s := scheduler.NewSchedulerForCapacityChecks(nil, repo, manager)
	if err := s.EnsureAgentPool("workspace-1", &model.PoolState{
		Name: "selector-owner", Selector: "shared", Mode: string(types.PoolModePrivate),
	}); err != nil {
		t.Fatal(err)
	}
	service := &Service{computeRepo: repo, scheduler: s}

	res, err := service.CreatePool(ctx, &pb.CreatePoolRequest{Pool: &pb.PoolConfig{Name: "requested", Selector: "shared"}})
	if err != nil || res.Ok {
		t.Fatalf("CreatePool() = %+v, %v; want registration conflict", res, err)
	}
	if state, err := repo.GetPoolState(ctx, "workspace-1", "requested"); err != nil || state != nil {
		t.Fatalf("partial pool state = %+v, %v; want rollback", state, err)
	}

	previous := &model.PoolState{
		Name: "requested", Selector: "old", Mode: string(types.PoolModePrivate), CreatedByTokenID: "owner-token",
	}
	if err := repo.SavePoolState(ctx, "workspace-1", previous); err != nil {
		t.Fatal(err)
	}
	res, err = service.CreatePool(ctx, &pb.CreatePoolRequest{Pool: &pb.PoolConfig{Name: "requested", Selector: "shared"}})
	if err != nil || res.Ok {
		t.Fatalf("CreatePool(update) = %+v, %v; want registration conflict", res, err)
	}
	state, err := repo.GetPoolState(ctx, "workspace-1", "requested")
	if err != nil || state == nil || state.Selector != "old" {
		t.Fatalf("restored pool state = %+v, %v; want old selector", state, err)
	}
}

func TestCreatePoolRemovesOldSelectorAfterReplacement(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	previous := &model.PoolState{
		Name: "private", Selector: "old", Mode: string(types.PoolModePrivate), CreatedByTokenID: "owner-token",
	}
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {previous}}}
	s := scheduler.NewSchedulerForCapacityChecks(nil, repo, scheduler.NewWorkerPoolManager())
	if err := s.EnsureAgentPool("workspace-1", previous); err != nil {
		t.Fatal(err)
	}
	service := &Service{computeRepo: repo, scheduler: s}

	res, err := service.CreatePool(ctx, &pb.CreatePoolRequest{Pool: &pb.PoolConfig{Name: "private", Selector: "new"}})
	if err != nil || !res.Ok {
		t.Fatalf("CreatePool() = %+v, %v", res, err)
	}
	if err := s.EnsureAgentPool("workspace-1", &model.PoolState{
		Name: "old-selector-replacement", Selector: "old", Mode: string(types.PoolModePrivate),
	}); err != nil {
		t.Fatalf("old selector controller was not removed: %v", err)
	}
}

func TestCreateBYOCPoolCreatesAWSCPUPrivatePool(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	templateURL := "https://s3.us-east-1.amazonaws.com/beam-byoc-templates-test/aws/byoc-template.yaml"
	service := &Service{
		computeRepo: repo,
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				HTTP: types.HTTPConfig{ExternalHost: "app.beam.cloud", ExternalPort: 443, TLS: true},
			},
			ManagedCompute: types.ManagedComputeConfig{
				BYOC: types.ManagedComputeBYOCConfig{
					AWS: types.ManagedComputeBYOCAWSConfig{TemplateURL: templateURL},
				},
			},
			Worker: types.WorkerConfig{
				ImageRegistry: "public.ecr.aws/n4e0e1y0",
				ImageName:     "beta9-worker",
				ImageTag:      "worker-test",
			},
		},
	}

	res, err := service.CreateBYOCPool(ctx, &pb.CreateBYOCPoolRequest{
		Provider:     "aws",
		PoolName:     "aws-cpu",
		Region:       "us-east-1",
		InstanceType: "i4i.xlarge",
		DesiredNodes: 2,
		MaxNodes:     4,
		AccountId:    "123456789012",
	})
	if err != nil {
		t.Fatalf("CreateBYOCPool() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("CreateBYOCPool() not ok: %s", res.ErrMsg)
	}
	if got, want := res.Pool.Name, "aws-cpu"; got != want {
		t.Fatalf("pool name = %q, want %q", got, want)
	}
	if got, want := res.Pool.Source, string(model.SourceAWS); got != want {
		t.Fatalf("pool source = %q, want %q", got, want)
	}
	if res.SetupUrl == "" {
		t.Fatal("setup url is required")
	}
	if res.ResourceName == "" || !strings.Contains(res.ResourceUrl, res.ResourceName) {
		t.Fatalf("resource response = name %q url %q, want URL containing resource name", res.ResourceName, res.ResourceUrl)
	}
	if !strings.Contains(res.SetupUrl, "templateURL="+url.QueryEscape(templateURL)) {
		t.Fatalf("setup url should reference the BYOC AWS S3 template URL: %s", res.SetupUrl)
	}
	for _, fragment := range []string{
		"templateURL=",
		"stackName=",
		"param_BeamGatewayURL=",
		"param_BeamJoinToken=",
		"param_BeamWorkerImage=public.ecr.aws%2Fn4e0e1y0%2Fbeta9-worker%3Aworker-test",
		"param_ContainerStartConcurrency=16",
		"param_DesiredCapacity=2",
		"param_MaxSize=4",
		"param_NetworkSlots=128",
		"param_NodeGPUCount=0",
		"param_NodeInstanceType=i4i.xlarge",
		"param_RootVolumeSizeGB=200",
		"param_TargetAWSAccountID=123456789012",
	} {
		if !strings.Contains(res.SetupUrl, fragment) {
			t.Fatalf("setup url missing %q: %s", fragment, res.SetupUrl)
		}
	}
	if got, want := len(repo.joinTokens), 1; got != want {
		t.Fatalf("join token count = %d, want %d", got, want)
	}
	joinTokenHash := ""
	for _, token := range repo.joinTokens {
		joinTokenHash = token.TokenHash
		if token.WorkspaceID != "workspace-1" || token.PoolName != "aws-cpu" {
			t.Fatalf("join token identity = %+v, want workspace/pool", token)
		}
		if !token.ExpiresAt.IsZero() {
			t.Fatalf("join token expiry = %s, want persistent BYOC bootstrap token", token.ExpiresAt)
		}
	}
	stored := repo.pools["workspace-1"][0]
	if stored.BYOC == nil || stored.BYOC.Provider != "aws" || stored.BYOC.AccountID != "123456789012" || stored.BYOC.Region != "us-east-1" || stored.BYOC.ResourceName != res.ResourceName {
		t.Fatalf("stored BYOC metadata = %+v, want aws account/region/resource", stored.BYOC)
	}
	if len(stored.Config.GetGpu()) != 0 {
		t.Fatalf("stored CPU pool GPU config = %v, want none", stored.Config.GetGpu())
	}
	if got := stored.BYOC.Labels[byocBootstrapJoinTokenHashLabel]; got == "" || got != joinTokenHash {
		t.Fatalf("stored BYOC join token hash = %q, want %q", got, joinTokenHash)
	}
	if got := stored.BYOC.Labels[byocBootstrapJoinTokenHashesLabel]; got == "" || got != joinTokenHash {
		t.Fatalf("stored BYOC join token hashes = %q, want %q", got, joinTokenHash)
	}
	for key, want := range map[string]string{
		"instance_type":            "i4i.xlarge",
		"desired_nodes":            "2",
		"max_nodes":                "4",
		"target_sandboxes":         "40",
		"sandboxes_per_node":       "20",
		"hourly_cost_micros":       "182000",
		"total_hourly_cost_micros": "364000",
	} {
		if got := stored.BYOC.Labels[key]; got != want {
			t.Fatalf("stored BYOC label %s = %q, want %q", key, got, want)
		}
	}
	if got, want := res.GetByoc().GetHourlyCostMicros(), int64(182000); got != want {
		t.Fatalf("BYOC hourly cost micros = %d, want %d", got, want)
	}
	if got, want := res.GetByoc().GetTotalHourlyCostMicros(), int64(364000); got != want {
		t.Fatalf("BYOC total hourly cost micros = %d, want %d", got, want)
	}
	if res.GetByoc().GetGpu() != "" || res.GetByoc().GetGpuCount() != 0 {
		t.Fatalf("CPU BYOC gpu = %q/%d, want none", res.GetByoc().GetGpu(), res.GetByoc().GetGpuCount())
	}
}

func TestCreateBYOCPoolRollsBackFailedRegistration(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	s := scheduler.NewSchedulerForCapacityChecks(nil, repo, scheduler.NewWorkerPoolManager())
	if err := s.EnsureAgentPool("workspace-1", &model.PoolState{
		Name: "selector-owner", Selector: "aws-cpu", Mode: string(types.PoolModePrivate),
	}); err != nil {
		t.Fatal(err)
	}
	service := &Service{
		computeRepo: repo,
		scheduler:   s,
		appConfig: types.AppConfig{ManagedCompute: types.ManagedComputeConfig{BYOC: types.ManagedComputeBYOCConfig{
			AWS: types.ManagedComputeBYOCAWSConfig{TemplateURL: "https://example.com/template.yaml"},
		}}},
	}

	res, err := service.CreateBYOCPool(ctx, &pb.CreateBYOCPoolRequest{
		Provider: "aws", PoolName: "aws-cpu", Region: "us-east-1", InstanceType: "i4i.xlarge", DesiredNodes: 1, MaxNodes: 1,
	})
	if err != nil || res.Ok {
		t.Fatalf("CreateBYOCPool() = %+v, %v; want registration conflict", res, err)
	}
	if state, err := repo.GetPoolState(ctx, "workspace-1", "aws-cpu"); err != nil || state != nil {
		t.Fatalf("partial BYOC pool state = %+v, %v; want rollback", state, err)
	}
}

func TestCreateBYOCPoolCreatesAWSGPUPrivatePool(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	templateURL := "https://s3.us-east-1.amazonaws.com/beam-byoc-templates-test/aws/byoc-template.yaml"
	service := &Service{
		computeRepo: repo,
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				HTTP: types.HTTPConfig{ExternalHost: "app.beam.cloud", ExternalPort: 443, TLS: true},
			},
			ManagedCompute: types.ManagedComputeConfig{
				BYOC: types.ManagedComputeBYOCConfig{
					AWS: types.ManagedComputeBYOCAWSConfig{TemplateURL: templateURL},
				},
			},
		},
	}

	res, err := service.CreateBYOCPool(ctx, &pb.CreateBYOCPoolRequest{
		Provider:     "aws",
		PoolName:     "aws-gpu",
		Region:       "us-east-1",
		InstanceType: "g6.xlarge",
		DesiredNodes: 2,
		MaxNodes:     2,
		AccountId:    "123456789012",
		Gpu:          "L4",
		GpuCount:     1,
	})
	if err != nil {
		t.Fatalf("CreateBYOCPool() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("CreateBYOCPool() not ok: %s", res.ErrMsg)
	}
	if got, want := res.Pool.GetConfig().GetGpu(), []string{"L4"}; !sameStrings(got, want) {
		t.Fatalf("pool GPU config = %v, want %v", got, want)
	}
	for _, fragment := range []string{
		"param_NodeInstanceType=g6.xlarge",
		"param_NodeGPUCount=1",
		"param_DesiredCapacity=2",
	} {
		if !strings.Contains(res.SetupUrl, fragment) {
			t.Fatalf("setup url missing %q: %s", fragment, res.SetupUrl)
		}
	}
	stored := repo.pools["workspace-1"][0]
	for key, want := range map[string]string{
		"instance_type":            "g6.xlarge",
		"gpu":                      "L4",
		"gpu_count":                "1",
		"capacity_unit":            "gpu",
		"capacity_unit_label":      "GPUs",
		"hourly_cost_micros":       "110000",
		"total_hourly_cost_micros": "220000",
	} {
		if got := stored.BYOC.Labels[key]; got != want {
			t.Fatalf("stored BYOC label %s = %q, want %q", key, got, want)
		}
	}
	if got, want := res.GetByoc().GetGpu(), "L4"; got != want {
		t.Fatalf("BYOC gpu = %q, want %q", got, want)
	}
	if got, want := res.GetByoc().GetGpuCount(), uint32(1); got != want {
		t.Fatalf("BYOC gpu count = %d, want %d", got, want)
	}
	if got, want := res.GetByoc().GetTotalHourlyCostMicros(), int64(220000); got != want {
		t.Fatalf("BYOC total hourly cost micros = %d, want %d", got, want)
	}
}

func TestCreateBYOCPoolRejectsChangingExistingAWSHardwareClass(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	service := &Service{
		computeRepo: repo,
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				HTTP: types.HTTPConfig{ExternalHost: "app.beam.cloud", ExternalPort: 443, TLS: true},
			},
			ManagedCompute: types.ManagedComputeConfig{
				BYOC: types.ManagedComputeBYOCConfig{
					AWS: types.ManagedComputeBYOCAWSConfig{
						TemplateURL: "https://s3.us-east-1.amazonaws.com/beam-byoc-templates-test/aws/byoc-template.yaml",
					},
				},
			},
		},
	}

	first, err := service.CreateBYOCPool(ctx, &pb.CreateBYOCPoolRequest{
		Provider:     "aws",
		PoolName:     "aws-cpu",
		Region:       "us-east-1",
		InstanceType: "i4i.xlarge",
		DesiredNodes: 1,
		AccountId:    "123456789012",
	})
	if err != nil || !first.Ok {
		t.Fatalf("first CreateBYOCPool() = (%v, %v)", first, err)
	}

	second, err := service.CreateBYOCPool(ctx, &pb.CreateBYOCPoolRequest{
		Provider:     "aws",
		PoolName:     "aws-cpu",
		Region:       "us-east-1",
		InstanceType: "g6.xlarge",
		DesiredNodes: 1,
		AccountId:    "123456789012",
		Gpu:          "L4",
		GpuCount:     1,
	})
	if err != nil {
		t.Fatalf("second CreateBYOCPool() error = %v", err)
	}
	if second.Ok {
		t.Fatal("CreateBYOCPool() unexpectedly allowed changing an existing AWS pool from CPU to GPU")
	}
	if !strings.Contains(second.ErrMsg, "create a new pool") {
		t.Fatalf("error = %q, want create a new pool guidance", second.ErrMsg)
	}
}

func TestCreateBYOCPoolKeepsExistingBYOCBootstrapJoinTokens(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	service := &Service{
		computeRepo: repo,
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				HTTP: types.HTTPConfig{ExternalHost: "app.beam.cloud", ExternalPort: 443, TLS: true},
			},
			ManagedCompute: types.ManagedComputeConfig{
				BYOC: types.ManagedComputeBYOCConfig{
					AWS: types.ManagedComputeBYOCAWSConfig{TemplateURL: "https://s3.us-east-1.amazonaws.com/beam-byoc-templates-test/aws/byoc-template.yaml"},
				},
			},
		},
	}

	first, err := service.CreateBYOCPool(ctx, &pb.CreateBYOCPoolRequest{
		Provider:     "aws",
		PoolName:     "aws-cpu",
		InstanceType: "i4i.xlarge",
		AccountId:    "123456789012",
	})
	if err != nil || !first.Ok {
		t.Fatalf("first CreateBYOCPool() = (%v, %v)", first, err)
	}
	firstHash := repo.pools["workspace-1"][0].BYOC.Labels[byocBootstrapJoinTokenHashLabel]

	second, err := service.CreateBYOCPool(ctx, &pb.CreateBYOCPoolRequest{
		Provider:     "aws",
		PoolName:     "aws-cpu",
		InstanceType: "i4i.xlarge",
		DesiredNodes: 2,
		MaxNodes:     2,
		AccountId:    "123456789012",
	})
	if err != nil || !second.Ok {
		t.Fatalf("second CreateBYOCPool() = (%v, %v)", second, err)
	}
	secondHash := repo.pools["workspace-1"][0].BYOC.Labels[byocBootstrapJoinTokenHashLabel]
	if firstHash == "" || secondHash == "" || firstHash == secondHash {
		t.Fatalf("BYOC join token hashes = first %q second %q, want a new setup token", firstHash, secondHash)
	}
	if repo.joinTokens[firstHash].Revoked {
		t.Fatal("previous BYOC bootstrap join token was revoked before pool deletion")
	}
	if repo.joinTokens[secondHash].Revoked {
		t.Fatal("current BYOC bootstrap join token was revoked")
	}
	hashes := strings.Split(repo.pools["workspace-1"][0].BYOC.Labels[byocBootstrapJoinTokenHashesLabel], ",")
	if got, want := hashes, []string{firstHash, secondHash}; !sameStrings(got, want) {
		t.Fatalf("stored BYOC bootstrap join token hashes = %v, want %v", got, want)
	}
}

func TestCreateBYOCPoolRejectsMissingAWSTemplateURL(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	service := &Service{computeRepo: repo}

	res, err := service.CreateBYOCPool(ctx, &pb.CreateBYOCPoolRequest{
		Provider:  "aws",
		PoolName:  "aws-cpu",
		AccountId: "123456789012",
	})
	if err != nil {
		t.Fatalf("CreateBYOCPool() error = %v", err)
	}
	if res.Ok {
		t.Fatal("CreateBYOCPool() unexpectedly succeeded")
	}
	if !strings.Contains(res.ErrMsg, "template URL is not configured") {
		t.Fatalf("error = %q, want missing template URL", res.ErrMsg)
	}
	if len(repo.pools["workspace-1"]) != 0 || len(repo.joinTokens) != 0 {
		t.Fatalf("misconfigured BYOC pool setup created state: pools=%d tokens=%d", len(repo.pools["workspace-1"]), len(repo.joinTokens))
	}
}

func TestCreateBYOCPoolRejectsNonS3AWSTemplateURL(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	service := &Service{
		computeRepo: &fakeComputeRepo{},
		appConfig: types.AppConfig{
			ManagedCompute: types.ManagedComputeConfig{
				BYOC: types.ManagedComputeBYOCConfig{
					AWS: types.ManagedComputeBYOCAWSConfig{
						TemplateURL: "https://app.stage.beam.cloud/api/v1/gateway/pools/byoc/aws/template.yaml",
					},
				},
			},
		},
	}

	res, err := service.CreateBYOCPool(ctx, &pb.CreateBYOCPoolRequest{
		Provider:  "aws",
		PoolName:  "aws-cpu",
		AccountId: "123456789012",
	})
	if err != nil {
		t.Fatalf("CreateBYOCPool() error = %v", err)
	}
	if res.Ok {
		t.Fatal("CreateBYOCPool() unexpectedly succeeded")
	}
	if !strings.Contains(res.ErrMsg, "CloudFormation-supported S3 URL") {
		t.Fatalf("error = %q, want unsupported URL", res.ErrMsg)
	}
}

func TestGetBYOCPoolReportsReadiness(t *testing.T) {
	now := time.Now().UTC()
	ctx := testAuthContext("workspace-1", "owner-token")
	resourceURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					BYOC: &model.BYOCProviderState{
						Provider:     "aws",
						AccountID:    "123456789012",
						Region:       "us-east-1",
						ResourceName: "beam-aws-cpu-test",
						ResourceURL:  resourceURL,
						DestroyURL:   resourceURL,
					},
				},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "aws-cpu"): {
				{
					WorkspaceID:     "workspace-1",
					PoolName:        "aws-cpu",
					MachineID:       "machine-1",
					Schedulable:     true,
					LastJoinAt:      now.Add(-time.Minute),
					LastHeartbeatAt: now,
				},
			},
		},
	}
	service := &Service{computeRepo: repo}

	res, err := service.GetBYOCPool(ctx, &pb.GetBYOCPoolRequest{PoolName: "aws-cpu"})
	if err != nil {
		t.Fatalf("GetBYOCPool() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("GetBYOCPool() not ok: %s", res.ErrMsg)
	}
	if !res.Ready {
		t.Fatal("expected pool to be ready")
	}
	if got, want := res.ReadyMachineCount, uint32(1); got != want {
		t.Fatalf("ready machines = %d, want %d", got, want)
	}
	if got, want := res.Byoc.GetResourceUrl(), resourceURL; got != want {
		t.Fatalf("BYOC resource url = %q, want %q", got, want)
	}
}

func TestListPrivatePoolsIncludesBYOCState(t *testing.T) {
	now := time.Now().UTC()
	ctx := testAuthContext("workspace-1", "owner-token")
	resourceURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					Config:           &pb.PoolConfig{Name: "aws-cpu", Regions: []string{"us-east-1"}},
					BYOC: &model.BYOCProviderState{
						Provider:     "aws",
						AccountID:    "123456789012",
						Region:       "us-east-1",
						ResourceName: "beam-aws-cpu-test",
						ResourceURL:  resourceURL,
						DestroyURL:   resourceURL,
						Labels: map[string]string{
							"instance_type":      "i4i.xlarge",
							"desired_nodes":      "2",
							"max_nodes":          "4",
							"target_sandboxes":   "40",
							"sandboxes_per_node": "20",
						},
					},
				},
				{Name: "attached-cpu", Mode: string(types.PoolModePrivate), CreatedByTokenID: "owner-token", Source: model.SourceAttached},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "aws-cpu"): {
				{
					WorkspaceID:     "workspace-1",
					PoolName:        "aws-cpu",
					MachineID:       "machine-1",
					Schedulable:     true,
					LastJoinAt:      now.Add(-time.Minute),
					LastHeartbeatAt: now,
				},
				{
					WorkspaceID: "workspace-1",
					PoolName:    "aws-cpu",
					MachineID:   "machine-2",
					Schedulable: true,
				},
			},
		},
	}
	service := &Service{computeRepo: repo}

	res, err := service.ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil {
		t.Fatalf("ListPrivatePools() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("ListPrivatePools() not ok: %s", res.ErrMsg)
	}
	if got, want := len(res.Pools), 2; got != want {
		t.Fatalf("pool count = %d, want %d", got, want)
	}
	var awsPool *pb.PrivatePool
	for _, pool := range res.Pools {
		if pool.GetName() == "aws-cpu" {
			awsPool = pool
		} else if pool.GetByoc() != nil {
			t.Fatalf("non-BYOC pool %q unexpectedly has BYOC state", pool.GetName())
		}
	}
	if awsPool == nil {
		t.Fatal("aws-cpu pool not found")
	}
	byoc := awsPool.GetByoc()
	if byoc == nil {
		t.Fatal("aws-cpu pool missing BYOC state")
	}
	if got, want := byoc.Provider, "aws"; got != want {
		t.Fatalf("provider = %q, want %q", got, want)
	}
	if got, want := byoc.AccountId, "123456789012"; got != want {
		t.Fatalf("account id = %q, want %q", got, want)
	}
	if got, want := byoc.Phase, "partially_ready"; got != want {
		t.Fatalf("phase = %q, want %q", got, want)
	}
	if got, want := byoc.MachineCount, uint32(2); got != want {
		t.Fatalf("machine count = %d, want %d", got, want)
	}
	if got, want := byoc.ReadyMachineCount, uint32(1); got != want {
		t.Fatalf("ready machine count = %d, want %d", got, want)
	}
	if got, want := byoc.DesiredNodes, uint32(2); got != want {
		t.Fatalf("desired nodes = %d, want %d", got, want)
	}
	if got, want := byoc.TargetSandboxes, uint32(40); got != want {
		t.Fatalf("target sandboxes = %d, want %d", got, want)
	}
	if got, want := byoc.HourlyCostMicros, int64(182000); got != want {
		t.Fatalf("hourly cost micros = %d, want %d", got, want)
	}
	if got, want := byoc.TotalHourlyCostMicros, int64(364000); got != want {
		t.Fatalf("total hourly cost micros = %d, want %d", got, want)
	}
}

func TestListPrivatePoolsCleansDeletedBYOCPool(t *testing.T) {
	now := time.Now().UTC()
	ctx := testAuthContext("workspace-1", "owner-token")
	resourceURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	tokenHash := hashComputeToken("byoc-token")
	machine := &model.AgentTokenState{
		WorkspaceID:      "workspace-1",
		PoolName:         "aws-cpu",
		MachineID:        "machine-1",
		TokenHash:        hashComputeToken("machine-token"),
		Schedulable:      true,
		LastJoinAt:       now.Add(-byocDeletedPoolRetention - time.Minute),
		LastHeartbeatAt:  now.Add(-byocDeletedPoolRetention - time.Second),
		LastDisconnectAt: now.Add(-byocDeletedPoolRetention),
	}
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					Config:           &pb.PoolConfig{Name: "aws-cpu", Regions: []string{"us-east-1"}},
					BYOC: &model.BYOCProviderState{
						Provider:     "aws",
						AccountID:    "123456789012",
						Region:       "us-east-1",
						ResourceName: "beam-aws-cpu-test",
						ResourceURL:  resourceURL,
						DestroyURL:   resourceURL,
						Labels: map[string]string{
							byocBootstrapJoinTokenHashLabel:   tokenHash,
							byocBootstrapJoinTokenHashesLabel: tokenHash,
							"instance_type":                   "i4i.xlarge",
							"desired_nodes":                   "1",
							"max_nodes":                       "1",
							"target_sandboxes":                "20",
							"sandboxes_per_node":              "20",
						},
					},
				},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "aws-cpu"): {machine},
		},
		joinTokens: map[string]*model.JoinTokenState{
			tokenHash: {
				TokenHash:   tokenHash,
				WorkspaceID: "workspace-1",
				PoolName:    "aws-cpu",
			},
		},
	}
	containerRepo := &fakeContainerRepo{}
	service := &Service{computeRepo: repo, containerRepo: containerRepo}

	res, err := service.ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil {
		t.Fatalf("ListPrivatePools() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("ListPrivatePools() not ok: %s", res.ErrMsg)
	}
	if got := len(res.Pools); got != 0 {
		t.Fatalf("pool count = %d, want 0 after BYOC cleanup", got)
	}
	if got := len(repo.pools["workspace-1"]); got != 0 {
		t.Fatalf("stored pool count = %d, want 0", got)
	}
	if got := len(repo.machines[fakeComputeKey("workspace-1", "aws-cpu")]); got != 0 {
		t.Fatalf("stored machine count = %d, want 0", got)
	}
	if token := repo.joinTokens[tokenHash]; token == nil || !token.Revoked {
		t.Fatalf("BYOC bootstrap token revoked = %v, want true", token != nil && token.Revoked)
	}
	if got, want := containerRepo.deletedRoutes, []string{"workspace-1/aws-cpu/machine-1"}; !sameStrings(got, want) {
		t.Fatalf("deleted routes = %v, want %v", got, want)
	}
}

func TestListPrivatePoolsCleansDeletedBYOCPoolWithoutMachinesWhenProviderResourceGone(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	resourceURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	tokenHash := hashComputeToken("byoc-token")
	roleARN := "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test"
	externalID := "beam-byoc-test-external-id"
	asgName := "beam-asg-aws-cpu-test"
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					Config:           &pb.PoolConfig{Name: "aws-cpu", Regions: []string{"us-east-1"}},
					BYOC: &model.BYOCProviderState{
						Provider:     "aws",
						AccountID:    "123456789012",
						Region:       "us-east-1",
						ResourceName: "beam-aws-cpu-test",
						ResourceURL:  resourceURL,
						DestroyURL:   resourceURL,
						Labels: map[string]string{
							byocBootstrapJoinTokenHashLabel:   tokenHash,
							byocBootstrapJoinTokenHashesLabel: tokenHash,
							"instance_type":                   "i4i.xlarge",
							"desired_nodes":                   "2",
							"max_nodes":                       "2",
							"target_sandboxes":                "40",
							"sandboxes_per_node":              "20",
							awsBYOCAutoScalingGroupNameLabel:  asgName,
							awsBYOCControlRoleArnLabel:        roleARN,
							awsBYOCControlRoleExternalIDLabel: externalID,
						},
					},
				},
			},
		},
		joinTokens: map[string]*model.JoinTokenState{
			tokenHash: {
				TokenHash:   tokenHash,
				WorkspaceID: "workspace-1",
				PoolName:    "aws-cpu",
			},
		},
	}
	var stackCheck awsBYOCResourceDeletedInput
	oldResourceDeleted := awsBYOCResourceDeleted
	awsBYOCResourceDeleted = func(ctx context.Context, input awsBYOCResourceDeletedInput) (bool, error) {
		stackCheck = input
		return true, nil
	}
	defer func() {
		awsBYOCResourceDeleted = oldResourceDeleted
	}()

	res, err := (&Service{computeRepo: repo}).ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil {
		t.Fatalf("ListPrivatePools() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("ListPrivatePools() not ok: %s", res.ErrMsg)
	}
	if got := len(res.Pools); got != 0 {
		t.Fatalf("pool count = %d, want 0 after provider-deleted BYOC cleanup", got)
	}
	if got := len(repo.pools["workspace-1"]); got != 0 {
		t.Fatalf("stored pool count = %d, want 0", got)
	}
	if token := repo.joinTokens[tokenHash]; token == nil || !token.Revoked {
		t.Fatalf("BYOC bootstrap token revoked = %v, want true", token != nil && token.Revoked)
	}
	if stackCheck.Region != "us-east-1" || stackCheck.RoleARN != roleARN || stackCheck.ExternalID != externalID || stackCheck.StackName != "beam-aws-cpu-test" {
		t.Fatalf("stack check input = %+v, want AWS control metadata", stackCheck)
	}
}

func TestListPrivatePoolsKeepsFreshBYOCPoolWhenProviderControlUnavailable(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	resourceURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					Config:           &pb.PoolConfig{Name: "aws-cpu", Regions: []string{"us-east-1"}},
					CreatedAt:        time.Now().UTC(),
					BYOC: &model.BYOCProviderState{
						Provider:     "aws",
						AccountID:    "123456789012",
						Region:       "us-east-1",
						ResourceName: "beam-aws-cpu-test",
						ResourceURL:  resourceURL,
						DestroyURL:   resourceURL,
						Labels: map[string]string{
							"instance_type":                   "i4i.xlarge",
							"desired_nodes":                   "2",
							"max_nodes":                       "2",
							"target_sandboxes":                "40",
							"sandboxes_per_node":              "20",
							awsBYOCAutoScalingGroupNameLabel:  "beam-asg-aws-cpu-test",
							awsBYOCControlRoleArnLabel:        "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test",
							awsBYOCControlRoleExternalIDLabel: "beam-byoc-test-external-id",
						},
					},
				},
			},
		},
	}
	oldResourceDeleted := awsBYOCResourceDeleted
	awsBYOCResourceDeleted = func(context.Context, awsBYOCResourceDeletedInput) (bool, error) {
		return false, errBYOCProviderControlUnavailable
	}
	defer func() {
		awsBYOCResourceDeleted = oldResourceDeleted
	}()

	res, err := (&Service{computeRepo: repo}).ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil {
		t.Fatalf("ListPrivatePools() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("ListPrivatePools() not ok: %s", res.ErrMsg)
	}
	if got := len(res.Pools); got != 1 {
		t.Fatalf("pool count = %d, want fresh creating BYOC pool to remain visible", got)
	}
	if got := len(repo.pools["workspace-1"]); got != 1 {
		t.Fatalf("stored pool count = %d, want 1", got)
	}
	if got, want := res.Pools[0].GetByoc().GetPhase(), "waiting_for_provider"; got != want {
		t.Fatalf("BYOC phase = %q, want %q", got, want)
	}
}

func TestListPrivatePoolsKeepsFreshBYOCPoolDuringProviderNotFoundRace(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	resourceURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					Config:           &pb.PoolConfig{Name: "aws-cpu", Regions: []string{"us-east-1"}},
					CreatedAt:        time.Now().UTC(),
					BYOC: &model.BYOCProviderState{
						Provider:     "aws",
						AccountID:    "123456789012",
						Region:       "us-east-1",
						ResourceName: "beam-aws-cpu-test",
						ResourceURL:  resourceURL,
						DestroyURL:   resourceURL,
						Labels: map[string]string{
							"instance_type":                   "i4i.xlarge",
							"desired_nodes":                   "2",
							"max_nodes":                       "2",
							"target_sandboxes":                "40",
							"sandboxes_per_node":              "20",
							awsBYOCAutoScalingGroupNameLabel:  "beam-asg-aws-cpu-test",
							awsBYOCControlRoleArnLabel:        "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test",
							awsBYOCControlRoleExternalIDLabel: "beam-byoc-test-external-id",
						},
					},
				},
			},
		},
	}
	oldResourceDeleted := awsBYOCResourceDeleted
	awsBYOCResourceDeleted = func(context.Context, awsBYOCResourceDeletedInput) (bool, error) {
		return false, errBYOCProviderResourceNotFound
	}
	defer func() {
		awsBYOCResourceDeleted = oldResourceDeleted
	}()

	res, err := (&Service{computeRepo: repo}).ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil {
		t.Fatalf("ListPrivatePools() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("ListPrivatePools() not ok: %s", res.ErrMsg)
	}
	if got := len(res.Pools); got != 1 {
		t.Fatalf("pool count = %d, want fresh creating BYOC pool to remain visible", got)
	}
	if got := len(repo.pools["workspace-1"]); got != 1 {
		t.Fatalf("stored pool count = %d, want 1", got)
	}
}

func TestListPrivatePoolsCleansOldNeverJoinedBYOCPoolWhenProviderControlUnavailable(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	tokenHash := hashComputeToken("byoc-token")
	resourceURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					Config:           &pb.PoolConfig{Name: "aws-cpu", Regions: []string{"us-east-1"}},
					CreatedAt:        time.Now().UTC().Add(-byocProviderControlUnavailableCleanupGrace - time.Minute),
					BYOC: &model.BYOCProviderState{
						Provider:     "aws",
						AccountID:    "123456789012",
						Region:       "us-east-1",
						ResourceName: "beam-aws-cpu-test",
						ResourceURL:  resourceURL,
						DestroyURL:   resourceURL,
						Labels: map[string]string{
							byocBootstrapJoinTokenHashLabel:   tokenHash,
							byocBootstrapJoinTokenHashesLabel: tokenHash,
							"instance_type":                   "i4i.xlarge",
							"desired_nodes":                   "2",
							"max_nodes":                       "2",
							"target_sandboxes":                "40",
							"sandboxes_per_node":              "20",
							awsBYOCAutoScalingGroupNameLabel:  "beam-asg-aws-cpu-test",
							awsBYOCControlRoleArnLabel:        "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test",
							awsBYOCControlRoleExternalIDLabel: "beam-byoc-test-external-id",
						},
					},
				},
			},
		},
		joinTokens: map[string]*model.JoinTokenState{
			tokenHash: {
				TokenHash:   tokenHash,
				WorkspaceID: "workspace-1",
				PoolName:    "aws-cpu",
			},
		},
	}
	oldResourceDeleted := awsBYOCResourceDeleted
	awsBYOCResourceDeleted = func(context.Context, awsBYOCResourceDeletedInput) (bool, error) {
		return false, errBYOCProviderControlUnavailable
	}
	defer func() {
		awsBYOCResourceDeleted = oldResourceDeleted
	}()

	res, err := (&Service{computeRepo: repo}).ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil {
		t.Fatalf("ListPrivatePools() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("ListPrivatePools() not ok: %s", res.ErrMsg)
	}
	if got := len(res.Pools); got != 0 {
		t.Fatalf("pool count = %d, want stale never-joined BYOC pool cleaned up", got)
	}
	if got := len(repo.pools["workspace-1"]); got != 0 {
		t.Fatalf("stored pool count = %d, want 0", got)
	}
	if token := repo.joinTokens[tokenHash]; token == nil || !token.Revoked {
		t.Fatalf("BYOC bootstrap token revoked = %v, want true", token != nil && token.Revoked)
	}
}

func TestListPrivatePoolsKeepsRecentlyDisconnectedBYOCPool(t *testing.T) {
	now := time.Now().UTC()
	ctx := testAuthContext("workspace-1", "owner-token")
	resourceURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					Config:           &pb.PoolConfig{Name: "aws-cpu", Regions: []string{"us-east-1"}},
					BYOC: &model.BYOCProviderState{
						Provider:     "aws",
						AccountID:    "123456789012",
						Region:       "us-east-1",
						ResourceName: "beam-aws-cpu-test",
						ResourceURL:  resourceURL,
						DestroyURL:   resourceURL,
						Labels: map[string]string{
							"instance_type":      "i4i.xlarge",
							"desired_nodes":      "1",
							"max_nodes":          "1",
							"target_sandboxes":   "20",
							"sandboxes_per_node": "20",
						},
					},
				},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "aws-cpu"): {
				{
					WorkspaceID:      "workspace-1",
					PoolName:         "aws-cpu",
					MachineID:        "machine-1",
					Schedulable:      true,
					LastJoinAt:       now.Add(-byocDeletedPoolRetention),
					LastHeartbeatAt:  now.Add(-byocDeletedPoolRetention + time.Minute),
					LastDisconnectAt: now.Add(-byocDeletedPoolRetention + time.Minute),
				},
			},
		},
	}
	service := &Service{computeRepo: repo}

	res, err := service.ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil {
		t.Fatalf("ListPrivatePools() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("ListPrivatePools() not ok: %s", res.ErrMsg)
	}
	if got := len(res.Pools); got != 1 {
		t.Fatalf("pool count = %d, want 1", got)
	}
	if got, want := res.Pools[0].GetByoc().GetPhase(), "nodes_booting"; got != want {
		t.Fatalf("BYOC phase = %q, want %q", got, want)
	}
}

func TestListPrivatePoolsCleansBYOCWhenProviderUnavailableAndMachinesDisconnected(t *testing.T) {
	now := time.Now().UTC()
	ctx := testAuthContext("workspace-1", "owner-token")
	resourceURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					Config:           &pb.PoolConfig{Name: "aws-cpu", Regions: []string{"us-east-1"}},
					BYOC: &model.BYOCProviderState{
						Provider:     "aws",
						AccountID:    "123456789012",
						Region:       "us-east-1",
						ResourceName: "beam-aws-cpu-test",
						ResourceURL:  resourceURL,
						DestroyURL:   resourceURL,
						Labels: map[string]string{
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
				},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "aws-cpu"): {
				{
					WorkspaceID:      "workspace-1",
					PoolName:         "aws-cpu",
					MachineID:        "machine-1",
					Schedulable:      true,
					LastJoinAt:       now.Add(-time.Minute),
					LastHeartbeatAt:  now.Add(-30 * time.Second),
					LastDisconnectAt: now,
				},
			},
		},
	}
	oldResourceDeleted := awsBYOCResourceDeleted
	awsBYOCResourceDeleted = func(context.Context, awsBYOCResourceDeletedInput) (bool, error) {
		return false, errBYOCProviderControlUnavailable
	}
	defer func() {
		awsBYOCResourceDeleted = oldResourceDeleted
	}()

	res, err := (&Service{computeRepo: repo}).ListPrivatePools(ctx, &pb.ListPrivatePoolsRequest{})
	if err != nil {
		t.Fatalf("ListPrivatePools() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("ListPrivatePools() not ok: %s", res.ErrMsg)
	}
	if got := len(res.Pools); got != 0 {
		t.Fatalf("pool count = %d, want 0 after provider-unavailable BYOC cleanup", got)
	}
	if got := len(repo.pools["workspace-1"]); got != 0 {
		t.Fatalf("stored pool count = %d, want 0", got)
	}
	if got := len(repo.machines[fakeComputeKey("workspace-1", "aws-cpu")]); got != 0 {
		t.Fatalf("stored machine count = %d, want 0", got)
	}
}

func TestReleasePrivateMachineTerminatesAWSBYOCNode(t *testing.T) {
	now := time.Now().UTC()
	roleARN := "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test"
	externalID := "beam-byoc-test-external-id"
	asgName := "beam-asg-aws-cpu-test"
	state := &model.PoolState{
		WorkspaceID:      "workspace-1",
		Name:             "aws-cpu",
		Mode:             string(types.PoolModePrivate),
		CreatedByTokenID: "owner-token",
		Source:           model.SourceAWS,
		BYOC: &model.BYOCProviderState{
			Provider: string(model.SourceAWS),
			Region:   "us-east-1",
			Labels: map[string]string{
				awsBYOCAutoScalingGroupNameLabel:  asgName,
				awsBYOCControlRoleArnLabel:        roleARN,
				awsBYOCControlRoleExternalIDLabel: externalID,
			},
		},
	}
	machine := &model.AgentTokenState{
		WorkspaceID:        "workspace-1",
		PoolName:           "aws-cpu",
		MachineID:          "machine-1",
		MachineFingerprint: "i-0123456789abcdef0",
		Hostname:           "i-0123456789abcdef0",
		Schedulable:        true,
		LastHeartbeatAt:    now,
	}
	repo := &fakeComputeRepo{
		pools:    map[string][]*model.PoolState{"workspace-1": {state}},
		machines: map[string][]*model.AgentTokenState{fakeComputeKey("workspace-1", "aws-cpu"): {machine}},
	}
	var releaseInput awsBYOCReleaseMachineInput
	oldReleaseMachine := awsBYOCReleaseMachine
	awsBYOCReleaseMachine = func(ctx context.Context, input awsBYOCReleaseMachineInput) error {
		releaseInput = input
		return nil
	}
	defer func() {
		awsBYOCReleaseMachine = oldReleaseMachine
	}()

	if err := (&Service{computeRepo: repo}).releasePrivateMachine(context.Background(), machine); err != nil {
		t.Fatalf("releasePrivateMachine() error = %v", err)
	}
	if releaseInput.Region != "us-east-1" || releaseInput.RoleARN != roleARN || releaseInput.ExternalID != externalID || releaseInput.AutoScalingGroupName != asgName {
		t.Fatalf("release input = %+v, want AWS control metadata", releaseInput)
	}
	if releaseInput.Machine != machine {
		t.Fatal("release did not pass the machine to the AWS provider")
	}
	if machine.Schedulable {
		t.Fatal("releasePrivateMachine() did not mark the machine unschedulable")
	}
	if got := len(repo.machines[fakeComputeKey("workspace-1", "aws-cpu")]); got != 0 {
		t.Fatalf("machine count after release = %d, want 0", got)
	}
}

func TestReleasePrivateMachineKeepsLocalStateWhenAWSBYOCTerminationFails(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "aws-cpu",
		Source:      model.SourceAWS,
		BYOC: &model.BYOCProviderState{
			Provider:     string(model.SourceAWS),
			Region:       "us-east-1",
			ResourceName: "beam-aws-cpu-test",
			Labels: map[string]string{
				awsBYOCAutoScalingGroupNameLabel:  "beam-asg-aws-cpu-test",
				awsBYOCControlRoleArnLabel:        "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test",
				awsBYOCControlRoleExternalIDLabel: "beam-byoc-test-external-id",
			},
		},
	}
	machine := &model.AgentTokenState{
		WorkspaceID:     "workspace-1",
		PoolName:        "aws-cpu",
		MachineID:       "machine-1",
		Schedulable:     true,
		LastHeartbeatAt: now,
	}
	repo := &fakeComputeRepo{
		pools:    map[string][]*model.PoolState{"workspace-1": {state}},
		machines: map[string][]*model.AgentTokenState{fakeComputeKey("workspace-1", "aws-cpu"): {machine}},
	}
	oldReleaseMachine := awsBYOCReleaseMachine
	awsBYOCReleaseMachine = func(context.Context, awsBYOCReleaseMachineInput) error {
		return errors.New("aws failed")
	}
	defer func() {
		awsBYOCReleaseMachine = oldReleaseMachine
	}()

	err := (&Service{computeRepo: repo}).releasePrivateMachine(context.Background(), machine)
	if err == nil || !strings.Contains(err.Error(), "aws failed") {
		t.Fatalf("releasePrivateMachine() error = %v, want AWS failure", err)
	}
	if !machine.Schedulable {
		t.Fatal("failed release should not mark the machine unschedulable")
	}
	if got := len(repo.machines[fakeComputeKey("workspace-1", "aws-cpu")]); got != 1 {
		t.Fatalf("machine count after failed release = %d, want 1", got)
	}
}

func TestReleasePrivateMachineCleansDisconnectedAWSBYOCWhenProviderControlUnavailable(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "aws-cpu",
		Source:      model.SourceAWS,
		BYOC: &model.BYOCProviderState{
			Provider:     string(model.SourceAWS),
			Region:       "us-east-1",
			ResourceName: "beam-aws-cpu-test",
			Labels: map[string]string{
				awsBYOCAutoScalingGroupNameLabel:  "beam-asg-aws-cpu-test",
				awsBYOCControlRoleArnLabel:        "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test",
				awsBYOCControlRoleExternalIDLabel: "beam-byoc-test-external-id",
			},
		},
	}
	machine := &model.AgentTokenState{
		WorkspaceID:      "workspace-1",
		PoolName:         "aws-cpu",
		MachineID:        "machine-1",
		Schedulable:      true,
		LastJoinAt:       now.Add(-time.Minute),
		LastHeartbeatAt:  now.Add(-30 * time.Second),
		LastDisconnectAt: now,
	}
	repo := &fakeComputeRepo{
		pools:    map[string][]*model.PoolState{"workspace-1": {state}},
		machines: map[string][]*model.AgentTokenState{fakeComputeKey("workspace-1", "aws-cpu"): {machine}},
	}
	oldReleaseMachine := awsBYOCReleaseMachine
	awsBYOCReleaseMachine = func(context.Context, awsBYOCReleaseMachineInput) error {
		return errBYOCProviderControlUnavailable
	}
	oldResourceDeleted := awsBYOCResourceDeleted
	awsBYOCResourceDeleted = func(context.Context, awsBYOCResourceDeletedInput) (bool, error) {
		return false, errBYOCProviderControlUnavailable
	}
	defer func() {
		awsBYOCReleaseMachine = oldReleaseMachine
		awsBYOCResourceDeleted = oldResourceDeleted
	}()

	if err := (&Service{computeRepo: repo}).releasePrivateMachine(context.Background(), machine); err != nil {
		t.Fatalf("releasePrivateMachine() error = %v", err)
	}
	if got := len(repo.pools["workspace-1"]); got != 0 {
		t.Fatalf("stored pool count = %d, want 0", got)
	}
	if got := len(repo.machines[fakeComputeKey("workspace-1", "aws-cpu")]); got != 0 {
		t.Fatalf("stored machine count = %d, want 0", got)
	}
}

func TestDeletePoolReleasesAWSBYOCMachines(t *testing.T) {
	now := time.Now().UTC()
	ctx := testAuthContext("workspace-1", "owner-token")
	machineA := &model.AgentTokenState{
		WorkspaceID:      "workspace-1",
		PoolName:         "aws-cpu",
		MachineID:        "machine-a",
		Schedulable:      true,
		LastJoinAt:       now,
		LastHeartbeatAt:  now,
		LastDisconnectAt: time.Time{},
	}
	machineB := &model.AgentTokenState{
		WorkspaceID:     "workspace-1",
		PoolName:        "aws-cpu",
		MachineID:       "machine-b",
		Schedulable:     true,
		LastJoinAt:      now,
		LastHeartbeatAt: now,
	}
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					BYOC: &model.BYOCProviderState{
						Provider: string(model.SourceAWS),
						Region:   "us-east-1",
						Labels: map[string]string{
							byocBootstrapJoinTokenHashLabel:   hashComputeToken("byoc-token"),
							byocBootstrapJoinTokenHashesLabel: strings.Join([]string{hashComputeToken("byoc-token"), hashComputeToken("old-byoc-token")}, ","),
							awsBYOCAutoScalingGroupNameLabel:  "beam-asg-aws-cpu-test",
							awsBYOCControlRoleArnLabel:        "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test",
							awsBYOCControlRoleExternalIDLabel: "beam-byoc-test-external-id",
						},
					},
				},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "aws-cpu"): {machineA, machineB},
		},
		joinTokens: map[string]*model.JoinTokenState{
			hashComputeToken("byoc-token"): {
				TokenHash:   hashComputeToken("byoc-token"),
				WorkspaceID: "workspace-1",
				PoolName:    "aws-cpu",
			},
			hashComputeToken("old-byoc-token"): {
				TokenHash:   hashComputeToken("old-byoc-token"),
				WorkspaceID: "workspace-1",
				PoolName:    "aws-cpu",
			},
		},
	}
	containerRepo := &fakeContainerRepo{}
	service := &Service{computeRepo: repo, containerRepo: containerRepo}
	oldReleaseMachine := awsBYOCReleaseMachine
	awsBYOCReleaseMachine = func(context.Context, awsBYOCReleaseMachineInput) error { return nil }
	defer func() { awsBYOCReleaseMachine = oldReleaseMachine }()

	res, err := service.DeletePool(ctx, &pb.DeletePoolRequest{Name: "aws-cpu"})
	if err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("DeletePool() not ok: %s", res.ErrMsg)
	}
	if got := len(repo.machines[fakeComputeKey("workspace-1", "aws-cpu")]); got != 0 {
		t.Fatalf("machine count after pool release = %d, want 0", got)
	}
	if got := len(repo.pools["workspace-1"]); got != 0 {
		t.Fatalf("pool count after pool release = %d, want 0", got)
	}
	if machineA.Schedulable || machineB.Schedulable {
		t.Fatal("DeletePool() did not mark BYOC machines unschedulable before deleting them")
	}
	if !repo.joinTokens[hashComputeToken("byoc-token")].Revoked {
		t.Fatal("DeletePool() did not revoke the BYOC bootstrap join token")
	}
	if !repo.joinTokens[hashComputeToken("old-byoc-token")].Revoked {
		t.Fatal("DeletePool() did not revoke the old BYOC bootstrap join token")
	}
	if got, want := containerRepo.deletedRoutes, []string{"workspace-1/aws-cpu/machine-a", "workspace-1/aws-cpu/machine-b"}; !sameStrings(got, want) {
		t.Fatalf("deleted machine routes = %v, want %v", got, want)
	}
}

func TestDeletePoolKeepsMachineThatReconnectsDuringProviderRelease(t *testing.T) {
	now := time.Now().UTC()
	key := fakeComputeKey("workspace-1", "aws-cpu")
	machine := &model.AgentTokenState{
		WorkspaceID:      "workspace-1",
		PoolName:         "aws-cpu",
		MachineID:        "machine-1",
		Schedulable:      true,
		LastHeartbeatAt:  now.Add(-2 * time.Minute),
		LastDisconnectAt: now.Add(-time.Minute),
	}
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {{
				Name:             "aws-cpu",
				Mode:             string(types.PoolModePrivate),
				CreatedByTokenID: "owner-token",
				Source:           model.SourceAWS,
				BYOC: &model.BYOCProviderState{
					Provider: string(model.SourceAWS),
					Region:   "us-east-1",
					Labels: map[string]string{
						awsBYOCAutoScalingGroupNameLabel:  "beam-asg-aws-cpu-test",
						awsBYOCControlRoleArnLabel:        "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test",
						awsBYOCControlRoleExternalIDLabel: "beam-byoc-test-external-id",
					},
				},
			}},
		},
		machines: map[string][]*model.AgentTokenState{key: {machine}},
	}

	oldReleaseMachine := awsBYOCReleaseMachine
	awsBYOCReleaseMachine = func(context.Context, awsBYOCReleaseMachineInput) error {
		reconnected := *machine
		reconnected.LastHeartbeatAt = time.Now().UTC()
		reconnected.LastDisconnectAt = time.Time{}
		repo.machines[key][0] = &reconnected
		return errBYOCProviderControlUnavailable
	}
	defer func() { awsBYOCReleaseMachine = oldReleaseMachine }()

	res, err := (&Service{computeRepo: repo}).DeletePool(
		testAuthContext("workspace-1", "owner-token"),
		&pb.DeletePoolRequest{Name: "aws-cpu"},
	)
	if err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}
	if res.Ok || !strings.Contains(res.ErrMsg, errBYOCProviderControlUnavailable.Error()) {
		t.Fatalf("DeletePool() response = %#v, want provider control error", res)
	}
	if got := len(repo.machines[key]); got != 1 || !model.AgentMachineConnected(repo.machines[key][0], time.Now().UTC()) {
		t.Fatalf("machine state after failed deletion = %#v, want connected machine", repo.machines[key])
	}
	if got := len(repo.pools["workspace-1"]); got != 1 {
		t.Fatalf("pool count after failed deletion = %d, want 1", got)
	}
}

func TestDeletePoolIsIdempotentWhenPoolAlreadyGone(t *testing.T) {
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{}}
	service := &Service{computeRepo: repo}

	res, err := service.DeletePool(testAuthContext("workspace-1", "owner-token"), &pb.DeletePoolRequest{Name: "codex-hetzner-latency"})
	if err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePool() response = %#v", res)
	}
}

func TestDeletePoolAllowsAnotherTokenInWorkspace(t *testing.T) {
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "pool-1",
					CreatedByTokenID: "creator-token",
					Mode:             string(types.PoolModePrivate),
				},
			},
			"workspace-2": {
				{Name: "pool-1", CreatedByTokenID: "other-workspace-token", Mode: string(types.PoolModePrivate)},
			},
		},
	}
	service := &Service{computeRepo: repo}

	res, err := service.DeletePool(
		testAuthContext("workspace-1", "current-token"),
		&pb.DeletePoolRequest{Name: "pool-1"},
	)
	if err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePool() response = %#v", res)
	}
	if got := len(repo.pools["workspace-1"]); got != 0 {
		t.Fatalf("stored pool count = %d, want 0", got)
	}
	if got := len(repo.pools["workspace-2"]); got != 1 {
		t.Fatalf("other workspace pool count = %d, want 1", got)
	}
}

func TestScaleBYOCPoolUpdatesAWSAutoScalingGroupAndState(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	resourceURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	roleARN := "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test"
	externalID := "beam-byoc-test-external-id"
	asgName := "beam-asg-aws-cpu-test"
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					Config:           &pb.PoolConfig{Name: "aws-cpu", Regions: []string{"us-east-1"}},
					BYOC: &model.BYOCProviderState{
						Provider:     "aws",
						AccountID:    "123456789012",
						Region:       "us-east-1",
						ResourceName: "beam-aws-cpu-test",
						ResourceURL:  resourceURL,
						DestroyURL:   resourceURL,
						Labels: map[string]string{
							"instance_type":                   "i4i.xlarge",
							"desired_nodes":                   "1",
							"max_nodes":                       "4",
							"target_sandboxes":                "20",
							"sandboxes_per_node":              "20",
							"hourly_cost_micros":              "182000",
							"total_hourly_cost_micros":        "182000",
							awsBYOCAutoScalingGroupNameLabel:  asgName,
							awsBYOCControlRoleArnLabel:        roleARN,
							awsBYOCControlRoleExternalIDLabel: externalID,
							awsBYOCControlRoleNameLabel:       "beam-control-aws-cpu-test",
						},
					},
				},
			},
		},
	}
	var scaleInput awsBYOCScaleAutoScalingGroupInput
	oldScaler := awsBYOCScaleAutoScalingGroup
	awsBYOCScaleAutoScalingGroup = func(ctx context.Context, input awsBYOCScaleAutoScalingGroupInput) error {
		scaleInput = input
		return nil
	}
	defer func() {
		awsBYOCScaleAutoScalingGroup = oldScaler
	}()

	res, err := (&Service{computeRepo: repo}).ScaleBYOCPool(ctx, &pb.ScaleBYOCPoolRequest{
		PoolName:     "aws-cpu",
		DesiredNodes: 3,
		MaxNodes:     5,
	})
	if err != nil {
		t.Fatalf("ScaleBYOCPool() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("ScaleBYOCPool() not ok: %s", res.ErrMsg)
	}
	if scaleInput.Region != "us-east-1" || scaleInput.RoleARN != roleARN || scaleInput.ExternalID != externalID || scaleInput.AutoScalingGroupName != asgName {
		t.Fatalf("scale input metadata = %+v, want region/role/external/asg", scaleInput)
	}
	if scaleInput.DesiredNodes != 3 || scaleInput.MaxNodes != 5 {
		t.Fatalf("scale input nodes = desired %d max %d, want 3/5", scaleInput.DesiredNodes, scaleInput.MaxNodes)
	}
	stored := repo.pools["workspace-1"][0]
	for key, want := range map[string]string{
		"desired_nodes":            "3",
		"max_nodes":                "5",
		"target_sandboxes":         "60",
		"total_hourly_cost_micros": "546000",
	} {
		if got := stored.BYOC.Labels[key]; got != want {
			t.Fatalf("stored label %s = %q, want %q", key, got, want)
		}
	}
	if !res.GetByoc().GetDirectScaleEnabled() {
		t.Fatal("scaled BYOC pool should report direct scale enabled")
	}
	if got, want := res.GetByoc().GetDesiredNodes(), uint32(3); got != want {
		t.Fatalf("response desired nodes = %d, want %d", got, want)
	}
}

func TestScaleBYOCPoolPrunesDisconnectedMachinesAboveDesired(t *testing.T) {
	now := time.Now().UTC()
	ctx := testAuthContext("workspace-1", "owner-token")
	resourceURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	roleARN := "arn:aws:iam::123456789012:role/beam-control-aws-cpu-test"
	externalID := "beam-byoc-test-external-id"
	asgName := "beam-asg-aws-cpu-test"
	connected := &model.AgentTokenState{
		WorkspaceID:     "workspace-1",
		PoolName:        "aws-cpu",
		MachineID:       "machine-connected",
		Schedulable:     true,
		LastJoinAt:      now.Add(-time.Minute),
		LastHeartbeatAt: now,
	}
	disconnected := &model.AgentTokenState{
		WorkspaceID:      "workspace-1",
		PoolName:         "aws-cpu",
		MachineID:        "machine-disconnected",
		Schedulable:      true,
		LastJoinAt:       now.Add(-time.Minute),
		LastHeartbeatAt:  now.Add(-30 * time.Second),
		LastDisconnectAt: now,
	}
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					Config:           &pb.PoolConfig{Name: "aws-cpu", Regions: []string{"us-east-1"}},
					BYOC: &model.BYOCProviderState{
						Provider:     "aws",
						AccountID:    "123456789012",
						Region:       "us-east-1",
						ResourceName: "beam-aws-cpu-test",
						ResourceURL:  resourceURL,
						DestroyURL:   resourceURL,
						Labels: map[string]string{
							"instance_type":                   "i4i.xlarge",
							"desired_nodes":                   "2",
							"max_nodes":                       "2",
							"target_sandboxes":                "40",
							"sandboxes_per_node":              "20",
							awsBYOCAutoScalingGroupNameLabel:  asgName,
							awsBYOCControlRoleArnLabel:        roleARN,
							awsBYOCControlRoleExternalIDLabel: externalID,
						},
					},
				},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "aws-cpu"): {connected, disconnected},
		},
	}
	oldScaler := awsBYOCScaleAutoScalingGroup
	awsBYOCScaleAutoScalingGroup = func(context.Context, awsBYOCScaleAutoScalingGroupInput) error {
		return nil
	}
	defer func() {
		awsBYOCScaleAutoScalingGroup = oldScaler
	}()

	res, err := (&Service{computeRepo: repo}).ScaleBYOCPool(ctx, &pb.ScaleBYOCPoolRequest{
		PoolName:     "aws-cpu",
		DesiredNodes: 1,
		MaxNodes:     1,
	})
	if err != nil {
		t.Fatalf("ScaleBYOCPool() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("ScaleBYOCPool() not ok: %s", res.ErrMsg)
	}
	machines := repo.machines[fakeComputeKey("workspace-1", "aws-cpu")]
	if got := len(machines); got != 1 {
		t.Fatalf("stored machine count = %d, want 1", got)
	}
	if got := machines[0].MachineID; got != "machine-connected" {
		t.Fatalf("remaining machine = %q, want connected machine", got)
	}
	if got, want := res.GetByoc().GetMachineCount(), uint32(1); got != want {
		t.Fatalf("response machine count = %d, want %d", got, want)
	}
}

func TestScaleBYOCPoolRejectsStackWithoutControlRole(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	resourceURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					Config:           &pb.PoolConfig{Name: "aws-cpu", Regions: []string{"us-east-1"}},
					BYOC: &model.BYOCProviderState{
						Provider:     "aws",
						AccountID:    "123456789012",
						Region:       "us-east-1",
						ResourceName: "beam-aws-cpu-test",
						ResourceURL:  resourceURL,
						DestroyURL:   resourceURL,
						Labels: map[string]string{
							"instance_type":      "i4i.xlarge",
							"desired_nodes":      "1",
							"max_nodes":          "1",
							"target_sandboxes":   "20",
							"sandboxes_per_node": "20",
						},
					},
				},
			},
		},
	}

	res, err := (&Service{computeRepo: repo}).ScaleBYOCPool(ctx, &pb.ScaleBYOCPoolRequest{
		PoolName:     "aws-cpu",
		DesiredNodes: 2,
		MaxNodes:     2,
	})
	if err != nil {
		t.Fatalf("ScaleBYOCPool() error = %v", err)
	}
	if res.Ok {
		t.Fatal("ScaleBYOCPool() unexpectedly succeeded without control role metadata")
	}
	if !strings.Contains(res.ErrMsg, "direct BYOC scaling is unavailable") {
		t.Fatalf("error = %q, want direct scale unavailable", res.ErrMsg)
	}
	if got := repo.pools["workspace-1"][0].BYOC.Labels["desired_nodes"]; got != "1" {
		t.Fatalf("desired_nodes changed to %q, want unchanged", got)
	}
}

func TestAWSBYOCTemplateDoesNotEmbedSecrets(t *testing.T) {
	template := AWSBYOCTemplate()
	if !strings.Contains(template, "NoEcho: true") {
		t.Fatal("template should mark BeamJoinToken NoEcho")
	}
	if strings.Contains(template, "sk_") || strings.Contains(template, "test-secret-token") {
		t.Fatalf("template appears to contain an embedded secret: %s", template)
	}
	if !strings.Contains(template, "AWS::AutoScaling::AutoScalingGroup") {
		t.Fatal("template should create an auto scaling group")
	}
	if !strings.Contains(template, "FromPort: 41641") {
		t.Fatal("template should allow Tailscale direct WireGuard UDP")
	}
	for _, fragment := range []string{
		"BeamWorkerImage:",
		"--worker-image '${BeamWorkerImage}'",
		"Default: i4i.xlarge",
		"AllowedValues:",
		"- i4i.large",
		"- i4i.xlarge",
		"- i4i.4xlarge",
		"- g6.xlarge",
		"- g5.xlarge",
		"NodeGPUCount:",
		"LatestGPUAmiId:",
		"/aws/service/deeplearning/ami/x86_64/base-oss-nvidia-driver-gpu-ubuntu-22.04/latest/ami-id",
		"UseGPUAMI:",
		"ImageId: !If [UseGPUAMI, !Ref LatestGPUAmiId, !Ref LatestCPUAmiId]",
		"nvme-Amazon_EC2_NVMe_Instance_Storage",
		"BEAM_AGENT_INSTALL_DOCKER=1",
		"nvidia-ctk runtime configure --runtime=docker",
		"GPU_ARGS=\"--max-gpus ${NodeGPUCount}\"",
		"$GPU_ARGS",
		"--state-dir \"$BEAM_STATE_DIR\"",
		"/etc/docker/daemon.json",
		"NetworkSlots:",
		"Default: 128",
		"--network-slots '${NetworkSlots}'",
		"RootVolumeSizeGB:",
		"VolumeSize: !Ref RootVolumeSizeGB",
		"VolumeType: gp3",
		"ContainerStartConcurrency:",
		"--container-start-concurrency '${ContainerStartConcurrency}'",
		"NodeSecurityGroupSelfIngress:",
		"IpProtocol: -1",
		"SourceSecurityGroupId: !Ref NodeSecurityGroup",
		"Description: Beam BYOC node-to-node private traffic",
		"NodeSecurityGroupVpcIngress:",
		"CidrIp: !Ref VpcCidr",
		"Description: Beam BYOC private VPC traffic",
		"TargetAWSAccountID:",
		"TargetAccountMustMatch:",
		"!Equals [!Ref TargetAWSAccountID, !Ref \"AWS::AccountId\"]",
		"BeamControlRole:",
		"BeamControlRolePrincipalArn:",
		"sts:ExternalId: !Ref BeamControlExternalId",
		"autoscaling:UpdateAutoScalingGroup",
		"autoscaling:TerminateInstanceInAutoScalingGroup",
		"cloudformation:DescribeStacks",
		"ec2:DescribeInstances",
		"autoScalingGroupName/${BeamAutoScalingGroupName}",
		"AutoScalingGroupName: !Ref BeamAutoScalingGroupName",
		"BEAM_AGENT_MACHINE_FINGERPRINT=\"$INSTANCE_ID\"",
		"BEAM_AGENT_HOSTNAME=\"$INSTANCE_ID\"",
	} {
		if !strings.Contains(template, fragment) {
			t.Fatalf("template missing %q", fragment)
		}
	}
}

func TestAWSBYOCStackDeletionErrorClassification(t *testing.T) {
	stackMissing := &smithy.GenericAPIError{
		Code:    "ValidationError",
		Message: "Stack with id beam-aws-cpu-test does not exist",
	}
	if !awsBYOCCloudFormationStackNotFound(stackMissing) {
		t.Fatal("CloudFormation missing-stack validation error should be recognized as not found")
	}

	assumeRoleDenied := &smithy.OperationError{
		ServiceID:     "STS",
		OperationName: "AssumeRole",
		Err: &smithy.GenericAPIError{
			Code:    "AccessDenied",
			Message: "User is not authorized to perform: sts:AssumeRole",
		},
	}
	if !awsBYOCControlRoleUnavailable(fmt.Errorf("get credentials: %w", assumeRoleDenied)) {
		t.Fatal("STS assume-role denial should be treated as the BYOC control role being unavailable")
	}

	cloudFormationDenied := &smithy.OperationError{
		ServiceID:     "CloudFormation",
		OperationName: "DescribeStacks",
		Err: &smithy.GenericAPIError{
			Code:    "AccessDenied",
			Message: "User is not authorized to perform: cloudformation:DescribeStacks",
		},
	}
	if awsBYOCControlRoleUnavailable(cloudFormationDenied) {
		t.Fatal("CloudFormation authorization errors should not be treated as stack deletion")
	}
	if !awsBYOCStackStatusCreating(cftypes.StackStatusCreateInProgress) {
		t.Fatal("CREATE_IN_PROGRESS should be treated as creating")
	}
	if awsBYOCStackStatusDeleted(cftypes.StackStatusCreateInProgress) {
		t.Fatal("CREATE_IN_PROGRESS should not be treated as deleted")
	}
	if !awsBYOCStackStatusDeleted(cftypes.StackStatusDeleteInProgress) {
		t.Fatal("DELETE_IN_PROGRESS should be treated as deleted")
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

func TestGPURequirementSatisfiedByConcreteMemoryVariant(t *testing.T) {
	tests := []struct {
		name      string
		required  string
		available string
		want      bool
	}{
		{name: "generic A100 accepts 40 GB", required: "A100", available: "A100-40", want: true},
		{name: "generic A100 accepts 80 GB", required: "A100", available: "NVIDIA A100 80GB PCI", want: true},
		{name: "generic V100 accepts 32 GB", required: "V100", available: "V100-32", want: true},
		{name: "80 GB requirement rejects 40 GB", required: "A100-80", available: "A100-40", want: false},
		{name: "different family rejected", required: "A4000", available: "H100", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := gpuRequirementSatisfiedBy(tt.required, tt.available); got != tt.want {
				t.Fatalf("gpuRequirementSatisfiedBy(%q, %q) = %v, want %v", tt.required, tt.available, got, tt.want)
			}
		})
	}
}

func TestEnforcePoolGPUTypeAcceptsConcreteVariantForGenericPool(t *testing.T) {
	service := &Service{}
	pool := &model.PoolState{
		Name:   "gpu-pool",
		Config: &pb.PoolConfig{Name: "gpu-pool", Gpu: []string{"A100"}},
	}
	agent := &model.AgentTokenState{
		MachineID: "machine-1",
		GPUs:      []string{"A100-40"},
		GPUCount:  1,
	}

	if err := service.enforcePoolGPUType(context.Background(), pool, agent); err != nil {
		t.Fatalf("enforcePoolGPUType() error = %v", err)
	}

	pool.Config.Gpu = []string{"A100-80"}
	if err := service.enforcePoolGPUType(context.Background(), pool, agent); err == nil {
		t.Fatal("enforcePoolGPUType() accepted A100-40 for an A100-80 requirement")
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
				TokenHash:     hashComputeToken("join-token"),
				WorkspaceID:   "workspace-1",
				PoolName:      "gpu-pool",
				PoolCreatedAt: now,
				ExpiresAt:     now.Add(time.Hour),
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

func TestJoinAgentAcceptsPersistentJoinToken(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					WorkspaceID:      "workspace-1",
					Name:             "aws-cpu",
					Config:           &pb.PoolConfig{Name: "aws-cpu"},
					CreatedByTokenID: "owner-token",
					CreatedAt:        now,
				},
			},
		},
		joinTokens: map[string]*model.JoinTokenState{
			hashComputeToken("byoc-token"): {
				TokenHash:     hashComputeToken("byoc-token"),
				WorkspaceID:   "workspace-1",
				PoolName:      "aws-cpu",
				PoolCreatedAt: now,
			},
		},
	}
	service := &Service{computeRepo: repo}

	res, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken:          "byoc-token",
		MachineFingerprint: "fingerprint-1",
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
					CreatedAt:        now,
				},
			},
		},
		joinTokens: map[string]*model.JoinTokenState{
			hashComputeToken("join-token"): {
				TokenHash:     hashComputeToken("join-token"),
				WorkspaceID:   "workspace-1",
				PoolName:      "gpu-pool",
				PoolCreatedAt: now,
				ExpiresAt:     now.Add(time.Hour),
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
					CreatedAt:        now,
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
				TokenHash:     hashComputeToken("join-token"),
				WorkspaceID:   "workspace-1",
				PoolName:      "cpu-pool",
				PoolCreatedAt: now,
				ExpiresAt:     now.Add(time.Hour),
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
		t.Fatal("JoinAgent() unexpectedly accepted a machine with GPU attributes after CPU-only initialization")
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
					CreatedAt:        now,
				},
			},
		},
		joinTokens: map[string]*model.JoinTokenState{
			hashComputeToken("join-token"): {
				TokenHash:     hashComputeToken("join-token"),
				WorkspaceID:   "workspace-1",
				PoolName:      "gpu-pool",
				PoolCreatedAt: now,
				ExpiresAt:     now.Add(time.Hour),
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

func TestGetAgentPoolVirtualizationReportsGPUVirtualized(t *testing.T) {
	workspaceID := "workspace-1"
	poolName := "pool-1"
	agentToken := "agent-token"
	service := &Service{
		appConfig: types.AppConfig{
			Tailscale: types.TailscaleConfig{
				Enabled:      true,
				AuthKey:      "tskey-auth-gateway",
				AgentAuthKey: "tskey-auth-worker",
			},
		},
		computeRepo: &fakeComputeRepo{
			pools: map[string][]*model.PoolState{
				workspaceID: {
					{
						Name: poolName,
						WorkerConfig: &types.WorkerPoolConfig{
							GPUVirtualized: true,
						},
					},
				},
			},
			machines: map[string][]*model.AgentTokenState{
				fakeComputeKey(workspaceID, poolName): {
					{
						TokenHash:   hashComputeToken(agentToken),
						WorkspaceID: workspaceID,
						PoolName:    poolName,
						MachineID:   "machine-1",
					},
				},
			},
		},
	}

	resp, err := service.GetAgentPoolVirtualization(context.Background(), &pb.GetAgentPoolVirtualizationRequest{AgentToken: agentToken})
	if err != nil {
		t.Fatalf("GetAgentPoolVirtualization() error = %v", err)
	}
	if !resp.GetOk() || !resp.GetGpuVirtualized() {
		t.Fatalf("GetAgentPoolVirtualization() = %+v, want gpu virtualized", resp)
	}
}

func TestAgentPoolGPUVirtualizedWithoutWorkerConfig(t *testing.T) {
	service := &Service{computeRepo: &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {{Name: "pool-1"}},
		},
	}}

	virtualized, err := service.agentPoolGPUVirtualized(context.Background(), &model.AgentTokenState{
		WorkspaceID: "workspace-1",
		PoolName:    "pool-1",
	})
	if err != nil || virtualized {
		t.Fatalf("agentPoolGPUVirtualized() = %v, %v, want a workspace pool to be unvirtualized", virtualized, err)
	}

	_, err = service.agentPoolGPUVirtualized(context.Background(), &model.AgentTokenState{
		WorkspaceID: "workspace-1",
		PoolName:    "missing",
	})
	if err == nil || err.Error() != "agent pool is unavailable" {
		t.Fatalf("agentPoolGPUVirtualized() error = %v", err)
	}
}

func TestRequestAgentTransportCredentialVendsNonEphemeralNode(t *testing.T) {
	workspaceID := "workspace-1"
	poolName := "pool-1"
	agentToken := "agent-token"
	service := &Service{
		appConfig: types.AppConfig{
			Tailscale: types.TailscaleConfig{
				Enabled:      true,
				AuthKey:      "tskey-auth-gateway",
				AgentAuthKey: "tskey-auth-worker",
			},
		},
		computeRepo: &fakeComputeRepo{
			machines: map[string][]*model.AgentTokenState{
				fakeComputeKey(workspaceID, poolName): {
					{
						TokenHash:   hashComputeToken(agentToken),
						WorkspaceID: workspaceID,
						PoolName:    poolName,
						MachineID:   "machine-1",
					},
				},
			},
		},
	}

	resp, err := service.RequestAgentTransportCredential(context.Background(), &pb.RequestAgentTransportCredentialRequest{
		AgentToken: agentToken,
		Transport:  types.BackendRouteTransportTSNet,
	})
	if err != nil {
		t.Fatalf("RequestAgentTransportCredential() error = %v", err)
	}
	if !resp.GetOk() || resp.GetEphemeral() {
		t.Fatalf("RequestAgentTransportCredential() = %+v, want ok non-ephemeral", resp)
	}
	if resp.GetHostname() != types.AgentTailnetHostnamePrefix+"machine-1" {
		t.Fatalf("hostname = %q, want deterministic per-machine", resp.GetHostname())
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

func TestAgentInstallCommandLeavesCapacityUnboundedByDefault(t *testing.T) {
	command := agentInstallCommand("https://app.stage.beam.cloud", "join-token", false, "worker:test")

	if strings.Contains(command, "--max-cpu") || strings.Contains(command, "--max-memory") {
		t.Fatalf("default install command should use detected host capacity, got %s", command)
	}
}

func TestAgentInstallCommandDoesNotUseSudoOnDarwin(t *testing.T) {
	command := agentInstallCommand("https://app.stage.beam.cloud", "join-token", false, "worker:test")

	if !strings.Contains(command, `uname -s`) || !strings.Contains(command, `Darwin`) {
		t.Fatalf("expected command to branch on Darwin, got %s", command)
	}
	if !strings.Contains(command, `then curl -fsSL 'https://app.stage.beam.cloud/install/agent' | sh -s -- --gateway 'https://app.stage.beam.cloud' --join-token 'join-token' --worker-image 'worker:test'`) {
		t.Fatalf("expected Darwin/root path to run without sudo, got %s", command)
	}
	if !strings.Contains(command, `else curl -fsSL 'https://app.stage.beam.cloud/install/agent' | sudo sh -s -- --gateway 'https://app.stage.beam.cloud' --join-token 'join-token' --worker-image 'worker:test'`) {
		t.Fatalf("expected non-root Linux path to use sudo, got %s", command)
	}
}

func TestAgentInstallCommandDevModeRunsWithoutSudo(t *testing.T) {
	command := agentInstallCommand("http://localhost:1994", "join-token", true, "worker:test")

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

func TestManagedBYOCNodeHourlyCostMicros(t *testing.T) {
	tests := []struct {
		name          string
		cpuMillicores int64
		memoryMB      int64
		config        types.ManagedComputeConfig
		want          int64
	}{
		{name: "i4i xlarge shape", cpuMillicores: 4_000, memoryMB: 32 * 1024, want: 182_000},
		{name: "single gpu xlarge shape", cpuMillicores: 4_000, memoryMB: 16 * 1024, want: 110_000},
		{name: "fractional resources round up", cpuMillicores: 250, memoryMB: 512, want: 4_625},
		{
			name:          "configured rate card",
			cpuMillicores: 4_000,
			memoryMB:      32 * 1024,
			config: types.ManagedComputeConfig{
				BYOC: types.ManagedComputeBYOCConfig{
					Pricing: types.ManagedComputeBYOCPricingConfig{
						CPUHourlyMicrosPerVCPU:  20_000,
						MemoryHourlyMicrosPerGB: 10_000,
					},
				},
			},
			want: 400_000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := managedBYOCNodeHourlyCostMicros(tt.cpuMillicores, tt.memoryMB, tt.config); got != tt.want {
				t.Fatalf("managedBYOCNodeHourlyCostMicros() = %d, want %d", got, tt.want)
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

	plan := model.Plan{
		Purchases:  []model.Purchase{{Count: 2, Offer: providers.Offer{HourlyCostMicros: 1_500_000}}},
		CostMicros: 42_000_000,
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

// reconcileManagedComputeAt runs one reconcile at a set time.
func (s *Service) reconcileManagedComputeAt(ctx context.Context, now time.Time) error {
	return s.reconcileManagedComputeWithClock(ctx, func() time.Time { return now })
}

// testOwner is the name the fake providers put on what a test service buys.
const testOwner = "beam-test"

var (
	a10gOffer = providers.Offer{
		ID: "A10G", Cloud: "lambda", InstanceType: "A10G", Region: "us-east",
		GPU: "A10G", GPUCount: 1, CPUMillicores: 4000, MemoryMB: 16 * 1024, StorageMB: 128 * 1024,
		HourlyCostMicros: 1_500_000,
	}
	cpx31Offer = providers.Offer{
		ID: "cpx31", Cloud: "hetzner", InstanceType: "cpx31", Region: "ash",
		CPUMillicores: 4000, MemoryMB: 8192, StorageMB: 163840, HourlyCostMicros: 31_200,
		DisplayName: "CPX31", Category: "shared", RegionDisplayName: "Ashburn", Latitude: 39.0438, Longitude: -77.4874,
	}
)

// fakeProvider is a provider that sells the given offers.
func fakeProvider(name string, offers ...providers.Offer) *providers.Fake {
	fake := providers.NewFake(name, testOwner)
	fake.SetOffers(offers...)
	return fake
}

// launchService is a service that can launch machines from the providers.
func launchService(repo repository.ComputeRepository, from ...providers.Provider) *Service {
	return &Service{
		computeRepo: repo,
		providers:   providers.RegistryOf(from...),
		billing: &fakeManagedBilling{
			launchDecision: billingDecision{OK: true, AvailableCents: 5000, RequiredCents: 2500},
		},
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				HTTP: types.HTTPConfig{ExternalHost: "app.beam.test", ExternalPort: 443, TLS: true},
			},
			Tailscale: types.TailscaleConfig{Enabled: true, AuthKey: "gateway-key", AgentAuthKey: "agent-key"},
			ManagedCompute: types.ManagedComputeConfig{
				Billing: types.ManagedComputeBillingConfig{MinimumCreditCents: 2500},
			},
		},
	}
}

func TestListPoolOffersReturnsCPUOffers(t *testing.T) {
	service := &Service{providers: providers.RegistryOf(fakeProvider("hetzner", cpx31Offer), fakeProvider("shadeform", a10gOffer))}

	res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{
		Pool: &pb.PoolConfig{Providers: []string{"hetzner"}},
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
	// An offer is one machine, and a provider that does not count its stock
	// is shown as having one.
	if offer.NodeCount != 1 || offer.GpuCount != 0 || offer.Available != 1 {
		t.Fatalf("offer node/gpu/available = %d/%d/%d, want 1/0/1", offer.NodeCount, offer.GpuCount, offer.Available)
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
	// The tenant is shown the price they pay, which carries the margin.
	if got, want := offer.HourlyCostMicros, int64(34_320); got != want {
		t.Fatalf("offer hourly cost = %d, want %d", got, want)
	}
}

func TestListPoolOffersAsksEveryProviderWhenThePoolNamesNone(t *testing.T) {
	service := &Service{providers: providers.RegistryOf(
		fakeProvider("shadeform", a10gOffer),
		fakeProvider("hetzner", cpx31Offer),
	)}

	res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{}})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	listed := []string{}
	for _, offer := range res.Offers {
		listed = append(listed, offer.Provider+"/"+offer.Id)
	}
	require.Equal(t, []string{"hetzner/cpx31", "shadeform/A10G"}, listed, "cheapest first")

	res, err = service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{Gpu: []string{"A10G"}, Regions: []string{"us-west"}}})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Empty(t, res.Offers, "the pool's regions narrow what is listed")
}

// spotA10GOffer is the same machine as a10gOffer, sold for a quarter of the
// price on the terms that its provider may take it back.
var spotA10GOffer = func() providers.Offer {
	spot := a10gOffer
	spot.Spot = true
	spot.HourlyCostMicros = a10gOffer.HourlyCostMicros / 4
	return spot
}()

// A spot machine costs less and can be taken back at any time. A pool that
// does not ask for one is neither shown nor sold one, however little it
// costs.
func TestAReservationIsOnDemandUnlessThePoolAsksForSpot(t *testing.T) {
	shadeform := fakeProvider("shadeform", spotA10GOffer, a10gOffer)
	service := &Service{providers: providers.RegistryOf(shadeform)}

	listed := func(pricing string) []*pb.PoolOffer {
		res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{Pricing: pricing}})
		require.NoError(t, err)
		require.True(t, res.Ok, res.ErrMsg)
		return res.Offers
	}

	for _, pricing := range []string{"", "on_demand", "on-demand", "On_Demand"} {
		offers := listed(pricing)
		require.Len(t, offers, 1, pricing)
		require.False(t, offers[0].Spot, pricing)
		require.Equal(t, service.billableOffer(a10gOffer).HourlyCostMicros, offers[0].HourlyCostMicros, "the on-demand machine, at its price")
	}

	offers := listed("spot")
	require.Len(t, offers, 1)
	require.True(t, offers[0].Spot, "a spot offer says that it is one")
	require.Equal(t, service.billableOffer(spotA10GOffer).HourlyCostMicros, offers[0].HourlyCostMicros)

	// Both ways of buying the one instance type, cheapest first.
	offers = listed("any")
	require.Len(t, offers, 2)
	require.Equal(t, []bool{true, false}, []bool{offers[0].Spot, offers[1].Spot})
	require.Equal(t, offers[0].Id, offers[1].Id)

	res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{Pricing: "preemptible"}})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Contains(t, res.ErrMsg, `pricing must be on_demand, spot or any, not "preemptible"`)

	// These are the offers a launch chooses from.
	chosen, err := service.collectPoolOffers(context.Background(), model.Pool{}, nil)
	require.NoError(t, err)
	require.Len(t, chosen, 1)
	require.False(t, chosen[0].Spot)
	require.Equal(t, a10gOffer.HourlyCostMicros, chosen[0].HourlyCostMicros)
}

func TestLaunchPoolCapacityBuysOnDemandWhenSpotCostsLess(t *testing.T) {
	shadeform := fakeProvider("shadeform", spotA10GOffer, a10gOffer)
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform)

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 1, OfferId: "A10G", Ttl: "1h", MaxSpend: 2},
	})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)

	creates := shadeform.Creates()
	require.Len(t, creates, 1)
	require.False(t, creates[0].Offer.Spot)
	require.Zero(t, creates[0].MaxHourlyCostMicros, "an on-demand machine's price does not move, so it has no limit")
	reservation := repo.pools["workspace-1"][0].Reservations[0]
	require.False(t, reservation.Spot)
	require.Equal(t, a10gOffer.HourlyCostMicros, reservation.HourlyCostMicros)
	require.Len(t, res.Pool.Reservations, 1)
	require.False(t, res.Pool.Reservations[0].Spot)
}

func TestLaunchPoolCapacityBuysASpotMachineWhenThePoolAsksForSpot(t *testing.T) {
	shadeform := fakeProvider("shadeform", spotA10GOffer, a10gOffer)
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform)

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 1, OfferId: "A10G", Ttl: "1h", MaxSpend: 2, Pricing: "spot"},
	})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)

	creates := shadeform.Creates()
	require.Len(t, creates, 1)
	require.True(t, creates[0].Offer.Spot)
	require.Equal(t, spotA10GOffer.HourlyCostMicros, creates[0].Offer.HourlyCostMicros)
	// The machine is kept for as long as it costs no more than its tenant
	// pays for it, which is the offer's price and Beam's margin.
	require.Equal(t, service.billableMicros(spotA10GOffer.HourlyCostMicros), creates[0].MaxHourlyCostMicros)
	require.Greater(t, creates[0].MaxHourlyCostMicros, spotA10GOffer.HourlyCostMicros)

	state := repo.pools["workspace-1"][0]
	require.Equal(t, "spot", state.Config.Pricing, "the pool remembers how its machines are bought")
	reservation := state.Reservations[0]
	require.True(t, reservation.Spot)
	require.Equal(t, spotA10GOffer.HourlyCostMicros, reservation.HourlyCostMicros)
	require.Equal(t, spotA10GOffer.HourlyCostMicros, reservation.CommittedMicros, "one hour at the spot price")
	require.Len(t, res.Pool.Reservations, 1)
	require.True(t, res.Pool.Reservations[0].Spot, "the machine is listed as the spot machine it is")
}

func TestLaunchPoolCapacityBuysWhicheverCostsLessWhenThePoolTakesEither(t *testing.T) {
	// One spot machine is in stock, and two are wanted.
	lastSpot := spotA10GOffer
	lastSpot.Available = 1
	shadeform := fakeProvider("shadeform", a10gOffer, lastSpot)
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform)

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 2, Ttl: "1h", MaxSpend: 4, Pricing: "any"},
	})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)

	bought := []bool{}
	for _, create := range shadeform.Creates() {
		bought = append(bought, create.Offer.Spot)
	}
	require.ElementsMatch(t, []bool{true, false}, bought, "the spot machine, and an on-demand one for the rest")
}

// A pool that was filled with on-demand machines and is then given spot ones
// holds both, and says so.
func TestAddingSpotMachinesToAnOnDemandPoolMakesItHoldEither(t *testing.T) {
	onDemand := &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 1, MaxSpend: 2}
	spot := &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 1, MaxSpend: 2, Pricing: "spot"}

	require.Equal(t, "any", mergePoolConfigForLaunch(onDemand, spot).Pricing)
	require.Equal(t, "any", mergePoolConfigForLaunch(spot, onDemand).Pricing)
	require.Equal(t, "spot", mergePoolConfigForLaunch(spot, spot).Pricing)
	require.Empty(t, mergePoolConfigForLaunch(onDemand, onDemand).Pricing)
	require.Empty(t, normalizePoolConfig(&pb.PoolConfig{Name: "pool-1", Pricing: "On-Demand"}).Pricing, "on-demand is written as nothing")
}

// Only a reservation is bought one way or the other. A pool of the tenant's
// own machines has no use for the setting.
func TestOnlyAReservationNamesHowItsMachinesAreSold(t *testing.T) {
	err := model.Pool{Name: "pool-1", Pricing: model.PricingSpot}.Validate()
	require.Error(t, err)
	require.NoError(t, model.Pool{Name: "pool-1"}.Validate())
}

func TestListPoolOffersNeedsAProvider(t *testing.T) {
	res, err := (&Service{}).ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{}})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Equal(t, "no compute providers are configured", res.ErrMsg)
}

func TestLaunchPoolCapacityCreatesProviderReservation(t *testing.T) {
	shadeform := fakeProvider("shadeform", a10gOffer)
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform)
	started := time.Now()

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{
			Name:      "pool-1",
			Gpu:       []string{"A10G"},
			Nodes:     1,
			OfferId:   "A10G",
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

	creates := shadeform.Creates()
	require.Len(t, creates, 1)
	state := repo.pools["workspace-1"][0]
	require.Len(t, state.Reservations, 1)
	reservation := state.Reservations[0]
	require.NotEmpty(t, reservation.MachineID, "reservation missing managed machine id")

	// The provider is given the machine's name, the command that joins it to
	// the pool, a tag that says which machine it is, and a time well past its
	// term at which to delete it should Beam not have done so.
	create := creates[0]
	require.Equal(t, a10gOffer.ID, create.Offer.ID)
	require.Contains(t, create.Name, reservation.MachineID)
	require.Equal(t, reservation.Name, create.Name)
	require.Contains(t, create.StartupScript, "app.beam.test")
	require.Equal(t, map[string]string{"beam-machine": reservation.MachineID}, create.Tags)
	require.WithinDuration(t, started.Add(time.Hour).Add(reservationDeletionGrace), create.Until, time.Minute)

	// The record says what was bought, from the offer, and on what terms.
	instances := shadeform.Everything()
	require.Len(t, instances, 1)
	require.Equal(t, instances[0].ID, reservation.ID)
	require.Equal(t, instances[0].ID, reservation.InstanceID)
	require.Equal(t, "pool-1", reservation.PoolName)
	require.Equal(t, "pool-1", reservation.Selector)
	require.Equal(t, "shadeform", reservation.Provider)
	require.Equal(t, "lambda", reservation.Cloud)
	require.Equal(t, "us-east", reservation.Region)
	require.Equal(t, "A10G", reservation.OfferID)
	require.Equal(t, "A10G", reservation.InstanceType)
	require.Equal(t, "A10G", reservation.GPU)
	require.Equal(t, uint32(1), reservation.GPUCount)
	require.Equal(t, uint32(1), reservation.NodeCount)
	require.Equal(t, int64(4000), reservation.CPUMillicores)
	require.Equal(t, int64(16*1024), reservation.MemoryMB)
	require.Equal(t, int64(128*1024), reservation.StorageMB)
	require.Equal(t, int64(1_500_000), reservation.HourlyCostMicros)
	require.Equal(t, int64(1_500_000), reservation.CommittedMicros, "one hour at the provider's price")
	require.Equal(t, model.SourceCLIReservation, reservation.Source)
	require.Equal(t, model.ReservationPending, reservation.Status)
	require.Equal(t, time.Hour, reservation.ExpiresAt.Sub(reservation.CreatedAt))
	require.Equal(t, time.Hour, reservation.BillingRenewalAt.Sub(reservation.CreatedAt))
	require.NotEmpty(t, reservation.RegistrationTokenHash)
}

func TestLaunchPoolCapacityBuysTheOfferInTheRegionAsked(t *testing.T) {
	// One instance type is sold in several regions and by several clouds.
	paris := a10gOffer
	paris.Cloud, paris.Region, paris.HourlyCostMicros = "scaleway", "paris", 1_000_000
	shadeform := fakeProvider("shadeform", paris, a10gOffer)
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform)

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 1, OfferId: "A10G", Ttl: "1h", MaxSpend: 2, Providers: []string{"shadeform"}, Regions: []string{"us-east"}},
	})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)

	creates := shadeform.Creates()
	require.Len(t, creates, 1)
	require.Equal(t, "us-east", creates[0].Offer.Region)
	require.Equal(t, "lambda", creates[0].Offer.Cloud)
	reservation := repo.pools["workspace-1"][0].Reservations[0]
	require.Equal(t, "us-east", reservation.Region)
	require.Equal(t, "lambda", reservation.Cloud)
}

func TestLaunchPoolCapacityBuysNothingWhenTheProviderIsOutOfStock(t *testing.T) {
	shadeform := fakeProvider("shadeform", a10gOffer)
	shadeform.RefuseCreates(fmt.Errorf("%w: none left in us-east", providers.ErrOutOfStock))
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform)

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 1, Ttl: "1h", MaxSpend: 2},
	})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Equal(t, launchErrorOutOfStock, res.ErrorCode)
	require.Contains(t, res.ErrMsg, "out of stock")
	require.Empty(t, repo.pools["workspace-1"], "no pool is left behind")
	requireEveryTokenRevoked(t, repo)
}

// A provider that refuses to sell says so in a way its caller can tell from
// a failure: the same machine may be had from another offer, and the
// dashboard goes on to the next one.
func TestALaunchSaysWhenItsProviderRefusedToSell(t *testing.T) {
	for code, refusal := range map[string]error{
		launchErrorOutOfStock:      fmt.Errorf("%w: none left in us-east", providers.ErrOutOfStock),
		launchErrorProviderQuota:   fmt.Errorf("%w: 8 GPUs at most", providers.ErrQuota),
		launchErrorProviderFailure: errors.New("internal server error"),
	} {
		shadeform := fakeProvider("shadeform", a10gOffer)
		shadeform.RefuseCreates(refusal)
		res, err := launchService(&fakeComputeRepo{}, shadeform).LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
			Pool: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 1, Ttl: "1h", MaxSpend: 2},
		})
		require.NoError(t, err)
		require.False(t, res.Ok)
		require.Equal(t, code, res.ErrorCode, refusal)
	}
}

func TestLaunchPoolCapacityDeletesWhatItBoughtWhenALaterMachineFails(t *testing.T) {
	// The catalogue says the offer is in stock, and the provider has one.
	shadeform := fakeProvider("shadeform", a10gOffer)
	shadeform.RefuseCreatesAfter(1, fmt.Errorf("%w: none left in us-east", providers.ErrOutOfStock))
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform)

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 2, Ttl: "1h", MaxSpend: 10},
	})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Equal(t, launchErrorOutOfStock, res.ErrorCode)
	require.Contains(t, res.ErrMsg, "out of stock")

	creates := shadeform.Creates()
	require.Len(t, creates, 1)
	require.Empty(t, shadeform.Everything(), "the machine that was bought is deleted again")
	require.Empty(t, repo.pools["workspace-1"], "no pool is left behind")
	require.Len(t, repo.joinTokens, 2)
	requireEveryTokenRevoked(t, repo)
}

func TestLaunchPoolCapacityCreatesCPUNodeReservation(t *testing.T) {
	hetzner := fakeProvider("hetzner", cpx31Offer)
	repo := &fakeComputeRepo{}
	service := launchService(repo, hetzner)

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Nodes: 1,
		Pool: &pb.PoolConfig{
			Name:      "cpu-pool",
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
	creates := hetzner.Creates()
	require.Len(t, creates, 1)
	require.Equal(t, "cpx31", creates[0].Offer.ID)
	require.Equal(t, "ash", creates[0].Offer.Region)

	state := repo.pools["workspace-1"][0]
	if got, want := state.ReservedNodes, uint32(1); got != want {
		t.Fatalf("reserved nodes = %d, want %d", got, want)
	}
	if got, want := len(state.Reservations), 1; got != want {
		t.Fatalf("reservation count = %d, want %d", got, want)
	}
	reservation := state.Reservations[0]
	if reservation.Provider != "hetzner" || reservation.NodeCount != 1 || reservation.GPUCount != 0 {
		t.Fatalf("reservation provider/node/gpu = %s/%d/%d", reservation.Provider, reservation.NodeCount, reservation.GPUCount)
	}
	if state.Config.Nodes != 1 {
		t.Fatalf("pool config nodes = %d, want 1", state.Config.Nodes)
	}
}

func TestLaunchPoolCapacityBuysSeveralMachinesOfAnOfferTheProviderDoesNotCount(t *testing.T) {
	hetzner := fakeProvider("hetzner", cpx31Offer)
	repo := &fakeComputeRepo{}
	service := launchService(repo, hetzner)

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Nodes: 3,
		Pool:  &pb.PoolConfig{Name: "cpu-pool", Ttl: "1h", MaxSpend: 1},
	})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Len(t, hetzner.Creates(), 3)
	require.Len(t, repo.pools["workspace-1"][0].Reservations, 3)
}

func TestLaunchPoolCapacityAddsReservationToExistingPool(t *testing.T) {
	shadeform := fakeProvider("shadeform", a10gOffer)

	now := time.Now().UTC()
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:                 "pool-1",
					Mode:                 string(types.PoolModePrivate),
					Selector:             "pool-1",
					CreatedByTokenID:     "token-1",
					ReservedNodes:        1,
					CommittedSpendMicros: 1_650_000,
					Reservations: []model.Reservation{
						{
							ID:               "reservation-1",
							PoolName:         "pool-1",
							Selector:         "pool-1",
							Provider:         "shadeform",
							OfferID:          "A10G",
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
	service := launchService(repo, shadeform)

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{
			Name:      "pool-1",
			Gpu:       []string{"A10G"},
			Nodes:     1,
			OfferId:   "A10G",
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
	if got := len(shadeform.Creates()); got != 1 {
		t.Fatalf("shadeform create calls = %d, want 1", got)
	}
	state := repo.pools["workspace-1"][0]
	if got, want := len(state.Reservations), 2; got != want {
		t.Fatalf("reservation count = %d, want %d", got, want)
	}
	bought := shadeform.Everything()
	require.Len(t, bought, 1)
	if got, want := state.Reservations[1].ID, bought[0].ID; got != want {
		t.Fatalf("new reservation id = %q, want %q", got, want)
	}
	if got, want := state.ReservedNodes, uint32(2); got != want {
		t.Fatalf("reserved nodes = %d, want %d", got, want)
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

// boughtFrom plants a machine at a fake provider and points the state's first
// reservation at it, as if the service had bought it there.
func boughtFrom(fake *providers.Fake, state *model.PoolState) string {
	id := fake.Plant(testOwner, providers.StatusActive, nil)
	state.Reservations[0].Provider = fake.Name()
	state.Reservations[0].InstanceID = id
	return id
}

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
	shadeform := fakeProvider("shadeform")
	instanceID := boughtFrom(shadeform, state)
	service := &Service{providers: providers.RegistryOf(shadeform)}

	// Plenty of balance: renew for another hour
	decision := billingDecision{OK: true, AvailableCents: 10_000}
	if !service.renewManagedReservations(context.Background(), "workspace-1", state, decision, 5, now) {
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
	if got, want := shadeform.Until(instanceID), expiresAt.Add(time.Hour).Add(reservationDeletionGrace); !got.Equal(want) {
		t.Fatalf("provider extended to %s, want %s", got, want)
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
	shadeform := fakeProvider("shadeform")
	instanceID := boughtFrom(shadeform, state)
	service := &Service{providers: providers.RegistryOf(shadeform)}

	// 16.5 cents/hr billable but only 10 cents headroom: do not renew
	decision := billingDecision{OK: true, AvailableCents: 15}
	if service.renewManagedReservations(context.Background(), "workspace-1", state, decision, 5, now) {
		t.Fatal("renewManagedReservations() renewed without sufficient balance")
	}
	if !state.Reservations[0].ExpiresAt.Equal(expiresAt) {
		t.Fatal("reservation expiry should be unchanged")
	}
	if !shadeform.Until(instanceID).IsZero() {
		t.Fatal("provider should not have been called")
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
	shadeform := fakeProvider("shadeform")
	boughtFrom(shadeform, state)
	service := &Service{providers: providers.RegistryOf(shadeform)}

	decision := billingDecision{OK: true, AvailableCents: 10_000}
	if service.renewManagedReservations(context.Background(), "workspace-1", state, decision, 0, now) {
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
	}, nil)
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
	}, nil)
	if err != nil {
		t.Fatalf("recordAgentMetrics() error = %v", err)
	}
}

func TestApplyAgentNetworkKeepsKnownAddressesOnBadReport(t *testing.T) {
	state := &model.AgentTokenState{}
	if !applyAgentNetwork(state, &pb.AgentNetworkInfo{PublicIp: "203.0.113.7", TailnetIp: "100.64.0.7", TailnetHostname: "beam-agent-m1", TailnetSsh: true}) {
		t.Fatal("first report must be a change")
	}
	// A failed lookup, a private address and garbage never erase what is known.
	if applyAgentNetwork(state, &pb.AgentNetworkInfo{PublicIp: "10.0.0.5", TailnetIp: "nope", TailnetHostname: "bad host", TailnetSsh: true}) {
		t.Fatal("invalid report must not change state")
	}
	want := model.AgentMachineNetwork{PublicIP: "203.0.113.7", TailnetIP: "100.64.0.7", TailnetHostname: "beam-agent-m1", TailnetSSH: true}
	if state.Network != want {
		t.Fatalf("network = %+v, want %+v", state.Network, want)
	}
}

func TestUpdateAgentAvailabilityDisablesMachineWorker(t *testing.T) {
	now := time.Now().UTC()
	agentToken := "agent-token"
	machine := &model.AgentTokenState{
		TokenHash:       hashComputeToken(agentToken),
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
		GPUIDs:          []string{"GPU-one"},
		GPUCount:        1,
		Executor:        types.DefaultAgentWorkerContainerMode,
		Schedulable:     true,
		LastJoinAt:      now,
		LastHeartbeatAt: now,
	}
	computeRepo := &fakeComputeRepo{
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "pool-1"): {machine},
		},
	}
	workerID := model.AgentMachineWorkerID(machine.MachineID)
	workerRepo := &fakeWorkerRepo{worker: &types.Worker{
		Id:        workerID,
		MachineId: machine.MachineID,
		PoolName:  machine.PoolName,
		Status:    types.WorkerStatusAvailable,
	}}
	containerRepo := &fakeContainerRepo{containers: []types.ContainerState{{
		ContainerId: "container-one",
		Status:      types.ContainerStatusRunning,
	}}}
	service := &Service{computeRepo: computeRepo, workerRepo: workerRepo, containerRepo: containerRepo}

	resp, err := service.UpdateAgentAvailability(context.Background(), &pb.UpdateAgentAvailabilityRequest{
		AgentToken:         agentToken,
		Schedulable:        false,
		Reason:             "vast_preempt",
		ObservedAtUnixNano: now.UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Ok {
		t.Fatalf("response not ok: %s", resp.ErrMsg)
	}
	if machine.Schedulable {
		t.Fatal("machine is still schedulable")
	}
	if machine.AvailabilityReason != "vast_preempt" {
		t.Fatalf("availability reason = %q", machine.AvailabilityReason)
	}
	if workerRepo.worker.Status != types.WorkerStatusDisabled {
		t.Fatalf("worker status = %s, want %s", workerRepo.worker.Status, types.WorkerStatusDisabled)
	}
	if got, want := strings.Join(containerRepo.stopped, ","), "container-one"; got != want {
		t.Fatalf("stopped containers = %q, want %q", got, want)
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

func TestRecordManagedUsageRecordsBYOCMachineWithManagedRates(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	activeWindow := 4 * time.Minute
	cursor := now.Add(-activeWindow)
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "aws-cpu",
		Source:      model.SourceAWS,
		BYOC: &model.BYOCProviderState{
			Provider: "aws",
		},
	}
	machine := &model.AgentTokenState{
		WorkspaceID:        "workspace-1",
		PoolName:           "aws-cpu",
		MachineID:          "machine-1",
		MachineFingerprint: "i-0123456789abcdef0",
		CPUMillicores:      4_000,
		MemoryMB:           32 * 1024,
		GPUs:               []string{"A10G"},
		GPUCount:           1,
		CreatedAt:          now.Add(-2 * time.Hour),
		LastJoinAt:         now.Add(-2 * time.Hour),
		LastHeartbeatAt:    now,
		BillingCursorAt:    cursor,
		Schedulable:        true,
	}
	billing := &fakeManagedBilling{}
	usageRepo := &fakeUsageMetricsRepo{}
	repo := &fakeComputeRepo{
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "aws-cpu"): {machine},
		},
	}
	service := &Service{billing: billing, usageMetricsRepo: usageRepo, computeRepo: repo}

	if !service.recordManagedUsage(context.Background(), "workspace-1", state, now, false) {
		t.Fatal("recordManagedUsage() did not report a state change")
	}
	if got, want := len(billing.usage), 1; got != want {
		t.Fatalf("billing usage count = %d, want %d", got, want)
	}
	usage := billing.usage[0]
	if got, want := usage.ReservationID, "byoc:machine-1"; got != want {
		t.Fatalf("reservation id = %q, want %q", got, want)
	}
	if got, want := usage.Provider, "byoc"; got != want {
		t.Fatalf("provider = %q, want %q", got, want)
	}
	if got, want := usage.Cloud, "aws"; got != want {
		t.Fatalf("cloud = %q, want %q", got, want)
	}
	if got, want := usage.HourlyCostMicros, int64(182_000); got != want {
		t.Fatalf("hourly cost micros = %d, want %d", got, want)
	}
	if got, want := usage.CostCents, 1.0; got < want-0.001 || got > want+0.001 {
		t.Fatalf("cost cents = %f, want about %f", got, want)
	}
	wantBillableDuration := time.Duration(float64(activeWindow) * (1.0 / managedCostCents(182_000, activeWindow))).Truncate(time.Second)
	if got, want := usage.DurationSeconds, wantBillableDuration.Seconds(); got != want {
		t.Fatalf("duration seconds = %f, want %f", got, want)
	}
	if got, want := usage.CPUMillicores, int64(4_000); got != want {
		t.Fatalf("cpu millicores = %d, want %d", got, want)
	}
	if got, want := usage.MemoryMB, int64(32*1024); got != want {
		t.Fatalf("memory mb = %d, want %d", got, want)
	}
	if got, want := usage.GPU, "A10G"; got != want {
		t.Fatalf("gpu = %q, want %q", got, want)
	}
	if want := cursor.Add(wantBillableDuration); !machine.BillingCursorAt.Equal(want) {
		t.Fatalf("machine billing cursor = %s, want %s", machine.BillingCursorAt, want)
	}
	if got, want := len(usageRepo.counters), 2; got != want {
		t.Fatalf("usage counter count = %d, want %d", got, want)
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
		appConfig:   types.AppConfig{ManagedCompute: types.ManagedComputeConfig{Billing: types.ManagedComputeBillingConfig{CreditGracePeriod: -1}}},
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

func TestReconcileManagedComputeWaitsOutCreditGraceBeforeTerminating(t *testing.T) {
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
	if got := saved.Reservations[0].Status; got != model.ReservationActive {
		t.Fatalf("reservation status in grace = %q, want active", got)
	}
	if saved.CreditLowSince.IsZero() {
		t.Fatal("grace start was not recorded")
	}
	if !machine.Schedulable {
		t.Fatal("machine was disabled during the credit grace period")
	}

	// Grace elapsed without a top-up: the pool is released.
	saved.CreditLowSince = time.Now().Add(-31 * time.Minute)
	if err := service.ReconcileManagedCompute(context.Background()); err != nil {
		t.Fatalf("ReconcileManagedCompute() error = %v", err)
	}
	saved = repo.pools["workspace-1"][0]
	if got, want := saved.Reservations[0].Status, model.ReservationTerminating; got != want {
		t.Fatalf("reservation status after grace = %q, want %q", got, want)
	}
	if machine.Schedulable {
		t.Fatal("machine still schedulable after grace expired")
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
				ID:               "reservation-1",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationActive,
				CreatedAt:        now.Add(-time.Hour),
				BillingCursorAt:  now,
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
	// The balance does not cover another hour, so the reservation is not
	// renewed and ends at its expiry.
	billing := &fakeManagedBilling{balanceDecision: billingDecision{OK: true, RequiredCents: 1, AvailableCents: 50}}
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
	if !strings.Contains(saved.Reservations[0].LastError, "provider \"shadeform\" is not configured") {
		t.Fatalf("reservation last error = %q, want missing provider error", saved.Reservations[0].LastError)
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

func TestReleasePrivateMachineCleansMachineScopedRedisState(t *testing.T) {
	ctx := context.Background()
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	computeRepo := repository.NewComputeRedisRepository(rdb)
	workerRepo := repository.NewWorkerRedisRepositoryForTest(rdb)
	machine := &model.AgentTokenState{
		TokenHash:       "agent-token-hash",
		WorkspaceID:     "workspace-1",
		PoolName:        "pool-1",
		MachineID:       "machine-1",
		Schedulable:     true,
		LastHeartbeatAt: time.Now().UTC(),
	}
	if err := computeRepo.SavePoolState(ctx, machine.WorkspaceID, &model.PoolState{
		Name: machine.PoolName,
		Mode: string(types.PoolModePrivate),
	}); err != nil {
		t.Fatal(err)
	}
	if err := computeRepo.SaveAgentTokenState(ctx, machine, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := computeRepo.SaveJoinTokenState(ctx, &model.JoinTokenState{
		TokenHash:   "revoked-join-token",
		WorkspaceID: machine.WorkspaceID,
		PoolName:    machine.PoolName,
		Revoked:     true,
	}, 0); err != nil {
		t.Fatal(err)
	}
	workerID := model.AgentMachineWorkerID(machine.MachineID)
	if err := workerRepo.AddWorker(&types.Worker{
		Id:        workerID,
		MachineId: machine.MachineID,
		PoolName:  machine.PoolName,
		Status:    types.WorkerStatusAvailable,
	}); err != nil {
		t.Fatal(err)
	}

	networkPrefix := common.WorkerNetworkPrefix("beta9", machine.MachineID)
	otherNetworkPrefix := common.WorkerNetworkPrefix("beta9", "machine-2")
	for i := 0; i < 24; i++ {
		if err := workerRepo.SetContainerIp(
			networkPrefix,
			fmt.Sprintf("network-slot:slot-%02d", i),
			fmt.Sprintf("192.168.0.%d", i+2),
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := workerRepo.SetContainerIp(otherNetworkPrefix, "network-slot:other", "192.168.1.2"); err != nil {
		t.Fatal(err)
	}

	locality := machine.WorkspaceID + "/" + machine.PoolName
	coordinator := cache.NewCoordinator(repository.NewCacheRedisRepository(rdb))
	targetLogicalHostID := "cache-host-workspace-1-pool-1-machine-1-path"
	for _, host := range []cache.CoordinatorHost{
		{
			LogicalHostID:  targetLogicalHostID,
			RegistrationID: workerID,
			PoolName:       machine.PoolName,
			Locality:       locality,
			NodeID:         machine.MachineID,
			CachePathID:    "path",
			PrivateAddr:    "10.0.0.1:2049",
		},
		{
			LogicalHostID:  "cache-host-workspace-1-pool-1-machine-2-path",
			RegistrationID: "worker-machine-2",
			PoolName:       machine.PoolName,
			Locality:       locality,
			NodeID:         "machine-2",
			CachePathID:    "path",
			PrivateAddr:    "10.0.0.2:2049",
		},
	} {
		if err := coordinator.RegisterHost(ctx, host, 30*time.Second); err != nil {
			t.Fatal(err)
		}
	}

	routeRevisionKeys := []string{
		common.RedisKeys.SchedulerBackendRouteMachineRevision(machine.WorkspaceID, machine.PoolName, machine.MachineID),
		common.RedisKeys.SchedulerBackendRouteMachineIDRevision(machine.MachineID),
	}
	for _, key := range routeRevisionKeys {
		if err := rdb.Set(ctx, key, "7", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}

	service := &Service{
		appConfig:   types.AppConfig{ClusterName: "beta9"},
		computeRepo: computeRepo,
		workerRepo:  workerRepo,
		redisClient: rdb,
	}
	if err := service.releasePrivateMachine(ctx, machine); err != nil {
		t.Fatal(err)
	}

	networkKeys, err := rdb.Keys(ctx, "worker:network:"+networkPrefix+"*")
	if err != nil || len(networkKeys) != 0 {
		t.Fatalf("released machine network keys = %v, %v; want none", networkKeys, err)
	}
	otherIP, err := workerRepo.GetContainerIp(otherNetworkPrefix, "network-slot:other")
	if err != nil || otherIP != "192.168.1.2" {
		t.Fatalf("other machine network allocation = %q, %v", otherIP, err)
	}
	cacheKeys, err := rdb.Keys(ctx, "cache:coordinator:host:"+targetLogicalHostID+"*")
	if err != nil || len(cacheKeys) != 0 {
		t.Fatalf("released machine cache keys = %v, %v; want none", cacheKeys, err)
	}
	hosts, err := coordinator.ListHosts(ctx, machine.PoolName, locality)
	if err != nil || len(hosts) != 1 || hosts[0].NodeID != "machine-2" {
		t.Fatalf("remaining cache hosts = %#v, %v; want only machine-2", hosts, err)
	}
	for _, key := range routeRevisionKeys {
		value, err := rdb.Get(ctx, key).Result()
		if err != nil || value != "7" {
			t.Fatalf("route revision %q = %q, %v; want preserved", key, value, err)
		}
	}
	joinToken, err := computeRepo.GetJoinTokenState(ctx, "revoked-join-token")
	if err != nil || joinToken == nil || !joinToken.Revoked {
		t.Fatalf("revoked join token = %#v, %v; want preserved", joinToken, err)
	}
}

func TestTerminateReservationClosesAReservationWhoseMachineIsAlreadyGone(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "codex-hetzner-latency",
		Reservations: []model.Reservation{
			{
				ID:               "reservation-1",
				Provider:         "hetzner",
				InstanceID:       "42",
				Source:           model.SourceCLIReservation,
				Status:           model.ReservationActive,
				CreatedAt:        now.Add(-time.Hour),
				ExpiresAt:        now.Add(time.Hour),
				HourlyCostMicros: 1_000_000,
			},
		},
	}
	// The provider has no machine 42 any more, and deleting it succeeds.
	hetzner := fakeProvider("hetzner")
	service := &Service{providers: providers.RegistryOf(hetzner)}

	service.terminateReservation(
		context.Background(),
		"workspace-1",
		state,
		&state.Reservations[0],
		reconcileReasonMachineReleased,
		machineReleasedMessage,
		now,
	)
	if got, want := state.Reservations[0].Status, model.ReservationDeleted; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
	if got := state.Reservations[0].LastError; got != "" {
		t.Fatalf("reservation last error = %q, want empty", got)
	}
	if got, want := hetzner.Deletes(), []string{"42"}; !sameStrings(got, want) {
		t.Fatalf("deleted reservations = %v, want %v", got, want)
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

func TestDeletePrivateMachineReleasesManagedReservationID(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID:      "workspace-1",
		Name:             "pool-1",
		Mode:             string(types.PoolModePrivate),
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

	res, err := service.DeletePrivateMachine(
		testAuthContext("workspace-1", "token-owner"),
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: "reservation-1"},
	)
	if err != nil {
		t.Fatalf("DeletePrivateMachine() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePrivateMachine() response = %#v", res)
	}
	if !repo.savedPool {
		t.Fatal("DeletePrivateMachine() did not persist the reservation transition")
	}
	if got, want := state.Reservations[0].Status, model.ReservationTerminating; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
	if got, want := state.Reservations[0].TerminatingReason, reconcileReasonMachineReleased; got != want {
		t.Fatalf("terminating reason = %q, want %q", got, want)
	}
	if !strings.Contains(state.Reservations[0].LastError, "provider \"shadeform\" is not configured") {
		t.Fatalf("reservation last error = %q, want missing provider error", state.Reservations[0].LastError)
	}
}

func TestDeletePrivateMachineBYOCMissingMachineIsIdempotentAndCleansDeletedPool(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	resourceURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "aws-cpu",
					Mode:             string(types.PoolModePrivate),
					CreatedByTokenID: "owner-token",
					Source:           model.SourceAWS,
					Config:           &pb.PoolConfig{Name: "aws-cpu", Regions: []string{"us-east-1"}},
					BYOC: &model.BYOCProviderState{
						Provider:     "aws",
						AccountID:    "123456789012",
						Region:       "us-east-1",
						ResourceName: "beam-aws-cpu-test",
						ResourceURL:  resourceURL,
						DestroyURL:   resourceURL,
						Labels: map[string]string{
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
				},
			},
		},
	}
	oldResourceDeleted := awsBYOCResourceDeleted
	awsBYOCResourceDeleted = func(context.Context, awsBYOCResourceDeletedInput) (bool, error) {
		return true, nil
	}
	defer func() {
		awsBYOCResourceDeleted = oldResourceDeleted
	}()

	res, err := (&Service{computeRepo: repo}).DeletePrivateMachine(
		ctx,
		&pb.DeleteMachineRequest{PoolName: "aws-cpu", MachineId: "machine-already-gone"},
	)
	if err != nil {
		t.Fatalf("DeletePrivateMachine() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePrivateMachine() response = %#v", res)
	}
	if got := len(repo.pools["workspace-1"]); got != 0 {
		t.Fatalf("stored pool count = %d, want 0", got)
	}
}

func TestDeletePrivateMachineMissingPrivateMachineIsIdempotent(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "pool-1",
					CreatedByTokenID: "owner-token",
					Mode:             string(types.PoolModePrivate),
				},
			},
		},
	}

	res, err := (&Service{computeRepo: repo}).DeletePrivateMachine(
		ctx,
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: "machine-already-deleted"},
	)
	if err != nil {
		t.Fatalf("DeletePrivateMachine() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePrivateMachine() response = %#v", res)
	}
}

func TestDeletePrivateMachineAllowsAnotherTokenInWorkspace(t *testing.T) {
	ctx := testAuthContext("workspace-1", "current-token")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					Name:             "pool-1",
					CreatedByTokenID: "creator-token",
					Mode:             string(types.PoolModePrivate),
				},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "pool-1"): {
				{WorkspaceID: "workspace-1", PoolName: "pool-1", MachineID: "machine-1"},
			},
			fakeComputeKey("workspace-2", "pool-1"): {
				{WorkspaceID: "workspace-2", PoolName: "pool-1", MachineID: "machine-1"},
			},
		},
	}

	res, err := (&Service{computeRepo: repo}).DeletePrivateMachine(
		ctx,
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: "machine-1"},
	)
	if err != nil {
		t.Fatalf("DeletePrivateMachine() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePrivateMachine() response = %#v", res)
	}
	if got := len(repo.machines[fakeComputeKey("workspace-1", "pool-1")]); got != 0 {
		t.Fatalf("stored machine count = %d, want 0", got)
	}
	if got := len(repo.machines[fakeComputeKey("workspace-2", "pool-1")]); got != 1 {
		t.Fatalf("other workspace machine count = %d, want 1", got)
	}
}

func TestDeletePrivateMachineReleasesManagedMachineID(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID:      "workspace-1",
		Name:             "pool-1",
		Mode:             string(types.PoolModePrivate),
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

	res, err := service.DeletePrivateMachine(
		testAuthContext("workspace-1", "token-owner"),
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: "machine-1"},
	)
	if err != nil {
		t.Fatalf("DeletePrivateMachine() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePrivateMachine() response = %#v", res)
	}
	if got, want := state.Reservations[0].Status, model.ReservationTerminating; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
}

func TestDeletePrivateMachineReleasesManagedProviderInstanceID(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID:      "workspace-1",
		Name:             "pool-1",
		Mode:             string(types.PoolModePrivate),
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

	res, err := service.DeletePrivateMachine(
		testAuthContext("workspace-1", "token-owner"),
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: "instance-1"},
	)
	if err != nil {
		t.Fatalf("DeletePrivateMachine() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePrivateMachine() response = %#v", res)
	}
	if got, want := state.Reservations[0].Status, model.ReservationTerminating; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
}

func TestDeletePrivateMachineByReservationIDCleansLinkedMachine(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID:      "workspace-1",
		Name:             "pool-1",
		Mode:             string(types.PoolModePrivate),
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

	res, err := service.DeletePrivateMachine(
		testAuthContext("workspace-1", "token-owner"),
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: "reservation-1"},
	)
	if err != nil {
		t.Fatalf("DeletePrivateMachine() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePrivateMachine() response = %#v", res)
	}
	if got, want := state.Reservations[0].Status, model.ReservationTerminating; got != want {
		t.Fatalf("reservation status = %q, want %q", got, want)
	}
	if machine.Schedulable {
		t.Fatal("DeletePrivateMachine() did not mark linked machine unschedulable")
	}
	if got := len(repo.machines[fakeComputeKey("workspace-1", "pool-1")]); got != 0 {
		t.Fatalf("machine count after release = %d, want 0", got)
	}
}

func TestDeletePrivateMachineDismissesFailedManagedReservation(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID:      "workspace-1",
		Name:             "pool-1",
		Mode:             string(types.PoolModePrivate),
		CreatedByTokenID: "token-owner",
		ReservedNodes:    1,
		Reservations: []model.Reservation{
			{
				ID:                "reservation-1",
				Provider:          "shadeform",
				InstanceID:        "instance-1",
				MachineID:         "machine-1",
				Name:              "node-1",
				Source:            model.SourceCLIReservation,
				Status:            model.ReservationFailed,
				CreatedAt:         now.Add(-time.Minute),
				ExpiresAt:         now.Add(time.Hour),
				LastStatusMessage: "provider failed to provision instance",
			},
		},
	}
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{"workspace-1": {state}},
	}
	service := &Service{computeRepo: repo}

	res, err := service.DeletePrivateMachine(
		testAuthContext("workspace-1", "token-owner"),
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: "machine-1"},
	)
	if err != nil {
		t.Fatalf("DeletePrivateMachine() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePrivateMachine() response = %#v", res)
	}
	if !repo.savedPool {
		t.Fatal("DeletePrivateMachine() did not persist the failed reservation removal")
	}
	if got := len(state.Reservations); got != 0 {
		t.Fatalf("reservation count after release = %d, want 0", got)
	}
	if got, want := state.ReservedNodes, uint32(0); got != want {
		t.Fatalf("reserved nodes = %d, want %d", got, want)
	}
}

func TestDeletePrivateMachineDismissesFailedUnlinkedManagedReservation(t *testing.T) {
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID:      "workspace-1",
		Name:             "pool-1",
		Mode:             string(types.PoolModePrivate),
		CreatedByTokenID: "token-owner",
		ReservedNodes:    1,
		Reservations: []model.Reservation{
			{
				ID:                "reservation-1",
				Provider:          "shadeform",
				InstanceID:        "instance-1",
				Name:              "node-1",
				Source:            model.SourceCLIReservation,
				Status:            model.ReservationFailed,
				CreatedAt:         now.Add(-time.Minute),
				ExpiresAt:         now.Add(time.Hour),
				LastStatusMessage: "provider failed to provision instance",
			},
		},
	}
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{"workspace-1": {state}},
	}
	service := &Service{computeRepo: repo}

	res, err := service.DeletePrivateMachine(
		testAuthContext("workspace-1", "token-owner"),
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: "reservation-1"},
	)
	if err != nil {
		t.Fatalf("DeletePrivateMachine() error = %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("DeletePrivateMachine() response = %#v", res)
	}
	if !repo.savedPool {
		t.Fatal("DeletePrivateMachine() did not persist the failed reservation removal")
	}
	if got := len(state.Reservations); got != 0 {
		t.Fatalf("reservation count after release = %d, want 0", got)
	}
	if got, want := state.ReservedNodes, uint32(0); got != want {
		t.Fatalf("reserved nodes = %d, want %d", got, want)
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

	err := service.assignManagedReservationToMachineLocked(
		context.Background(),
		state,
		&model.JoinTokenState{TokenHash: "token-hash"},
		&model.AgentTokenState{WorkspaceID: "workspace-1", PoolName: "pool-1", MachineID: "machine-1"},
	)
	if err != nil {
		t.Fatalf("assignManagedReservationToMachineLocked() error = %v", err)
	}

	if !repo.savedPool {
		t.Fatal("assignManagedReservationToMachineLocked() did not persist the pool state")
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

	err := (&Service{}).assignManagedReservationToMachineLocked(context.Background(), state, token, machine)
	if err == nil {
		t.Fatal("assignManagedReservationToMachineLocked() accepted a closed managed reservation")
	}
}

func testAuthContext(workspaceID, tokenID string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: workspaceID},
		Token: &types.Token{
			ExternalId: tokenID,
			TokenType:  types.TokenTypeWorkspace,
		},
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
	sshStates  map[string]*model.MachineSSHState
	sshMu      sync.Mutex
	savedPool  bool
}

func (r *fakeComputeRepo) ProviderOwner(ctx context.Context) (string, error) {
	return testOwner, nil
}

func (r *fakeComputeRepo) WithPoolStateLock(ctx context.Context, workspaceID, name string, fn func(context.Context) error) error {
	return fn(ctx)
}

func (r *fakeComputeRepo) PruneAgentMachineIndex(ctx context.Context, workspaceID, poolName string) error {
	return nil
}

func (r *fakeComputeRepo) WithMachineSSHStateLock(ctx context.Context, workspaceID, poolName, machineID string, fn func(context.Context) error) error {
	r.sshMu.Lock()
	defer r.sshMu.Unlock()
	return fn(ctx)
}

func (r *fakeComputeRepo) SaveMachineSSHState(ctx context.Context, state *model.MachineSSHState) error {
	if r.sshStates == nil {
		r.sshStates = map[string]*model.MachineSSHState{}
	}
	r.sshStates[fakeComputeSSHKey(state.WorkspaceID, state.PoolName, state.MachineID)] = state
	return nil
}

func (r *fakeComputeRepo) GetMachineSSHState(ctx context.Context, workspaceID, poolName, machineID string) (*model.MachineSSHState, error) {
	return r.sshStates[fakeComputeSSHKey(workspaceID, poolName, machineID)], nil
}

func (r *fakeComputeRepo) DeleteMachineSSHState(ctx context.Context, workspaceID, poolName, machineID string) error {
	delete(r.sshStates, fakeComputeSSHKey(workspaceID, poolName, machineID))
	return nil
}

func fakeComputeSSHKey(workspaceID, poolName, machineID string) string {
	return workspaceID + "\x00" + poolName + "\x00" + machineID
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
	states := r.pools[workspaceID]
	kept := states[:0]
	for _, state := range states {
		if state == nil || state.Name != name {
			kept = append(kept, state)
		}
	}
	if len(kept) == 0 {
		delete(r.pools, workspaceID)
		return nil
	}
	r.pools[workspaceID] = kept
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
	for _, machines := range r.machines {
		for _, machine := range machines {
			if machine != nil && machine.TokenHash == tokenHash {
				return machine, nil
			}
		}
	}
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

func (b *fakeManagedBilling) CheckBalance(context.Context, string, string) (billingDecision, error) {
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
	worker  *types.Worker
	workers []*types.Worker
	status  types.WorkerStatus
}

func (r *fakeWorkerRepo) AddWorker(worker *types.Worker) error {
	r.worker = worker
	return nil
}

func (r *fakeWorkerRepo) GetWorkerById(workerID string) (*types.Worker, error) {
	if r.worker == nil || r.worker.Id != workerID {
		return nil, &types.ErrWorkerNotFound{WorkerId: workerID}
	}
	return r.worker, nil
}

func (r *fakeWorkerRepo) GetAllWorkersInPool(poolName string) ([]*types.Worker, error) {
	workers := r.workers
	if len(workers) == 0 && r.worker != nil {
		workers = []*types.Worker{r.worker}
	}
	out := make([]*types.Worker, 0, len(workers))
	for _, worker := range workers {
		if worker != nil && worker.PoolName == poolName {
			out = append(out, worker)
		}
	}
	return out, nil
}

func (r *fakeWorkerRepo) UpdateWorkerStatus(workerID string, status types.WorkerStatus) error {
	if r.worker == nil || r.worker.Id != workerID {
		return &types.ErrWorkerNotFound{WorkerId: workerID}
	}
	r.worker.Status = status
	r.status = status
	return nil
}

func (r *fakeWorkerRepo) RemoveWorker(workerID string) error {
	if r.worker == nil || r.worker.Id != workerID {
		return &types.ErrWorkerNotFound{WorkerId: workerID}
	}
	r.worker = nil
	return nil
}

func (r *fakeWorkerRepo) RemoveWorkerNetworkState(context.Context, string) error {
	return nil
}

type fakeContainerRepo struct {
	repository.ContainerRepository
	containers        []types.ContainerState
	stopped           []string
	deletedRoutes     []string
	routesByMachine   map[string][]types.BackendRoute
	routesByMachineID map[string][]types.BackendRoute
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

func (r *fakeContainerRepo) DeleteBackendRoutesByMachine(ctx context.Context, workspaceID, poolName, machineID string) error {
	r.deletedRoutes = append(r.deletedRoutes, workspaceID+"/"+poolName+"/"+machineID)
	return nil
}

func (r *fakeContainerRepo) ListBackendRoutesByMachine(ctx context.Context, workspaceID, poolName, machineID string) ([]types.BackendRoute, error) {
	return append([]types.BackendRoute(nil), r.routesByMachine[workspaceID+"/"+poolName+"/"+machineID]...), nil
}

func (r *fakeContainerRepo) ListBackendRoutesByMachineID(ctx context.Context, machineID string) ([]types.BackendRoute, error) {
	return append([]types.BackendRoute(nil), r.routesByMachineID[machineID]...), nil
}

func TestAgentRoutesUseMachineIndex(t *testing.T) {
	tenantRoute := types.BackendRoute{
		RouteID:     "route-tenant",
		WorkspaceID: "tenant-1",
		PoolName:    "provider-pool-1",
		MachineID:   "machine-1",
		Kind:        types.BackendRouteKindContainer,
		State:       types.BackendRouteStateOpening,
	}
	repo := &fakeContainerRepo{
		routesByMachineID: map[string][]types.BackendRoute{
			"machine-1": {
				tenantRoute,
				{RouteID: "other-pool", WorkspaceID: "tenant-1", PoolName: "other-pool", MachineID: "machine-1"},
				{RouteID: "other-machine", WorkspaceID: "tenant-1", PoolName: "provider-pool-1", MachineID: "machine-2"},
			},
		},
	}
	service := &Service{containerRepo: repo}

	routes, err := service.agentRoutesForMachine(context.Background(), &model.AgentTokenState{
		WorkspaceID: "provider-1",
		PoolName:    "provider-pool-1",
		MachineID:   "machine-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 || routes[0].RouteId != tenantRoute.RouteID {
		t.Fatalf("routes = %#v, want only route for the agent's machine and pool", routes)
	}
}

func TestAgentCanManageWorkloadRouteOnOwnMachine(t *testing.T) {
	agentState := &model.AgentTokenState{
		WorkspaceID: "provider-1",
		PoolName:    "provider-pool-1",
		MachineID:   "machine-1",
	}
	tenantRoute := types.BackendRoute{WorkspaceID: "tenant-1", PoolName: "provider-pool-1", MachineID: "machine-1"}
	if !agentCanManageRoute(agentState, tenantRoute) {
		t.Fatal("agent could not manage workload route on its own machine")
	}
	for _, route := range []types.BackendRoute{
		{WorkspaceID: "tenant-1", PoolName: "other-pool", MachineID: "machine-1"},
		{WorkspaceID: "tenant-1", PoolName: "provider-pool-1", MachineID: "machine-2"},
	} {
		if agentCanManageRoute(agentState, route) {
			t.Fatalf("agent managed out-of-scope route: %#v", route)
		}
	}
}

func TestListMachineContainersReturnsActiveContainers(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"workspace-1": {
				{
					WorkspaceID:      "workspace-1",
					Name:             "provider-pool-1",
					Mode:             string(types.PoolModeProvider),
					Config:           &pb.PoolConfig{Name: "provider-pool-1"},
					CreatedByTokenID: "owner-token",
				},
			},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("workspace-1", "provider-pool-1"): {
				{
					WorkspaceID: "workspace-1",
					PoolName:    "provider-pool-1",
					MachineID:   "machine-1",
					Schedulable: true,
				},
			},
		},
	}
	containerRepo := &fakeContainerRepo{
		containers: []types.ContainerState{
			{
				ContainerId: "container-1",
				StubId:      "stub-1",
				Status:      types.ContainerStatusRunning,
				WorkspaceId: "tenant-1",
				Gpu:         "RTX5090",
				GpuCount:    1,
				Cpu:         1000,
				Memory:      1024,
				ScheduledAt: 100,
			},
			{
				ContainerId: "container-2",
				StubId:      "stub-2",
				Status:      types.ContainerStatusPending,
				WorkspaceId: "tenant-2",
				ScheduledAt: 200,
			},
		},
	}
	service := &Service{computeRepo: repo, containerRepo: containerRepo}

	res, err := service.ListMachineContainers(ctx, &pb.ListMachineContainersRequest{
		PoolName:  "provider-pool-1",
		MachineId: "machine-1",
	})
	if err != nil {
		t.Fatalf("ListMachineContainers() error = %v", err)
	}
	if !res.Ok {
		t.Fatalf("ListMachineContainers() not ok: %s", res.ErrMsg)
	}
	if len(res.Containers) != 2 {
		t.Fatalf("container count = %d, want 2", len(res.Containers))
	}
	// Most recently scheduled first.
	if res.Containers[0].ContainerId != "container-2" || res.Containers[1].ContainerId != "container-1" {
		t.Fatalf("container order = %s, %s; want container-2 first", res.Containers[0].ContainerId, res.Containers[1].ContainerId)
	}
	if res.Containers[1].WorkspaceId != "tenant-1" || res.Containers[1].Gpu != "RTX5090" {
		t.Fatalf("container fields = %+v, want tenant workspace and gpu mapped", res.Containers[1])
	}

	missing, err := service.ListMachineContainers(ctx, &pb.ListMachineContainersRequest{
		PoolName:  "provider-pool-1",
		MachineId: "machine-unknown",
	})
	if err != nil {
		t.Fatalf("ListMachineContainers() error = %v", err)
	}
	if missing.Ok {
		t.Fatal("ListMachineContainers() should reject unknown machines")
	}
}

func TestListMachineContainersClusterAdminPrefersManagedPool(t *testing.T) {
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: "selected-workspace"},
		Token: &types.Token{
			Active:    true,
			TokenType: types.TokenTypeClusterAdmin,
		},
	})
	computeRepo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"selected-workspace": {{Name: "shared-name"}},
		},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("admin-workspace", "shared-name"): {{
				WorkspaceID: "admin-workspace",
				PoolName:    "shared-name",
				MachineID:   "managed-machine",
			}},
		},
	}
	managedRepo := &fakeComputeRepo{pools: map[string][]*model.PoolState{
		"admin-workspace": {{Name: "shared-name"}},
	}}
	service := &Service{
		backendRepo:     &fakeManagedPoolBackendRepo{},
		computeRepo:     computeRepo,
		managedPoolRepo: &fakeManagedPoolRepo{repo: managedRepo},
	}

	response, err := service.ListMachineContainers(ctx, &pb.ListMachineContainersRequest{
		PoolName:  "shared-name",
		MachineId: "managed-machine",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !response.Ok {
		t.Fatalf("managed pool was shadowed by selected workspace: %s", response.ErrMsg)
	}
}

func TestListMachineContainersReturnsManagedPoolStoreError(t *testing.T) {
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: "selected-workspace"},
		Token: &types.Token{
			Active:    true,
			TokenType: types.TokenTypeClusterAdmin,
		},
	})
	service := &Service{
		backendRepo: &fakeManagedPoolBackendRepo{},
		computeRepo: &fakeComputeRepo{pools: map[string][]*model.PoolState{
			"selected-workspace": {{Name: "shared-name"}},
		}},
		managedPoolRepo: &fakeManagedPoolRepo{
			repo:   &fakeComputeRepo{},
			getErr: errors.New("managed pool store unavailable"),
		},
	}

	response, err := service.ListMachineContainers(ctx, &pb.ListMachineContainersRequest{
		PoolName:  "shared-name",
		MachineId: "machine-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Ok || response.ErrMsg != "managed pool store unavailable" {
		t.Fatalf("response = %+v, want managed pool store error", response)
	}
}

// standIn puts a value in a package variable, such as a call to AWS, and puts
// back what was there when the test ends.
func standIn[T any](t *testing.T, variable *T, value T) {
	t.Helper()
	previous := *variable
	*variable = value
	t.Cleanup(func() { *variable = previous })
}

// faultyPools is a repository in which reading or saving a pool's record, or
// saving a join token, fails with the error set for it.
//
// It holds a pool's lock the way the Redis repository does: what the work
// done under the lock returns comes back joined with the lock's own errors,
// of which a healthy lock has none.
type faultyPools struct {
	*fakeComputeRepo
	getErr, saveErr, tokenErr error
}

func (r *faultyPools) WithPoolStateLock(ctx context.Context, workspaceID, name string, fn func(context.Context) error) error {
	var leaseErr, releaseErr error
	return errors.Join(fn(ctx), leaseErr, releaseErr)
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

// faultyProvider is a provider whose offers cannot be listed, that cannot be
// asked about a machine, or that cannot be told how long a machine is wanted,
// while the error for that call is set. It counts how often it is told how
// long a machine is wanted. Creates and deletes fail when the fake it wraps
// is told to fail them.
type faultyProvider struct {
	*providers.Fake
	offersErr, getErr, extendErr error
	asked, extends               int
}

func (p *faultyProvider) Offers(ctx context.Context, filter providers.OfferFilter) ([]providers.Offer, error) {
	p.asked++
	if p.offersErr != nil {
		return nil, p.offersErr
	}
	return p.Fake.Offers(ctx, filter)
}

func (p *faultyProvider) Get(ctx context.Context, id string) (providers.Instance, error) {
	if p.getErr != nil {
		return providers.Instance{}, p.getErr
	}
	return p.Fake.Get(ctx, id)
}

func (p *faultyProvider) Extend(ctx context.Context, id string, until time.Time) error {
	p.extends++
	if p.extendErr != nil {
		return p.extendErr
	}
	return p.Fake.Extend(ctx, id, until)
}

// takeSelector gives the service a scheduler in which another pool of the
// workspace answers to the selector already.
func takeSelector(t *testing.T, service *Service, selector string) {
	t.Helper()
	service.scheduler = scheduler.NewSchedulerForCapacityChecks(nil, service.computeRepo, scheduler.NewWorkerPoolManager())
	require.NoError(t, service.scheduler.EnsureAgentPool("workspace-1", &model.PoolState{
		Name: "selector-owner", Selector: selector, Mode: string(types.PoolModePrivate),
	}))
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

// boughtMachine is a pool with one machine bought from a provider, and what a
// test needs to watch the service look after it.
type boughtMachine struct {
	service  *Service
	provider *faultyProvider
	repo     *fakeComputeRepo
	events   *capturedEvents
	state    *model.PoolState
	instance string
	// now is when the fixture was made, moved on by every check. expiry is
	// when the machine's term ended as the fixture was made.
	now, expiry time.Time
}

const boughtTokenHash = "join-token-hash"

// boughtMachineAt is the pool at the given time. Its machine was bought an
// hour before, is in the given state at its provider, and is wanted for the
// given time more.
func boughtMachineAt(now time.Time, status providers.Status, wantedFor time.Duration) *boughtMachine {
	provider := &faultyProvider{Fake: fakeProvider("shadeform")}
	instance := provider.Plant(testOwner, status, nil)
	recorded := model.ReservationPending
	if status == providers.StatusActive {
		recorded = model.ReservationActive
	}
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Mode:        string(types.PoolModePrivate),
		Reservations: []model.Reservation{{
			ID:                    instance,
			InstanceID:            instance,
			Provider:              provider.Name(),
			Name:                  "beam-pool-1-machine-1",
			MachineID:             "machine-1",
			Source:                model.SourceCLIReservation,
			Status:                recorded,
			CreatedAt:             now.Add(-time.Hour),
			ExpiresAt:             now.Add(wantedFor),
			HourlyCostMicros:      1_000_000,
			RegistrationTokenHash: boughtTokenHash,
		}},
	}
	repo := &fakeComputeRepo{
		pools:      map[string][]*model.PoolState{"workspace-1": {state}},
		joinTokens: map[string]*model.JoinTokenState{boughtTokenHash: {TokenHash: boughtTokenHash, ExpiresAt: now.Add(time.Hour)}},
	}
	events := &capturedEvents{}
	return &boughtMachine{
		// This fixture deliberately dates purchases an hour back for billing
		// assertions. Tests of the initial provider boot deadline set their
		// own shorter timeout and purchase timestamp.
		service: &Service{computeRepo: repo, eventRepo: events, providers: providers.RegistryOf(provider),
			appConfig: types.AppConfig{ManagedCompute: types.ManagedComputeConfig{
				Reservations: types.ManagedComputeReservationsConfig{BootTimeout: types.Duration(24 * time.Hour)},
			}}},
		provider: provider,
		repo:     repo,
		events:   events,
		state:    state,
		instance: instance,
		now:      now,
		expiry:   now.Add(wantedFor),
	}
}

// newBoughtMachine has a machine that is still booting and is wanted for six
// hours.
func newBoughtMachine(t *testing.T) *boughtMachine {
	t.Helper()
	return boughtMachineAt(time.Now().UTC(), providers.StatusPending, 6*time.Hour)
}

// newRenewedMachine has a running machine whose term is about to end, in a
// workspace that can pay for more.
func newRenewedMachine(t *testing.T) *boughtMachine {
	t.Helper()
	m := boughtMachineAt(time.Now().UTC().Truncate(time.Second), providers.StatusActive, 90*time.Second)
	m.state.Reservations[0].BillingCursorAt = m.now.Add(-time.Minute)
	m.service.billing = &fakeManagedBilling{balanceDecision: healthyBalance}
	return m
}

// newExtendedPool has a running machine that is wanted for another six hours,
// as its pool is.
func newExtendedPool(t *testing.T) *boughtMachine {
	t.Helper()
	p := boughtMachineAt(time.Now().UTC(), providers.StatusActive, 6*time.Hour)
	p.state.Config = &pb.PoolConfig{Name: "pool-1", Ttl: "6h", MaxSpend: 10}
	p.state.ExpiresAt = p.expiry
	p.state.Reservations[0].CommittedMicros = 7_000_000
	return p
}

// check runs the reconciler over the pool once, after the given time has
// passed, and returns the reservation as it was saved.
func (m *boughtMachine) check(t *testing.T, after time.Duration) model.Reservation {
	t.Helper()
	m.now = m.now.Add(after)
	m.repo.savedPool = false
	require.NoError(t, m.service.reconcilePool(context.Background(), "workspace-1", m.state, m.now))
	return m.repo.pools["workspace-1"][0].Reservations[0]
}

// extend asks for the pool to be extended, as its owner does.
func (m *boughtMachine) extend(t *testing.T, in *pb.ExtendPoolCapacityRequest) *pb.ExtendPoolCapacityResponse {
	t.Helper()
	m.repo.savedPool = false
	res, err := m.service.ExtendPoolCapacity(testAuthContext("workspace-1", "owner-token"), in)
	require.NoError(t, err)
	return res
}

func (m *boughtMachine) tokenRevoked() bool {
	return m.repo.joinTokens[boughtTokenHash].Revoked
}

// reported returns the status of every event of the given kind, in order, and
// forgets the events it has seen.
func reported(events *capturedEvents, action string) []string {
	statuses := []string{}
	for _, event := range events.events {
		if event.Action == action {
			statuses = append(statuses, event.Status)
		}
	}
	events.events = nil
	return statuses
}

// byocTokenHash is the hash of the token the stack of byocPool joins with.
var byocTokenHash = hashComputeToken("byoc-token")

// byocPool is a pool whose machines run in the tenant's own AWS account. The
// stack it was set up with gave Beam a role to ask about it with.
func byocPool(now time.Time) *model.PoolState {
	stackURL := awsCloudFormationStackURL("us-east-1", "beam-aws-cpu-test")
	return &model.PoolState{
		Name:             "aws-cpu",
		Selector:         "aws-cpu",
		Mode:             string(types.PoolModePrivate),
		Status:           types.ComputePoolStatusActive,
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
				byocBootstrapJoinTokenHashLabel:   byocTokenHash,
				byocBootstrapJoinTokenHashesLabel: byocTokenHash,
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

// silentBYOCPool is a BYOC pool whose only machine was last heard from the
// given time ago, with the token its stack joins with.
func silentBYOCPool(silentFor time.Duration) *fakeComputeRepo {
	now := time.Now().UTC()
	return &fakeComputeRepo{
		pools: map[string][]*model.PoolState{"workspace-1": {byocPool(now)}},
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
			byocTokenHash: {TokenHash: byocTokenHash, WorkspaceID: "workspace-1", PoolName: "aws-cpu"},
		},
	}
}

// byocTemplateURL is where byocService keeps the template of a tenant's
// stack.
const byocTemplateURL = "https://s3.us-east-1.amazonaws.com/beam-byoc-templates-test/aws/byoc-template.yaml"

// byocService is a service that can set up pools in a tenant's AWS account,
// and look after the machines that join them.
func byocService(repo repository.ComputeRepository) *Service {
	return &Service{
		computeRepo:   repo,
		containerRepo: &fakeContainerRepo{},
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				HTTP: types.HTTPConfig{ExternalHost: "app.beam.cloud", ExternalPort: 443, TLS: true},
			},
			ManagedCompute: types.ManagedComputeConfig{BYOC: types.ManagedComputeBYOCConfig{
				AWS: types.ManagedComputeBYOCAWSConfig{TemplateURL: byocTemplateURL},
			}},
		},
	}
}

// anAWSCPUPool is a request for a pool of CPU machines on AWS, which a test
// may change before it sends it.
func anAWSCPUPool() *pb.CreateBYOCPoolRequest {
	return &pb.CreateBYOCPoolRequest{
		Provider:     "aws",
		PoolName:     "aws-cpu",
		Region:       "us-east-1",
		InstanceType: "i4i.xlarge",
		DesiredNodes: 1,
		AccountId:    "123456789012",
	}
}

// stackCheckAnswers makes the check of a pool's AWS stack answer as given,
// and returns how often it was asked.
func stackCheckAnswers(t *testing.T, deleted bool, err error) *int {
	t.Helper()
	asked := 0
	standIn(t, &awsBYOCResourceDeleted, func(context.Context, awsBYOCResourceDeletedInput) (bool, error) {
		asked++
		return deleted, err
	})
	return &asked
}

// buyingTestService is a gateway that has credentials for one provider,
// verda.
func buyingTestService(config types.AppConfig) (*Service, *fakeComputeRepo) {
	repo := &fakeComputeRepo{}
	service := managedPoolTestService(config, repo)
	service.providers = providers.RegistryOf(providers.NewFake("verda", "test"))
	return service, repo
}

// spotPool is the least that makes a pool buy its machines.
func spotPool(change ...func(*types.WorkerPoolConfig)) types.WorkerPoolConfig {
	pool := types.WorkerPoolConfig{
		GPUType:      "L40S",
		Provisioning: types.WorkerPoolProvisioningConfig{Providers: []string{"verda"}, Spot: true, MaxNodes: 4},
	}
	for _, apply := range change {
		apply(&pool)
	}
	return pool
}

var joinTokenInScript = regexp.MustCompile(`--join-token '?([^' ]+)'?`)

// joinAs runs what the machine's startup script would: a machine with one
// L40S joins with the token in the script.
func joinAs(t *testing.T, service *Service, script, hostname string) *pb.JoinAgentResponse {
	t.Helper()
	token := joinTokenInScript.FindStringSubmatch(script)
	require.NotNil(t, token, "no join token in %q", script)
	response, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken:          token[1],
		MachineFingerprint: "fingerprint-" + hostname,
		Hostname:           hostname,
		CpuCount:           20,
		MemoryMb:           60_000,
		Gpu:                []string{"L40S"},
		GpuCount:           1,
		Schedulable:        true,
	})
	require.NoError(t, err)
	return response
}

// joinedByHand makes a pool of L40S machines, l40s, and joins one to it. It
// returns the command the machine joined with, and the machine as it joined.
func joinedByHand(t *testing.T, service *Service) (script string, machine model.AgentTokenState) {
	t.Helper()
	ctx := context.Background()
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "l40s", types.WorkerPoolConfig{GPUType: "L40S"})
	require.NoError(t, err)
	bootstrap, err := service.CreateManagedMachine(ctx, clusterAdminAuth(), "l40s")
	require.NoError(t, err)
	response := joinAs(t, service, bootstrap.InstallCommand, "by-hand")
	require.True(t, response.Ok, response.ErrMsg)

	joined, err := service.computeRepo.GetAgentMachineState(ctx, "admin-workspace", "l40s", response.MachineId)
	require.NoError(t, err)
	require.NotNil(t, joined)
	return bootstrap.InstallCommand, *joined
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
			service := launchService(repo, shadeform)
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
	sshWithoutAKey := func(service *Service) { service.appConfig.ManagedCompute.SSH.Enabled = true }
	selectorTaken := func(service *Service) { takeSelector(t, service, "pool-1") }
	for name, tt := range map[string]struct {
		arrange  func(*Service)
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
			service := launchService(repo, shadeform)
			if tt.arrange != nil {
				tt.arrange(service)
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
			require.Empty(t, shadeform.Everything())
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

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), launchService(repo, shadeform), 1)
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

	require.Len(t, reported(events, types.EventComputeActionPoolReserved), 1, "the launch is reported once")
}

func TestListPoolOffersSkipsAProviderThatCannotBeAsked(t *testing.T) {
	shadeform := &faultyProvider{Fake: fakeProvider("shadeform"), offersErr: errors.New("shadeform is down")}
	service := &Service{providers: providers.RegistryOf(shadeform, fakeProvider("hetzner", cpx31Offer))}

	res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{}})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Len(t, res.Offers, 1, "what the other providers sell is still listed")
	require.Equal(t, "hetzner", res.Offers[0].Provider)
}

func TestLaunchPoolCapacityBuysFromAProviderThatAnswersWhenAnotherDoesNot(t *testing.T) {
	shadeform := &faultyProvider{Fake: fakeProvider("shadeform"), offersErr: errors.New("shadeform is down")}
	hetzner := fakeProvider("hetzner", cpx31Offer)
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform, hetzner)

	res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Nodes: 1,
		Pool:  &pb.PoolConfig{Name: "cpu-pool", Ttl: "1h", MaxSpend: 1},
	})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Len(t, hetzner.Creates(), 1)
}

func TestListPoolOffersFailsWhenNoProviderAnswers(t *testing.T) {
	hetzner := &faultyProvider{Fake: fakeProvider("hetzner", cpx31Offer)}
	service := &Service{providers: providers.RegistryOf(
		hetzner,
		&faultyProvider{Fake: fakeProvider("shadeform"), offersErr: errors.New("shadeform is down")},
	)}

	// The pool names the one that is down, so nobody is left to answer.
	res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{
		Pool: &pb.PoolConfig{Providers: []string{"shadeform"}},
	})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Contains(t, res.ErrMsg, "shadeform: shadeform is down")
	require.Zero(t, hetzner.asked)

	hetzner.offersErr = errors.New("hetzner token expired")
	res, err = service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{}})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Contains(t, res.ErrMsg, "hetzner: hetzner token expired", "every failure is named")
	require.Contains(t, res.ErrMsg, "shadeform: shadeform is down")
}

// Every provider that is configured sells reservations, and the cheapest of
// what they all sell is what is bought.
func TestEveryConfiguredProviderSellsReservations(t *testing.T) {
	cheaper := a10gOffer
	cheaper.HourlyCostMicros = a10gOffer.HourlyCostMicros / 2
	verda := &faultyProvider{Fake: fakeProvider("verda", cheaper)}
	shadeform := fakeProvider("shadeform", a10gOffer)
	repo := &fakeComputeRepo{}
	service := launchService(repo, verda, shadeform)

	res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{}})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	listed := []string{}
	for _, offer := range res.Offers {
		listed = append(listed, offer.Provider)
	}
	require.Equal(t, []string{"verda", "shadeform"}, listed, "cheapest first")

	res, err = service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{Providers: []string{"shadeform"}}})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Len(t, res.Offers, 1, "a pool that names providers is shown theirs alone")
	require.Equal(t, "shadeform", res.Offers[0].Provider)

	launched, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 1, Ttl: "1h", MaxSpend: 2},
	})
	require.NoError(t, err)
	require.True(t, launched.Ok, launched.ErrMsg)
	require.Len(t, verda.Creates(), 1)
	require.Empty(t, shadeform.Creates())
}

// The providers are asked at the same time, and what they answer is put
// together in the same order whichever of them answers first.
func TestProvidersAreAskedWhatTheySellAtTheSameTime(t *testing.T) {
	release := make(chan struct{})
	asked := make(chan string, 2)
	slow := func(name string, offer providers.Offer) *waitingProvider {
		return &waitingProvider{Fake: fakeProvider(name, offer), asked: asked, release: release}
	}
	service := &Service{providers: providers.RegistryOf(slow("aws", a10gOffer), slow("gcp", a10gOffer))}

	done := make(chan []providers.Offer, 1)
	go func() {
		offers, err := service.collectPoolOffers(context.Background(), model.Pool{}, nil)
		require.NoError(t, err)
		done <- offers
	}()

	// Both are asked before either has answered.
	waiting := []string{}
	for range 2 {
		select {
		case name := <-asked:
			waiting = append(waiting, name)
		case <-time.After(5 * time.Second):
			t.Fatalf("only %v were asked while no provider had answered", waiting)
		}
	}
	require.ElementsMatch(t, []string{"aws", "gcp"}, waiting)
	close(release)

	offers := <-done
	require.Len(t, offers, 2)
	require.Equal(t, []string{"aws", "gcp"}, []string{offers[0].Provider, offers[1].Provider})
}

// waitingProvider says that it was asked what it sells, and answers when it
// is released.
type waitingProvider struct {
	*providers.Fake
	asked   chan<- string
	release <-chan struct{}
}

func (p *waitingProvider) Offers(ctx context.Context, filter providers.OfferFilter) ([]providers.Offer, error) {
	p.asked <- p.Name()
	<-p.release
	return p.Fake.Offers(ctx, filter)
}

// A provider whose client panics has not answered. The others' offers are
// still listed.
func TestAProviderThatPanicsDoesNotHideWhatTheOthersSell(t *testing.T) {
	service := &Service{providers: providers.RegistryOf(
		&panickingProvider{Fake: fakeProvider("aws")},
		fakeProvider("shadeform", a10gOffer),
	)}

	res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{}})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Len(t, res.Offers, 1)
	require.Equal(t, "shadeform", res.Offers[0].Provider)

	res, err = service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{Providers: []string{"aws"}}})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Contains(t, res.ErrMsg, "aws: listing offers panicked: catalogue has no prices")
}

type panickingProvider struct{ *providers.Fake }

func (p *panickingProvider) Offers(context.Context, providers.OfferFilter) ([]providers.Offer, error) {
	panic("catalogue has no prices")
}

// The config can name the providers that sell reservations. The providers
// that pools buy their own machines from are configured in the same place,
// and what one of those sells is then not for a tenant to reserve, however
// little it costs, and it is not asked.
func TestOnlyProvidersTheConfigNamesSellReservations(t *testing.T) {
	cheaper := a10gOffer
	cheaper.HourlyCostMicros = a10gOffer.HourlyCostMicros / 2
	verda := &faultyProvider{Fake: fakeProvider("verda", cheaper)}
	shadeform := fakeProvider("shadeform", a10gOffer)
	repo := &fakeComputeRepo{}
	service := launchService(repo, verda, shadeform)
	service.appConfig.ManagedCompute.Reservations.Providers = []string{"hetzner", "shadeform"}

	for _, named := range [][]string{nil, {"verda", "shadeform"}} {
		res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{Providers: named}})
		require.NoError(t, err)
		require.True(t, res.Ok, res.ErrMsg)
		require.Len(t, res.Offers, 1)
		require.Equal(t, "shadeform", res.Offers[0].Provider)
	}
	res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{Providers: []string{"verda"}}})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Empty(t, res.Offers)

	launched, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Nodes: 1, Ttl: "1h", MaxSpend: 2},
	})
	require.NoError(t, err)
	require.True(t, launched.Ok, launched.ErrMsg)
	require.Len(t, shadeform.Creates(), 1)
	require.Empty(t, verda.Creates())
	require.Zero(t, verda.asked)

	// With nobody to sell a reservation there is nothing to list.
	service.providers = providers.RegistryOf(verda)
	res, err = service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{}})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Equal(t, "no compute providers are configured", res.ErrMsg)
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
// calls over a network do. It counts how often it is asked what it has, and
// cannot be asked while listErr is set.
type overTheWire struct {
	*providers.Fake
	// afterCreate, when set, runs once after the next create.
	afterCreate func()
	lists       int
	listErr     error
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
	if err := cmp.Or(ctx.Err(), p.listErr); err != nil {
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
	require.Empty(t, shadeform.Everything(), "the machine that was bought is deleted although the caller has gone")
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
	bought := shadeform.Everything()
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
	require.Empty(t, shadeform.Everything(), "the reconciler deleted the machine")
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
	require.Empty(t, shadeform.Everything())
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

	bought := shadeform.Everything()
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
	// The provider answers, and turns the purchase down for a reason this
	// service does not know.
	shadeform := &overTheWire{Fake: fakeProvider("shadeform", a10gOffer)}
	shadeform.RefuseCreates(&providers.StatusError{Method: "POST", Path: "/instances/create", Status: 400, Body: "no such region"})
	repo := &fakeComputeRepo{}

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), launchService(repo, shadeform), 1)
	require.False(t, res.Ok)
	require.Equal(t, 1, shadeform.lists)
	require.Empty(t, shadeform.Deletes(), "the provider made nothing")
	require.Empty(t, repo.pools["workspace-1"])
}

// unansweredPurchase is a launch that failed because its provider gave the
// purchase no answer, and listed no machine for it when the launch was undone.
type unansweredPurchase struct {
	service  *Service
	provider *overTheWire
	repo     *fakeComputeRepo
}

func launchUnanswered(t *testing.T) *unansweredPurchase {
	t.Helper()
	var ended time.Time
	provider := &overTheWire{Fake: fakeProvider("shadeform", a10gOffer)}
	provider.RefuseCreates(&providers.StatusError{Method: "POST", Path: "/instances/create", Status: 504, Body: "upstream timed out"})
	provider.afterCreate = func() {
		// The purchase took a while to end.
		time.Sleep(time.Millisecond)
		ended = time.Now()
	}
	p := &unansweredPurchase{provider: provider, repo: &fakeComputeRepo{}}
	p.service = launchService(p.repo, provider)

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), p.service, 1)
	require.False(t, res.Ok)
	require.Equal(t, launchErrorProviderFailure, res.ErrorCode)
	require.Contains(t, res.ErrMsg, "does not list it yet")

	// The pool's record is the only trace of the purchase.
	require.Len(t, p.repo.pools["workspace-1"], 1)
	state := p.repo.pools["workspace-1"][0]
	require.Len(t, state.Reservations, 1)
	require.Zero(t, state.ReservedNodes)
	require.Zero(t, state.CommittedSpendMicros)
	reservation := p.reservation()
	require.Empty(t, computeReservationInstanceID(reservation))
	require.NotEmpty(t, reservation.MachineID)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Equal(t, reconcileReasonLaunchFailed, reservation.TerminatingReason)
	require.Contains(t, reservation.LastError, "does not list it yet")
	require.False(t, reservation.CreatedAt.Before(ended), "the provider's time to list the machine begins when the purchase ends")
	requireEveryTokenRevoked(t, p.repo)
	return p
}

func (p *unansweredPurchase) reservation() model.Reservation {
	return p.repo.pools["workspace-1"][0].Reservations[0]
}

// reconcile runs one pass at the given time after the purchase.
func (p *unansweredPurchase) reconcile(t *testing.T, after time.Duration) model.Reservation {
	t.Helper()
	require.NoError(t, p.service.reconcileManagedComputeAt(context.Background(), p.reservation().CreatedAt.Add(after)))
	return p.reservation()
}

func (p *unansweredPurchase) deletePool(t *testing.T) *pb.DeletePoolResponse {
	t.Helper()
	res, err := p.service.DeletePool(testAuthContext("workspace-1", "token-1"), &pb.DeletePoolRequest{Name: "pool-1"})
	require.NoError(t, err)
	return res
}

func TestAPurchaseThatWentUnansweredIsDeletedOnceItsProviderListsIt(t *testing.T) {
	p := launchUnanswered(t)

	// The provider had made the machine all the same, and comes to list it.
	made := p.provider.Plant(testOwner, providers.StatusPending, map[string]string{machineTag: p.reservation().MachineID})
	// Another machine of this control plane is not taken for it.
	other := p.provider.Plant(testOwner, providers.StatusActive, map[string]string{"beam-pool-machine": "spot-1"})

	reservation := p.reconcile(t, time.Minute)
	require.Equal(t, model.ReservationDeleted, reservation.Status)
	require.Equal(t, made, reservation.InstanceID, "the record names the machine that was deleted")
	require.Empty(t, reservation.LastError)
	require.Equal(t, []string{made}, p.provider.Deletes())
	require.Len(t, p.provider.Everything(), 1)
	require.Equal(t, other, p.provider.Everything()[0].ID)
	require.True(t, p.deletePool(t).Ok)
}

func TestAPurchaseThatWentUnansweredIsGivenUpOnceItsProviderHasHadTimeToListIt(t *testing.T) {
	p := launchUnanswered(t)

	// The provider may still be making the machine.
	require.Equal(t, model.ReservationTerminating, p.reconcile(t, time.Minute).Status)
	require.Empty(t, p.provider.Deletes())

	// Nor can the pool be deleted from under the purchase.
	res := p.deletePool(t)
	require.False(t, res.Ok)
	require.Contains(t, res.ErrMsg, "does not list it yet")
	require.Equal(t, model.ReservationTerminating, p.reservation().Status)

	// It made none: there is nothing to delete.
	reservation := p.reconcile(t, providers.LaunchGrace)
	require.Equal(t, model.ReservationDeleted, reservation.Status)
	require.Empty(t, reservation.LastError)
	require.Empty(t, p.provider.Deletes())
	require.True(t, p.deletePool(t).Ok)
	require.Empty(t, p.repo.pools["workspace-1"])
}

func TestAPurchaseThatWentUnansweredDoesNotStandInTheWayOfTheNextLaunch(t *testing.T) {
	p := launchUnanswered(t)
	unanswered := p.reservation()

	// The provider sells again.
	p.provider.RefuseCreates(nil)
	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), p.service, 1)
	require.True(t, res.Ok, res.ErrMsg)
	state := p.repo.pools["workspace-1"][0]
	require.Len(t, state.Reservations, 2)
	require.EqualValues(t, 1, state.ReservedNodes)

	// The machine that was bought is not taken for the one that was not.
	require.NoError(t, p.service.reconcileManagedComputeAt(context.Background(), unanswered.CreatedAt.Add(providers.LaunchGrace)))
	require.Empty(t, p.provider.Deletes())
	require.Len(t, p.provider.Everything(), 1)
	for _, reservation := range p.repo.pools["workspace-1"][0].Reservations {
		if reservation.MachineID == unanswered.MachineID {
			require.Equal(t, model.ReservationDeleted, reservation.Status)
			require.Empty(t, computeReservationInstanceID(reservation))
			continue
		}
		require.True(t, reservation.Open(), "the machine of the second launch is kept: %s", reservation.Status)
		require.Equal(t, p.provider.Everything()[0].ID, computeReservationInstanceID(reservation))
	}
}

func TestAReservationThatNamesNoMachineDeletesNothing(t *testing.T) {
	// A machine bought for a pool of the platform carries no tenant's tag.
	shadeform := fakeProvider("shadeform", a10gOffer)
	shadeform.Plant(testOwner, providers.StatusActive, map[string]string{"beam-pool-machine": "spot-1"})
	service := launchService(&fakeComputeRepo{}, shadeform)

	reservation := model.Reservation{Provider: "shadeform", Source: model.SourceCLIReservation}
	require.NoError(t, service.deleteInstance(context.Background(), &reservation, time.Now()))
	require.Empty(t, shadeform.Deletes())
	require.Empty(t, computeReservationInstanceID(reservation))
}

func TestAFailedLaunchKeepsAPurchaseItCouldNotLookFor(t *testing.T) {
	// The provider turns the purchase down, and then cannot be asked what
	// it has.
	shadeform := &overTheWire{Fake: fakeProvider("shadeform", a10gOffer), listErr: errors.New("shadeform is down")}
	shadeform.RefuseCreates(&providers.StatusError{Method: "POST", Path: "/instances/create", Status: 400, Body: "no such region"})
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform)

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), service, 1)
	require.False(t, res.Ok)
	require.Contains(t, res.ErrMsg, "shadeform is down")
	require.Len(t, repo.pools["workspace-1"], 1)
	reservation := repo.pools["workspace-1"][0].Reservations[0]
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Contains(t, reservation.LastError, "shadeform is down")

	// Nothing is concluded while the provider stays down.
	require.NoError(t, service.reconcileManagedComputeAt(context.Background(), reservation.CreatedAt.Add(providers.LaunchGrace)))
	require.Equal(t, model.ReservationTerminating, repo.pools["workspace-1"][0].Reservations[0].Status)

	// It answers again, and lists no machine for the purchase.
	shadeform.listErr = nil
	require.NoError(t, service.reconcileManagedComputeAt(context.Background(), reservation.CreatedAt.Add(2*providers.LaunchGrace)))
	reservation = repo.pools["workspace-1"][0].Reservations[0]
	require.Equal(t, model.ReservationDeleted, reservation.Status)
	require.Empty(t, reservation.LastError)
	require.Empty(t, shadeform.Deletes())
}

func TestStatusCheckReportsAChangeOnlyWhenSomethingChanged(t *testing.T) {
	m := newBoughtMachine(t)
	const statusUpdated = types.EventComputeActionReservationStatusUpdated

	// Nothing has changed at the provider. The time of the check is saved, so
	// that the provider is not asked again by the next pass, and nothing is
	// reported.
	reservation := m.check(t, 0)
	require.Equal(t, model.ReservationPending, reservation.Status)
	require.Equal(t, m.now, reservation.LastStatusCheckAt)
	require.True(t, m.repo.savedPool)
	require.Empty(t, reported(m.events, statusUpdated))

	for range 3 {
		m.check(t, 2*reconcileStatusCheckInterval)
	}
	require.Empty(t, reported(m.events, statusUpdated), "a machine that is still booting is not news")

	// The machine comes up.
	m.provider.SetStatus(m.instance, providers.StatusActive, "")
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationActive, reservation.Status)
	require.Equal(t, []string{string(model.ReservationActive)}, reported(m.events, statusUpdated))

	m.check(t, 2*reconcileStatusCheckInterval)
	require.Empty(t, reported(m.events, statusUpdated), "a machine that is still running is not news")

	// The provider's wording, and the machine's address, are changes too.
	m.provider.SetStatus(m.instance, providers.StatusActive, "Running startup script")
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, "Running startup script", reservation.LastStatusMessage)
	require.Len(t, reported(m.events, statusUpdated), 1)

	m.provider.SetAddress(m.instance, "203.0.113.7")
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, "203.0.113.7", reservation.PublicIP)
	require.Len(t, reported(m.events, statusUpdated), 1)

	m.check(t, 2*reconcileStatusCheckInterval)
	require.Empty(t, reported(m.events, statusUpdated))
}

func TestStatusCheckAsksTheProviderAtMostOncePerInterval(t *testing.T) {
	m := newBoughtMachine(t)
	m.check(t, 0)

	m.provider.SetStatus(m.instance, providers.StatusActive, "")
	reservation := m.check(t, reconcileStatusCheckInterval/2)
	require.Equal(t, model.ReservationPending, reservation.Status, "checked too recently to ask again")

	reservation = m.check(t, reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationActive, reservation.Status)
}

func TestStatusCheckLeavesAMachineAloneWhenItsStatusIsUnknown(t *testing.T) {
	for _, was := range []model.ReservationStatus{model.ReservationPending, model.ReservationActive} {
		t.Run(string(was), func(t *testing.T) {
			m := newBoughtMachine(t)
			m.state.Reservations[0].Status = was
			m.provider.SetStatus(m.instance, providers.StatusUnknown, "resizing")

			reservation := m.check(t, 0)
			require.Equal(t, was, reservation.Status, "a status nobody understands says nothing about the machine")
			require.Empty(t, m.provider.Deletes())
			require.False(t, m.tokenRevoked())
		})
	}
}

func TestStatusCheckClosesAReservationWhoseMachineIsGone(t *testing.T) {
	for name, lose := range map[string]func(*boughtMachine){
		"the provider no longer knows it":  func(m *boughtMachine) { m.provider.Forget(m.instance) },
		"the provider says it was deleted": func(m *boughtMachine) { m.provider.SetStatus(m.instance, providers.StatusDeleted, "") },
	} {
		t.Run(name, func(t *testing.T) {
			m := newBoughtMachine(t)
			m.state.Reservations[0].Status = model.ReservationActive
			m.state.ReservedNodes = 1
			lose(m)

			// The machine went as its term ran out, so none of the term was
			// lost with it.
			reservation := m.check(t, m.expiry.Sub(m.now))
			require.Equal(t, model.ReservationDeleted, reservation.Status)
			require.Equal(t, instanceGoneMessage, reservation.LastStatusMessage)
			require.Empty(t, reservation.LastError)
			require.True(t, m.tokenRevoked(), "nothing can join the pool as a machine that is gone")
			require.Equal(t, []string{string(model.ReservationDeleted)}, reported(m.events, types.EventComputeActionReservationStatusUpdated))
			require.Empty(t, m.provider.Deletes(), "there is nothing left to delete")
			require.Zero(t, m.repo.pools["workspace-1"][0].ReservedNodes, "a pool counts only the machines it still has")

			// It is closed: the provider is not asked about it again.
			m.check(t, 2*reconcileStatusCheckInterval)
			require.Empty(t, reported(m.events, types.EventComputeActionReservationStatusUpdated))
		})
	}
}

func TestStatusCheckListsARunningMachineThatWentBeforeItsTerm(t *testing.T) {
	for name, tc := range map[string]struct {
		spot    bool
		status  model.ReservationStatus
		reason  string
		message string
		action  string
	}{
		"on demand": {
			status: model.ReservationFailed, reason: reconcileReasonProviderFailed,
			message: instanceGoneMessage, action: types.EventComputeActionReservationStatusUpdated,
		},
		"spot": {
			spot: true, status: model.ReservationInterrupted, reason: reconcileReasonInterrupted,
			message: instanceInterruptedMessage, action: types.EventComputeActionReservationInterrupted,
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := newBoughtMachine(t)
			m.state.Reservations[0].Status = model.ReservationActive
			m.state.Reservations[0].Spot = tc.spot
			m.state.ReservedNodes = 1
			m.provider.SetStatus(m.instance, providers.StatusDeleted, "Instance deleted successfully.")

			reservation := m.check(t, time.Hour)
			require.Equal(t, tc.status, reservation.Status)
			require.Equal(t, tc.reason, reservation.TerminatingReason)
			require.Equal(t, tc.message, reservation.LastStatusMessage, "the provider's words say nothing of where the machine went")
			require.True(t, reservation.Dismissable(), "it is listed until its tenant dismisses it")
			require.True(t, m.tokenRevoked())
			require.Equal(t, []string{string(tc.status)}, reported(m.events, tc.action))
			require.Empty(t, m.provider.Deletes(), "there is nothing left to delete")
			require.Zero(t, m.repo.pools["workspace-1"][0].ReservedNodes)
		})
	}
}

func TestStatusCheckClosesAMachineThatNeverCameUpWithoutListingIt(t *testing.T) {
	m := newBoughtMachine(t)
	m.provider.SetStatus(m.instance, providers.StatusDeleted, "")

	reservation := m.check(t, time.Hour)
	require.Equal(t, model.ReservationDeleted, reservation.Status)
	require.Empty(t, reservation.TerminatingReason)
}

func TestReconcileCountsOnlyTheNodesOfOpenReservations(t *testing.T) {
	m := newBoughtMachine(t)
	// A record saved before closing a reservation recounted the pool's nodes.
	m.state.Reservations[0].Status = model.ReservationDeleted
	m.state.ReservedNodes = 1

	m.check(t, 0)
	require.Zero(t, m.repo.pools["workspace-1"][0].ReservedNodes)
	require.True(t, m.repo.savedPool, "the corrected count is saved")
}

func TestReleasingAMachineStopsCountingItAtOnce(t *testing.T) {
	m := boughtMachineAt(time.Now().UTC(), providers.StatusActive, 6*time.Hour)
	m.state.ReservedNodes = 1

	res, err := m.service.DeletePrivateMachine(
		testAuthContext("workspace-1", "owner-token"),
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: "machine-1"},
	)
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Equal(t, model.ReservationDeleted, m.state.Reservations[0].Status)
	require.Zero(t, m.repo.pools["workspace-1"][0].ReservedNodes, "the count does not wait for the reconciler")
}

func TestDeleteMachineFailureAsksToRetryWhenThePoolIsBusy(t *testing.T) {
	busy := deleteMachineFailed(fmt.Errorf("release machine: %w", errors.New("redislock: not obtained")))
	require.False(t, busy.Ok)
	require.Equal(t, "the pool is busy with another change; try again in a few seconds", busy.ErrMsg)

	other := deleteMachineFailed(errors.New("provider unavailable"))
	require.Equal(t, "provider unavailable", other.ErrMsg)
}

func TestStatusCheckWaitsForAProviderToKnowOfAMachineItHasJustSold(t *testing.T) {
	m := newBoughtMachine(t)
	m.state.Reservations[0].CreatedAt = m.now
	m.provider.Forget(m.instance)

	reservation := m.check(t, 0)
	require.Equal(t, model.ReservationPending, reservation.Status, "the machine may yet be there")
	require.Contains(t, reservation.LastError, "instance not found")
	require.False(t, m.tokenRevoked())

	reservation = m.check(t, providers.LaunchGrace-time.Second)
	require.Equal(t, model.ReservationPending, reservation.Status)

	reservation = m.check(t, reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationDeleted, reservation.Status, "a provider that still has no such machine never made it")
	require.Empty(t, reservation.LastError)
	require.True(t, m.tokenRevoked())
}

func TestStatusCheckKeepsAReservationWhenTheProviderCannotBeAsked(t *testing.T) {
	m := newBoughtMachine(t)
	m.state.Reservations[0].Status = model.ReservationActive
	m.service.providers = providers.RegistryOf()

	reservation := m.check(t, 0)
	require.Equal(t, model.ReservationActive, reservation.Status)
	require.Equal(t, `provider "shadeform" is not configured`, reservation.LastError)
	require.False(t, m.tokenRevoked())
}

func TestStatusCheckDeletesAMachineTheProviderFailedToStart(t *testing.T) {
	m := newBoughtMachine(t)
	m.provider.SetStatus(m.instance, providers.StatusFailed, "no capacity in the zone")

	reservation := m.check(t, 0)
	require.Equal(t, model.ReservationFailed, reservation.Status)
	require.Equal(t, reconcileReasonProviderFailed, reservation.TerminatingReason)
	require.Equal(t, "no capacity in the zone", reservation.LastStatusMessage)
	require.Empty(t, reservation.LastError)
	require.Equal(t, []string{m.instance}, m.provider.Deletes(), "a failed machine is not left at the provider")
	require.True(t, m.tokenRevoked())

	require.Empty(t, m.provider.Everything())
}

func TestStatusCheckDeletesWhatIsLeftOfAMachineTheProviderTookBack(t *testing.T) {
	m := newBoughtMachine(t)
	m.state.Reservations[0].Status = model.ReservationActive
	m.provider.SetStatus(m.instance, providers.StatusInterrupted, "")

	reservation := m.check(t, 0)
	require.Equal(t, model.ReservationFailed, reservation.Status, "a machine that was not bought as spot was not the provider's to take")
	require.Equal(t, reconcileReasonProviderFailed, reservation.TerminatingReason)
	require.Equal(t, instanceInterruptedMessage, reservation.LastStatusMessage)
	require.Equal(t, []string{m.instance}, m.provider.Deletes(), "a stopped machine or its disk is not left at the provider")
	require.True(t, m.tokenRevoked())
	require.Empty(t, reported(m.events, types.EventComputeActionReservationInterrupted))
}

// newSpotMachine has a running spot machine that is wanted for six hours, in
// a workspace that can pay for it.
func newSpotMachine(t *testing.T) (*boughtMachine, *fakeManagedBilling) {
	t.Helper()
	m := boughtMachineAt(time.Now().UTC().Truncate(time.Second), providers.StatusActive, 6*time.Hour)
	m.state.Reservations[0].Spot = true
	m.state.Reservations[0].BillingCursorAt = m.now
	billing := &fakeManagedBilling{balanceDecision: healthyBalance}
	m.service.billing = billing
	return m, billing
}

// A spot machine is sold on the terms that its provider may take it back.
// When it does, the reservation ends as interrupted and not as a failure,
// what is left of the machine is deleted, and the tenant is billed for the
// time the machine was theirs and no longer.
func TestASpotMachineItsProviderTakesBackEndsItsReservationAndItsBilling(t *testing.T) {
	m, billing := newSpotMachine(t)

	// While the machine runs, its time is billed.
	reservation := m.check(t, 10*time.Minute)
	require.Equal(t, model.ReservationActive, reservation.Status)
	require.Len(t, billing.usage, 1)
	require.Equal(t, 10*time.Minute, billing.usage[0].EndAt.Sub(billing.usage[0].StartAt))

	m.provider.SetStatus(m.instance, providers.StatusInterrupted, "")
	reservation = m.check(t, 10*time.Minute)
	require.Equal(t, model.ReservationInterrupted, reservation.Status)
	require.Equal(t, reconcileReasonInterrupted, reservation.TerminatingReason)
	require.Equal(t, instanceInterruptedMessage, reservation.LastStatusMessage)
	require.Empty(t, reservation.LastError)
	require.True(t, reservation.Closed())
	require.False(t, reservation.ActiveAt(m.now), "the machine no longer counts towards what the pool has")
	require.Equal(t, []string{m.instance}, m.provider.Deletes(), "a stopped machine or its disk is not left at the provider")
	require.Empty(t, m.provider.Everything())
	require.True(t, m.tokenRevoked())
	require.Equal(t, []string{reconcileReasonInterrupted}, reported(m.events, types.EventComputeActionReservationInterrupted))

	// The time up to the check that found the machine gone is billed, once.
	require.Len(t, billing.usage, 2)
	require.Equal(t, 10*time.Minute, billing.usage[1].EndAt.Sub(billing.usage[1].StartAt))

	// Nothing more is billed however long the reservation stays listed, and
	// the provider is not asked about the machine again.
	for range 3 {
		reservation = m.check(t, time.Hour)
	}
	require.Equal(t, model.ReservationInterrupted, reservation.Status)
	require.Len(t, billing.usage, 2)
	require.Len(t, m.provider.Deletes(), 1)
	require.Empty(t, reported(m.events, types.EventComputeActionReservationInterrupted), "it is reported once")
}

func TestASpotMachineThatCannotBeDeletedYetStillEndsAsInterrupted(t *testing.T) {
	m, _ := newSpotMachine(t)
	m.provider.SetStatus(m.instance, providers.StatusInterrupted, "spot capacity was reclaimed")
	m.provider.FailDeletes(errors.New("provider is down"))

	reservation := m.check(t, 0)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Equal(t, reconcileReasonInterrupted, reservation.TerminatingReason)
	require.Equal(t, "spot capacity was reclaimed", reservation.LastStatusMessage)
	require.Empty(t, reported(m.events, types.EventComputeActionReservationInterrupted), "it is not reported until the machine is gone")

	m.provider.FailDeletes(nil)
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationInterrupted, reservation.Status, "it ends as the interruption it was, not as a release")
	require.Equal(t, "spot capacity was reclaimed", reservation.LastStatusMessage)
	require.Empty(t, reservation.LastError)
	require.Equal(t, []string{reconcileReasonInterrupted}, reported(m.events, types.EventComputeActionReservationInterrupted))
	require.Empty(t, m.provider.Everything())
}

// An interrupted reservation stays in the pool's record, where its tenant
// sees what became of the machine, until the tenant dismisses it. Nothing is
// left at the provider to delete by then.
func TestAnInterruptedReservationIsListedUntilItIsDismissed(t *testing.T) {
	m, _ := newSpotMachine(t)
	m.state.CreatedByTokenID = "owner-token"
	m.provider.SetStatus(m.instance, providers.StatusInterrupted, "")
	reservation := m.check(t, 0)
	require.Equal(t, model.ReservationInterrupted, reservation.Status)

	listed := providerInstanceToProto(reservation, nil)
	require.Equal(t, "interrupted", listed.Status)
	require.True(t, listed.Spot)
	require.Equal(t, instanceInterruptedMessage, listed.StatusMessage)

	res, err := m.service.DeletePrivateMachine(
		testAuthContext("workspace-1", "owner-token"),
		&pb.DeleteMachineRequest{PoolName: "pool-1", MachineId: reservation.ID},
	)
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Empty(t, m.state.Reservations)
	require.Len(t, m.provider.Deletes(), 1, "the machine was deleted when it was taken back, and is not deleted again")
}

func TestDeletingAPoolLeavesAnInterruptedMachineAlone(t *testing.T) {
	m, _ := newSpotMachine(t)
	m.provider.SetStatus(m.instance, providers.StatusInterrupted, "")
	m.check(t, 0)
	require.Len(t, m.provider.Deletes(), 1)

	res, err := m.service.DeletePool(testAuthContext("workspace-1", "owner-token"), &pb.DeletePoolRequest{Name: "pool-1"})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Len(t, m.provider.Deletes(), 1, "nothing of it is left at the provider")
}

func TestStatusCheckKeepsDeletingAFailedMachineUntilTheProviderLetsItGo(t *testing.T) {
	m := newBoughtMachine(t)
	m.provider.SetStatus(m.instance, providers.StatusFailed, "")
	m.provider.FailDeletes(errors.New("provider is down"))

	reservation := m.check(t, 0)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Equal(t, reconcileReasonProviderFailed, reservation.TerminatingReason)
	require.Equal(t, instanceFailedMessage, reservation.LastStatusMessage)
	require.Equal(t, "provider is down", reservation.LastError)

	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Len(t, m.provider.Deletes(), 2, "the delete is tried again")

	m.provider.FailDeletes(nil)
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationFailed, reservation.Status, "it ends as the failure it was, not as a release")
	require.Equal(t, instanceFailedMessage, reservation.LastStatusMessage)
	require.Empty(t, reservation.LastError)
	require.Len(t, m.provider.Deletes(), 3)

	require.Empty(t, m.provider.Everything())
}

func TestFailedMachineThatHasAlsoExpiredStillEndsAsAFailure(t *testing.T) {
	m := newBoughtMachine(t)
	m.provider.SetStatus(m.instance, providers.StatusFailed, "")
	m.provider.FailDeletes(errors.New("provider is down"))

	// The machine failed and is past the time it was wanted for. One pass
	// asks the provider to delete it once, as the failure it is.
	reservation := m.check(t, 7*time.Hour)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Equal(t, reconcileReasonProviderFailed, reservation.TerminatingReason)
	require.Len(t, m.provider.Deletes(), 1)

	m.provider.FailDeletes(nil)
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationFailed, reservation.Status)
}

func TestExpiredReservationIsDeletedAtTheProvider(t *testing.T) {
	m := newBoughtMachine(t)
	m.provider.SetStatus(m.instance, providers.StatusActive, "")

	reservation := m.check(t, 7*time.Hour)
	require.Equal(t, model.ReservationDeleted, reservation.Status)
	require.Equal(t, reconcileReasonReservationExpired, reservation.TerminatingReason)
	require.Equal(t, []string{m.instance}, m.provider.Deletes())
	require.True(t, m.tokenRevoked())
}

func TestDeletePoolDeletesItsMachinesAtTheProvider(t *testing.T) {
	m := newBoughtMachine(t)

	res, err := m.service.DeletePool(testAuthContext("workspace-1", "owner-token"), &pb.DeletePoolRequest{Name: "pool-1"})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Equal(t, []string{m.instance}, m.provider.Deletes())
	require.Empty(t, m.repo.pools["workspace-1"])
	require.True(t, m.tokenRevoked())
}

func TestDeletePoolKeepsThePoolWhileAMachineCannotBeDeleted(t *testing.T) {
	m := newBoughtMachine(t)
	m.provider.FailDeletes(errors.New("provider is down"))

	// The pool's record is the only record of the machine. Dropping it would
	// leave the machine running, and charged for, with nothing to find it by.
	res, err := m.service.DeletePool(testAuthContext("workspace-1", "owner-token"), &pb.DeletePoolRequest{Name: "pool-1"})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Contains(t, res.ErrMsg, `pool "pool-1" was not deleted`)
	require.Contains(t, res.ErrMsg, "provider is down")
	require.Len(t, m.repo.pools["workspace-1"], 1)
	require.True(t, m.repo.savedPool)
	reservation := m.repo.pools["workspace-1"][0].Reservations[0]
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Equal(t, reconcileReasonMachineReleased, reservation.TerminatingReason)
	require.True(t, m.tokenRevoked())

	// The reconciler goes on trying, and then the pool can be deleted.
	m.provider.FailDeletes(nil)
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationDeleted, reservation.Status)

	res, err = m.service.DeletePool(testAuthContext("workspace-1", "owner-token"), &pb.DeletePoolRequest{Name: "pool-1"})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Empty(t, m.repo.pools["workspace-1"])
	require.Empty(t, m.provider.Everything())
}

func TestDeletePoolDeletesAMachineLeftBehindByAnOlderFailure(t *testing.T) {
	// Reservations that failed before failed machines were deleted may still
	// have a machine at the provider.
	m := newBoughtMachine(t)
	m.state.Reservations[0].Status = model.ReservationFailed

	res, err := m.service.DeletePool(testAuthContext("workspace-1", "owner-token"), &pb.DeletePoolRequest{Name: "pool-1"})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Equal(t, []string{m.instance}, m.provider.Deletes())
	require.Empty(t, m.repo.pools["workspace-1"])
}

func TestARenewalStandsWhenTheProviderCannotBeToldOfIt(t *testing.T) {
	m := newRenewedMachine(t)
	m.provider.extendErr = errors.New("shadeform: 503 service unavailable")

	// The term is renewed for the hour the workspace can pay for.
	reservation := m.check(t, 0)
	require.Equal(t, m.expiry.Add(time.Hour), reservation.ExpiresAt)
	require.Contains(t, reservation.LastError, "503 service unavailable")

	// The provider stays down past the time the term would have ended. The
	// machine is running and paid for, and it is kept.
	for after := time.Minute; after <= 3*time.Minute; after += time.Minute {
		reservation = m.check(t, time.Minute)
		require.Equal(t, model.ReservationActive, reservation.Status, "after %s", after)
	}
	require.Empty(t, m.provider.Deletes())
	require.Len(t, m.provider.Everything(), 1)
}

func TestAProviderThatCouldNotBeToldOfARenewalIsToldOnTheNextPass(t *testing.T) {
	m := newRenewedMachine(t)
	m.provider.extendErr = errors.New("shadeform: 503 service unavailable")
	reservation := m.check(t, 0)
	require.Equal(t, m.expiry.Add(time.Hour), reservation.ExpiresAt, "the term is renewed all the same")
	require.Equal(t, 1, m.provider.extends)
	require.True(t, m.provider.Until(m.instance).IsZero(), "the provider has not heard of the renewal")

	// It is still down a minute later, and is asked again although the term
	// is not due for another renewal.
	m.check(t, time.Minute)
	require.Equal(t, 2, m.provider.extends)

	m.provider.extendErr = nil
	reservation = m.check(t, time.Minute)
	require.Equal(t, 3, m.provider.extends)
	require.Equal(t, reservation.ExpiresAt.Add(reservationDeletionGrace), m.provider.Until(m.instance))
	require.Empty(t, reservation.LastError)

	// Once it has been told, it is left alone until the next renewal.
	m.check(t, time.Minute)
	require.Equal(t, 3, m.provider.extends)
}

func TestARenewalStandsWhenTheProviderIsNotConfigured(t *testing.T) {
	m := newRenewedMachine(t)
	configured := m.service.providers
	m.service.providers = providers.RegistryOf()

	reservation := m.check(t, 0)
	require.Equal(t, m.expiry.Add(time.Hour), reservation.ExpiresAt)
	reservation = m.check(t, 3*time.Minute)
	require.Equal(t, model.ReservationActive, reservation.Status)
	require.Contains(t, reservation.LastError, `provider "shadeform" is not configured`)

	// The provider is configured again, and hears of the renewal it missed.
	m.service.providers = configured
	reservation = m.check(t, 2*time.Minute)
	require.Equal(t, reservation.ExpiresAt.Add(reservationDeletionGrace), m.provider.Until(m.instance))
}

// logged is what was logged while a test ran.
type logged struct {
	mu    sync.Mutex
	lines bytes.Buffer
}

func (l *logged) Write(line []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lines.Write(line)
}

// failures returns the entries that say an operation on a reservation failed,
// by operation.
func (l *logged) failures(t *testing.T) map[string]map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	failures := map[string]map[string]any{}
	for _, line := range bytes.Split(bytes.TrimSpace(l.lines.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		entry := map[string]any{}
		require.NoError(t, json.Unmarshal(line, &entry))
		if operation, ok := entry["operation"].(string); ok {
			failures[operation] = entry
		}
	}
	return failures
}

func captureLog(t *testing.T) *logged {
	t.Helper()
	captured := &logged{}
	standIn(t, &log.Logger, zerolog.New(captured))
	return captured
}

func TestAFailureOnAReservationIsLoggedWithWhatItIsAbout(t *testing.T) {
	for name, tc := range map[string]struct {
		// breaks makes something fail for the machine before the pass.
		breaks    func(m *boughtMachine)
		operation string
		problem   string
	}{
		"usage that billing does not take": {
			breaks: func(m *boughtMachine) {
				m.service.billing.(*fakeManagedBilling).usageErr = errors.New("billing callback unavailable")
			},
			operation: "record usage",
			problem:   "billing callback unavailable",
		},
		"a provider that cannot be told of a renewal": {
			breaks:    func(m *boughtMachine) { m.provider.extendErr = errors.New("shadeform: 503 service unavailable") },
			operation: "extend at provider",
			problem:   "503 service unavailable",
		},
		"a provider that cannot be asked about the machine": {
			breaks:    func(m *boughtMachine) { m.provider.getErr = errors.New("shadeform: connection reset") },
			operation: "check status",
			problem:   "connection reset",
		},
		"a provider that is not configured": {
			breaks:    func(m *boughtMachine) { m.service.providers = providers.RegistryOf() },
			operation: "check status",
			problem:   `provider "shadeform" is not configured`,
		},
		"a machine that cannot be deleted when its term ends": {
			breaks: func(m *boughtMachine) {
				// The workspace cannot pay for another hour.
				m.service.billing.(*fakeManagedBilling).balanceDecision.AvailableCents = 50
				m.state.Reservations[0].ExpiresAt = m.now.Add(-time.Second)
				m.provider.FailDeletes(errors.New("provider is down"))
			},
			operation: "delete machine",
			problem:   "provider is down",
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := newRenewedMachine(t)
			tc.breaks(m)
			captured := captureLog(t)

			reservation := m.check(t, 0)

			entry, ok := captured.failures(t)[tc.operation]
			require.True(t, ok, "nothing was logged for %q: %v", tc.operation, captured.failures(t))
			require.Equal(t, "warn", entry["level"])
			require.Equal(t, "workspace-1", entry["workspace_id"])
			require.Equal(t, "pool-1", entry["pool_name"])
			require.Equal(t, m.instance, entry["reservation_id"])
			require.Contains(t, entry["error"], tc.problem)
			require.Contains(t, reservation.LastError, tc.problem, "and the reservation says so too")
		})
	}
}

func TestAFailedExtensionIsLogged(t *testing.T) {
	p := newExtendedPool(t)
	p.provider.extendErr = errors.New("shadeform: 503 service unavailable")
	captured := captureLog(t)

	res := p.extend(t, &pb.ExtendPoolCapacityRequest{Name: "pool-1", Ttl: "48h"})
	require.True(t, res.Ok, res.ErrMsg)

	entry, ok := captured.failures(t)["extend"]
	require.True(t, ok, "nothing was logged: %v", captured.failures(t))
	require.Equal(t, "workspace-1", entry["workspace_id"])
	require.Equal(t, "pool-1", entry["pool_name"])
	require.Equal(t, p.instance, entry["reservation_id"])
	require.Contains(t, entry["error"], "503 service unavailable")
	reservation := p.repo.pools["workspace-1"][0].Reservations[0]
	require.Contains(t, reservation.LastError, "503 service unavailable")
	require.Equal(t, p.expiry, reservation.ExpiresAt, "the machine keeps the term its provider knows")
}

func TestAStatusCheckDoesNotClearWhatBillingRecordedInTheSamePass(t *testing.T) {
	m := newRenewedMachine(t)
	m.service.billing.(*fakeManagedBilling).usageErr = errors.New("billing callback unavailable")

	// Usage cannot be recorded, pass after pass, while the provider answers
	// every status check. The record goes on saying what is wrong.
	for pass, wait := range []time.Duration{0, time.Minute, time.Minute} {
		reservation := m.check(t, wait)
		require.Contains(t, reservation.LastError, "billing callback unavailable", "pass %d", pass+1)
	}
}

// A provider that deletes machines by itself is a safety net for a machine
// Beam has lost track of. It is told a time a week past the machine's term
// wherever it is told one, so that it never ends a machine Beam still runs.
func TestAProviderIsToldAMachineIsWantedUntilAWeekPastItsTerm(t *testing.T) {
	require.Equal(t, 7*24*time.Hour, reservationDeletionGrace)

	t.Run("when the machine is bought", func(t *testing.T) {
		shadeform := fakeProvider("shadeform", a10gOffer)
		repo := &fakeComputeRepo{}

		res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), launchService(repo, shadeform), 1)
		require.True(t, res.Ok, res.ErrMsg)
		reservation := repo.pools["workspace-1"][0].Reservations[0]
		require.Equal(t, reservation.ExpiresAt.Add(reservationDeletionGrace), shadeform.Creates()[0].Until)
		require.Equal(t, reservation.ExpiresAt.Add(reservationDeletionGrace), shadeform.Until(reservation.ID))
	})

	t.Run("when its term is renewed", func(t *testing.T) {
		m := newRenewedMachine(t)

		reservation := m.check(t, 0)
		require.Equal(t, m.expiry.Add(time.Hour), reservation.ExpiresAt)
		require.Equal(t, reservation.ExpiresAt.Add(reservationDeletionGrace), m.provider.Until(m.instance))
	})

	t.Run("when its pool is extended", func(t *testing.T) {
		p := newExtendedPool(t)

		res := p.extend(t, &pb.ExtendPoolCapacityRequest{Name: "pool-1", Ttl: "48h"})
		require.True(t, res.Ok, res.ErrMsg)
		reservation := p.repo.pools["workspace-1"][0].Reservations[0]
		require.True(t, reservation.ExpiresAt.After(p.expiry))
		require.Equal(t, reservation.ExpiresAt.Add(reservationDeletionGrace), p.provider.Until(p.instance))
	})
}

func TestCreatePoolOverAPoolKeepsWhatItDoesNotSet(t *testing.T) {
	now := time.Now().UTC()
	existing := byocPool(now)
	existing.CreditLowSince = now.Add(-time.Hour)
	existing.BillingDegradedSince = now.Add(-2 * time.Hour)
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {existing}}}
	service := &Service{computeRepo: repo}

	res, err := service.CreatePool(testAuthContext("workspace-1", "owner-token"), &pb.CreatePoolRequest{Pool: &pb.PoolConfig{Name: "aws-cpu"}})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)

	stored := repo.pools["workspace-1"][0]
	require.NotNil(t, stored.BYOC, "the pool's cloud resources, and the tokens its stack joins with")
	require.Equal(t, byocTokenHash, stored.BYOC.Labels[byocBootstrapJoinTokenHashLabel])
	require.Equal(t, now.Add(-time.Hour), stored.CreditLowSince, "a grace period is not started again")
	require.Equal(t, now.Add(-2*time.Hour), stored.BillingDegradedSince)
	require.Equal(t, model.SourceAWS, stored.Source)
	require.Equal(t, now.Add(-24*time.Hour), stored.CreatedAt)
	require.True(t, service.poolNeedsManagedBilling(stored, now), "its machines are still billed")
}

func TestLaunchIntoABYOCPoolIsRefused(t *testing.T) {
	now := time.Now().UTC()
	existing := byocPool(now)
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

func TestCreateBYOCPoolThatCannotBeRegisteredLeavesNoPoolAndNoToken(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	service := byocService(repo)
	takeSelector(t, service, "aws-cpu")

	res, err := service.CreateBYOCPool(ctx, anAWSCPUPool())
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
			repo := silentBYOCPool(time.Second)
			stackCheckAnswers(t, true, nil)

			res := listPoolMachines(t, ctx, byocService(repo), in)
			require.True(t, res.Ok, res.ErrMsg)
			require.Empty(t, res.Machines)
			require.Empty(t, repo.pools["workspace-1"], "what Beam held for the pool is cleaned up")
			require.Empty(t, repo.machines[fakeComputeKey("workspace-1", "aws-cpu")])
			require.True(t, repo.joinTokens[byocTokenHash].Revoked)
		})
	}
}

func TestListPoolMachinesListsTheMachinesOfABYOCPoolWhoseStackExists(t *testing.T) {
	repo := silentBYOCPool(time.Second)
	stackCheckAnswers(t, false, nil)

	res := listPoolMachines(t, testAuthContext("workspace-1", "owner-token"), byocService(repo), &pb.ListPoolMachinesRequest{})
	require.True(t, res.Ok, res.ErrMsg)
	require.Equal(t, []string{"machine-1"}, machineIDs(res.Machines))
	require.Len(t, repo.pools["workspace-1"], 1)
}

func TestGetBYOCPoolDoesNotFindAPoolWhoseStackIsGone(t *testing.T) {
	repo := silentBYOCPool(time.Second)
	stackCheckAnswers(t, true, nil)

	res, err := byocService(repo).GetBYOCPool(testAuthContext("workspace-1", "owner-token"), &pb.GetBYOCPoolRequest{PoolName: "aws-cpu"})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Equal(t, "pool not found", res.ErrMsg)
	require.Empty(t, repo.pools["workspace-1"], "what Beam held for the pool is cleaned up")
	require.True(t, repo.joinTokens[byocTokenHash].Revoked)
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
			standIn(t, &awsBYOCScaleAutoScalingGroup, func(context.Context, awsBYOCScaleAutoScalingGroupInput) error { return tt.awsErr })
			repo := &faultyPools{saveErr: tt.saveErr, fakeComputeRepo: &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {
				byocPool(time.Now().UTC()),
				{Name: "bought", Mode: string(types.PoolModePrivate), Source: model.SourceCLIReservation},
			}}}}

			res, err := byocService(repo).ScaleBYOCPool(testAuthContext("workspace-1", "owner-token"),
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
			p.service.computeRepo = &faultyPools{fakeComputeRepo: p.repo, saveErr: tt.saveErr}

			res := p.extend(t, tt.in)
			require.False(t, res.Ok)
			require.Equal(t, tt.want, res.ErrMsg)
			require.Nil(t, res.Pool)
		})
	}
}

func TestScaleBYOCPoolAndExtendPoolCapacityAnswerUnderALeasedLock(t *testing.T) {
	standIn(t, &awsBYOCScaleAutoScalingGroup, func(context.Context, awsBYOCScaleAutoScalingGroupInput) error { return nil })
	repo := &faultyPools{fakeComputeRepo: &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {
		byocPool(time.Now().UTC()),
	}}}}
	scaled, err := byocService(repo).ScaleBYOCPool(testAuthContext("workspace-1", "owner-token"),
		&pb.ScaleBYOCPoolRequest{PoolName: "aws-cpu", DesiredNodes: 2})
	require.NoError(t, err)
	require.True(t, scaled.Ok, scaled.ErrMsg)
	require.Equal(t, uint32(2), scaled.Byoc.DesiredNodes)

	p := newExtendedPool(t)
	p.service.computeRepo = &faultyPools{fakeComputeRepo: p.repo}
	extended := p.extend(t, &pb.ExtendPoolCapacityRequest{Name: "pool-1", Ttl: "48h"})
	require.True(t, extended.Ok, extended.ErrMsg)
	require.Equal(t, "pool-1", extended.Pool.Name)
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
	const principal = "arn:aws:iam::999999999999:role/beam-gateway"
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
		"templateURL":                       byocTemplateURL,
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
	// The pool's machine is connected.
	repo := silentBYOCPool(0)
	machine := repo.machines[fakeComputeKey("workspace-1", "aws-cpu")][0]

	var asked awsBYOCResourceDeletedInput
	standIn(t, &awsBYOCResourceDeleted, func(_ context.Context, input awsBYOCResourceDeletedInput) (bool, error) {
		asked = input
		return false, nil
	})
	var released awsBYOCReleaseMachineInput
	standIn(t, &awsBYOCReleaseMachine, func(_ context.Context, input awsBYOCReleaseMachineInput) error {
		released = input
		return nil
	})
	service := byocService(repo)
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
	// The pool's machine is connected.
	repo := silentBYOCPool(0)
	delete(repo.pools["workspace-1"][0].BYOC.Labels, awsBYOCControlRoleArnLabel)
	calls := 0
	standIn(t, &awsBYOCResourceDeleted, func(context.Context, awsBYOCResourceDeletedInput) (bool, error) { calls++; return false, nil })
	standIn(t, &awsBYOCReleaseMachine, func(context.Context, awsBYOCReleaseMachineInput) error { calls++; return nil })
	standIn(t, &awsBYOCScaleAutoScalingGroup, func(context.Context, awsBYOCScaleAutoScalingGroupInput) error { calls++; return nil })
	service := byocService(repo)
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

// requireBYOCPoolKept lists the workspace's pools, and checks that the BYOC
// pool is among them and that what Beam holds for it is still there.
func requireBYOCPoolKept(t *testing.T, repo *fakeComputeRepo) {
	t.Helper()
	res, err := byocService(repo).ListPrivatePools(testAuthContext("workspace-1", "owner-token"), &pb.ListPrivatePoolsRequest{})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Len(t, res.Pools, 1, "the pool is listed")
	require.Len(t, repo.pools["workspace-1"], 1, "its record is kept")
	require.Len(t, repo.machines[fakeComputeKey("workspace-1", "aws-cpu")], 1, "and so is its machine's")
	require.False(t, repo.joinTokens[byocTokenHash].Revoked, "the token in the tenant's stack still joins")
}

// requireBYOCPoolCleanedUp lists the workspace's pools, and checks that the
// BYOC pool is not among them and that what Beam held for it is gone.
func requireBYOCPoolCleanedUp(t *testing.T, repo *fakeComputeRepo) {
	t.Helper()
	res, err := byocService(repo).ListPrivatePools(testAuthContext("workspace-1", "owner-token"), &pb.ListPrivatePoolsRequest{})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Empty(t, res.Pools)
	require.Empty(t, repo.pools["workspace-1"])
	require.True(t, repo.joinTokens[byocTokenHash].Revoked)
}

func TestABYOCPoolIsCleanedUpWhenAWSSaysItsStackIsGone(t *testing.T) {
	// Its machine was heard from a moment ago, and AWS knows better.
	repo := silentBYOCPool(time.Second)
	stackCheckAnswers(t, true, nil)

	requireBYOCPoolCleanedUp(t, repo)
}

func TestABYOCPoolThatAWSCannotBeAskedAboutIsCleanedUpOnceItsMachinesStaySilent(t *testing.T) {
	// A stack from before stacks gave Beam a role to ask with.
	withoutRole := func(repo *fakeComputeRepo) {
		labels := repo.pools["workspace-1"][0].BYOC.Labels
		delete(labels, awsBYOCControlRoleArnLabel)
		delete(labels, awsBYOCControlRoleExternalIDLabel)
	}

	repo := silentBYOCPool(byocDeletedPoolRetention - time.Minute)
	withoutRole(repo)
	asked := stackCheckAnswers(t, false, nil)
	requireBYOCPoolKept(t, repo)

	repo = silentBYOCPool(byocDeletedPoolRetention + time.Minute)
	withoutRole(repo)
	requireBYOCPoolCleanedUp(t, repo)
	require.Zero(t, *asked, "there is nobody to ask")
}

// tokensInRedis is a gateway that keeps its join tokens in Redis, as a real
// one does, and has one pool that buys its machines.
func tokensInRedis(t *testing.T) (*Service, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)

	service, _ := buyingTestService(types.AppConfig{})
	service.computeRepo = repository.NewComputeRedisRepository(rdb)
	_, err = service.CreateManagedPool(context.Background(), clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)
	return service, server
}

// A pool that buys its machines makes a join token for every machine it
// buys, and the token does not expire. One that stayed in Redis after its
// machine was given back would stay for good.
func TestTheJoinTokenOfAForgottenMachineIsRemoved(t *testing.T) {
	ctx := context.Background()
	service, server := tokensInRedis(t)

	script, tokenHash, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	key := common.RedisKeys.ComputeJoinToken(tokenHash)
	require.True(t, server.Exists(key))

	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", tokenHash))
	require.False(t, joinAs(t, service, script, "verda-1").Ok, "the token admits nothing from the moment it is revoked")

	server.FastForward(revokedJoinTokenKept)
	require.False(t, server.Exists(key), "the token is still in Redis")
	response := joinAs(t, service, script, "verda-1")
	require.False(t, response.Ok)
	require.Equal(t, "join token is invalid or expired", response.ErrMsg)

	// Forgetting the machine again finds no token, and does not mind.
	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", tokenHash))
	require.False(t, server.Exists(key))
}

// Earlier releases kept a revoked token in Redis, marked as revoked. Such a
// token admits nothing, and goes when it is revoked again.
func TestAJoinTokenThatWasKeptAsRevokedAdmitsNothing(t *testing.T) {
	ctx := context.Background()
	service, server := tokensInRedis(t)

	script, tokenHash, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	token, err := service.computeRepo.GetJoinTokenState(ctx, tokenHash)
	require.NoError(t, err)
	token.Revoked = true
	require.NoError(t, service.computeRepo.SaveJoinTokenState(ctx, token, 0))

	response := joinAs(t, service, script, "verda-1")
	require.False(t, response.Ok)
	require.Equal(t, "join token is invalid or expired", response.ErrMsg)

	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", tokenHash))
	server.FastForward(revokedJoinTokenKept)
	require.False(t, server.Exists(common.RedisKeys.ComputeJoinToken(tokenHash)))
}

// An agent that is refused goes on with the identity it has saved. These
// tests pin what a refused join leaves behind: what was there before it, and
// nothing else.

// unwritableWorkers is a worker repository that can refuse to add a worker,
// which fails a join after its machine has been saved.
type unwritableWorkers struct {
	*fakeWorkerRepo
	addErr error
}

func (r *unwritableWorkers) AddWorker(worker *types.Worker) error {
	if r.addErr != nil {
		return r.addErr
	}
	return r.fakeWorkerRepo.AddWorker(worker)
}

func joinTestService() (*Service, *fakeComputeRepo, *unwritableWorkers) {
	service, repo := buyingTestService(types.AppConfig{})
	workers := &unwritableWorkers{fakeWorkerRepo: &fakeWorkerRepo{}}
	service.workerRepo = workers
	service.scheduler = scheduler.NewSchedulerForCapacityChecks(workers, repo, scheduler.NewWorkerPoolManager())
	return service, repo, workers
}

func TestARefusedJoinLeavesAMachineThatHadJoinedAsItWas(t *testing.T) {
	ctx := context.Background()

	for name, refuse := range map[string]func(*Service, *unwritableWorkers, string) *pb.JoinAgentResponse{
		"before the machine is saved": func(service *Service, _ *unwritableWorkers, script string) *pb.JoinAgentResponse {
			token := joinTokenInScript.FindStringSubmatch(script)[1]
			response, err := service.JoinAgent(ctx, &pb.JoinAgentRequest{
				JoinToken: token, MachineFingerprint: "fingerprint-by-hand", Hostname: "by-hand",
				CpuCount: 20, MemoryMb: 60_000, Schedulable: true,
			})
			require.NoError(t, err)
			return response
		},
		"after the machine is saved": func(service *Service, workers *unwritableWorkers, script string) *pb.JoinAgentResponse {
			workers.addErr = errors.New("the worker cannot be written")
			return joinAs(t, service, script, "by-hand")
		},
	} {
		t.Run(name, func(t *testing.T) {
			service, repo, workers := joinTestService()
			script, joined := joinedByHand(t, service)
			worker := workers.worker
			require.NotNil(t, worker)

			response := refuse(service, workers, script)
			require.False(t, response.Ok)

			machine, err := repo.GetAgentMachineState(ctx, "admin-workspace", "l40s", joined.MachineID)
			require.NoError(t, err)
			require.NotNil(t, machine, "the machine is still in its pool")
			require.Equal(t, joined.TokenHash, machine.TokenHash, "under the token its agent holds")
			require.Same(t, worker, workers.worker, "with its worker")
		})
	}
}

func TestARefusedFirstJoinLeavesNothing(t *testing.T) {
	ctx := context.Background()
	service, repo, workers := joinTestService()
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "l40s", types.WorkerPoolConfig{GPUType: "L40S"})
	require.NoError(t, err)
	bootstrap, err := service.CreateManagedMachine(ctx, clusterAdminAuth(), "l40s")
	require.NoError(t, err)

	workers.addErr = errors.New("the worker cannot be written")
	response := joinAs(t, service, bootstrap.InstallCommand, "by-hand")
	require.False(t, response.Ok)
	require.Contains(t, response.ErrMsg, "the worker cannot be written")

	machine, err := repo.GetAgentMachineState(ctx, "admin-workspace", "l40s", bootstrap.MachineID)
	require.NoError(t, err)
	require.Nil(t, machine)
	require.Nil(t, workers.worker)
}

// The worker of a machine that a pool bought stands for it from the purchase
// on, and requests wait on it. A join that fails is tried again by the
// machine; what gives up on it is the pool's boot timeout.
func TestARefusedJoinLeavesTheWorkerOfAMachineThatWasBought(t *testing.T) {
	ctx := context.Background()
	service, repo, workers := joinTestService()
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)
	script, _, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	booting := &types.Worker{Id: model.AgentMachineWorkerID("machine-1"), MachineId: "machine-1", PoolName: "spot-l40s", Status: types.WorkerStatusPending}
	workers.worker = booting

	workers.addErr = errors.New("the worker cannot be written")
	require.False(t, joinAs(t, service, script, "verda-1").Ok)

	require.Same(t, booting, workers.worker)
	machine, err := repo.GetAgentMachineState(ctx, "admin-workspace", "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.Nil(t, machine, "the machine has not joined")

	workers.addErr = nil
	require.True(t, joinAs(t, service, script, "verda-1").Ok)
}

func TestARefusedJoinLeavesAMachineOfAWorkspacesOwnPoolAsItWas(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{"workspace-1": {{
			WorkspaceID: "workspace-1",
			Name:        "own",
			Mode:        string(types.PoolModePrivate),
			Config:      &pb.PoolConfig{Name: "own"},
			CreatedAt:   now,
		}}},
		joinTokens: map[string]*model.JoinTokenState{hashComputeToken("join-token"): {
			TokenHash:     hashComputeToken("join-token"),
			WorkspaceID:   "workspace-1",
			PoolName:      "own",
			PoolCreatedAt: now,
		}},
	}
	workers := &unwritableWorkers{fakeWorkerRepo: &fakeWorkerRepo{}}
	service := &Service{
		computeRepo: repo,
		workerRepo:  workers,
		scheduler:   scheduler.NewSchedulerForCapacityChecks(workers, repo, scheduler.NewWorkerPoolManager()),
	}
	join := func() *pb.JoinAgentResponse {
		response, err := service.JoinAgent(ctx, &pb.JoinAgentRequest{
			JoinToken: "join-token", MachineFingerprint: "fingerprint-1",
			CpuCount: 4, MemoryMb: 8192, Schedulable: true,
		})
		require.NoError(t, err)
		return response
	}

	first := join()
	require.True(t, first.Ok, first.ErrMsg)
	joined, err := repo.GetAgentMachineState(ctx, "workspace-1", "own", first.MachineId)
	require.NoError(t, err)
	tokenHash := joined.TokenHash
	worker := workers.worker
	require.NotNil(t, worker)

	workers.addErr = errors.New("the worker cannot be written")
	require.False(t, join().Ok)

	machine, err := repo.GetAgentMachineState(ctx, "workspace-1", "own", first.MachineId)
	require.NoError(t, err)
	require.NotNil(t, machine, "the machine is still in its pool")
	require.Equal(t, tokenHash, machine.TokenHash, "under the token its agent holds")
	require.Same(t, worker, workers.worker, "with its worker")
}

func TestCreateManagedPoolThatBuysItsMachines(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{})

	// A provisioning block is all it takes: no mode, and spot machines make
	// the pool preemptable.
	pool, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)
	require.Equal(t, types.PoolModeExternal, pool.Config.Mode)
	require.True(t, pool.Config.Preemptable)
	require.Equal(t, spotPool().Provisioning, pool.Config.Provisioning)

	onDemand, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "on-demand-l40s", spotPool(func(pool *types.WorkerPoolConfig) {
		pool.Provisioning.Spot = false
	}))
	require.NoError(t, err)
	require.False(t, onDemand.Config.Preemptable)

	// A pool cannot buy from a provider the gateway has no credentials for.
	_, err = service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-a10g", spotPool(func(pool *types.WorkerPoolConfig) {
		pool.Provisioning.Providers = []string{"nebius"}
	}))
	require.ErrorIs(t, err, model.ErrInvalidManagedPoolConfig)
	require.ErrorContains(t, err, `provisioning.providers: "nebius" is not configured under providers (configured: verda)`)

	_, err = service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-a10g", spotPool(func(pool *types.WorkerPoolConfig) {
		pool.Provisioning.MaxNodes = 0
	}))
	require.ErrorIs(t, err, model.ErrInvalidManagedPoolConfig)
	require.ErrorContains(t, err, "provisioning.maxNodes must be at least 1")

	// The same goes for a change to a pool that exists.
	_, err = service.UpdateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool(func(pool *types.WorkerPoolConfig) {
		pool.Provisioning.Providers = []string{"nebius"}
	}))
	require.ErrorIs(t, err, model.ErrInvalidManagedPoolConfig)
}

// Nor can a pool buy from a provider that is disabled. It is told that, and
// not that the provider lacks credentials, which it has.
func TestCreateManagedPoolThatBuysFromADisabledProvider(t *testing.T) {
	const disabled, selling = "vast", "shadeform"
	vast := types.VastProviderConfig{ApiKey: "synthetic-key"}
	for name, tc := range map[string]struct {
		config types.ProviderConfig
		others string
	}{
		"another provider sells": {
			config: types.ProviderConfig{Disabled: []string{disabled}, Vast: vast, Shadeform: types.ShadeformProviderConfig{ApiKey: "synthetic-key"}},
			others: "enabled: " + selling,
		},
		"no provider sells": {
			config: types.ProviderConfig{Disabled: []string{disabled}, Vast: vast},
			others: "none are enabled",
		},
	} {
		t.Run(name, func(t *testing.T) {
			service, repo := buyingTestService(types.AppConfig{})
			service.providers = providers.NewRegistry(tc.config, testOwner)
			_, err := service.CreateManagedPool(context.Background(), clusterAdminAuth(), "spot-l40s", spotPool(func(pool *types.WorkerPoolConfig) {
				pool.Provisioning.Providers = []string{disabled}
			}))
			require.ErrorIs(t, err, model.ErrInvalidManagedPoolConfig)
			require.ErrorContains(t, err, fmt.Sprintf("provisioning.providers: %q is disabled under providers.disabled (%s)", disabled, tc.others))
			require.Empty(t, repo.pools, "a pool that cannot buy is not made")
		})
	}
}

func TestBuyingPoolsAreThePoolsThisReplicaRunsThatBuy(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"spot-a10g":   spotPool(func(pool *types.WorkerPoolConfig) { pool.GPUType = "A10G" }),
		"hand-joined": {Mode: types.PoolModeExternal},
		"kubernetes":  {Mode: types.PoolModeLocal},
	}}})
	require.NoError(t, service.ReconcileManagedPools(ctx))
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)
	_, err = service.CreateManagedPool(ctx, clusterAdminAuth(), "public-cpu", types.WorkerPoolConfig{Mode: types.PoolModeExternal})
	require.NoError(t, err)

	pools, unloaded, err := service.BuyingPools(ctx)
	require.NoError(t, err)
	require.Empty(t, unloaded)
	require.ElementsMatch(t, []string{"spot-a10g", "spot-l40s"}, slices.Sorted(maps.Keys(pools)))
	// A pool from the config file is what the API would have made of it.
	require.Equal(t, types.PoolModeExternal, pools["spot-a10g"].Mode)
	require.True(t, pools["spot-a10g"].Preemptable)
	require.Equal(t, 4, pools["spot-l40s"].Provisioning.MaxNodes)

	// A replica whose config no longer has the pool does not buy for it,
	// though the pool's state is still stored.
	newer, _ := buyingTestService(types.AppConfig{})
	newer.managedPoolRepo = service.managedPoolRepo
	pools, unloaded, err = newer.BuyingPools(ctx)
	require.NoError(t, err)
	require.Empty(t, unloaded)
	require.Equal(t, []string{"spot-l40s"}, slices.Sorted(maps.Keys(pools)))

	// A pool that no longer buys is not listed.
	_, err = service.UpdateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", types.WorkerPoolConfig{GPUType: "L40S"})
	require.NoError(t, err)
	pools, unloaded, err = service.BuyingPools(ctx)
	require.NoError(t, err)
	require.Empty(t, unloaded)
	require.Equal(t, []string{"spot-a10g"}, slices.Sorted(maps.Keys(pools)))
}

// The provisioner drains the machines of a pool it is not told about, so a
// pool that failed to load must not look like a pool that is gone. It is
// named, so that the pools that did load are not held up by it.
func TestBuyingPoolsSaysWhenNotEveryPoolLoaded(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"spot-a10g": spotPool(func(pool *types.WorkerPoolConfig) { pool.GPUType = "A10G" }),
		"spot-t4":   spotPool(func(pool *types.WorkerPoolConfig) { pool.GPUType = "T4"; pool.Provisioning.MaxNodes = 0 }),
	}}})
	_ = service.ReconcileManagedPools(ctx)

	pools, unloaded, err := service.BuyingPools(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"spot-a10g"}, slices.Sorted(maps.Keys(pools)))
	require.Len(t, unloaded, 1)
	require.ErrorContains(t, unloaded["spot-t4"], "provisioning.maxNodes must be at least 1")

	// The pools cannot be read at all.
	service.backendRepo = &fakeManagedPoolBackendRepo{workspaceFailures: 1}
	pools, unloaded, err = service.BuyingPools(ctx)
	require.Error(t, err)
	require.Nil(t, pools)
	require.Nil(t, unloaded)
}

// A pool that was made through the API and that a replica's config file now
// defines too is refused by that replica. Its machines are still running.
func TestBuyingPoolsNamesAPoolThatConflictsWithTheConfigFile(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{})
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)

	newer, _ := buyingTestService(types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"spot-l40s": spotPool(),
		"spot-a10g": spotPool(func(pool *types.WorkerPoolConfig) { pool.GPUType = "A10G" }),
	}}})
	newer.managedPoolRepo = service.managedPoolRepo
	_ = newer.ReconcileManagedPools(ctx)

	pools, unloaded, err := newer.BuyingPools(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"spot-a10g"}, slices.Sorted(maps.Keys(pools)))
	require.Len(t, unloaded, 1)
	require.ErrorContains(t, unloaded["spot-l40s"], `API-managed pool "spot-l40s" conflicts with config`)
}

func TestJoinScriptLetsOneMachineJoinItsPool(t *testing.T) {
	ctx := context.Background()
	service, repo := buyingTestService(types.AppConfig{
		GatewayService: types.GatewayServiceConfig{HTTP: types.HTTPConfig{ExternalHost: "gateway.example.com", ExternalPort: 443, TLS: true}},
	})
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)

	script, tokenHash, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.Contains(t, script, "https://gateway.example.com/install/agent")

	// The token is for that machine in that pool, and does not expire: how
	// long a machine takes to boot is its provider's business.
	token := repo.joinTokens[tokenHash]
	require.NotNil(t, token)
	require.Equal(t, "machine-1", token.MachineID)
	require.Equal(t, "spot-l40s", token.PoolName)
	require.Equal(t, string(types.PoolModeExternal), token.Mode)
	require.NotEmpty(t, token.ManagedPoolInstanceID)
	require.True(t, token.ExpiresAt.IsZero())

	response := joinAs(t, service, script, "verda-1")
	require.True(t, response.Ok, response.ErrMsg)
	joined, err := repo.GetAgentMachineState(ctx, "admin-workspace", "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.NotNil(t, joined, "the machine joined under the ID it was bought with")

	// Only pools that buy hand out such scripts.
	_, err = service.CreateManagedPool(ctx, clusterAdminAuth(), "public-cpu", types.WorkerPoolConfig{Mode: types.PoolModeExternal})
	require.NoError(t, err)
	_, _, err = service.JoinScript(ctx, "public-cpu", "machine-2")
	require.ErrorContains(t, err, `pool "public-cpu" does not buy machines`)
	_, _, err = service.JoinScript(ctx, "no-such-pool", "machine-3")
	require.ErrorContains(t, err, `pool "no-such-pool" does not buy machines`)
}

func TestForgetMachineRemovesAMachineThatJoined(t *testing.T) {
	ctx := context.Background()
	service, repo := buyingTestService(types.AppConfig{})
	workers := &fakeWorkerRepo{}
	service.workerRepo = workers
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)

	script, tokenHash, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.True(t, joinAs(t, service, script, "verda-1").Ok)
	workers.worker = &types.Worker{Id: model.AgentMachineWorkerID("machine-1"), MachineId: "machine-1", PoolName: "spot-l40s", Status: types.WorkerStatusAvailable}

	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", tokenHash))

	require.Nil(t, workers.worker, "its worker is removed, which returns its requests to the backlog")
	machine, err := repo.GetAgentMachineState(ctx, "admin-workspace", "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.Nil(t, machine)
	require.True(t, repo.joinTokens[tokenHash].Revoked)
	require.False(t, joinAs(t, service, script, "verda-1").Ok, "it cannot join again")

	// Forgetting is safe to repeat: a pass that failed half-way runs again.
	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", tokenHash))
}

func TestForgetMachineRemovesAMachineThatNeverJoined(t *testing.T) {
	ctx := context.Background()
	service, repo := buyingTestService(types.AppConfig{})
	workers := &fakeWorkerRepo{}
	service.workerRepo = workers
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)

	script, tokenHash, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	// All there is of it: the worker that stands for it while it boots.
	workers.worker = &types.Worker{Id: model.AgentMachineWorkerID("machine-1"), MachineId: "machine-1", PoolName: "spot-l40s", Status: types.WorkerStatusPending}

	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", tokenHash))
	require.Nil(t, workers.worker)
	require.True(t, repo.joinTokens[tokenHash].Revoked)

	// Should it boot after all, it is not let in.
	response := joinAs(t, service, script, "verda-1")
	require.False(t, response.Ok)
	require.Equal(t, "join token is invalid or expired", response.ErrMsg)

	// A machine with no worker and no token is forgotten without complaint.
	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-2", ""))
}

func TestForgetMachineLeavesOtherMachinesAlone(t *testing.T) {
	ctx := context.Background()
	service, repo := buyingTestService(types.AppConfig{})
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)

	first, firstHash, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	second, _, err := service.JoinScript(ctx, "spot-l40s", "machine-2")
	require.NoError(t, err)
	require.True(t, joinAs(t, service, first, "verda-1").Ok)
	require.True(t, joinAs(t, service, second, "verda-2").Ok)
	other := &types.Worker{Id: model.AgentMachineWorkerID("machine-2"), MachineId: "machine-2", PoolName: "spot-l40s"}
	service.workerRepo = &fakeWorkerRepo{worker: other}

	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", firstHash))

	machine, err := repo.GetAgentMachineState(ctx, "admin-workspace", "spot-l40s", "machine-2")
	require.NoError(t, err)
	require.NotNil(t, machine)
	require.Same(t, other, service.workerRepo.(*fakeWorkerRepo).worker)
}

// A pool of the config file is stored once, as it was then. Whether it buys
// its machines is for the config to say.
func TestAPoolOfTheConfigFileBuysWhenItsConfigSaysSo(t *testing.T) {
	ctx := context.Background()
	older, _ := buyingTestService(types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"l40s": {Mode: types.PoolModeExternal, GPUType: "L40S"},
	}}})
	require.NoError(t, older.ReconcileManagedPools(ctx))

	service, repo := buyingTestService(types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"l40s": spotPool(),
	}}})
	service.managedPoolRepo = older.managedPoolRepo
	require.NoError(t, service.ReconcileManagedPools(ctx))
	stored, err := service.managedPoolRepo.GetManagedPoolState(ctx, "admin-workspace", "l40s")
	require.NoError(t, err)
	require.False(t, stored.WorkerConfig.Buys(), "the record is as it was first stored")

	script, _, err := service.JoinScript(ctx, "l40s", "machine-1")
	require.NoError(t, err)
	require.True(t, joinAs(t, service, script, "verda-1").Ok)

	_, err = service.CreateManagedMachine(ctx, clusterAdminAuth(), "l40s")
	require.ErrorIs(t, err, model.ErrManagedPoolBuysMachines)
	err = service.DeleteManagedMachine(ctx, clusterAdminAuth(), "l40s", "machine-1")
	require.ErrorIs(t, err, model.ErrManagedPoolBuysMachines)
	machine, err := repo.GetAgentMachineState(ctx, "admin-workspace", "l40s", "machine-1")
	require.NoError(t, err)
	require.NotNil(t, machine)
}

// A pool either buys its machines or has them joined by hand. Machines the
// provisioner has no record of would never be given back, and would count
// against nothing.
func TestAPoolThatBuysTakesNoMachinesByHand(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{})
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)

	_, err = service.CreateManagedMachine(ctx, clusterAdminAuth(), "spot-l40s")
	require.ErrorIs(t, err, model.ErrManagedPoolBuysMachines)
	require.ErrorContains(t, err, `"spot-l40s" gets its machines from verda; raise provisioning.maxNodes for more`)
	// The API answers a conflict with 409 and the error's own words.
	require.ErrorIs(t, err, model.ErrManagedPoolConflict)
	require.NotErrorIs(t, model.ErrManagedPoolConflict, model.ErrManagedPoolBuysMachines)
}

func TestAPoolWithMachinesJoinedByHandCannotStartBuying(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{})
	_, byHand := joinedByHand(t, service)

	_, err := service.UpdateManagedPool(ctx, clusterAdminAuth(), "l40s", spotPool())
	require.ErrorIs(t, err, model.ErrManagedPoolBuysMachines)
	state, err := service.managedPoolRepo.GetManagedPoolState(ctx, "admin-workspace", "l40s")
	require.NoError(t, err)
	require.False(t, state.WorkerConfig.Buys(), "the pool is as it was")

	// With the machine removed, it can.
	require.NoError(t, service.DeleteManagedMachine(ctx, clusterAdminAuth(), "l40s", byHand.MachineID))
	pool, err := service.UpdateManagedPool(ctx, clusterAdminAuth(), "l40s", spotPool())
	require.NoError(t, err)
	require.True(t, pool.Config.Buys())

	// And its limits can be changed while it holds machines it bought.
	script, _, err := service.JoinScript(ctx, "l40s", "machine-1")
	require.NoError(t, err)
	require.True(t, joinAs(t, service, script, "verda-1").Ok)
	pool, err = service.UpdateManagedPool(ctx, clusterAdminAuth(), "l40s", spotPool(func(pool *types.WorkerPoolConfig) {
		pool.Provisioning.MaxNodes = 8
	}))
	require.NoError(t, err)
	require.Equal(t, 8, pool.Config.Provisioning.MaxNodes)
}

func TestAPoolThatBuysCanBeDeletedWhileItHoldsMachines(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{})

	// A pool whose machines were joined by hand cannot: nobody would take
	// them away.
	joinedByHand(t, service)
	require.ErrorIs(t, service.DeleteManagedPool(ctx, clusterAdminAuth(), "l40s"), model.ErrManagedPoolInUse)

	// The provisioner gives back what a pool bought once the pool is gone.
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)
	script, _, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.True(t, joinAs(t, service, script, "verda-1").Ok)

	require.NoError(t, service.DeleteManagedPool(ctx, clusterAdminAuth(), "spot-l40s"))
	pools, unloaded, err := service.BuyingPools(ctx)
	require.NoError(t, err)
	require.Empty(t, pools)
	require.Empty(t, unloaded)

	// A machine that was still booting does not join the pool's successor.
	_, err = service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)
	require.False(t, joinAs(t, service, script, "verda-1").Ok)
}

func TestManagedPoolAlsoServesOtherGPUTypes(t *testing.T) {
	config, err := normalizeManagedPoolConfig(types.WorkerPoolConfig{
		Mode:       types.PoolModeExternal,
		GPUType:    "RTX4090",
		AlsoServes: []string{"A10G"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !config.Serves("A10G") {
		t.Fatalf("alsoServes = %v, want A10G kept", config.AlsoServes)
	}

	for name, config := range map[string]types.WorkerPoolConfig{
		"cpu pool":     {AlsoServes: []string{"A10G"}},
		"its own type": {GPUType: "RTX4090", AlsoServes: []string{"RTX4090"}},
		"any":          {GPUType: "RTX4090", AlsoServes: []string{"any"}},
	} {
		t.Run(name, func(t *testing.T) {
			config.Mode = types.PoolModeExternal
			if _, err := normalizeManagedPoolConfig(config); err == nil {
				t.Fatal("alsoServes that cannot route anything was accepted")
			}
		})
	}
}

// A workspace can give a pool of its own the name of a pool that serves
// everyone. What that pool holds is not this pool's.
func TestAManagedPoolHoldsOnlyItsOwnWorkers(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{})
	workers := &fakeWorkerRepo{}
	service.workerRepo = workers
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "l40s", types.WorkerPoolConfig{GPUType: "L40S"})
	require.NoError(t, err)

	workers.worker = &types.Worker{Id: "its-own", PoolName: "l40s", WorkspaceId: "admin-workspace", ControlPlaneManaged: true}
	_, err = service.UpdateManagedPool(ctx, clusterAdminAuth(), "l40s", spotPool())
	require.ErrorIs(t, err, model.ErrManagedPoolBuysMachines)
	require.ErrorIs(t, service.DeleteManagedPool(ctx, clusterAdminAuth(), "l40s"), model.ErrManagedPoolInUse)

	workers.worker = &types.Worker{Id: "a-workspaces", PoolName: "l40s", WorkspaceId: "workspace-1"}
	_, err = service.UpdateManagedPool(ctx, clusterAdminAuth(), "l40s", spotPool())
	require.NoError(t, err)
	_, err = service.UpdateManagedPool(ctx, clusterAdminAuth(), "l40s", types.WorkerPoolConfig{GPUType: "L40S"})
	require.NoError(t, err)
	require.NoError(t, service.DeleteManagedPool(ctx, clusterAdminAuth(), "l40s"))
}

// The provisioner gives back what a pool bought. A machine removed by hand
// would be paid for until the provisioner missed its worker.
func TestAMachineThatWasBoughtIsNotRemovedByHand(t *testing.T) {
	ctx := context.Background()
	service, repo := buyingTestService(types.AppConfig{})
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)
	script, _, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.True(t, joinAs(t, service, script, "verda-1").Ok)

	err = service.DeleteManagedMachine(ctx, clusterAdminAuth(), "spot-l40s", "machine-1")
	require.ErrorIs(t, err, model.ErrManagedPoolBuysMachines)
	require.ErrorContains(t, err, `"spot-l40s" gives its machines back itself`)
	machine, err := repo.GetAgentMachineState(ctx, "admin-workspace", "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.NotNil(t, machine)

	// One that was joined by hand is.
	_, byHand := joinedByHand(t, service)
	require.NoError(t, service.DeleteManagedMachine(ctx, clusterAdminAuth(), "l40s", byHand.MachineID))
	machine, err = repo.GetAgentMachineState(ctx, "admin-workspace", "l40s", byHand.MachineID)
	require.NoError(t, err)
	require.Nil(t, machine)
}

// heardOffers is what the market's sampler last heard the providers sell,
// by provider. A provider it has not heard from lately is not in it.
type heardOffers map[string][]providers.Offer

func (h heardOffers) Offers(ctx context.Context, provider string) ([]providers.Offer, bool) {
	offers, fresh := h[provider]
	return offers, fresh
}

// What is for sale is listed from what the sampler last heard, so that a
// page of offers does not wait for seven providers. A provider the sampler
// has not heard from lately is asked. A purchase asks the provider itself:
// it buys what is for sale now, at the price it is for sale at now.
func TestOffersAreListedFromWhatTheSamplerLastHeardAndBoughtFromTheProviderItself(t *testing.T) {
	now := a10gOffer
	now.HourlyCostMicros = 2 * a10gOffer.HourlyCostMicros
	shadeform := &faultyProvider{Fake: fakeProvider("shadeform", now)}
	verda := &faultyProvider{Fake: fakeProvider("verda", a10gOffer)}
	heard := a10gOffer
	heard.Provider = "shadeform"
	other := a10gOffer
	other.ID, other.InstanceType, other.GPU, other.Provider = "H100", "H100", "H100", "shadeform"
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform, verda)
	service.sampledOffers = heardOffers{"shadeform": {heard, other}}

	res, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{Pool: &pb.PoolConfig{Gpu: []string{"A10G"}}})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Len(t, res.Offers, 2, "what was heard is put through what the pool asks for, like what a provider answers")
	listed := map[string]int64{}
	for _, offer := range res.Offers {
		listed[offer.Provider] = offer.HourlyCostMicros
	}
	require.Equal(t, service.billableMicros(a10gOffer.HourlyCostMicros), listed["shadeform"], "the price the sampler heard, and not the one the provider has now")
	require.Equal(t, service.billableMicros(a10gOffer.HourlyCostMicros), listed["verda"], "a provider the sampler has not heard from is asked")

	launched, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
		Pool: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Providers: []string{"shadeform"}, Nodes: 1, Ttl: "1h", MaxSpend: 5},
	})
	require.NoError(t, err)
	require.True(t, launched.Ok, launched.ErrMsg)
	require.Len(t, shadeform.Creates(), 1)
	require.Equal(t, now.HourlyCostMicros, shadeform.Creates()[0].Offer.HourlyCostMicros, "the machine is bought at the price it is for sale at now")
}

// outcomesOf is what the service told the market's history became of its
// machines.
func outcomesOf(t *testing.T, service *Service, log *watch.MemoryLog) []watch.Outcome {
	t.Helper()
	service.outcomes.Flush(context.Background())
	var outcomes []watch.Outcome
	for _, record := range log.All("outcomes") {
		var outcome watch.Outcome
		require.NoError(t, json.Unmarshal(record.Body, &outcome))
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func recordOutcomes(service *Service) *watch.MemoryLog {
	log := watch.NewMemoryLog()
	service.outcomes = watch.NewRecorder(log, watch.Streams{}, nil)
	return log
}

// A provider that gives a price and no word on its stock is known to have
// had a machine, or not, only from being asked for one. Every purchase is
// written to the market's history with how it went.
func TestEveryPurchaseIsWrittenToTheMarketsHistoryWithHowItWent(t *testing.T) {
	provider := &faultyProvider{Fake: fakeProvider("aws", spotA10GOffer)}
	service := launchService(&fakeComputeRepo{}, provider)
	log := recordOutcomes(service)
	launch := func() *pb.LaunchPoolCapacityResponse {
		res, err := service.LaunchPoolCapacity(testAuthContext("workspace-1", "token-1"), &pb.LaunchPoolCapacityRequest{
			Pool: &pb.PoolConfig{Name: "pool-1", Gpu: []string{"A10G"}, Pricing: "spot", Nodes: 1, Ttl: "1h", MaxSpend: 5},
		})
		require.NoError(t, err)
		return res
	}

	require.True(t, launch().Ok)
	provider.RefuseCreates(fmt.Errorf("%w: no spot capacity", providers.ErrOutOfStock))
	require.False(t, launch().Ok)

	outcomes := outcomesOf(t, service, log)
	require.Len(t, outcomes, 2)
	bought, refused := outcomes[0], outcomes[1]
	require.Equal(t, watch.OutcomeCreate, bought.Event)
	require.Equal(t, watch.ResultOK, bought.Result)
	require.Equal(t, watch.SourceReservation, bought.Source)
	require.Equal(t, "aws", bought.Provider)
	require.Equal(t, spotA10GOffer.Region, bought.Region)
	require.Equal(t, watch.Geo(spotA10GOffer.Region), bought.Geo)
	require.Equal(t, "A10G", bought.GPU)
	require.True(t, bought.Spot)
	require.Equal(t, spotA10GOffer.HourlyCostMicros, bought.HourlyMicros, "what the provider charges, not what the tenant pays")
	require.Equal(t, watch.ResultOutOfStock, refused.Result)
	require.Contains(t, refused.Err, "no spot capacity")
}

// What became of a machine after it was bought is written too: how long it
// took to come up, and how it ended. That a spot machine was taken back is
// what tells how long one is kept.
func TestWhatBecomesOfAReservedMachineIsWrittenToTheMarketsHistory(t *testing.T) {
	m := boughtMachineAt(time.Now().UTC().Truncate(time.Second), providers.StatusPending, 6*time.Hour)
	m.state.Reservations[0].Spot = true
	m.state.Reservations[0].GPU, m.state.Reservations[0].GPUCount = "A10G", 1
	m.state.Reservations[0].Region = "newyork-usa-1"
	m.state.Reservations[0].BillingCursorAt = m.now
	m.service.billing = &fakeManagedBilling{balanceDecision: healthyBalance}
	log := recordOutcomes(m.service)

	m.check(t, time.Minute)
	require.Empty(t, outcomesOf(t, m.service, log), "it is still booting")

	m.provider.SetStatus(m.instance, providers.StatusActive, "")
	m.check(t, time.Minute)
	m.check(t, time.Minute)
	m.provider.SetStatus(m.instance, providers.StatusInterrupted, "")
	m.check(t, 10*time.Minute)
	m.check(t, 10*time.Minute)

	outcomes := outcomesOf(t, m.service, log)
	require.Len(t, outcomes, 2, "each is written once")
	up, ended := outcomes[0], outcomes[1]
	require.Equal(t, watch.OutcomeActive, up.Event)
	require.Equal(t, time.Hour+2*time.Minute, up.Took, "from when it was bought")
	require.Equal(t, watch.OutcomeEnd, ended.Event)
	require.Equal(t, watch.ResultInterrupted, ended.Result)
	require.Equal(t, time.Hour+13*time.Minute, ended.Took)
	require.Equal(t, "shadeform", ended.Provider)
	require.Equal(t, "us-east", ended.Geo)
	require.Equal(t, "A10G", ended.GPU)
	require.True(t, ended.Spot)
	require.Equal(t, int64(1_000_000), ended.HourlyMicros)
}

// eventsOf is the outcomes of one event, in the order they were written.
func eventsOf(outcomes []watch.Outcome, event string) []watch.Outcome {
	var of []watch.Outcome
	for _, outcome := range outcomes {
		if outcome.Event == event {
			of = append(of, outcome)
		}
	}
	return of
}

// A purchase is written to the market's history when it is made, before
// the launch it is part of is known to succeed. A machine that the launch
// gives back when it fails is written as given back: the history has no
// machine that was bought and never ended.
func TestAMachineThatAFailedLaunchGivesBackHasAnEndInTheMarketsHistory(t *testing.T) {
	shadeform := fakeProvider("shadeform", a10gOffer)
	shadeform.RefuseCreatesAfter(1, fmt.Errorf("%w: none left in us-east", providers.ErrOutOfStock))
	service := launchService(&fakeComputeRepo{}, shadeform)
	log := recordOutcomes(service)

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), service, 2)
	require.False(t, res.Ok)
	require.Empty(t, shadeform.Everything(), "the machine that was bought is deleted again")

	outcomes := outcomesOf(t, service, log)
	purchases := eventsOf(outcomes, watch.OutcomeCreate)
	require.Len(t, purchases, 2)
	require.Equal(t, watch.ResultOK, purchases[0].Result)
	require.Equal(t, watch.ResultOutOfStock, purchases[1].Result)

	ends := eventsOf(outcomes, watch.OutcomeEnd)
	require.Len(t, ends, 1, "the machine that was bought ended, and the one that was refused never was")
	ended := ends[0]
	require.Equal(t, watch.ResultReleased, ended.Result, "it is not held against its provider")
	require.Equal(t, watch.SourceReservation, ended.Source)
	require.GreaterOrEqual(t, ended.Took, time.Duration(0))
	ended.At, ended.Event, ended.Result, ended.Took = purchases[0].At, purchases[0].Event, purchases[0].Result, purchases[0].Took
	require.Equal(t, purchases[0], ended, "it says the same of what the machine was bought as")
}

// A machine whose purchase went unanswered was made all the same, and is
// given back like any other.
func TestAMachineWhosePurchaseWentUnansweredHasAnEndOnceItIsGivenBack(t *testing.T) {
	shadeform := fakeProvider("shadeform", a10gOffer)
	shadeform.LoseAnswers(1)
	service := launchService(&fakeComputeRepo{}, shadeform)
	log := recordOutcomes(service)

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), service, 1)
	require.False(t, res.Ok)
	require.Len(t, shadeform.Deletes(), 1)

	outcomes := outcomesOf(t, service, log)
	require.Equal(t, watch.ResultUnanswered, eventsOf(outcomes, watch.OutcomeCreate)[0].Result)
	ends := eventsOf(outcomes, watch.OutcomeEnd)
	require.Len(t, ends, 1)
	require.Equal(t, watch.ResultReleased, ends[0].Result)
}

// A machine that a failed launch cannot give back is left to the
// reconciler, which writes its end when it has deleted it: once.
func TestAMachineAFailedLaunchCouldNotGiveBackEndsWhenItIsDeleted(t *testing.T) {
	shadeform := fakeProvider("shadeform", a10gOffer)
	shadeform.RefuseCreatesAfter(1, fmt.Errorf("%w: none left in us-east", providers.ErrOutOfStock))
	shadeform.FailDeletes(errors.New("provider is down"))
	repo := &fakeComputeRepo{}
	service := launchService(repo, shadeform)
	log := recordOutcomes(service)

	res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), service, 2)
	require.False(t, res.Ok)
	require.Len(t, shadeform.Everything(), 1)
	require.Empty(t, eventsOf(outcomesOf(t, service, log), watch.OutcomeEnd), "it is still there")

	shadeform.FailDeletes(nil)
	left := repo.pools["workspace-1"][0].Reservations[0]
	require.NoError(t, service.reconcileManagedComputeAt(context.Background(), left.CreatedAt.Add(time.Minute)))
	require.NoError(t, service.reconcileManagedComputeAt(context.Background(), left.CreatedAt.Add(2*time.Minute)))
	require.Empty(t, shadeform.Everything())

	ends := eventsOf(outcomesOf(t, service, log), watch.OutcomeEnd)
	require.Len(t, ends, 1)
	require.Equal(t, watch.ResultReleased, ends[0].Result)
	require.Equal(t, time.Minute, ends[0].Took)
}

// A purchase that its provider never made a machine for has nothing to end,
// whether that is known at once or only after the provider has had its
// time to list one.
func TestAPurchaseThatMadeNoMachineHasNoEndInTheMarketsHistory(t *testing.T) {
	p := launchUnanswered(t)
	log := recordOutcomes(p.service)

	require.Equal(t, model.ReservationDeleted, p.reconcile(t, providers.LaunchGrace).Status)
	require.Empty(t, p.provider.Deletes(), "the provider made none")
	require.Empty(t, outcomesOf(t, p.service, log))
}

func TestAMachineATenantGivesBackIsNotHeldAgainstItsProvider(t *testing.T) {
	for reason, result := range map[string]string{
		reconcileReasonInterrupted:        watch.ResultInterrupted,
		reconcileReasonProviderFailed:     watch.ResultFailed,
		reconcileReasonReservationExpired: watch.ResultExpired,
		reconcileReasonMachineReleased:    watch.ResultReleased,
		reconcileReasonLaunchFailed:       watch.ResultReleased,
		reconcileReasonCreditExhausted:    watch.ResultReleased,
	} {
		require.Equal(t, result, reservationEnd(reason), reason)
	}
}
