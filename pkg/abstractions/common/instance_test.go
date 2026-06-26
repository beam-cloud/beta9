package abstractions

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestConsumeScaleResultDoesNotBlockWhenChannelIsFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := &AutoscaledInstance{
		Ctx:            ctx,
		ScaleEventChan: make(chan int, 1),
		Stub:           &types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeEndpointDeployment)}},
		StubConfig:     &types.StubConfigV1{Autoscaler: &types.Autoscaler{}},
	}
	instance.ScaleEventChan <- 1

	done := make(chan struct{})
	go func() {
		defer close(done)
		instance.ConsumeScaleResult(&AutoscalerResult{DesiredContainers: 3, ResultValid: true})
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ConsumeScaleResult blocked on a full scale channel")
	}

	select {
	case got := <-instance.ScaleEventChan:
		if got != 3 {
			t.Fatalf("scale event = %d, want latest desired container count", got)
		}
	default:
		t.Fatal("expected latest scale event to be queued")
	}
}

func TestConsumeScaleResultHonorsPodDeploymentMinimum(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := &AutoscaledInstance{
		Ctx:            ctx,
		ScaleEventChan: make(chan int, 1),
		Stub:           &types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypePodDeployment)}},
		StubConfig:     &types.StubConfigV1{Autoscaler: &types.Autoscaler{MinContainers: 2}},
	}

	instance.ConsumeScaleResult(&AutoscalerResult{DesiredContainers: 0, ResultValid: true})

	select {
	case got := <-instance.ScaleEventChan:
		if got != 2 {
			t.Fatalf("scale event = %d, want min container count", got)
		}
	default:
		t.Fatal("expected scale event to be queued")
	}
}

func TestConsumeScaleResultLetsServeScaleToZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := &AutoscaledInstance{
		Ctx:            ctx,
		ScaleEventChan: make(chan int, 1),
		Stub:           &types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeASGIServe)}},
		StubConfig:     &types.StubConfigV1{Autoscaler: &types.Autoscaler{MinContainers: 2}},
	}

	instance.ConsumeScaleResult(&AutoscalerResult{DesiredContainers: 0, ResultValid: true})

	select {
	case got := <-instance.ScaleEventChan:
		if got != 0 {
			t.Fatalf("scale event = %d, want serve to scale to zero", got)
		}
	default:
		t.Fatal("expected scale event to be queued")
	}
}

func TestHandleScalingEventInactiveStopsRunningContainers(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		t.Fatal(err)
	}

	containerRepo := repository.NewContainerRedisRepositoryForTest(rdb)
	state := &types.ContainerState{
		ContainerId: "pod-test-stub-00000000",
		StubId:      "test-stub",
		WorkspaceId: "test-workspace",
		Status:      types.ContainerStatusRunning,
		ScheduledAt: time.Now().Unix(),
		StartedAt:   time.Now().Unix(),
		Cpu:         100,
		Memory:      128,
	}
	if err := containerRepo.SetContainerState(state.ContainerId, state); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopped := make(chan int, 1)
	instance := &AutoscaledInstance{
		Ctx:             ctx,
		CancelFunc:      cancel,
		Lock:            common.NewRedisLock(rdb),
		InstanceLockKey: "test-instance-lock",
		IsActive:        false,
		Stub:            &types.StubWithRelated{Stub: types.Stub{ExternalId: state.StubId, Type: types.StubType(types.StubTypePodDeployment)}},
		StubConfig:      &types.StubConfigV1{Autoscaler: &types.Autoscaler{MinContainers: 1, MaxContainers: 1}},
		ContainerRepo:   containerRepo,
		StopContainersFunc: func(containersToStop int) error {
			stopped <- containersToStop
			return nil
		},
	}

	if err := instance.HandleScalingEvent(1); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-stopped:
		if got != 1 {
			t.Fatalf("containersToStop = %d, want 1", got)
		}
	default:
		t.Fatal("expected inactive instance to stop running container")
	}
}

func TestSyncMirrorsDeploymentActiveState(t *testing.T) {
	stubConfig, err := json.Marshal(&types.StubConfigV1{
		Autoscaler: &types.Autoscaler{MaxContainers: 1, TasksPerContainer: 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	workspace := types.Workspace{Id: 1, Name: "workspace"}
	stub := types.Stub{
		ExternalId:  "stub",
		Type:        types.StubType(types.StubTypePodDeployment),
		Config:      string(stubConfig),
		WorkspaceId: workspace.Id,
	}
	backendRepo := &testInstanceControllerBackendRepo{
		deployments: []types.DeploymentWithRelated{{
			Deployment: types.Deployment{Active: true},
			Workspace:  workspace,
			Stub:       stub,
		}},
	}

	instance := &AutoscaledInstance{
		Ctx:         context.Background(),
		IsActive:    false,
		BackendRepo: backendRepo,
		Stub:        &types.StubWithRelated{Stub: stub, Workspace: workspace},
		StubConfig:  &types.StubConfigV1{},
	}

	if err := instance.Sync(); err != nil {
		t.Fatal(err)
	}
	if !instance.IsActive {
		t.Fatal("expected active deployment to reactivate the instance")
	}

	backendRepo.deployments[0].Active = false
	if err := instance.Sync(); err != nil {
		t.Fatal(err)
	}
	if instance.IsActive {
		t.Fatal("expected inactive deployment to deactivate the instance")
	}
}

func TestInstanceControllerSkipsInactiveDeploymentWithoutInstanceOrContainers(t *testing.T) {
	controller, _ := newTestInstanceController(t, types.DeploymentWithRelated{
		Deployment: types.Deployment{Active: false},
		Stub:       types.Stub{ExternalId: "stub-inactive", Type: types.StubType(types.StubTypePodDeployment)},
	})

	if err := controller.Load(&types.DeploymentFilter{ShowDeleted: true}); err != nil {
		t.Fatal(err)
	}

	if controller.created != 0 {
		t.Fatalf("created instances = %d, want 0", controller.created)
	}
}

func TestInstanceControllerDeactivatesExistingInactiveDeployment(t *testing.T) {
	instance := &testAutoscaledInstance{}
	controller, _ := newTestInstanceController(t, types.DeploymentWithRelated{
		Deployment: types.Deployment{Active: false},
		Stub:       types.Stub{ExternalId: "stub-inactive", Type: types.StubType(types.StubTypePodDeployment)},
	})
	controller.instances["stub-inactive"] = instance

	if err := controller.Load(&types.DeploymentFilter{ShowDeleted: true}); err != nil {
		t.Fatal(err)
	}

	if controller.created != 0 {
		t.Fatalf("created instances = %d, want 0", controller.created)
	}
	if instance.syncs != 1 {
		t.Fatalf("syncs = %d, want 1", instance.syncs)
	}
	if len(instance.scalingEvents) != 1 || instance.scalingEvents[0] != 0 {
		t.Fatalf("scaling events = %v, want [0]", instance.scalingEvents)
	}
}

func TestInstanceControllerSyncsActiveDeploymentAndQueuesScale(t *testing.T) {
	controller, _ := newTestInstanceController(t, types.DeploymentWithRelated{
		Deployment: types.Deployment{Active: true},
		Stub:       types.Stub{ExternalId: "stub-active", Type: types.StubType(types.StubTypePodDeployment)},
	})

	if err := controller.Load(&types.DeploymentFilter{ShowDeleted: true}); err != nil {
		t.Fatal(err)
	}

	if controller.created != 1 {
		t.Fatalf("created instances = %d, want 1", controller.created)
	}

	instance := controller.instances["stub-active"]
	if instance.syncs != 1 {
		t.Fatalf("syncs = %d, want 1", instance.syncs)
	}
	if len(instance.scaleResults) != 1 || instance.scaleResults[0] != 0 {
		t.Fatalf("scale results = %v, want [0]", instance.scaleResults)
	}
}

func TestInstanceControllerScaleToZeroPreservesRunningDeploymentContainers(t *testing.T) {
	config := &types.StubConfigV1{
		Autoscaler: &types.Autoscaler{
			Type:              types.QueueDepthAutoscaler,
			MinContainers:     0,
			MaxContainers:     2,
			TasksPerContainer: 1,
		},
	}
	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}

	controller, containerRepo := newTestInstanceController(t, types.DeploymentWithRelated{
		Deployment: types.Deployment{Active: true},
		Stub: types.Stub{
			ExternalId: "stub-scale-to-zero",
			Type:       types.StubType(types.StubTypePodDeployment),
			Config:     string(configBytes),
		},
	})

	for _, containerID := range []string{"pod-stub-scale-to-zero-00000000", "pod-stub-scale-to-zero-11111111"} {
		state := &types.ContainerState{
			ContainerId: containerID,
			StubId:      "stub-scale-to-zero",
			WorkspaceId: "workspace",
			Status:      types.ContainerStatusRunning,
			ScheduledAt: time.Now().Unix(),
			StartedAt:   time.Now().Unix(),
			Cpu:         100,
			Memory:      128,
		}
		if err := containerRepo.SetContainerState(state.ContainerId, state); err != nil {
			t.Fatal(err)
		}
	}

	if err := controller.Load(&types.DeploymentFilter{ShowDeleted: true}); err != nil {
		t.Fatal(err)
	}

	instance := controller.instances["stub-scale-to-zero"]
	if len(instance.scaleResults) != 1 || instance.scaleResults[0] != 2 {
		t.Fatalf("scale results = %v, want [2]", instance.scaleResults)
	}
}

func TestInstanceControllerCreatesInactiveDeploymentOnlyForStaleContainers(t *testing.T) {
	controller, containerRepo := newTestInstanceController(t, types.DeploymentWithRelated{
		Deployment: types.Deployment{Active: false},
		Stub:       types.Stub{ExternalId: "stub-inactive", Type: types.StubType(types.StubTypePodDeployment)},
	})

	state := &types.ContainerState{
		ContainerId: "pod-stub-inactive-00000000",
		StubId:      "stub-inactive",
		WorkspaceId: "workspace",
		Status:      types.ContainerStatusRunning,
		ScheduledAt: time.Now().Unix(),
		StartedAt:   time.Now().Unix(),
		Cpu:         100,
		Memory:      128,
	}
	if err := containerRepo.SetContainerState(state.ContainerId, state); err != nil {
		t.Fatal(err)
	}

	if err := controller.Load(&types.DeploymentFilter{ShowDeleted: true}); err != nil {
		t.Fatal(err)
	}

	if controller.created != 1 {
		t.Fatalf("created instances = %d, want 1", controller.created)
	}

	instance := controller.instances["stub-inactive"]
	if instance.syncs != 1 {
		t.Fatalf("syncs = %d, want 1", instance.syncs)
	}
	if len(instance.scalingEvents) != 1 || instance.scalingEvents[0] != 0 {
		t.Fatalf("scaling events = %v, want [0]", instance.scalingEvents)
	}
}

func TestConsumeContainerEventDoesNotBlockWhenChannelIsFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := &AutoscaledInstance{
		Ctx:                ctx,
		ContainerEventChan: make(chan types.ContainerEvent, 1),
	}
	instance.ContainerEventChan <- types.ContainerEvent{ContainerId: "old", Change: 1}

	done := make(chan struct{})
	go func() {
		defer close(done)
		instance.ConsumeContainerEvent(types.ContainerEvent{ContainerId: "new", Change: 1})
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ConsumeContainerEvent blocked on a full event channel")
	}

	<-instance.ContainerEventChan

	select {
	case got := <-instance.ContainerEventChan:
		if got.ContainerId != "new" {
			t.Fatalf("container event = %q, want async queued event", got.ContainerId)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("async container event was not queued after channel space became available")
	}
}

type testInstanceController struct {
	*InstanceController
	created   int
	instances map[string]*testAutoscaledInstance
}

func newTestInstanceController(t *testing.T, deployments ...types.DeploymentWithRelated) (*testInstanceController, repository.ContainerRepository) {
	t.Helper()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		t.Fatal(err)
	}

	containerRepo := repository.NewContainerRedisRepositoryForTest(rdb)
	testController := &testInstanceController{instances: map[string]*testAutoscaledInstance{}}
	backendRepo := &testInstanceControllerBackendRepo{deployments: deployments}

	testController.InstanceController = NewInstanceController(
		context.Background(),
		func(ctx context.Context, stubId string, options ...func(IAutoscaledInstance)) (IAutoscaledInstance, error) {
			testController.created++
			instance := &testAutoscaledInstance{}
			testController.instances[stubId] = instance
			return instance, nil
		},
		func(stubId string) (IAutoscaledInstance, bool) {
			instance, exists := testController.instances[stubId]
			if !exists {
				return nil, false
			}
			return instance, true
		},
		[]string{types.StubTypePodDeployment},
		backendRepo,
		containerRepo,
		rdb,
	)

	return testController, containerRepo
}

type testInstanceControllerBackendRepo struct {
	repository.BackendRepository
	deployments []types.DeploymentWithRelated
}

func (r *testInstanceControllerBackendRepo) ListDeploymentsWithRelated(ctx context.Context, filters types.DeploymentFilter) ([]types.DeploymentWithRelated, error) {
	return r.deployments, nil
}

type testAutoscaledInstance struct {
	syncs         int
	scaleResults  []int
	scalingEvents []int
}

func (i *testAutoscaledInstance) ConsumeScaleResult(result *AutoscalerResult) {
	i.scaleResults = append(i.scaleResults, result.DesiredContainers)
}

func (i *testAutoscaledInstance) ConsumeContainerEvent(types.ContainerEvent) {}

func (i *testAutoscaledInstance) HandleScalingEvent(desiredContainers int) error {
	i.scalingEvents = append(i.scalingEvents, desiredContainers)
	return nil
}

func (i *testAutoscaledInstance) Sync() error {
	i.syncs++
	return nil
}
