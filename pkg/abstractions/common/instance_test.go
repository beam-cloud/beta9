package abstractions

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
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
	instance, containerRepo, _ := newTestAutoscaledInstance(t, false, nil)
	seedTestContainer(t, containerRepo, types.ContainerStatusRunning)
	stopped := make(chan int, 1)
	instance.StopContainersFunc = func(containersToStop int) error {
		stopped <- containersToStop
		return nil
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

// A stopping container remains the writer of a writable durable disk until its
// final snapshot is published, so its replacement starts only once it is gone.
func TestHandleScalingEventWaitsForStoppingDiskWriter(t *testing.T) {
	instance, containerRepo, _ := newTestAutoscaledInstance(t, true, []*pb.DurableDisk{{Name: "home"}})
	writer := seedTestContainer(t, containerRepo, types.ContainerStatusStopping)
	started := countStarts(t, instance)

	if err := instance.HandleScalingEvent(1); err != nil {
		t.Fatal(err)
	}
	if *started != 0 {
		t.Fatalf("started %d containers beside the stopping writer, want 0", *started)
	}

	if err := containerRepo.DeleteContainerState(writer); err != nil {
		t.Fatal(err)
	}
	if err := instance.HandleScalingEvent(1); err != nil {
		t.Fatal(err)
	}
	if *started != 1 {
		t.Fatalf("started %d containers once the writer was gone, want 1", *started)
	}
}

func TestHandleScalingEventStartsBesideStoppingContainerWithoutWritableDisk(t *testing.T) {
	for _, tc := range []struct {
		name  string
		disks []*pb.DurableDisk
	}{
		{name: "read-only disk", disks: []*pb.DurableDisk{{Name: "models", ReadOnly: true}}},
		{name: "no disk"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instance, containerRepo, _ := newTestAutoscaledInstance(t, true, tc.disks)
			seedTestContainer(t, containerRepo, types.ContainerStatusStopping)
			started := countStarts(t, instance)

			if err := instance.HandleScalingEvent(1); err != nil {
				t.Fatal(err)
			}
			if *started != 1 {
				t.Fatalf("started %d containers, want 1", *started)
			}
		})
	}
}

// An always-on app that crash-loops while its database restarts must start
// again once the database is back, though nothing sends it traffic.
func TestHandleScalingEventRetriesAlwaysOnDeploymentAfterFailureCooldown(t *testing.T) {
	instance, containerRepo, server := newTestAutoscaledInstance(t, true, nil)
	instance.FailedContainerThreshold = types.FailedDeploymentContainerThreshold
	seedFailedContainers(t, containerRepo, instance.FailedContainerThreshold)
	events := &stubStateEvents{states: make(chan string, 1)}
	instance.EventRepo = events
	started := countStarts(t, instance)

	if err := instance.HandleScalingEvent(1); err != nil {
		t.Fatal(err)
	}
	if instance.Ctx.Err() != nil {
		t.Fatal("cancelled the always-on instance during its failure cooldown")
	}
	if *started != 0 {
		t.Fatalf("started %d containers during the failure cooldown, want 0", *started)
	}
	select {
	case state := <-events.states:
		if state != types.StubStateDegraded {
			t.Fatalf("stub state = %q, want %q", state, types.StubStateDegraded)
		}
	case <-time.After(time.Second):
		t.Fatal("expected the failing deployment to report itself degraded")
	}

	server.FastForward(types.ContainerFailureCooldown)
	if err := instance.HandleScalingEvent(1); err != nil {
		t.Fatal(err)
	}
	if *started != 1 {
		t.Fatalf("started %d containers after the failure cooldown, want 1", *started)
	}
}

func TestHandleScalingEventStopsFailingInstanceWithoutMinimum(t *testing.T) {
	for _, tc := range []struct {
		name          string
		active        bool
		minContainers uint
	}{
		{name: "scale to zero", active: true, minContainers: 0},
		{name: "inactive", active: false, minContainers: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instance, containerRepo, _ := newTestAutoscaledInstance(t, tc.active, nil)
			instance.StubConfig.Autoscaler.MinContainers = tc.minContainers
			instance.FailedContainerThreshold = types.FailedDeploymentContainerThreshold
			seedFailedContainers(t, containerRepo, instance.FailedContainerThreshold)
			started := countStarts(t, instance)

			if err := instance.HandleScalingEvent(1); err != nil {
				t.Fatal(err)
			}
			if instance.Ctx.Err() == nil {
				t.Fatal("expected the idle failing instance to stop")
			}
			if *started != 0 {
				t.Fatalf("started %d containers, want 0", *started)
			}
		})
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

	historical := backendRepo.deployments[0]
	historical.Active = false
	backendRepo.deployments = append(backendRepo.deployments, historical)
	instance.IsActive = false
	if err := instance.Sync(); err != nil {
		t.Fatal(err)
	}
	if !instance.IsActive {
		t.Fatal("expected rollback to reactivate a stub with an inactive historical deployment")
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
	}, types.DeploymentWithRelated{
		Deployment: types.Deployment{Active: false},
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
	if len(instance.scalingEvents) != 0 {
		t.Fatalf("historical deployment stopped an active instance: %v", instance.scalingEvents)
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

// newTestRedis starts an in-memory Redis for one test and returns a client and
// a container repository on it.
func newTestRedis(t *testing.T) (*miniredis.Miniredis, *common.RedisClient, repository.ContainerRepository) {
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
	return server, rdb, repository.NewContainerRedisRepositoryForTest(rdb)
}

// newTestAutoscaledInstance builds the instance of a single-container pod
// deployment, "test-stub", on its own Redis.
func newTestAutoscaledInstance(t *testing.T, active bool, disks []*pb.DurableDisk) (*AutoscaledInstance, repository.ContainerRepository, *miniredis.Miniredis) {
	t.Helper()
	server, rdb, containerRepo := newTestRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &AutoscaledInstance{
		Ctx:             ctx,
		CancelFunc:      cancel,
		Lock:            common.NewRedisLock(rdb),
		InstanceLockKey: "test-instance-lock",
		IsActive:        active,
		Workspace:       &types.Workspace{ExternalId: "test-workspace"},
		Stub:            &types.StubWithRelated{Stub: types.Stub{ExternalId: "test-stub", Type: types.StubType(types.StubTypePodDeployment)}},
		StubConfig:      &types.StubConfigV1{Autoscaler: &types.Autoscaler{MinContainers: 1, MaxContainers: 1}, Disks: disks},
		ContainerRepo:   containerRepo,
	}, containerRepo, server
}

// seedFailedContainers records count containers of "test-stub" that exited
// with an error.
func seedFailedContainers(t *testing.T, containerRepo repository.ContainerRepository, count int) {
	t.Helper()
	for n := range count {
		containerId := fmt.Sprintf("pod-test-stub-failed-%d", n)
		state := &types.ContainerState{
			ContainerId: containerId,
			StubId:      "test-stub",
			WorkspaceId: "test-workspace",
			Status:      types.ContainerStatusRunning,
			ScheduledAt: time.Now().Unix(),
			Cpu:         100,
			Memory:      128,
		}
		if err := containerRepo.SetContainerState(containerId, state); err != nil {
			t.Fatal(err)
		}
		if err := containerRepo.SetContainerExitCode(containerId, int(types.ContainerExitCodeUnknownError)); err != nil {
			t.Fatal(err)
		}
		if err := containerRepo.DeleteContainerState(containerId); err != nil {
			t.Fatal(err)
		}
	}
}

// stubStateEvents records the unhealthy states an instance reports for its stub.
type stubStateEvents struct {
	repository.EventRepository
	states chan string
}

func (e *stubStateEvents) PushStubStateUnhealthy(workspaceId, stubId, currentState, previousState, reason string, failedContainers []string) {
	e.states <- currentState
}

// seedTestContainer records a container of "test-stub" in the given status.
func seedTestContainer(t *testing.T, containerRepo repository.ContainerRepository, status types.ContainerStatus) string {
	t.Helper()
	state := &types.ContainerState{
		ContainerId: "pod-test-stub-00000000",
		StubId:      "test-stub",
		WorkspaceId: "test-workspace",
		Status:      status,
		ScheduledAt: time.Now().Unix(),
		StartedAt:   time.Now().Unix(),
		Cpu:         100,
		Memory:      128,
	}
	if err := containerRepo.SetContainerState(state.ContainerId, state); err != nil {
		t.Fatal(err)
	}
	return state.ContainerId
}

// countStarts records the containers the instance starts; it must not stop any.
func countStarts(t *testing.T, instance *AutoscaledInstance) *int {
	started := new(int)
	instance.StartContainersFunc = func(containersToStart int) error {
		*started += containersToStart
		return nil
	}
	instance.StopContainersFunc = func(int) error {
		t.Error("a stopping container must not cause another to stop")
		return nil
	}
	return started
}

func newTestInstanceController(t *testing.T, deployments ...types.DeploymentWithRelated) (*testInstanceController, repository.ContainerRepository) {
	t.Helper()
	_, rdb, containerRepo := newTestRedis(t)
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
