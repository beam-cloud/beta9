package abstractions

import (
	"context"
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
