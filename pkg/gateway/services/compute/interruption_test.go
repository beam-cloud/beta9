package compute

import (
	"context"
	"errors"
	"testing"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

// interruptionFixture is a gateway with one machine that has joined a
// managed pool, on real repositories.
type interruptionFixture struct {
	service    *Service
	workerRepo repository.WorkerRepository
	events     *capturedEvents
	machine    *model.AgentTokenState
	workerID   string
}

const interruptedAgentToken = "agent-token"

// newInterruptionFixture has a machine that its pool, spot-l40s, bought.
func newInterruptionFixture(t *testing.T) *interruptionFixture {
	t.Helper()
	return interruptionFixtureIn(t, "spot-l40s", spotPool())
}

// interruptionFixtureIn has a machine in a managed pool of this name and
// config.
func interruptionFixtureIn(t *testing.T, name string, config types.WorkerPoolConfig) *interruptionFixture {
	t.Helper()
	ctx := context.Background()
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	config, err = normalizeManagedPoolConfig(config)
	require.NoError(t, err)
	pool := newManagedPoolState("admin", name, types.WorkerPoolManagementSourceAPI, "operator-token", config, time.Now())

	f := &interruptionFixture{
		workerRepo: repository.NewWorkerRedisRepositoryForTest(rdb),
		events:     &capturedEvents{},
		machine: &model.AgentTokenState{
			TokenHash: hashComputeToken(interruptedAgentToken), WorkspaceID: "admin", PoolName: name, MachineID: "machine-1",
			Mode: string(types.PoolModeExternal), ManagedPoolInstanceID: pool.ManagedInstanceID,
			Schedulable: true, LastHeartbeatAt: time.Now(),
		},
	}
	f.workerID = model.AgentMachineWorkerID(f.machine.MachineID)
	f.service = &Service{
		workerRepo:      f.workerRepo,
		computeRepo:     repository.NewComputeRedisRepository(rdb),
		managedPoolRepo: repository.NewManagedPoolRedisRepository(rdb),
		eventRepo:       f.events,
	}
	require.NoError(t, f.service.managedPoolRepo.SaveManagedPoolState(ctx, "admin", pool))
	require.NoError(t, f.service.saveComputeAgentTokenState(ctx, f.machine))
	return f
}

func (f *interruptionFixture) addWorker(t *testing.T) {
	t.Helper()
	require.NoError(t, f.workerRepo.AddWorker(&types.Worker{
		Id: f.workerID, WorkspaceId: f.machine.WorkspaceID, MachineId: f.machine.MachineID, PoolName: f.machine.PoolName,
		Status: types.WorkerStatusAvailable, TotalCpu: 8000, FreeCpu: 8000, TotalMemory: 16000, FreeMemory: 16000,
	}))
}

func (f *interruptionFixture) report(t *testing.T, in *pb.ReportAgentInterruptionRequest) *pb.ReportAgentInterruptionResponse {
	t.Helper()
	response, err := f.service.ReportAgentInterruption(context.Background(), in)
	require.NoError(t, err)
	return response
}

// cordoned reports what the gateway has on record for the machine.
func (f *interruptionFixture) cordoned(t *testing.T) bool {
	t.Helper()
	machine, err := f.service.computeRepo.GetAgentMachineState(context.Background(), f.machine.WorkspaceID, f.machine.PoolName, f.machine.MachineID)
	require.NoError(t, err)
	require.NotNil(t, machine)
	return machine.Cordoned
}

// A machine that was bought, and that its provider is about to take back,
// gets no new work: its worker is cordoned, and so is the machine, so that a
// worker which is started again does not undo it.
func TestAMachineGivenNoticeIsCordoned(t *testing.T) {
	f := newInterruptionFixture(t)
	f.addWorker(t)
	observedAt := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)

	response := f.report(t, &pb.ReportAgentInterruptionRequest{
		AgentToken:         interruptedAgentToken,
		Source:             "aws",
		Detail:             "terminate",
		ObservedAtUnixNano: observedAt.UnixNano(),
		ExpectedAtUnixNano: observedAt.Add(2 * time.Minute).UnixNano(),
	})
	require.True(t, response.Ok, response.ErrMsg)

	require.True(t, f.cordoned(t))
	worker, err := f.workerRepo.GetWorkerById(f.workerID)
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusDisabled, worker.Status)
	require.True(t, worker.CordonRequested)

	require.Len(t, f.events.events, 1)
	event := f.events.events[0]
	require.Equal(t, types.EventComputeActionMachineInterruptionNotice, event.Action)
	require.Equal(t, "machine-1", event.MachineID)
	require.Equal(t, "spot-l40s", event.PoolName)
	require.Equal(t, "admin", event.WorkspaceID)
	require.Equal(t, observedAt, event.Timestamp)
	require.Equal(t, "the machine's provider is about to take it back: terminate (at 2026-11-13T10:02:00Z)", event.Message)
	require.Equal(t, map[string]string{"notice_source": "aws"}, event.Attrs)
	require.NotNil(t, event.Schedulable)
	require.False(t, *event.Schedulable)
}

// A machine that was joined by hand is not the gateway's to give back: its
// cloud may stop it and start it again, and nothing would lift a cordon. The
// notice is passed on and the machine is left as it is.
func TestAMachineJoinedByHandIsNotCordonedByANotice(t *testing.T) {
	tests := []struct {
		name    string
		fixture func(t *testing.T) *interruptionFixture
	}{
		{"in a managed pool that does not buy", func(t *testing.T) *interruptionFixture {
			return interruptionFixtureIn(t, "l40s", types.WorkerPoolConfig{GPUType: "L40S"})
		}},
		{"in a workspace's own pool", func(t *testing.T) *interruptionFixture {
			f := newInterruptionFixture(t)
			f.machine.WorkspaceID, f.machine.PoolName, f.machine.ManagedPoolInstanceID = "workspace-1", "own-pool", ""
			require.NoError(t, f.service.saveComputeAgentTokenState(context.Background(), f.machine))
			return f
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := test.fixture(t)
			f.addWorker(t)

			response := f.report(t, &pb.ReportAgentInterruptionRequest{AgentToken: interruptedAgentToken, Source: "aws", Detail: "stop"})
			require.True(t, response.Ok, response.ErrMsg)

			require.False(t, f.cordoned(t))
			worker, err := f.workerRepo.GetWorkerById(f.workerID)
			require.NoError(t, err)
			require.Equal(t, types.WorkerStatusAvailable, worker.Status)
			require.False(t, worker.CordonRequested)

			require.Len(t, f.events.events, 1)
			event := f.events.events[0]
			require.Equal(t, types.EventComputeActionMachineInterruptionNotice, event.Action)
			require.Equal(t, f.machine.PoolName, event.PoolName)
			require.Equal(t, "the machine's provider is about to take it back: stop", event.Message)
			require.Nil(t, event.Schedulable, "the notice changed nothing about the machine")
		})
	}
}

// Whether a machine was bought cannot be told by a replica that cannot load
// its pool. The notice is not acknowledged, so the agent tries again and
// may reach a replica that can.
func TestNoticeForAMachineWhosePoolCannotBeLoadedIsNotAcknowledged(t *testing.T) {
	f := newInterruptionFixture(t)
	f.addWorker(t)
	// This replica's config file defines a pool that was made through the
	// API, which it refuses.
	f.service.appConfig.Worker.Pools = map[string]types.WorkerPoolConfig{"spot-l40s": spotPool()}

	response := f.report(t, &pb.ReportAgentInterruptionRequest{AgentToken: interruptedAgentToken, Source: "aws"})
	require.False(t, response.Ok)
	require.Contains(t, response.ErrMsg, `API-managed pool "spot-l40s" conflicts with config`)
	require.False(t, f.cordoned(t))
	require.Empty(t, f.events.events)
}

// Notice can come before the machine has a worker. The machine keeps it, and
// the worker it gets starts out cordoned.
func TestAMachineGivenNoticeBeforeItHasAWorkerIsCordoned(t *testing.T) {
	f := newInterruptionFixture(t)

	response := f.report(t, &pb.ReportAgentInterruptionRequest{AgentToken: interruptedAgentToken, Source: "gcp"})
	require.True(t, response.Ok, response.ErrMsg)
	require.True(t, f.cordoned(t))
	require.Len(t, f.events.events, 1)
	require.Equal(t, "the machine's provider is about to take it back", f.events.events[0].Message)
	require.WithinDuration(t, time.Now(), f.events.events[0].Timestamp, time.Minute, "an agent that gave no time is taken to mean now")
}

// The agent may say so more than once: its first report can be lost on the
// way back.
func TestAMachineGivenNoticeTwiceStaysCordoned(t *testing.T) {
	f := newInterruptionFixture(t)
	f.addWorker(t)

	for range 2 {
		response := f.report(t, &pb.ReportAgentInterruptionRequest{AgentToken: interruptedAgentToken, Source: "aws"})
		require.True(t, response.Ok, response.ErrMsg)
	}
	require.True(t, f.cordoned(t))
	worker, err := f.workerRepo.GetWorkerById(f.workerID)
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusDisabled, worker.Status)
}

// Only the machine's own agent can say that the machine is going.
func TestNoticeFromAnUnknownAgentIsRefused(t *testing.T) {
	f := newInterruptionFixture(t)
	f.addWorker(t)

	for _, token := range []string{"", "another-token"} {
		response := f.report(t, &pb.ReportAgentInterruptionRequest{AgentToken: token, Source: "aws"})
		require.False(t, response.Ok)
		require.Equal(t, "invalid agent token", response.ErrMsg)
	}

	require.False(t, f.cordoned(t))
	worker, err := f.workerRepo.GetWorkerById(f.workerID)
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusAvailable, worker.Status)
	require.Empty(t, f.events.events)
}

// A cordon that cannot be recorded is not reported as done: the agent tries
// again.
func TestNoticeThatCannotBeRecordedIsNotAcknowledged(t *testing.T) {
	f := newInterruptionFixture(t)
	f.addWorker(t)
	f.service.workerRepo = failingCordonRepo{f.workerRepo}

	response := f.report(t, &pb.ReportAgentInterruptionRequest{AgentToken: interruptedAgentToken, Source: "aws"})
	require.False(t, response.Ok)
	require.Contains(t, response.ErrMsg, "redis is down")
	require.Empty(t, f.events.events)
}

type failingCordonRepo struct{ repository.WorkerRepository }

func (failingCordonRepo) SetWorkerCordon(string, bool) error { return errors.New("redis is down") }
