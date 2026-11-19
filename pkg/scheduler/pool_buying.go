package scheduler

import (
	"errors"
	"slices"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// A pool with a provisioning block buys its machines. This is how it comes
// to buy one, from a request that finds no room to a machine for it:
//
//  1. The request buys nothing. It tells the pool that it waits for a
//     machine (want), on every pass for as long as it waits, and takes that
//     back when it is placed or fails (unwant). What it said lapses once it
//     stops saying it, so nothing is bought for a request that is gone.
//  2. Once a second, on one replica, the pool's sizer has the pool plan. The
//     pool puts the requests that wait into the room it has and has coming,
//     and is short of a machine for those that fit none of it, and for its
//     floor (shortfall).
//  3. The provisioner works out which machines are best to buy for all of
//     that together, and buys the first (pkg/provisioner/solve.go). The pool
//     counts that machine's room and asks again, until it is short of
//     nothing or can buy nothing.
//  4. A machine has a Pending worker from the moment it is bought
//     (buyMachine). A request waits for it as for any worker that is
//     starting, and goes on saying that it waits: the plan then counts it
//     into the room that is coming, and no other request with it.
//  5. A pool may buy machines that their provider can take back, and ones
//     that it cannot. Work that may not be interrupted waits for the second
//     kind (MachineNeed.OnDemand). The rest runs where the least is lost
//     with a machine, when there is a choice (scoredWorker.offKind).

// buyer returns the controller of a pool that buys its machines, and nil for
// any other pool.
func buyer(controller WorkerPoolController) *AgentWorkerPoolController {
	if wpc, ok := controller.(*AgentWorkerPoolController); ok && wpc.workerPoolConfig.Buys() && wpc.provisioner != nil {
		return wpc
	}
	return nil
}

// machineFor returns the controller of a pool that buys its machines, and
// the machine it has to have for the request: one with room for it, of a
// kind it may run on.
func (s *Scheduler) machineFor(pool *WorkerPool, request *types.ContainerRequest) (*AgentWorkerPoolController, MachineNeed) {
	controller := buyer(pool.Controller)
	if controller == nil {
		return nil, MachineNeed{}
	}
	size := s.workerSizeFor(pool, request)
	need := controller.machineNeed(size.cpu, size.memory, size.gpuCount)
	need.OnDemand, need.HoursLost = !request.Preemptable, hoursLost(request)
	return controller, need
}

// hoursLost is how many hours of work are lost when the machine a request
// runs on is taken back. It decides how much cheaper a spot machine has to
// be to be bought for the request, and which workers the request prefers.
func hoursLost(request *types.ContainerRequest) float64 {
	switch kind := request.Stub.Type.Kind(); {
	case request.IsBuildRequest(), request.HasDurableDiskMount():
	case kind == types.StubTypeEndpoint, kind == types.StubTypeASGI, string(request.Stub.Type) == types.StubTypePodDeployment:
		return 0.1 // another replica starts
	case kind == types.StubTypeFunction, kind == types.StubTypeTaskQueue, kind == types.StubTypeScheduledJob:
		return 0.5 // the task runs again
	}
	// Nobody runs a sandbox, a session or a build again: a working day.
	return 8
}

// canAddWorker reports whether the pool can add a worker of this size for
// the request. A pool that buys its machines can when it can buy one of a
// kind the request may run on.
func (s *Scheduler) canAddWorker(pool *WorkerPool, request *types.ContainerRequest, size workerSize) (bool, error) {
	if controller, need := s.machineFor(pool, request); controller != nil {
		return controller.provisioner.CanProvision(s.ctx, controller.poolName(), need), nil
	}
	return pool.Controller.CanAddWorker(size.cpu, size.memory, size.gpuCount)
}

// want tells a pool that buys its machines that the request waits for one.
// The request says so on every pass for as long as it waits, for a machine
// to be bought and then for it to boot. It may ask such a pool as often as
// it likes.
func (s *Scheduler) want(request *types.ContainerRequest, pool *WorkerPool) {
	controller, need := s.machineFor(pool, request)
	if controller == nil {
		return
	}
	if err := controller.provisioner.Want(s.ctx, controller.poolName(), request.ContainerId, need); err != nil {
		requestLog(log.Warn(), request).Str("pool_name", pool.Name).Err(err).Msg("unable to tell the pool that the request waits for a machine")
	}
}

// unwant takes back what want said, once the request is placed or fails.
func (s *Scheduler) unwant(request *types.ContainerRequest) {
	if s.provisioner == nil {
		return
	}
	if err := s.provisioner.Unwant(s.ctx, request.ContainerId); err != nil {
		requestLog(log.Warn(), request).Err(err).Msg("unable to tell the pool that the request no longer waits for a machine")
	}
}

// plan buys the machines the pool is short of. The pool's sizer runs it, on
// one replica at a time, with every request that waits for the pool, its
// floor, and all it has and has coming in view. So requests that fit one
// machine share it, and replicas buy nothing twice.
func (wpc *AgentWorkerPoolController) plan(sizing *types.WorkerPoolSizingConfig) {
	pool := wpc.poolName()
	// Workers are read before wants: a request that is placed in between
	// has then given up its want, and is not bought for again.
	workers, err := wpc.workerRepo.GetAllWorkersInPool(pool)
	wants, wantsErr := wpc.provisioner.Wants(wpc.ctx, pool)
	booting, bootingErr := wpc.provisioner.Booting(wpc.ctx, pool)
	if err := errors.Join(err, wantsErr, bootingErr); err != nil {
		log.Error().Str("pool_name", pool).Err(err).Msg("failed to plan the pool's machines")
		return
	}
	slices.SortFunc(wants, MachineNeed.Compare)
	// The room there is: the workers that take work, and that of each
	// machine that is coming, whatever has become of its Pending worker.
	rooms := slices.DeleteFunc(workers, func(worker *types.Worker) bool { return worker.Status != types.WorkerStatusAvailable })
	for _, machine := range booting {
		coming := wpc.bootingMachineWorker(machine)
		if !slices.ContainsFunc(rooms, func(room *types.Worker) bool { return room.Id == coming.Id }) {
			rooms = append(rooms, coming)
		}
	}

	// The provisioner works out what to buy for all the pool is short of,
	// and buys the first machine of that. The rest is worked out again with
	// that machine's room counted: what there is to buy changes as it is
	// bought.
	for {
		short := wpc.shortfall(wants, sizing, rooms)
		if len(short) == 0 || !wpc.provisioner.CanProvision(wpc.ctx, pool, short...) {
			return
		}
		worker, err := wpc.buyMachine(short...)
		if err != nil {
			log.Error().Str("pool_name", pool).Err(err).Msg("failed to add worker")
			return
		}
		rooms = append(rooms, worker)
	}
}

// shortfall returns what the pool lacks a machine for: each waiting request
// that fits no room once those before it have taken theirs, the hardest to
// fit first, and a worker of the default size when what is then left is
// under the floor.
//
// A request is taken to fit any worker with room for it. One that the
// scheduler will not place there for another reason waits until the worker
// fills up or leaves.
func (wpc *AgentWorkerPoolController) shortfall(wants []MachineNeed, sizing *types.WorkerPoolSizingConfig, rooms []*types.Worker) []MachineNeed {
	left := make([]*types.Worker, len(rooms))
	for i, room := range rooms {
		worker := *room
		worker.Status = types.WorkerStatusAvailable // one that is coming counts in full
		left[i] = &worker
	}

	var short []MachineNeed
	for _, want := range wants {
		cpu := wpc.workerPoolConfig.CPUShareMultiplier.Schedulable(want.CPU)
		i := slices.IndexFunc(left, func(room *types.Worker) bool {
			return room.FreeCpu >= cpu && room.FreeMemory >= want.Memory && room.FreeGpuCount >= want.GPUCount &&
				!(want.OnDemand && room.Preemptable)
		})
		if i < 0 {
			short = append(short, want)
			continue
		}
		left[i].FreeCpu, left[i].FreeMemory, left[i].FreeGpuCount = left[i].FreeCpu-cpu, left[i].FreeMemory-want.Memory, left[i].FreeGpuCount-want.GPUCount
	}
	if poolCapacityFromWorkers(left).belowMinimum(sizing) {
		short = append(short, wpc.machineNeed(sizing.DefaultWorkerCpu, sizing.DefaultWorkerMemory, sizing.DefaultWorkerGpuCount))
	}
	return short
}
