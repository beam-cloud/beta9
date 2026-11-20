package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// A pool with a provisioning block buys its machines. This is how it comes
// to buy one, from a request that finds no room to a machine for it:
//
//  1. The request buys nothing. It joins the waitlist of the pool, on every
//     pass for as long as it waits (joinWaitlist), and leaves it when it is
//     placed or fails (leaveWaitlist). Its place lapses once it stops asking
//     for it, so nothing is bought for a request that is gone.
//  2. Once a second, on one replica, the pool's sizer has the pool plan. The
//     pool puts the requests on its waitlist into the room it has and has
//     coming, and is short of a machine for those that fit none of it, and
//     for its floor (shortfall).
//  3. The provisioner works out which machines are best to buy for all of
//     that together, and buys the first (pkg/provisioner/solve.go). The pool
//     counts that machine's room and asks again, until it is short of
//     nothing or can buy nothing.
//  4. A machine has a Pending worker from the moment it is bought
//     (buyMachine). A request waits for it as for any worker that is
//     starting, and stays on the waitlist: the plan then counts it into the
//     room that is coming, and no other request with it.
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

const (
	// waitlistLapse is how long a request keeps its place on the waitlist
	// after it last asked for it.
	waitlistLapse = 15 * time.Second
	// waitlistRefresh is how long a request leaves its place as it is. It
	// passes several times a second, and writes its place this often.
	waitlistRefresh = waitlistLapse / 3
)

// Waitlist is the requests that wait for a pool to buy a machine, and the
// machine each has to have. It is kept in Redis, by container ID: any
// replica may hear from a request, and one of them plans the pool.
type Waitlist struct {
	rdb   *common.RedisClient
	clock common.Clock
}

// NewWaitlist returns the waitlist of every scheduler on rdb. A nil clock
// is the wall clock.
func NewWaitlist(rdb *common.RedisClient, clock common.Clock) *Waitlist {
	return &Waitlist{rdb: rdb, clock: common.ClockOrReal(clock)}
}

// waiting is a request's place on the waitlist.
type waiting struct {
	Pool string      `json:"pool"`
	Need MachineNeed `json:"need"`
	At   time.Time   `json:"at"`
}

// Add has the request with this ID wait for the pool to have a machine that
// holds need, in place of what it waited for before.
func (w *Waitlist) Add(ctx context.Context, pool, id string, need MachineNeed) error {
	now := w.clock.Now()
	data, err := json.Marshal(waiting{Pool: pool, Need: need, At: now})
	if err != nil {
		return err
	}
	_, err = w.rdb.Pipelined(ctx, func(write redis.Pipeliner) error {
		write.HSet(ctx, common.RedisKeys.SchedulerWaitlist(), id, data)
		write.HSet(ctx, common.RedisKeys.SchedulerWaitlistPools(), pool, now.UnixMilli())
		return nil
	})
	return err
}

// Remove has the requests wait no longer.
func (w *Waitlist) Remove(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	return w.rdb.HDel(ctx, common.RedisKeys.SchedulerWaitlist(), ids...).Err()
}

// For returns what the requests that wait for the pool need. A request whose
// place has lapsed is forgotten: it was placed, has failed, or was given up.
//
// The waitlist is one for all pools. A pool that no request has joined for
// as long as a place lasts has nothing on it, and reads none of it.
func (w *Waitlist) For(ctx context.Context, pool string) ([]MachineNeed, error) {
	joined, err := w.rdb.HGet(ctx, common.RedisKeys.SchedulerWaitlistPools(), pool).Int64()
	if errors.Is(err, redis.Nil) || (err == nil && w.clock.Now().Sub(time.UnixMilli(joined)) >= waitlistLapse) {
		return nil, nil
	}
	stored, err := w.rdb.HGetAll(ctx, common.RedisKeys.SchedulerWaitlist()).Result()
	if err != nil {
		return nil, fmt.Errorf("read the requests that wait for a machine: %w", err)
	}
	var needs []MachineNeed
	var lapsed []string
	for id, data := range stored {
		var place waiting
		switch err := json.Unmarshal([]byte(data), &place); {
		case err != nil || w.clock.Now().Sub(place.At) >= waitlistLapse:
			lapsed = append(lapsed, id)
		case place.Pool == pool:
			needs = append(needs, place.Need)
		}
	}
	// Whoever reads next removes what this could not.
	_ = w.Remove(ctx, lapsed...)
	return needs, nil
}

// joinWaitlist puts the request on the waitlist of a pool that buys its
// machines. The request asks on every pass for as long as it waits, for a
// machine to be bought and then for it to boot. It may ask such a pool as
// often as it likes: it carries when it last did, and its place is written
// again only once that is waitlistRefresh ago, or for another pool.
func (s *Scheduler) joinWaitlist(request *types.ContainerRequest, pool *WorkerPool) {
	buying := buyer(pool.Controller)
	if buying == nil {
		return
	}
	name, now := buying.poolName(), s.now()
	if request.WaitlistPool == name && now.Sub(request.WaitlistedAt) < waitlistRefresh {
		return
	}
	need := buying.machineFor(request, s.workerSizeFor(pool, request))
	if err := s.waitlist.Add(s.ctx, name, request.ContainerId, need); err != nil {
		requestLog(log.Warn(), request).Str("pool_name", pool.Name).Err(err).Msg("unable to have the request wait for the pool to buy a machine")
		return
	}
	request.WaitlistPool, request.WaitlistedAt = name, now
}

// leaveWaitlist takes the requests off the waitlist, once they are placed or
// fail.
func (s *Scheduler) leaveWaitlist(requests ...*types.ContainerRequest) {
	ids := make([]string, len(requests))
	for i, request := range requests {
		ids[i] = request.ContainerId
		request.WaitlistPool, request.WaitlistedAt = "", time.Time{}
	}
	if err := s.waitlist.Remove(s.ctx, ids...); err != nil {
		log.Warn().Strs("container_ids", ids).Err(err).Msg("unable to take the requests off the waitlist for a machine")
	}
}

// canAddWorker reports whether the pool can add a worker of this size for
// the request. A pool that buys its machines can when it can buy one of a
// kind the request may run on.
func (s *Scheduler) canAddWorker(pool *WorkerPool, request *types.ContainerRequest, size workerSize) (bool, error) {
	if buying := buyer(pool.Controller); buying != nil {
		return buying.canBuyMachine(buying.machineFor(request, size)), nil
	}
	return pool.Controller.CanAddWorker(size.cpu, size.memory, size.gpuCount)
}

// machineFor is the machine the pool has to have for the request: one with
// room for a worker of this size, of a kind the request may run on.
func (wpc *AgentWorkerPoolController) machineFor(request *types.ContainerRequest, size workerSize) MachineNeed {
	need := wpc.machineNeed(size.cpu, size.memory, size.gpuCount)
	need.OnDemand, need.HoursLost = !request.Preemptable, hoursLost(request)
	return need
}

// replicaHoursLost is how many hours of work are lost when the machine under
// a replica of a managed endpoint is taken back: its model loads again for
// a quarter of an hour, and what it served waits or is turned away. A
// replica so goes where machines are kept before where they come up soonest.
const replicaHoursLost = 8

// hoursLost is how many hours of work are lost when the machine a request
// runs on is taken back. It decides how much cheaper a spot machine has to
// be to be bought for the request, and which workers the request prefers.
func hoursLost(request *types.ContainerRequest) float64 {
	switch kind := request.Stub.Type.Kind(); {
	case request.Stub.Type.IsManagedEndpoint():
		return replicaHoursLost
	case request.IsBuildRequest(), request.HasDurableDiskMount():
	case kind == types.StubTypeEndpoint, kind == types.StubTypeASGI, string(request.Stub.Type) == types.StubTypePodDeployment:
		return 0.1 // another replica starts
	case kind == types.StubTypeFunction, kind == types.StubTypeTaskQueue, kind == types.StubTypeScheduledJob:
		return 0.5 // the task runs again
	}
	// Nobody runs a sandbox, a session or a build again: a working day.
	return 8
}

// plan buys the machines the pool is short of. The pool's sizer runs it, on
// one replica at a time, with the pool's waitlist, its floor, and all it has
// and has coming in view. So requests that fit one machine share it, and
// replicas buy nothing twice.
func (wpc *AgentWorkerPoolController) plan(sizing *types.WorkerPoolSizingConfig) {
	pool := wpc.poolName()
	// Workers are read before the waitlist: a request that is placed in
	// between has then left it, and is not bought for again.
	workers, err := wpc.workerRepo.GetAllWorkersInPool(pool)
	waiting, waitingErr := wpc.waitlist.For(wpc.ctx, pool)
	booting, bootingErr := wpc.provisioner.Booting(wpc.ctx, pool)
	if err := errors.Join(err, waitingErr, bootingErr); err != nil {
		log.Error().Str("pool_name", pool).Err(err).Msg("failed to plan the pool's machines")
		return
	}
	slices.SortFunc(waiting, MachineNeed.Compare)
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
		short := wpc.shortfall(waiting, sizing, rooms)
		if len(short) == 0 || !wpc.canBuyMachine(short...) {
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
func (wpc *AgentWorkerPoolController) shortfall(waiting []MachineNeed, sizing *types.WorkerPoolSizingConfig, rooms []*types.Worker) []MachineNeed {
	left := make([]*types.Worker, len(rooms))
	for i, room := range rooms {
		worker := *room
		worker.Status = types.WorkerStatusAvailable // one that is coming counts in full
		left[i] = &worker
	}

	var short []MachineNeed
	for _, need := range waiting {
		cpu := wpc.workerPoolConfig.CPUShareMultiplier.Schedulable(need.CPU)
		i := slices.IndexFunc(left, func(room *types.Worker) bool {
			return room.FreeCpu >= cpu && room.FreeMemory >= need.Memory && room.FreeGpuCount >= need.GPUCount &&
				!(need.OnDemand && room.Preemptable)
		})
		if i < 0 {
			short = append(short, need)
			continue
		}
		left[i].FreeCpu, left[i].FreeMemory, left[i].FreeGpuCount = left[i].FreeCpu-cpu, left[i].FreeMemory-need.Memory, left[i].FreeGpuCount-need.GPUCount
	}
	if poolCapacityFromWorkers(left).belowMinimum(sizing) {
		short = append(short, wpc.machineNeed(sizing.DefaultWorkerCpu, sizing.DefaultWorkerMemory, sizing.DefaultWorkerGpuCount))
	}
	return short
}
