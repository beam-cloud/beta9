package managedendpoint

import (
	"cmp"
	"context"
	"fmt"
	"runtime/debug"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/common/llmroute"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// controller reconciles replicas against the registry and fleet.yaml. One
// gateway holds the lock at a time.

const (
	controllerLockKey  = "managed_endpoint:controller"
	controllerLockTTL  = 30 * time.Second
	terminalRetention  = 30 * time.Minute
	schedulingGrace    = 10 * time.Minute
	loadingGrace       = 30 * time.Minute
	containerLostGrace = 20 * time.Second
	maxStartsPerTick   = 8
)

type controller struct {
	s    *Service
	lock *common.RedisLock

	metricsMu   sync.Mutex
	lastMetrics map[string]llmroute.EngineMetrics // replica id -> previous scrape

	startedAt time.Time // heartbeats could not arrive before this; see silentFor
}

func newController(s *Service) *controller {
	return &controller{s: s, lock: common.NewRedisLock(s.rdb), lastMetrics: map[string]llmroute.EngineMetrics{}, startedAt: time.Now()}
}

func (c *controller) run(ctx context.Context) {
	ticker := time.NewTicker(c.s.config.Reconcile.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		err := c.lock.WithLease(ctx, controllerLockKey, common.RedisLockOptions{TtlS: int(controllerLockTTL.Seconds()), Retries: 0}, c.reconcile)
		if err != nil && !common.IsRedisLockNotObtained(err) {
			log.Error().Err(err).Msg("managed endpoints: reconcile failed")
		}
	}
}

// silentFor is how long a replica has gone without a heartbeat this gateway
// could have observed, so a gateway deploy does not fail every replica.
func (c *controller) silentFor(lastHeartbeat, now time.Time) time.Duration {
	if c.startedAt.After(lastHeartbeat) {
		lastHeartbeat = c.startedAt
	}
	return now.Sub(lastHeartbeat)
}

func (c *controller) reconcile(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("reconcile panicked: %v\n%s", r, debug.Stack())
		}
	}()

	replicas, err := c.s.repo.ListAllReplicas(ctx)
	if err != nil {
		return err
	}
	live := c.observeReplicas(ctx, replicas)
	inv, err := c.inventory(live)
	if err != nil {
		return err
	}
	endpoints, err := c.s.repo.ListEndpoints(ctx)
	if err != nil {
		return err
	}
	fleet, err := c.s.repo.GetFleet(ctx)
	if err != nil {
		return err
	}
	byID := make(map[string]*types.ManagedEndpoint, len(endpoints))
	for _, endpoint := range endpoints {
		byID[endpoint.Spec.ID] = endpoint
		c.retire(ctx, endpoint, fleet, live, inv)
	}
	for _, gpu := range fleet.GPUs() {
		c.fill(ctx, gpu, fleet.Entries(gpu), byID, live, inv)
	}
	return nil
}

type eligiblePool struct {
	Name     string
	Locality string
}

// clusterInventory is where replicas may run: opted-in pools per GPU key and
// the idle GPUs each has.
type clusterInventory struct {
	pools map[string][]eligiblePool
	free  map[string]map[string]uint32 // gpu key -> pool -> free GPUs
}

func (c *controller) poolConfig(name string) (types.WorkerPoolConfig, bool) {
	if cfg, ok := c.s.appConfig.Worker.Pools[name]; ok {
		return cfg, true
	}
	if c.s.scheduler != nil {
		return c.s.scheduler.PoolConfig(name)
	}
	return types.WorkerPoolConfig{}, false
}

func (c *controller) poolConfigs() map[string]types.WorkerPoolConfig {
	out := map[string]types.WorkerPoolConfig{}
	if c.s.scheduler != nil {
		for name, cfg := range c.s.scheduler.PoolConfigs() {
			out[name] = cfg
		}
	}
	for name, cfg := range c.s.appConfig.Worker.Pools {
		out[name] = cfg
	}
	return out
}

func localityOf(name string, cfg types.WorkerPoolConfig) string {
	return cmp.Or(strings.TrimSpace(cfg.Locality), name)
}

// inventory is the idle GPUs replicas may fill: free GPUs of ready, opted-in
// workers, less replicas still being scheduled (the worker has not reserved
// them yet) and less the pool's minFree* floor, which stays idle for
// serverless work. A pool with no CPU or memory above its floor has no room
// either, however many GPUs are idle. This snapshot guides placement; the
// scheduler enforces the same floor when it admits each replica.
func (c *controller) inventory(replicas []*types.EndpointReplica) (*clusterInventory, error) {
	workers, err := c.s.workers.GetAllWorkers()
	if err != nil {
		return nil, err
	}
	inv := &clusterInventory{pools: map[string][]eligiblePool{}, free: map[string]map[string]uint32{}}
	type slack struct{ cpu, memory int64 }
	ready := map[string]*slack{}
	for name, cfg := range c.poolConfigs() {
		if cfg.ManagedEndpoints.Enabled {
			key := types.GPUKey(cfg.GPUType)
			inv.pools[key] = append(inv.pools[key], eligiblePool{Name: name, Locality: localityOf(name, cfg)})
		}
	}
	for _, pools := range inv.pools {
		slices.SortFunc(pools, func(a, b eligiblePool) int { return strings.Compare(a.Name, b.Name) })
	}
	for _, w := range workers {
		// Only workers that can take a container now: a pending worker's GPUs
		// are not usable by serverless yet, so they must not satisfy the floor.
		if w == nil || w.Status != types.WorkerStatusAvailable || w.Gpu == "" {
			continue
		}
		if cfg, ok := c.poolConfig(w.PoolName); !ok || !cfg.ManagedEndpoints.Enabled {
			continue
		}
		key := types.GPUKey(w.Gpu)
		if inv.free[key] == nil {
			inv.free[key] = map[string]uint32{}
		}
		inv.free[key][w.PoolName] += w.FreeGpuCount
		if ready[w.PoolName] == nil {
			ready[w.PoolName] = &slack{}
		}
		ready[w.PoolName].cpu += w.FreeCpu
		ready[w.PoolName].memory += w.FreeMemory
	}
	for _, r := range replicas {
		if r.Status != types.ReplicaStatusScheduling || r.GPU == types.CPUInventoryKey {
			continue
		}
		if free, ok := inv.free[r.GPU][r.PoolName]; ok {
			inv.free[r.GPU][r.PoolName] = free - min(free, max(r.GPUCount, 1))
		}
	}
	for key, pools := range inv.free {
		for name, free := range pools {
			cfg, _ := c.poolConfig(name)
			floor, _ := strconv.ParseUint(cfg.PoolSizing.MinFreeGPU, 10, 32)
			minCPU, _ := scheduler.ParseCPU(cfg.PoolSizing.MinFreeCPU)
			minMemory, _ := scheduler.ParseMemory(cfg.PoolSizing.MinFreeMemory)
			if s := ready[name]; (minCPU > 0 && s.cpu <= minCPU) || (minMemory > 0 && s.memory <= minMemory) {
				free = 0
			}
			inv.free[key][name] = free - min(free, uint32(floor))
		}
	}
	return inv, nil
}

func (inv *clusterInventory) canPlace(gpu string, count uint32) bool {
	if inv == nil {
		return false
	}
	if gpu == types.CPUInventoryKey {
		return len(inv.pools[gpu]) > 0
	}
	for _, pool := range inv.pools[gpu] {
		if inv.free[gpu][pool.Name] >= max(count, 1) {
			return true
		}
	}
	return false
}

// place reserves GPUs for one replica in the pool with the most idle GPUs.
// Nothing is ever submitted without room: the scheduler is never asked to
// wait for or provision a worker.
func (inv *clusterInventory) place(gpu string, count uint32) (eligiblePool, bool) {
	pools := inv.pools[gpu]
	if len(pools) == 0 {
		return eligiblePool{}, false
	}
	if gpu == types.CPUInventoryKey {
		return pools[0], true
	}
	need := max(count, 1)
	var best eligiblePool
	bestFree := uint32(0)
	for _, pool := range pools {
		if free := inv.free[gpu][pool.Name]; free >= need && free > bestFree {
			best, bestFree = pool, free
		}
	}
	if bestFree == 0 {
		return eligiblePool{}, false
	}
	inv.free[gpu][best.Name] -= need
	return best, true
}

func liveReplicas(replicas []*types.EndpointReplica, endpointID, gpu string, version uint) (live []*types.EndpointReplica, ready int) {
	for _, r := range replicas {
		if r.EndpointID != endpointID || r.GPU != gpu || r.Version != version || !r.Alive() {
			continue
		}
		live = append(live, r)
		if r.Status == types.ReplicaStatusReady {
			ready++
		}
	}
	return live, ready
}

// scaleDownOrder puts the least valuable replicas first: not ready, then least loaded, then newest.
func scaleDownOrder(replicas []*types.EndpointReplica) {
	sort.SliceStable(replicas, func(i, j int) bool {
		a, b := replicas[i], replicas[j]
		if aReady, bReady := a.Status == types.ReplicaStatusReady, b.Status == types.ReplicaStatusReady; aReady != bReady {
			return !aReady
		}
		if a.Capacity.InFlight != b.Capacity.InFlight {
			return a.Capacity.InFlight < b.Capacity.InFlight
		}
		return a.StartedAt.After(b.StartedAt)
	})
}

// retire drains replicas the endpoint no longer wants (older version, GPU
// type it no longer fills, or the whole endpoint), one per tick. A serving
// stale replica stays until a current one is ready or can start on idle
// capacity; when nothing is idle it is drained to make room, unless it is the
// only replica serving.
func (c *controller) retire(ctx context.Context, endpoint *types.ManagedEndpoint, fleet *types.Fleet, live []*types.EndpointReplica, inv *clusterInventory) {
	spec := &endpoint.Spec
	if !endpoint.Enabled() {
		for _, r := range live {
			if r.EndpointID == spec.ID {
				_ = c.drainReplica(ctx, r, spec.DrainSeconds, false, "endpoint retired")
			}
		}
		return
	}
	placements := fleet.Placements(spec.ID)
	matches := func(r *types.EndpointReplica) bool {
		_, listed := placements[r.GPU]
		return r.Version == endpoint.Version && listed
	}
	var currentReady, serving int
	for _, r := range live {
		if r.EndpointID != spec.ID || !r.Serving() {
			continue
		}
		serving++
		if matches(r) {
			currentReady++
		}
	}
	for _, r := range live {
		if r.EndpointID != spec.ID || !r.Alive() || matches(r) {
			continue
		}
		reason := fmt.Sprintf("version %d retired", r.Version)
		if _, listed := placements[r.GPU]; !listed {
			reason = "removed from fleet.yaml"
		}
		if r.Serving() && currentReady == 0 && len(placements) > 0 {
			if inv.canPlace(r.GPU, spec.Gpu[r.GPU].Count) {
				continue
			}
			if serving < 2 {
				log.Warn().Str("endpoint_id", spec.ID).Str("replica_id", r.ID).Uint("version", endpoint.Version).
					Msg("managed endpoints: rollout waiting; the only serving replica holds the last GPU")
				continue
			}
			reason = fmt.Sprintf("version %d retired (making room for version %d)", r.Version, endpoint.Version)
		}
		if err := c.drainReplica(ctx, r, spec.DrainSeconds, false, reason); err == nil {
			return
		}
	}
}

// fill converges one GPU type on its priority order: each entry takes idle
// GPUs up to its cap and drains down to it when over. When an entry is short
// and nothing is idle, the entries below it give back one replica per tick;
// an entry still starting a replica waits instead.
func (c *controller) fill(ctx context.Context, gpu string, entries []types.FleetEntry, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica, inv *clusterInventory) {
	var short *types.ManagedEndpoint
	reclaimed := false
	for _, e := range entries {
		endpoint := endpoints[e.EndpointID]
		if endpoint == nil || !endpoint.Enabled() {
			continue
		}
		current, ready := liveReplicas(live, e.EndpointID, gpu, endpoint.Version)
		n := uint32(len(current))
		switch {
		case e.MaxReplicas > 0 && n > e.MaxReplicas:
			scaleDownOrder(current)
			for _, r := range current[:n-e.MaxReplicas] {
				_ = c.drainReplica(ctx, r, endpoint.Spec.DrainSeconds, false, "over fleet.yaml cap")
			}
		case short != nil:
			if n == 0 || reclaimed {
				continue
			}
			scaleDownOrder(current)
			if err := c.drainReplica(ctx, current[0], endpoint.Spec.DrainSeconds, false, "gpu reclaimed for "+short.Spec.ID); err == nil {
				reclaimed = true
			}
		case e.MaxReplicas == 0 || n < e.MaxReplicas:
			want := uint32(maxStartsPerTick)
			if e.MaxReplicas > 0 {
				want = e.MaxReplicas - n
			}
			if noRoom := c.grow(ctx, endpoint, gpu, want, inv); noRoom && ready == len(current) {
				short = endpoint
			}
		}
	}
}

// grow starts up to want replicas and reports whether it stopped for lack of
// idle GPUs. A failed start backs off the (endpoint, gpu).
func (c *controller) grow(ctx context.Context, endpoint *types.ManagedEndpoint, gpu string, want uint32, inv *clusterInventory) (noRoom bool) {
	if backoff, _ := c.s.repo.InScheduleBackoff(ctx, endpoint.Spec.ID, gpu); backoff {
		return false
	}
	for range min(want, maxStartsPerTick) {
		pool, ok := inv.place(gpu, endpoint.Spec.Gpu[gpu].Count)
		if !ok {
			log.Debug().Str("endpoint_id", endpoint.Spec.ID).Str("gpu", gpu).Msg("managed endpoints: no idle capacity in any eligible pool")
			return true
		}
		if _, err := c.startReplica(ctx, endpoint, gpu, pool); err != nil {
			log.Warn().Err(err).Str("endpoint_id", endpoint.Spec.ID).Str("gpu", gpu).Msg("managed endpoints: start replica failed")
			return false
		}
	}
	return false
}
