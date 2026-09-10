package managedendpoint

import (
	"cmp"
	"context"
	"fmt"
	"math"
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

// controller reconciles replicas against the registry and config.yaml. One
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
	}
	blocked, protectionErr := c.reconcileProtection(ctx, fleet, byID, live)
	for _, endpoint := range endpoints {
		c.retire(ctx, endpoint, fleet, live, inv, blocked)
	}
	demand := c.readDemand(ctx, fleet, live)
	for group := range blocked {
		if d := demand[group.endpointID]; d != nil {
			delete(d.gpus, group.gpu)
		}
	}
	for _, gpu := range fleet.GPUs() {
		entries := fleet.Entries(gpu)
		entries = slices.DeleteFunc(entries, func(entry types.FleetEntry) bool {
			return blocked[protectionGroup{endpointID: entry.EndpointID, gpu: gpu}]
		})
		c.fillWithDemand(ctx, gpu, entries, byID, live, inv, demand)
	}
	return protectionErr
}

type eligiblePool struct {
	Name     string
	Locality string
}

// clusterInventory is where replicas may run: opted-in pools per GPU key and
// the idle GPUs each has.
type clusterInventory struct {
	pools          map[string][]eligiblePool
	free           map[string]map[string]uint32 // gpu key -> pool -> free GPUs
	workers        map[string]*inventoryWorker
	floors         map[string]uint32 // pool -> GPUs reserved for serverless
	resourceFloors map[string]replicaResources
	pending        map[string]bool // CPU/memory of unassigned starts is not in worker accounting yet
}

type inventoryWorker struct {
	pool, gpu             string
	free, total           uint32
	cpu, memory           int64
	totalCPU, totalMemory int64
}

type replicaResources struct{ cpu, memory int64 }

// Matches scheduler.capacityMemoryForScheduling and worker capacity accounting.
func reservedReplicaResources(cpu, memory int64) (replicaResources, bool) {
	if cpu < 0 || memory < 0 || memory > (math.MaxInt64-99)/125 {
		return replicaResources{}, false
	}
	return replicaResources{cpu: cpu, memory: (memory*125 + 99) / 100}, true
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
// workers, less replicas still being scheduled and the pool's minFree* floor.
// The scheduler enforces the same floor at admission.
func (c *controller) inventory(replicas []*types.EndpointReplica) (*clusterInventory, error) {
	workers, err := c.s.workers.GetAllWorkers()
	if err != nil {
		return nil, err
	}
	inv := &clusterInventory{
		pools: map[string][]eligiblePool{}, free: map[string]map[string]uint32{},
		workers: map[string]*inventoryWorker{}, floors: map[string]uint32{},
		resourceFloors: map[string]replicaResources{}, pending: map[string]bool{},
	}
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
		// Match admission: hosted requests do not opt into Preemptable workers.
		// Pending workers cannot serve anything yet or satisfy the floor.
		if w == nil || w.Status != types.WorkerStatusAvailable || w.Gpu == "" || w.Preemptable {
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
		inv.workers[w.Id] = &inventoryWorker{
			pool: w.PoolName, gpu: key, free: w.FreeGpuCount, total: w.TotalGpuCount,
			cpu: w.FreeCpu, memory: w.FreeMemory, totalCPU: w.TotalCpu, totalMemory: w.TotalMemory,
		}
		if ready[w.PoolName] == nil {
			ready[w.PoolName] = &slack{}
		}
		ready[w.PoolName].cpu += w.FreeCpu
		ready[w.PoolName].memory += w.FreeMemory
	}
	for _, r := range replicas {
		if r.Status != types.ReplicaStatusScheduling || r.GPU == types.CPUInventoryKey || r.WorkerID != "" {
			continue
		}
		if free, ok := inv.free[r.GPU][r.PoolName]; ok {
			inv.pending[r.PoolName] = true
			need := min(free, max(r.GPUCount, 1))
			inv.free[r.GPU][r.PoolName] = free - need
			// Pending requests have no worker yet. Reserve on one worker when
			// possible, otherwise conservatively account for every fragment.
			for need > 0 {
				worker := inv.idlestWorker(r.GPU, r.PoolName)
				if worker == nil || worker.free == 0 {
					break
				}
				taken := min(need, worker.free)
				worker.free -= taken
				need -= taken
			}
		}
	}
	for key, pools := range inv.free {
		for name, free := range pools {
			cfg, _ := c.poolConfig(name)
			floor, _ := strconv.ParseUint(cfg.PoolSizing.MinFreeGPU, 10, 32)
			inv.floors[name] = uint32(floor)
			minCPU, _ := scheduler.ParseCPU(cfg.PoolSizing.MinFreeCPU)
			minMemory, _ := scheduler.ParseMemory(cfg.PoolSizing.MinFreeMemory)
			inv.resourceFloors[name] = replicaResources{cpu: minCPU, memory: minMemory}
			if s := ready[name]; (minCPU > 0 && s.cpu <= minCPU) || (minMemory > 0 && s.memory <= minMemory) {
				free = 0
			}
			inv.free[key][name] = free - min(free, uint32(floor))
		}
	}
	return inv, nil
}

func (inv *clusterInventory) idlestWorker(gpu, pool string) *inventoryWorker {
	var best *inventoryWorker
	var bestID string
	for id, worker := range inv.workers {
		if worker.gpu == gpu && worker.pool == pool && (best == nil || worker.free > best.free || worker.free == best.free && id < bestID) {
			best, bestID = worker, id
		}
	}
	return best
}

func (inv *clusterInventory) canPlace(gpu string, count uint32) bool {
	if inv == nil {
		return false
	}
	if gpu == types.CPUInventoryKey {
		return len(inv.pools[gpu]) > 0
	}
	for _, pool := range inv.pools[gpu] {
		if worker := inv.idlestWorker(gpu, pool.Name); worker != nil && worker.free >= max(count, 1) && inv.free[gpu][pool.Name] >= max(count, 1) {
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
		worker := inv.idlestWorker(gpu, pool.Name)
		if free := inv.free[gpu][pool.Name]; worker != nil && worker.free >= need && free >= need && free > bestFree {
			best, bestFree = pool, free
		}
	}
	if bestFree == 0 {
		return eligiblePool{}, false
	}
	inv.free[gpu][best.Name] -= need
	inv.idlestWorker(gpu, best.Name).free -= need
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

// scaleDownOrder drains unprotected replicas first, then not ready, least loaded, and newest.
func scaleDownOrder(replicas []*types.EndpointReplica) {
	sort.SliceStable(replicas, func(i, j int) bool {
		a, b := replicas[i], replicas[j]
		if a.Protected != b.Protected {
			return !a.Protected
		}
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
// capacity. The explicit replace policy permits downtime to release the last
// GPU; the default waits for capacity and keeps the sole serving replica.
func (c *controller) retire(ctx context.Context, endpoint *types.ManagedEndpoint, fleet *types.Fleet, live []*types.EndpointReplica, inv *clusterInventory, blocked map[protectionGroup]bool) {
	spec := &endpoint.Spec
	if !endpoint.Enabled() {
		for _, r := range live {
			if r.EndpointID == spec.ID && !blocked[protectionGroup{endpointID: r.EndpointID, gpu: r.GPU}] {
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
	currentStarting := false
	startingOnGPU := map[string]bool{}
	protectedReady := map[string]uint32{}
	for _, r := range live {
		if r.EndpointID == spec.ID && matches(r) && r.Alive() && !r.Serving() {
			currentStarting = true
			startingOnGPU[r.GPU] = true
		}
		if r.EndpointID != spec.ID || !r.Serving() {
			continue
		}
		serving++
		if r.Protected {
			protectedReady[r.GPU]++
		}
		if matches(r) {
			currentReady++
		}
	}
	var stale []*types.EndpointReplica
	for _, r := range live {
		if r.EndpointID != spec.ID || !r.Alive() || matches(r) || blocked[protectionGroup{endpointID: r.EndpointID, gpu: r.GPU}] {
			continue
		}
		stale = append(stale, r)
	}
	// Release a stale extra before using the replace policy on a protected
	// minimum. Keep the shared observation order intact for the fill pass.
	scaleDownOrder(stale)
	for _, r := range stale {
		reason := fmt.Sprintf("version %d retired", r.Version)
		if _, listed := placements[r.GPU]; !listed {
			reason = "removed from config.yaml"
		}
		placement, sameGPU := placements[r.GPU]
		canReplace := spec.Rollout == "replace" && sameGPU && inv != nil &&
			len(inv.pools[r.GPU]) > 0 && r.GPUCount >= max(spec.Gpu[r.GPU].Count, 1)
		if r.Serving() && r.Protected && sameGPU && placement.MinReplicas > 0 && protectedReady[r.GPU] <= placement.MinReplicas {
			// Protection transfers to a ready replacement before retirement.
			// A ready replica on another GPU cannot cover this GPU's minimum.
			if startingOnGPU[r.GPU] || inv.canPlace(r.GPU, spec.Gpu[r.GPU].Count) || !canReplace {
				continue
			}
			reason = fmt.Sprintf("version %d retired (making room for version %d)", r.Version, endpoint.Version)
		}
		if r.Serving() && currentReady == 0 && len(placements) > 0 {
			if currentStarting {
				continue
			}
			if inv.canPlace(r.GPU, spec.Gpu[r.GPU].Count) {
				continue
			}
			if serving < 2 && !canReplace {
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

// placementTarget counts both observed replicas and successful starts in this
// pass. Scheduling/loading replicas satisfy the minimum and cap, so a second
// pass cannot duplicate a start or reclaim more capacity while it warms up.
type placementTarget struct {
	entry         types.FleetEntry
	endpoint      *types.ManagedEndpoint
	replicas      []*types.EndpointReplica
	started       uint32
	resources     *replicaResources
	resourcesRead bool
	demand        *endpointDemand
	replacing     bool
}

func (c *controller) targetResources(ctx context.Context, target *placementTarget) *replicaResources {
	if target.resourcesRead {
		return target.resources
	}
	target.resourcesRead = true
	if c.s.backend == nil {
		return nil
	}
	stub, err := c.s.backend.GetStubByExternalId(ctx, target.endpoint.StubID)
	if err != nil || stub == nil || stub.ExternalId != target.endpoint.StubID {
		return nil
	}
	config, err := stub.UnmarshalConfig()
	if err != nil || config == nil {
		return nil
	}
	resources, ok := reservedReplicaResources(config.Runtime.Cpu, config.Runtime.Memory)
	if ok {
		target.resources = &resources
	}
	return target.resources
}

func (t *placementTarget) count() uint32 {
	n := t.started
	for _, replica := range t.replicas {
		if replica.Alive() {
			n++
		}
	}
	return n
}

func (t *placementTarget) starting() bool {
	if t.started > 0 {
		return true
	}
	for _, replica := range t.replicas {
		if replica.Alive() && !replica.Serving() {
			return true
		}
	}
	return false
}

// fill satisfies GPU minimums, then requested on-demand capacity, then hot
// surplus. Requests can reclaim hot extras; hot surplus only reclaims from
// lower priorities and never displaces a recently requested on-demand copy.
func (c *controller) fill(ctx context.Context, gpu string, entries []types.FleetEntry, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica, inv *clusterInventory) {
	c.fillWithDemand(ctx, gpu, entries, endpoints, live, inv, nil)
}

func (c *controller) fillWithDemand(ctx context.Context, gpu string, entries []types.FleetEntry, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica, inv *clusterInventory, demand map[string]*endpointDemand) {
	targets := make([]*placementTarget, 0, len(entries))
	for _, entry := range entries {
		endpoint := endpoints[entry.EndpointID]
		if endpoint == nil || !endpoint.Enabled() {
			continue
		}
		current, _ := liveReplicas(live, entry.EndpointID, gpu, endpoint.Version)
		target := &placementTarget{entry: entry, endpoint: endpoint, replicas: current, demand: demand[entry.EndpointID]}
		target.replacing = slices.ContainsFunc(live, func(replica *types.EndpointReplica) bool {
			return replica.EndpointID == entry.EndpointID && replica.GPU == gpu && replica.Version != endpoint.Version && replica.Alive()
		})
		targets = append(targets, target)
		if entry.Serverless && target.demand != nil && !target.demand.warm && target.demand.active == 0 {
			for _, replica := range live {
				if replica.EndpointID == entry.EndpointID && replica.GPU == gpu && replica.Alive() {
					_ = c.drainReplica(ctx, replica, endpoint.Spec.DrainSeconds, false, "on-demand endpoint idle")
				}
			}
		}
		// A changed cap is an explicit scale-down, including protected replicas.
		// Do this for all entries, even while a higher-priority minimum waits.
		if n := target.count(); entry.MaxReplicas > 0 && n > entry.MaxReplicas {
			scaleDownOrder(current)
			for _, replica := range current[:n-entry.MaxReplicas] {
				_ = c.drainReplica(ctx, replica, endpoint.Spec.DrainSeconds, false, "over config.yaml cap")
			}
		}
	}

	protected := map[string]uint32{}
	for _, replica := range live {
		if replica.GPU == gpu && replica.Alive() && replica.Protected {
			protected[replica.EndpointID]++
		}
	}
	reclaimed := false
	for phase := range 3 {
		minimum := phase == 0
		for index, target := range targets {
			if !minimum && (phase == 1) != target.entry.Serverless {
				continue
			}
			limit := target.entry.MinReplicas
			if !minimum {
				limit = target.entry.MaxReplicas
				if limit == 0 {
					limit = target.count() + maxStartsPerTick
				}
				if target.entry.Serverless {
					d := target.demand
					if d == nil || d.starting {
						continue
					}
					// A serving old version may cover traffic while its
					// replacement loads. Its capacity must not stall rollout.
					replace := target.replacing && target.count() == 0 && d.warm
					if d.active <= d.capacity && !replace {
						continue
					}
					// Capacity is learned from the engine heartbeat. Bring up
					// one copy before deciding whether demand needs another.
					limit = min(limit, target.count()+1)
				}
			}
			if n := target.count(); n < limit {
				want := min(limit-n, maxStartsPerTick-target.started)
				var protectedBudget uint32
				if minimum && target.entry.ProtectMinimum {
					// Old versions keep their protected slot while a replacement
					// loads; reconciliation transfers it when the new copy is ready.
					protectedBudget = min(want, target.entry.MinReplicas-min(target.entry.MinReplicas, protected[target.entry.EndpointID]))
				}
				started, noRoom := c.grow(ctx, target.endpoint, gpu, want, protectedBudget, inv, target.entry.Serverless)
				target.started += started
				if started > 0 && target.demand != nil {
					target.demand.starting = true
				}
				protected[target.entry.EndpointID] += min(started, protectedBudget)
				if noRoom && !target.starting() && !reclaimed {
					reclaimed = c.reclaim(ctx, gpu, index, targets, live, inv, minimum)
					if reclaimed && target.demand != nil {
						target.demand.starting = true
					}
				}
			}
		}
		if minimum {
			for _, target := range targets {
				if target.count() < target.entry.MinReplicas {
					// Also holds capacity during backoff or a per-tick start
					// limit; another minimum may still have started above.
					return
				}
			}
		}
	}
}

// reclaim releases one replica only after proving a single ready worker can
// fit the request using idle capacity and eligible surplus. Summing GPUs
// across workers or across a pool floor would evict models without progress.
func (c *controller) reclaim(ctx context.Context, gpu string, index int, targets []*placementTarget, live []*types.EndpointReplica, inv *clusterInventory, minimum bool) bool {
	if inv == nil || c.s.containers == nil || gpu == types.CPUInventoryKey {
		return false
	}
	target := targets[index]
	// Both minimums and requested on-demand models outrank hot surplus.
	preferSurplus := minimum || target.entry.Serverless
	resources := c.targetResources(ctx, target)
	if resources == nil {
		return false // Never destroy a replica for an unknown request shape.
	}
	if target.entry.Serverless && target.demand != nil {
		for otherGPU, placement := range target.demand.gpus {
			if otherGPU == gpu || placement.MaxReplicas > 0 && target.demand.counts[otherGPU] >= placement.MaxReplicas {
				continue
			}
			if inv.canFit(otherGPU, target.endpoint.Spec.Gpu[otherGPU].Count, *resources) {
				if backoff, err := c.s.repo.InScheduleBackoff(ctx, target.endpoint.Spec.ID, otherGPU); err == nil && !backoff {
					return false // Use an eligible idle alternative before taking a hot extra.
				}
			}
		}
	}
	need := uint64(max(target.endpoint.Spec.Gpu[gpu].Count, 1))
	reason := "gpu reclaimed for " + target.endpoint.Spec.ID
	settling := map[string]bool{}
	for _, replica := range live {
		if replica.Status == types.ReplicaStatusDraining || replica.Status == types.ReplicaStatusEvicting {
			if replica.StatusReason == reason {
				return false // Wait for the capacity already being released.
			}
			if replica.GPU == gpu {
				settling[replica.WorkerID] = true
			}
		}
	}
	type victim struct {
		replica   *types.EndpointReplica
		owner     *placementTarget
		resources replicaResources
	}
	byWorker := map[string][]victim{}
	for offset := range len(targets) {
		ownerIndex := offset
		if preferSurplus {
			ownerIndex = len(targets) - 1 - offset // least priority gives back first
		}
		owner := targets[ownerIndex]
		if target.entry.Serverless && owner.entry.Serverless {
			continue // A requested model only reclaims hot extras.
		}
		if !minimum && owner.entry.Serverless && (owner.demand == nil || owner.demand.warm || owner.demand.active > 0) {
			continue // Hot surplus must not churn a requested on-demand copy.
		}
		if ownerIndex == index || (!preferSurplus && ownerIndex < index) || owner.count() <= owner.entry.MinReplicas {
			continue
		}
		current := append([]*types.EndpointReplica(nil), owner.replicas...)
		scaleDownOrder(current)
		for _, replica := range current {
			worker := inv.workers[replica.WorkerID]
			if !replica.Alive() || replica.Protected || worker == nil || worker.gpu != gpu || worker.pool != replica.PoolName {
				continue
			}
			if settling[replica.WorkerID] || inv.pending[worker.pool] || uint64(worker.total) < need || worker.totalCPU < resources.cpu || worker.totalMemory < resources.memory {
				continue
			}
			if !slices.ContainsFunc(inv.pools[gpu], func(pool eligiblePool) bool { return pool.Name == worker.pool }) {
				continue
			}
			state, err := c.s.containers.GetContainerState(replica.ContainerID)
			if err != nil || state == nil || !state.Evictable || state.Evicting || state.Status != types.ContainerStatusRunning {
				continue
			}
			if state.WorkerId != replica.WorkerID || types.GPUKey(state.Gpu) != gpu || state.GpuCount != max(replica.GPUCount, 1) {
				continue
			}
			released, ok := reservedReplicaResources(state.Cpu, state.Memory)
			if ok {
				byWorker[replica.WorkerID] = append(byWorker[replica.WorkerID], victim{replica, owner, released})
			}
		}
	}
	workerIDs := make([]string, 0, len(byWorker))
	for id := range byWorker {
		workerIDs = append(workerIDs, id)
	}
	slices.Sort(workerIDs)
	for _, id := range workerIDs {
		worker := inv.workers[id]
		var poolFree uint64
		var poolResources replicaResources
		for _, w := range inv.workers {
			if w.gpu == gpu && w.pool == worker.pool {
				poolFree += uint64(w.free)
				poolResources.cpu += w.cpu
				poolResources.memory += w.memory
			}
		}
		used := map[*placementTarget]uint32{}
		var released uint64
		var releasedResources replicaResources
		var first *victim
		for _, candidate := range byWorker[id] {
			if used[candidate.owner] >= candidate.owner.count()-candidate.owner.entry.MinReplicas {
				continue
			}
			used[candidate.owner]++
			released += uint64(max(candidate.replica.GPUCount, 1))
			releasedResources.cpu += candidate.resources.cpu
			releasedResources.memory += candidate.resources.memory
			if first == nil {
				copy := candidate
				first = &copy
			}
			floor := inv.resourceFloors[worker.pool]
			if uint64(worker.free)+released >= need && poolFree+released >= uint64(inv.floors[worker.pool])+need &&
				worker.cpu+releasedResources.cpu >= resources.cpu && worker.memory+releasedResources.memory >= resources.memory &&
				poolResources.cpu+releasedResources.cpu-resources.cpu >= floor.cpu && poolResources.memory+releasedResources.memory-resources.memory >= floor.memory {
				return c.drainReplica(ctx, first.replica, first.owner.endpoint.Spec.DrainSeconds, false, reason) == nil
			}
		}
	}
	return false
}

// canFit proves that another configured GPU has idle resources, including the
// pool's CPU/memory floors. A GPU-only check could strand a request behind a
// worker with insufficient memory while usable hot surplus exists elsewhere.
func (inv *clusterInventory) canFit(gpu string, count uint32, resources replicaResources) bool {
	need := max(count, 1)
	for _, pool := range inv.pools[gpu] {
		if inv.pending[pool.Name] || inv.free[gpu][pool.Name] < need {
			continue
		}
		var total replicaResources
		fits := false
		for _, worker := range inv.workers {
			if worker.pool != pool.Name || worker.gpu != gpu {
				continue
			}
			total.cpu += worker.cpu
			total.memory += worker.memory
			fits = fits || worker.free >= need && worker.cpu >= resources.cpu && worker.memory >= resources.memory
		}
		floor := inv.resourceFloors[pool.Name]
		if fits && total.cpu-resources.cpu >= floor.cpu && total.memory-resources.memory >= floor.memory {
			return true
		}
	}
	return false
}

// grow starts up to want replicas and reports successful starts and whether
// it stopped for lack of idle GPUs. A failed start backs off (endpoint, gpu).
func (c *controller) grow(ctx context.Context, endpoint *types.ManagedEndpoint, gpu string, want, protectedBudget uint32, inv *clusterInventory, serverless bool) (started uint32, noRoom bool) {
	if backoff, err := c.s.repo.InScheduleBackoff(ctx, endpoint.Spec.ID, gpu); err != nil || backoff {
		return 0, false
	}
	for range min(want, maxStartsPerTick) {
		pool, ok := inv.place(gpu, endpoint.Spec.Gpu[gpu].Count)
		if !ok {
			log.Debug().Str("endpoint_id", endpoint.Spec.ID).Str("gpu", gpu).Msg("managed endpoints: no idle capacity in any eligible pool")
			return started, true
		}
		protected := !serverless && (started < protectedBudget || !c.s.config.Preemption.Enabled)
		if _, err := c.startReplica(ctx, endpoint, gpu, pool, protected); err != nil {
			log.Warn().Err(err).Str("endpoint_id", endpoint.Spec.ID).Str("gpu", gpu).Msg("managed endpoints: start replica failed")
			_ = c.s.repo.SetScheduleBackoff(ctx, endpoint.Spec.ID, gpu, c.s.config.Reconcile.FailureBackoff)
			return started, false
		}
		started++
		if inv.pending == nil {
			inv.pending = map[string]bool{}
		}
		inv.pending[pool.Name] = true
	}
	return started, false
}
