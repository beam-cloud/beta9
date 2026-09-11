package managedendpoint

import (
	"cmp"
	"context"
	"fmt"
	"maps"
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
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// controller reconciles replicas against the registry and config.yaml. One
// gateway holds the lock at a time. Each tick: observe replicas, measure idle
// GPUs, assign protection, start what config.yaml wants, retire what it does not.

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
	blocked, protectErr := c.protect(ctx, fleet, byID, live)
	demand := c.readDemand(ctx, fleet, live)
	for _, gpu := range fleet.GPUs() {
		entries := slices.DeleteFunc(fleet.Entries(gpu), func(entry types.FleetEntry) bool {
			return blocked[group{entry.EndpointID, gpu}]
		})
		c.fill(ctx, gpu, entries, byID, live, inv, demand)
	}
	// After fill: a rollout starts its replacement on idle capacity before
	// the stale copy is asked to go.
	for _, endpoint := range endpoints {
		c.retire(ctx, endpoint, fleet, live, blocked)
	}
	return protectErr
}

type group struct{ endpointID, gpu string }

// Inventory ------------------------------------------------------------------

type eligiblePool struct {
	Name     string
	Locality string
}

type inventoryWorker struct {
	pool, gpu   string
	free, total uint32
	cpu, memory int64 // free, in scheduler units
}

// clusterInventory is where replicas may run: opted-in pools per GPU key and
// the idle GPUs of their ready workers. The scheduler enforces the same pool
// floors at admission; a start it refuses backs the endpoint off.
type clusterInventory struct {
	pools   map[string][]eligiblePool
	workers map[string]*inventoryWorker
	floors  map[string]uint32 // pool -> minFreeGPU
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

// pools lists the opted-in pools hosting a GPU key.
func (c *controller) pools(gpu string) []string {
	var out []string
	for name, cfg := range c.poolConfigs() {
		if cfg.ManagedEndpoints.Enabled && types.GPUKey(cfg.GPUType) == gpu {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

func localityOf(name string, cfg types.WorkerPoolConfig) string {
	return cmp.Or(strings.TrimSpace(cfg.Locality), name)
}

func (c *controller) inventory(replicas []*types.EndpointReplica) (*clusterInventory, error) {
	workers, err := c.s.workers.GetAllWorkers()
	if err != nil {
		return nil, err
	}
	inv := &clusterInventory{pools: map[string][]eligiblePool{}, workers: map[string]*inventoryWorker{}, floors: map[string]uint32{}}
	for name, cfg := range c.poolConfigs() {
		if !cfg.ManagedEndpoints.Enabled {
			continue
		}
		key := types.GPUKey(cfg.GPUType)
		inv.pools[key] = append(inv.pools[key], eligiblePool{Name: name, Locality: localityOf(name, cfg)})
		floor, _ := strconv.ParseUint(cfg.PoolSizing.MinFreeGPU, 10, 32)
		inv.floors[name] = uint32(floor)
	}
	for _, pools := range inv.pools {
		slices.SortFunc(pools, func(a, b eligiblePool) int { return strings.Compare(a.Name, b.Name) })
	}
	for _, w := range workers {
		// Match admission: hosted requests do not opt into Preemptable workers.
		if w == nil || w.Status != types.WorkerStatusAvailable || w.Gpu == "" || w.Preemptable {
			continue
		}
		if cfg, ok := c.poolConfig(w.PoolName); !ok || !cfg.ManagedEndpoints.Enabled {
			continue
		}
		inv.workers[w.Id] = &inventoryWorker{pool: w.PoolName, gpu: types.GPUKey(w.Gpu), free: w.FreeGpuCount, total: w.TotalGpuCount, cpu: w.FreeCpu, memory: w.FreeMemory}
	}
	// Replicas still waiting for a worker hold GPUs no worker reports yet.
	for _, r := range replicas {
		if r.Status != types.ReplicaStatusScheduling || r.WorkerID != "" || r.GPU == types.CPUInventoryKey {
			continue
		}
		for need := max(r.GPUCount, 1); need > 0; {
			w := inv.idlest(r.GPU, r.PoolName)
			if w == nil || w.free == 0 {
				break
			}
			taken := min(need, w.free)
			w.free, need = w.free-taken, need-taken
		}
	}
	return inv, nil
}

func (inv *clusterInventory) idlest(gpu, pool string) *inventoryWorker {
	var best *inventoryWorker
	var bestID string
	for id, w := range inv.workers {
		if w.gpu == gpu && w.pool == pool && (best == nil || w.free > best.free || w.free == best.free && id < bestID) {
			best, bestID = w, id
		}
	}
	return best
}

func (inv *clusterInventory) poolFree(gpu, pool string) uint32 {
	var free uint32
	for _, w := range inv.workers {
		if w.gpu == gpu && w.pool == pool {
			free += w.free
		}
	}
	return free
}

// idle is a pool's placeable GPUs: idle GPUs above its floor.
func (inv *clusterInventory) idle(gpu, pool string) uint32 {
	free := inv.poolFree(gpu, pool)
	return free - min(free, inv.floors[pool])
}

func (inv *clusterInventory) canPlace(gpu string, count uint32) bool {
	_, ok := inv.pick(gpu, count)
	return ok
}

// pick finds room for one replica on a single worker in the pool with the most
// idle GPUs. Nothing is ever submitted without room: the scheduler is never
// asked to wait for or provision a worker.
func (inv *clusterInventory) pick(gpu string, count uint32) (*inventoryWorker, bool) {
	if inv == nil {
		return nil, false
	}
	need := max(count, 1)
	var best *inventoryWorker
	var bestIdle uint32
	for _, pool := range inv.pools[gpu] {
		w := inv.idlest(gpu, pool.Name)
		if idle := inv.idle(gpu, pool.Name); w != nil && w.free >= need && idle >= need && idle > bestIdle {
			best, bestIdle = w, idle
		}
	}
	return best, best != nil
}

// place reserves the GPUs pick found. CPU replicas go to the first opted-in pool.
func (inv *clusterInventory) place(gpu string, count uint32) (eligiblePool, bool) {
	if inv == nil || len(inv.pools[gpu]) == 0 {
		return eligiblePool{}, false
	}
	if gpu == types.CPUInventoryKey {
		return inv.pools[gpu][0], true
	}
	w, ok := inv.pick(gpu, count)
	if !ok {
		return eligiblePool{}, false
	}
	w.free -= max(count, 1)
	i := slices.IndexFunc(inv.pools[gpu], func(p eligiblePool) bool { return p.Name == w.pool })
	return inv.pools[gpu][i], true
}

// Placement ------------------------------------------------------------------

// slot is one config.yaml entry on one GPU type and what runs for it now.
type slot struct {
	entry     types.FleetEntry
	endpoint  *types.ManagedEndpoint
	replicas  []*types.EndpointReplica // current version on this GPU
	demand    *endpointDemand          // serverless placements only
	started   uint32
	replacing bool // an older version is still alive on this GPU
	res       *replicaResources
	resRead   bool
}

func (s *slot) count() uint32 {
	n := s.started
	for _, r := range s.replicas {
		if r.Alive() {
			n++
		}
	}
	return n
}

func (s *slot) starting() bool {
	return s.started > 0 || slices.ContainsFunc(s.replicas, func(r *types.EndpointReplica) bool { return r.Alive() && !r.Serving() })
}

const (
	passMinimum    = iota // configured minimums, priority order
	passServerless        // one copy per requested serverless model
	passSurplus           // hot extras up to each cap
)

// want is how many replicas this pass allows the slot; 0 when it does not apply.
func (s *slot) want(pass int) uint32 {
	if pass == passMinimum {
		return s.entry.MinReplicas
	}
	if s.entry.Serverless != (pass == passServerless) {
		return 0
	}
	limit := s.entry.MaxReplicas
	if limit == 0 {
		limit = s.count() + maxStartsPerTick
	}
	if !s.entry.Serverless {
		return limit
	}
	d := s.demand
	if d == nil || d.starting {
		return 0
	}
	// Capacity is learned from the engine heartbeat: bring up one copy before
	// deciding whether demand needs another. A serving old version may carry
	// traffic while its replacement loads; that must not stall the rollout.
	requested := d.active > d.capacity || d.pending && d.active >= d.capacity
	if !requested && !(s.replacing && s.count() == 0 && d.warm) {
		return 0
	}
	return min(limit, s.count()+1)
}

// scaleDownOrder drains unprotected replicas first, then not ready, least loaded, and newest.
func scaleDownOrder(replicas []*types.EndpointReplica) {
	sort.SliceStable(replicas, func(i, j int) bool {
		a, b := replicas[i], replicas[j]
		if a.Protected != b.Protected {
			return !a.Protected
		}
		if aReady, bReady := a.Serving(), b.Serving(); aReady != bReady {
			return !aReady
		}
		if a.Capacity.InFlight != b.Capacity.InFlight {
			return a.Capacity.InFlight < b.Capacity.InFlight
		}
		return a.StartedAt.After(b.StartedAt)
	})
}

// slots builds the GPU's placements and applies their scale-downs: an idle
// serverless model returns to zero, a lowered cap drains the extras.
func (c *controller) slots(ctx context.Context, gpu string, entries []types.FleetEntry, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica, demand map[string]*endpointDemand) []*slot {
	now := time.Now()
	var out []*slot
	for _, entry := range entries {
		endpoint := endpoints[entry.EndpointID]
		if endpoint == nil || !endpoint.Enabled() {
			continue
		}
		s := &slot{entry: entry, endpoint: endpoint, demand: demand[entry.EndpointID]}
		var all []*types.EndpointReplica
		for _, r := range live {
			if r.EndpointID != entry.EndpointID || r.GPU != gpu || !r.Alive() {
				continue
			}
			all = append(all, r)
			if r.Version == endpoint.Version {
				s.replicas = append(s.replicas, r)
			} else {
				s.replacing = true
			}
		}
		if entry.Serverless && s.demand != nil && !s.demand.warm && s.demand.active == 0 {
			for _, r := range all {
				if !initialDemandGrace(r, now) {
					_ = c.drainReplica(ctx, r, endpoint.Spec.DrainSeconds, false, "on-demand endpoint idle")
				}
			}
		}
		if n := s.count(); entry.MaxReplicas > 0 && n > entry.MaxReplicas {
			scaleDownOrder(s.replicas)
			for _, r := range s.replicas[:n-entry.MaxReplicas] {
				_ = c.drainReplica(ctx, r, endpoint.Spec.DrainSeconds, false, "over config.yaml cap")
			}
		}
		out = append(out, s)
	}
	return out
}

// fill starts replicas for one GPU type: minimums in priority order, then one
// copy per requested serverless model, then hot surplus up to each cap. When
// no GPU is idle, at most one replica per tick is reclaimed; see reclaim.
func (c *controller) fill(ctx context.Context, gpu string, entries []types.FleetEntry, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica, inv *clusterInventory, demand map[string]*endpointDemand) {
	slots := c.slots(ctx, gpu, entries, endpoints, live, demand)
	protected := map[string]uint32{}
	for _, r := range live {
		if r.GPU == gpu && r.Alive() && r.Protected {
			protected[r.EndpointID]++
		}
	}
	reclaimed := false
	for pass := passMinimum; pass <= passSurplus; pass++ {
		for i, s := range slots {
			want := s.want(pass)
			if s.count() >= want {
				continue
			}
			if backoff, err := c.s.repo.InScheduleBackoff(ctx, s.entry.EndpointID, gpu); err != nil || backoff {
				continue
			}
			for s.count() < want && s.started < maxStartsPerTick {
				pool, ok := inv.place(gpu, s.endpoint.Spec.Gpu[gpu].Count)
				if !ok {
					if !reclaimed && !s.starting() {
						reclaimed = c.reclaim(ctx, gpu, i, slots, live, inv, pass)
						if reclaimed && s.demand != nil {
							s.demand.starting = true
						}
					}
					break
				}
				// Old versions keep their protected slot while a replacement
				// loads; protect() transfers it once the new copy is ready.
				protect := !s.entry.Serverless && (!c.s.config.Preemption.Enabled ||
					pass == passMinimum && s.entry.ProtectMinimum && protected[s.entry.EndpointID] < s.entry.MinReplicas)
				if _, err := c.startReplica(ctx, s.endpoint, gpu, pool, protect); err != nil {
					log.Warn().Err(err).Str("endpoint_id", s.entry.EndpointID).Str("gpu", gpu).Msg("managed endpoints: start replica failed")
					_ = c.s.repo.SetScheduleBackoff(ctx, s.entry.EndpointID, gpu, c.s.config.Reconcile.FailureBackoff)
					break
				}
				s.started++
				if protect {
					protected[s.entry.EndpointID]++
				}
				if s.demand != nil {
					s.demand.starting = true
				}
			}
		}
		// An unmet minimum holds the remaining capacity, also through backoff
		// or the per-tick start limit.
		if pass == passMinimum && slices.ContainsFunc(slots, func(s *slot) bool { return s.count() < s.entry.MinReplicas }) {
			return
		}
	}
}

// resources is the request shape a replica of the slot reserves, or nil when unknown.
func (c *controller) resources(ctx context.Context, s *slot) *replicaResources {
	if s.resRead {
		return s.res
	}
	s.resRead = true
	if c.s.backend == nil {
		return nil
	}
	stub, err := c.s.backend.GetStubByExternalId(ctx, s.endpoint.StubID)
	if err != nil || stub == nil || stub.ExternalId != s.endpoint.StubID {
		return nil
	}
	config, err := stub.UnmarshalConfig()
	if err != nil || config == nil {
		return nil
	}
	if res, ok := reservedReplicaResources(config.Runtime.Cpu, config.Runtime.Memory); ok {
		s.res = &res
	}
	return s.res
}

// reclaim drains one replica so slots[index] can start. A minimum may take any
// surplus, least priority first; a requested serverless copy takes hot surplus
// only; hot surplus takes only lower-priority hot surplus, never a serverless
// copy (idle ones drain on their own). The victims on one worker must free
// room for the whole request, or nothing is drained.
func (c *controller) reclaim(ctx context.Context, gpu string, index int, slots []*slot, live []*types.EndpointReplica, inv *clusterInventory, pass int) bool {
	if inv == nil || c.s.containers == nil || gpu == types.CPUInventoryKey {
		return false
	}
	s := slots[index]
	res := c.resources(ctx, s)
	if res == nil {
		return false // never destroy a replica for an unknown request shape
	}
	reason := "gpu reclaimed for " + s.endpoint.Spec.ID
	settling := map[string]bool{}
	for _, r := range live {
		if r.Status == types.ReplicaStatusDraining || r.Status == types.ReplicaStatusEvicting {
			if r.StatusReason == reason {
				return false // wait for the release already under way
			}
			settling[r.WorkerID] = true
		}
	}
	if s.entry.Serverless {
		// Use idle capacity of another configured GPU type before taking a hot extra.
		for other, placement := range s.demand.gpus {
			n := uint32(0)
			for _, r := range live {
				if r.EndpointID == s.endpoint.Spec.ID && r.GPU == other && r.Alive() {
					n++
				}
			}
			if other == gpu || placement.MaxReplicas > 0 && n >= placement.MaxReplicas || !inv.canPlace(other, s.endpoint.Spec.Gpu[other].Count) {
				continue
			}
			if backoff, err := c.s.repo.InScheduleBackoff(ctx, s.endpoint.Spec.ID, other); err == nil && !backoff {
				return false
			}
		}
	}
	need := max(s.endpoint.Spec.Gpu[gpu].Count, 1)
	type victim struct {
		replica *types.EndpointReplica
		owner   *slot
		res     replicaResources
	}
	byWorker := map[string][]victim{}
	for i := len(slots) - 1; i >= 0; i-- { // least priority gives back first
		owner := slots[i]
		if i == index || owner.entry.Serverless && pass != passMinimum || pass == passSurplus && i < index || owner.count() <= owner.entry.MinReplicas {
			continue
		}
		candidates := slices.Clone(owner.replicas)
		scaleDownOrder(candidates)
		for _, r := range candidates {
			w := inv.workers[r.WorkerID]
			if !r.Alive() || r.Protected || w == nil || w.gpu != gpu || w.pool != r.PoolName || settling[r.WorkerID] || w.total < need {
				continue
			}
			state, err := c.s.containers.GetContainerState(r.ContainerID)
			if err != nil || state == nil || !state.Evictable || state.Evicting || state.Status != types.ContainerStatusRunning || state.WorkerId != r.WorkerID {
				continue
			}
			if released, ok := reservedReplicaResources(state.Cpu, state.Memory); ok {
				byWorker[r.WorkerID] = append(byWorker[r.WorkerID], victim{r, owner, released})
			}
		}
	}
	for _, id := range slices.Sorted(maps.Keys(byWorker)) {
		w := inv.workers[id]
		free, pool, cpu, memory := w.free, inv.poolFree(gpu, w.pool), w.cpu, w.memory
		taken := map[*slot]uint32{}
		for _, v := range byWorker[id] {
			if taken[v.owner] >= v.owner.count()-v.owner.entry.MinReplicas {
				continue
			}
			taken[v.owner]++
			gpus := max(v.replica.GPUCount, 1)
			free, pool, cpu, memory = free+gpus, pool+gpus, cpu+v.res.cpu, memory+v.res.memory
			if free >= need && pool >= inv.floors[w.pool]+need && cpu >= res.cpu && memory >= res.memory {
				first := byWorker[id][0]
				return c.drainReplica(ctx, first.replica, first.owner.endpoint.Spec.DrainSeconds, false, reason) == nil
			}
		}
	}
	return false
}

// Retirement -----------------------------------------------------------------

// retire drains what config.yaml no longer wants: every replica of a disabled
// endpoint or an unlisted GPU type, and stale versions. A serving stale replica
// goes once a current one serves on its GPU and the GPU keeps its minimum;
// rollout=replace instead accepts downtime and frees the GPU once nothing
// current is on its way. One serving replica per endpoint per tick keeps
// rollouts rolling.
func (c *controller) retire(ctx context.Context, endpoint *types.ManagedEndpoint, fleet *types.Fleet, live []*types.EndpointReplica, blocked map[group]bool) {
	spec := &endpoint.Spec
	placements := fleet.Placements(spec.ID)
	if !endpoint.Enabled() {
		placements = nil
	}
	var stale []*types.EndpointReplica
	serving, currentServing, currentStarting, settling := map[string]uint32{}, map[string]uint32{}, map[string]bool{}, map[string]bool{}
	for _, r := range live {
		if r.EndpointID != spec.ID || blocked[group{spec.ID, r.GPU}] {
			continue
		}
		_, placed := placements[r.GPU]
		current := placed && r.Version == endpoint.Version
		switch {
		case r.Status == types.ReplicaStatusDraining || r.Status == types.ReplicaStatusEvicting:
			settling[r.GPU] = true
		case !r.Alive():
		case current && r.Serving():
			serving[r.GPU]++
			currentServing[r.GPU]++
		case current:
			currentStarting[r.GPU] = true
		default:
			if r.Serving() {
				serving[r.GPU]++
			}
			stale = append(stale, r)
		}
	}
	scaleDownOrder(stale)
	for _, r := range stale {
		placement, placed := placements[r.GPU]
		wasServing := r.Serving()
		reason := fmt.Sprintf("version %d retired", r.Version)
		switch {
		case !endpoint.Enabled():
			reason = "endpoint retired"
		case !placed:
			// A GPU move is a replacement: the old type serves until a new one does.
			if wasServing && len(placements) > 0 && len(currentServing) == 0 {
				continue
			}
			reason = "removed from config.yaml"
		case wasServing && (currentServing[r.GPU] == 0 || serving[r.GPU]-1 < max(placement.MinReplicas, 1)):
			// Keep the GPU's serving count at its minimum, on the new version
			// alone or mixed, unless the app chose to replace in place.
			if spec.Rollout != "replace" || currentStarting[r.GPU] || settling[r.GPU] {
				continue
			}
			reason = fmt.Sprintf("version %d retired (making room for version %d)", r.Version, endpoint.Version)
		}
		if err := c.drainReplica(ctx, r, spec.DrainSeconds, false, reason); err == nil && wasServing && endpoint.Enabled() {
			return
		}
	}
}
