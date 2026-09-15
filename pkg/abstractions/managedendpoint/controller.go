package managedendpoint

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"slices"
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
	s *Service

	metricsMu   sync.Mutex
	lastMetrics map[string]llmroute.EngineMetrics // replica id -> previous scrape

	startedAt time.Time // heartbeats could not arrive before this; see silentFor
}

func newController(s *Service) *controller {
	return &controller{s: s, lastMetrics: map[string]llmroute.EngineMetrics{}, startedAt: time.Now()}
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
		err := c.s.lock.WithLease(ctx, controllerLockKey, common.RedisLockOptions{TtlS: int(controllerLockTTL.Seconds()), Retries: 0}, c.reconcile)
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

// group is one endpoint's placement on one GPU type, the unit protection,
// fill and retire reason about.
type group struct{ endpointID, gpu string }

// protectedReplicas selects the copies that cover each preemption:false
// minimum: serving current first, then serving old (so a rollout keeps its
// minimum until the replacement is ready), then existing roles to avoid churn.
// With cluster preemption off, every hot replica is protected.
func protectedReplicas(fleet *types.Fleet, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica, preemptionEnabled bool) map[string]bool {
	desired := map[string]bool{}
	rank := func(endpoint *types.ManagedEndpoint, r *types.EndpointReplica) int {
		n := 0
		if r.Serving() {
			n += 8
		}
		if r.Version == endpoint.Version {
			n += 1
			if r.Serving() {
				n += 4
			}
		}
		if r.Protected {
			n += 2
		}
		return n
	}
	if !preemptionEnabled {
		for _, r := range live {
			if r.Alive() && !fleet.Placements(r.EndpointID)[r.GPU].Serverless {
				desired[r.ID] = true
			}
		}
		return desired
	}
	for id, endpoint := range endpoints {
		if !endpoint.Enabled() {
			continue
		}
		for gpu, placement := range fleet.Placements(id) {
			if !placement.ProtectsMinimum() || placement.MinReplicas == 0 {
				continue
			}
			var candidates []*types.EndpointReplica
			for _, r := range live {
				if r.EndpointID == id && r.GPU == gpu && r.Alive() {
					candidates = append(candidates, r)
				}
			}
			slices.SortFunc(candidates, func(a, b *types.EndpointReplica) int {
				return cmp.Or(cmp.Compare(rank(endpoint, b), rank(endpoint, a)), a.StartedAt.Compare(b.StartedAt), strings.Compare(a.ID, b.ID))
			})
			for _, r := range candidates[:min(int(placement.MinReplicas), len(candidates))] {
				desired[r.ID] = true
			}
		}
	}
	return desired
}

// protect applies protectedReplicas: promote before demoting so a policy
// change or rollout never drops below the serving minimum. The repository
// change fences scheduler eviction without restarting the engine. A failed
// group keeps its roles and is skipped by fill and retire this tick.
func (c *controller) protect(ctx context.Context, fleet *types.Fleet, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica) (map[group]bool, error) {
	desired := protectedReplicas(fleet, endpoints, live, c.s.config.Preemption.Enabled)
	blocked := map[group]bool{}
	var errs []error
	for _, want := range []bool{true, false} {
		for _, r := range live {
			g := group{r.EndpointID, r.GPU}
			if blocked[g] || !r.Alive() {
				continue
			}
			if desired[r.ID] != want || r.Protected == want {
				continue // not this phase, or already right
			}
			updated, err := c.s.repo.SetReplicaProtection(ctx, r.ID, want)
			if err != nil {
				blocked[g] = true
				errs = append(errs, fmt.Errorf("replica %s: set protection: %w", r.ID, err))
				continue
			}
			*r = *updated
		}
	}
	return blocked, errors.Join(errs...)
}

// endpointDemand is what the controller reads from a serverless endpoint's
// demand lease and the serving capacity it saw for it this tick.
type endpointDemand struct {
	active   int64
	warm     bool
	pending  bool                            // a recent authorized capacity rejection requested startup
	capacity int64                           // serving capacity across every GPU type, including hot copies
	starting bool                            // wait for capacity to become known before adding another copy
	gpus     map[string]types.FleetPlacement // configured on-demand GPU alternatives
}

// initialDemandGrace keeps a fresh serverless replica while its first
// requests arrive: through startup, then one idle timeout after ready.
func initialDemandGrace(replica *types.EndpointReplica, now time.Time) bool {
	if !replica.ReadyAt.IsZero() {
		return now.Sub(replica.ReadyAt) < demandIdleTimeout
	}
	return !replica.StartedAt.IsZero() && now.Sub(replica.StartedAt) < serverlessStartupTimeout
}

func (c *controller) readDemand(ctx context.Context, fleet *types.Fleet, live []*types.EndpointReplica) map[string]*endpointDemand {
	out := make(map[string]*endpointDemand)
	for id := range fleet.Endpoints {
		if fleet.Serverless(id) {
			// A failed read leaves nil: neither scale up nor scale down based
			// on unknown demand. Hot placements continue independently.
			out[id], _ = c.s.demand(ctx, id, demandRead, "", 0)
			if demand := out[id]; demand != nil {
				demand.gpus = make(map[string]types.FleetPlacement)
				for gpu, placement := range fleet.Placements(id) {
					if placement.Serverless {
						demand.gpus[gpu] = placement
					}
				}
			}
		}
	}
	for _, replica := range live {
		if replica.Status == types.ReplicaStatusDraining || replica.Status == types.ReplicaStatusEvicting {
			if id, reclaimed := strings.CutPrefix(replica.StatusReason, "gpu reclaimed for "); reclaimed {
				if demand := out[id]; demand != nil {
					demand.starting = true // wait for a donor on any GPU type to release capacity
				}
			}
		}
		demand := out[replica.EndpointID]
		if demand == nil || !replica.Alive() {
			continue
		}
		if !replica.Serving() {
			demand.starting = true
		} else {
			capacity := replica.Capacity.MaxConcurrency
			if capacity <= 0 {
				capacity = math.MaxInt64 // routing treats zero as unbounded
			}
			demand.capacity += min(capacity, math.MaxInt64-demand.capacity)
		}
	}
	return out
}

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
