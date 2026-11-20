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
	"github.com/beam-cloud/beta9/pkg/scheduler"
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
	machineGrace       = 30 * time.Minute // a Spot replica's machine is bought and boots before it is scheduled
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

// silent reports a harness replica whose heartbeats stopped: its engine or
// its machine is gone, whatever its record still says.
func (s *Service) silent(replica *types.EndpointReplica, now time.Time) bool {
	return replica.HarnessEnabled && !replica.LastHeartbeat.IsZero() &&
		s.controller.silentFor(replica.LastHeartbeat, now) > s.config.ReplicaStaleAfter
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
	inv, err := c.inventory(live, replicas)
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
// With cluster preemption off, every hot replica is protected. A Spot replica
// always is: its machine was bought for it, and only its provider takes it.
func protectedReplicas(fleet *types.Fleet, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica, preemptionEnabled bool) map[string]bool {
	desired := map[string]bool{}
	for _, r := range live {
		if r.Alive() && r.Spot {
			desired[r.ID] = true
		}
	}
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
			if r.Alive() && !r.Spot && !fleet.Placements(r.EndpointID)[r.GPU].Serverless {
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
				if r.EndpointID == id && r.GPU == gpu && r.Alive() && !r.Spot {
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
	Spot     bool
}

type inventoryWorker struct {
	pool, gpu   string
	free, total uint32
	cpu, memory int64 // free, in scheduler units
	// booting is a machine a Spot pool has bought that has yet to join.
	booting bool
}

// clusterInventory is where replicas may run. A pool that opted in is one of
// two kinds. Most lend replicas the GPUs their ready workers have idle
// (pools): nothing is provisioned for a replica there, and the scheduler
// enforces the same pool floors at admission. A Spot pool buys machines for
// the replicas of Spot placements (spot), and runs no others.
type clusterInventory struct {
	pools   map[string][]eligiblePool // by GPU key, sorted by name
	spot    map[string][]eligiblePool // by GPU key, sorted by name
	workers map[string]*inventoryWorker
	floors  map[string]uint32 // pool -> minFreeGPU
	// outlook is what to expect of a Spot pool for a replica; see
	// scheduler.PoolOutlook.
	outlook func(pool string, gpus uint32, res replicaResources) scheduler.MachineOutlook
	// buying is the Spot pools with a replica that waits for a machine no
	// worker stands for yet.
	buying map[string]bool
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

// hosts reports whether a pool on this cluster takes the replicas of a
// placement on the GPU type: a Spot pool for a Spot placement, and a pool
// that lends its idle GPUs for any other.
func (c *controller) hosts(gpu string, spot bool) bool {
	for _, cfg := range c.poolConfigs() {
		if cfg.ManagedEndpoints.Enabled && types.GPUKey(cfg.GPUType) == gpu && cfg.AllowsManagedEndpointSpot() == spot {
			return true
		}
	}
	return false
}

func localityOf(name string, cfg types.WorkerPoolConfig) string {
	return cmp.Or(strings.TrimSpace(cfg.Locality), name)
}

func (c *controller) inventory(replicas, known []*types.EndpointReplica) (*clusterInventory, error) {
	workers, err := c.s.workers.GetAllWorkers()
	if err != nil {
		return nil, err
	}
	inv := &clusterInventory{
		pools: map[string][]eligiblePool{}, spot: map[string][]eligiblePool{},
		workers: map[string]*inventoryWorker{}, floors: map[string]uint32{}, buying: map[string]bool{},
	}
	if c.s.scheduler != nil {
		inv.outlook = func(pool string, gpus uint32, res replicaResources) scheduler.MachineOutlook {
			return c.s.scheduler.PoolOutlook(pool, res.cpu, res.memory, gpus)
		}
	}
	configs := c.poolConfigs()
	for name, cfg := range configs {
		if !cfg.ManagedEndpoints.Enabled {
			continue
		}
		key := types.GPUKey(cfg.GPUType)
		pool := eligiblePool{Name: name, Locality: localityOf(name, cfg), Spot: cfg.AllowsManagedEndpointSpot()}
		if pool.Spot {
			inv.spot[key] = append(inv.spot[key], pool)
			continue
		}
		inv.pools[key] = append(inv.pools[key], pool)
		floor, _ := strconv.ParseUint(cfg.PoolSizing.MinFreeGPU, 10, 32)
		inv.floors[name] = uint32(floor)
	}
	for _, pools := range []map[string][]eligiblePool{inv.pools, inv.spot} {
		for _, pools := range pools {
			slices.SortFunc(pools, func(a, b eligiblePool) int { return strings.Compare(a.Name, b.Name) })
		}
	}
	// A worker that a replica was scheduled on has joined. When its machine
	// is lost its pool makes it pending again, with all its room, until the
	// pool lets go of the machine: that is not a machine on its way.
	joined := map[string]bool{}
	for _, r := range known {
		joined[r.WorkerID] = true
	}
	for _, w := range workers {
		if w == nil || w.Gpu == "" {
			continue
		}
		// A machine its provider may take back hosts the replicas of Spot
		// placements, and no others: admission holds the same line.
		cfg, ok := configs[w.PoolName]
		if !ok || !cfg.ManagedEndpoints.Enabled || w.Preemptable != cfg.AllowsManagedEndpointSpot() {
			continue
		}
		// The worker of a machine a Spot pool bought is pending until the
		// machine joins, and the scheduler keeps its room for what waits.
		booting := w.Status == types.WorkerStatusPending && w.Preemptable && cfg.Buys() && !joined[w.Id]
		if w.Status != types.WorkerStatusAvailable && !booting {
			continue
		}
		inv.workers[w.Id] = &inventoryWorker{pool: w.PoolName, gpu: types.GPUKey(w.Gpu), free: w.FreeGpuCount, total: w.TotalGpuCount, cpu: w.FreeCpu, memory: w.FreeMemory, booting: booting}
	}
	// Replicas still waiting for a worker hold GPUs no worker reports yet.
	for _, r := range replicas {
		if r.Status != types.ReplicaStatusScheduling || r.WorkerID != "" || r.GPU == types.CPUInventoryKey {
			continue
		}
		if r.Spot {
			// It takes a machine of its pool, or waits for one to be bought.
			res := c.reserved(r)
			if w := inv.spotWorker(r.GPU, r.PoolName, r.GPUCount, res); w != nil {
				inv.takeSpot(w, r.GPUCount, res)
			} else {
				inv.buying[r.PoolName] = true
			}
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

// reserved is what the replica's container holds on its worker, and nothing
// when that is not known.
func (c *controller) reserved(r *types.EndpointReplica) replicaResources {
	if c.s.containers == nil {
		return replicaResources{}
	}
	state, err := c.s.containers.GetContainerState(r.ContainerID)
	if err != nil || state == nil {
		return replicaResources{}
	}
	res, _ := reservedReplicaResources(state.Cpu, state.Memory)
	return res
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

// spotWorker is the machine of a Spot pool that a replica would take: one
// with room for all of it, a ready one before one that is booting, and the
// fullest of those, as the scheduler packs them.
func (inv *clusterInventory) spotWorker(gpu, pool string, count uint32, res replicaResources) *inventoryWorker {
	for _, booting := range []bool{false, true} {
		var best *inventoryWorker
		var bestID string
		for id, w := range inv.workers {
			fits := w.pool == pool && w.gpu == gpu && w.booting == booting && w.free >= max(count, 1) && w.cpu >= res.cpu && w.memory >= res.memory
			if fits && (best == nil || w.free < best.free || w.free == best.free && id < bestID) {
				best, bestID = w, id
			}
		}
		if best != nil {
			return best
		}
	}
	return nil
}

func (inv *clusterInventory) takeSpot(w *inventoryWorker, count uint32, res replicaResources) {
	w.free, w.cpu, w.memory = w.free-max(count, 1), w.cpu-res.cpu, w.memory-res.memory
}

// spotRoom reports whether a Spot machine that is ready has room for a replica.
func (inv *clusterInventory) spotRoom(gpu string, count uint32, res replicaResources) bool {
	return inv != nil && slices.ContainsFunc(inv.spot[gpu], func(pool eligiblePool) bool {
		w := inv.spotWorker(gpu, pool.Name, count, res)
		return w != nil && !w.booting
	})
}

// spotChoice is a Spot pool a replica could go to.
type spotChoice struct {
	pool    eligiblePool
	worker  *inventoryWorker // the machine there it would take; nil when the pool would buy one
	outlook scheduler.MachineOutlook
}

func (c spotChoice) ready() bool { return c.worker != nil && !c.worker.booting }

// wait is how long until the replica has a machine: no time on one that is
// ready, and otherwise as long as the pool's machines take to come up.
func (c spotChoice) wait() time.Duration {
	if c.ready() {
		return 0
	}
	return c.outlook.ComeUp
}

// load is what the pool counts as holding, where held is the endpoint's
// replicas by pool: one more for each machine it loses an hour, and for each
// hour the replica would wait for a machine there.
func (c spotChoice) load(held map[string]int) float64 {
	return float64(held[c.pool.Name]) + c.outlook.Taken + c.wait().Hours()
}

// placeSpot picks the Spot pool for a replica, among those that have a
// machine with room for it, ready or booting, or can buy one.
//
// A minimum is there to survive the loss of a machine, and Spot machines are
// lost a market at a time, so it is spread: its next copy goes to the pool
// with the least load. So among pools that keep their machines a minimum is
// spread evenly, and goes where it serves soonest among equals. A pool that
// loses its machines as fast as it buys them is passed over until the others
// hold several, and how long a machine takes to come up decides between
// pools that keep theirs about as long.
//
// A copy started for requests that are waiting goes where a machine is
// ready, as it serves minutes sooner there, and among such pools as a
// minimum does.
func (inv *clusterInventory) placeSpot(gpu string, count uint32, res replicaResources, held map[string]int, spread bool) (spotChoice, bool) {
	if inv == nil {
		return spotChoice{}, false
	}
	before := func(a, b spotChoice) bool {
		switch {
		case !spread && a.ready() != b.ready():
			return a.ready()
		case a.load(held) != b.load(held):
			return a.load(held) < b.load(held)
		}
		// Of two that are level, a machine that is bought comes first.
		return a.worker != nil && b.worker == nil
	}
	var best *spotChoice
	for _, pool := range inv.spot[gpu] {
		choice := spotChoice{pool: pool, worker: inv.spotWorker(gpu, pool.Name, count, res)}
		if inv.outlook != nil {
			choice.outlook = inv.outlook(pool.Name, max(count, 1), res)
		}
		// A pool is asked for one machine at a time: what it can still buy,
		// and the room left on the machine it gets, is known once it has it.
		if choice.worker == nil && (inv.buying[pool.Name] || !choice.outlook.CanBuy) {
			continue
		}
		if best == nil || before(choice, *best) {
			best = &choice
		}
	}
	switch {
	case best == nil:
		return spotChoice{}, false
	case best.worker == nil:
		inv.buying[best.pool.Name] = true
	default:
		inv.takeSpot(best.worker, count, res)
	}
	return *best, true
}
