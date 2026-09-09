package managedendpoint

import (
	"cmp"
	"context"
	"fmt"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/common/llmroute"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// The controller is the reconciliation loop: observe replicas, retire the
// ones no endpoint wants any more, then fill each GPU type's idle GPUs with
// its fleet.yaml priority list. Exactly one gateway replica runs it at a time
// (Redis lock); others stand by and take over when it lapses.

const (
	controllerLockKey  = "managed_endpoint:controller"
	controllerLockTTL  = 30 * time.Second
	stubCacheTTL       = time.Minute
	terminalRetention  = 30 * time.Minute
	schedulingGrace    = 10 * time.Minute
	loadingGrace       = 30 * time.Minute
	containerLostGrace = 20 * time.Second
	maxStartsPerTick   = 8
)

type controller struct {
	s    *Service
	lock *common.RedisLock

	stubMu    sync.Mutex
	stubCache map[string]cachedStub

	metricsMu   sync.Mutex
	lastMetrics map[string]llmroute.EngineMetrics // replica id -> previous scrape

	// startedAt is when this gateway came up. Heartbeats cannot arrive while
	// no gateway is serving the harness RPC, so a replica is only stale once
	// it has been silent for the full window *while we were listening*;
	// otherwise every gateway deploy would fail every serving replica.
	startedAt time.Time
}

type cachedStub struct {
	stub    *types.StubWithRelated
	config  *types.StubConfigV1
	fetched time.Time
}

func newController(s *Service) *controller {
	return &controller{s: s, lock: common.NewRedisLock(s.rdb), stubCache: map[string]cachedStub{}, lastMetrics: map[string]llmroute.EngineMetrics{}, startedAt: time.Now()}
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
		// The lease is renewed while a pass runs and the pass's context is
		// cancelled if renewal fails, so a slow pass (many probes) cannot
		// outlive the lock and mutate state under a new leader.
		err := c.lock.WithLease(ctx, controllerLockKey, common.RedisLockOptions{TtlS: int(controllerLockTTL.Seconds()), Retries: 0}, c.reconcile)
		if err != nil && !common.IsRedisLockNotObtained(err) {
			log.Error().Err(err).Msg("managed endpoints: reconcile failed")
		}
	}
}

// silentFor reports how long a replica has gone without a heartbeat that we
// could have observed.
func (c *controller) silentFor(lastHeartbeat, now time.Time) time.Duration {
	if c.startedAt.After(lastHeartbeat) {
		lastHeartbeat = c.startedAt
	}
	return now.Sub(lastHeartbeat)
}

// reconcile is one pass over every endpoint. A panic in one pass is reported
// as an error rather than taking the gateway down.
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
	slices.SortFunc(endpoints, func(a, b *types.ManagedEndpoint) int { return strings.Compare(a.Spec.ID, b.Spec.ID) })
	byID := make(map[string]*types.ManagedEndpoint, len(endpoints))
	for _, endpoint := range endpoints {
		byID[endpoint.Spec.ID] = endpoint
		c.retire(ctx, endpoint, fleet, live)
	}
	for _, gpu := range fleet.GPUs() {
		c.fill(ctx, gpu, fleet.Priority[gpu], byID, live, inv)
	}
	return nil
}

// stub returns the stub record and parsed config for a stub id, cached briefly.
func (c *controller) stub(ctx context.Context, stubID string) (*types.StubWithRelated, *types.StubConfigV1, error) {
	c.stubMu.Lock()
	entry, ok := c.stubCache[stubID]
	c.stubMu.Unlock()
	if ok && time.Since(entry.fetched) < stubCacheTTL {
		return entry.stub, entry.config, nil
	}
	stub, err := c.s.backend.GetStubByExternalId(ctx, stubID)
	if err != nil {
		return nil, nil, err
	}
	if stub == nil || stub.ExternalId == "" {
		return nil, nil, fmt.Errorf("stub %q: %w", stubID, errNotFound)
	}
	config, err := stub.UnmarshalConfig()
	if err != nil {
		return nil, nil, err
	}
	c.stubMu.Lock()
	c.stubCache[stubID] = cachedStub{stub: stub, config: config, fetched: time.Now()}
	c.stubMu.Unlock()
	return stub, config, nil
}

// --- inventory -----------------------------------------------------------------

// eligiblePool is a pool that opted into managed endpoints.
type eligiblePool struct {
	Name     string
	Locality string
}

// clusterInventory is where replicas may run: the endpoint-enabled pools per
// GPU key ("cpu" or a GPU type) and how many GPUs of that type each pool's
// workers currently have free.
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

// poolConfigs lists every known pool: the static worker config plus pools
// the scheduler registered at runtime (agent pools).
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

// localityOf is the network domain of a pool: its configured locality or,
// failing that, the pool name.
func localityOf(name string, cfg types.WorkerPoolConfig) string {
	return cmp.Or(strings.TrimSpace(cfg.Locality), name)
}

// inventory lists the eligible pools and their free GPUs. GPUs of replicas
// still being scheduled are counted as taken: the worker has not reserved
// them yet, and without this every pass until it does would start more.
func (c *controller) inventory(replicas []*types.EndpointReplica) (*clusterInventory, error) {
	workers, err := c.s.workers.GetAllWorkers()
	if err != nil {
		return nil, err
	}
	inv := &clusterInventory{pools: map[string][]eligiblePool{}, free: map[string]map[string]uint32{}}
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
		if w == nil || w.Status == types.WorkerStatusDisabled || w.Gpu == "" {
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
	}
	for _, r := range replicas {
		if r.Status != types.ReplicaStatusScheduling || r.GPU == types.CPUInventoryKey {
			continue
		}
		if free, ok := inv.free[r.GPU][r.PoolName]; ok {
			inv.free[r.GPU][r.PoolName] = free - min(free, max(r.GPUCount, 1))
		}
	}
	return inv, nil
}

// place picks the pool for one replica and reserves its GPUs in the in-memory
// inventory: the eligible pool with the most idle GPUs of the type. Replicas
// only ever fill capacity that is idle right now; when no eligible pool has
// room nothing is submitted (the scheduler is never asked to wait for or
// provision a worker) and the next pass tries again. CPU replicas have no GPU
// inventory to check and go to the first eligible CPU pool; the scheduler
// fits them by cpu/memory. A replica is never submitted without a pool, so it
// cannot land on a pool that did not opt in.
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

// --- endpoints -----------------------------------------------------------------

// liveReplicas returns the alive replicas of one endpoint version on one GPU
// type, and how many of them are ready.
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

// scaleDownOrder sorts replicas so the least valuable drain first: not yet
// ready before ready, then the least loaded, then the newest.
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

// retire drains the replicas an endpoint no longer wants: all of them when it
// is retired, otherwise those that no longer match the template (older
// version, or a GPU type fleet.yaml no longer lists it on), one per tick. A
// stale replica that is serving is kept until a matching replica is ready to
// take the traffic, whether the change is a new version or a move to another
// GPU type; only when fleet.yaml lists the endpoint nowhere is it drained
// outright.
func (c *controller) retire(ctx context.Context, endpoint *types.ManagedEndpoint, fleet *types.Fleet, live []*types.EndpointReplica) {
	spec := &endpoint.Spec
	if !endpoint.Enabled() {
		for _, r := range live {
			if r.EndpointID == spec.ID {
				_ = c.drainReplica(ctx, r, spec.DrainSeconds, false, "endpoint retired")
			}
		}
		return
	}
	listed := len(fleet.Placements(spec.ID)) > 0
	matches := func(r *types.EndpointReplica) bool {
		return r.Version == endpoint.Version && fleet.Lists(spec.ID, r.GPU)
	}
	var currentReady int
	for _, r := range live {
		if r.EndpointID == spec.ID && matches(r) && r.Serving() {
			currentReady++
		}
	}
	for _, r := range live {
		if r.EndpointID != spec.ID || !r.Alive() || matches(r) {
			continue
		}
		if r.Serving() && currentReady == 0 && listed {
			continue
		}
		reason := fmt.Sprintf("version %d retired", r.Version)
		if !fleet.Lists(spec.ID, r.GPU) {
			reason = "removed from fleet.yaml"
		}
		if err := c.drainReplica(ctx, r, spec.DrainSeconds, false, reason); err == nil {
			return
		}
	}
}

// fill converges one GPU type on its priority list. In order, every entry
// takes the idle GPUs it may have (up to its cap) and drains down to its cap
// when it is over. When an entry is short of GPUs and none are idle, the
// entries below it give one replica back per tick so the GPUs move up the
// list; an entry that is still starting a replica, or backing off after a
// failed start, waits instead so a replica that cannot come up never drains
// the others. An uncapped entry therefore leaves nothing to those below it.
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
		case e.Max > 0 && n > e.Max:
			scaleDownOrder(current)
			for _, r := range current[:n-e.Max] {
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
		case e.Max == 0 || n < e.Max:
			want := uint32(maxStartsPerTick)
			if e.Max > 0 {
				want = e.Max - n
			}
			if noRoom := c.grow(ctx, endpoint, gpu, want, inv); noRoom && ready == len(current) {
				short = endpoint
			}
		}
	}
}

// grow starts up to want replicas on gpu, bounded per tick, and reports
// whether it stopped because no eligible pool had an idle GPU. After a start
// failure the (endpoint, gpu) backs off so a crash-looping replica is not
// resubmitted on every pass.
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
		if _, err := c.startReplica(ctx, startSpec{Endpoint: endpoint, GPU: gpu, Pool: pool}); err != nil {
			log.Warn().Err(err).Str("endpoint_id", endpoint.Spec.ID).Str("gpu", gpu).Msg("managed endpoints: start replica failed")
			return false
		}
	}
	return false
}
