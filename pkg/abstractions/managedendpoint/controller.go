package managedendpoint

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
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

// The controller is the reconciliation loop: observe replicas, compute the
// GPU inventory endpoints may occupy, divide it between endpoints as
// fleet.yaml says, and start / drain replicas to converge. Exactly one
// gateway replica runs it at a time (Redis lock); others stand by and take
// over when it lapses.

const (
	controllerLockKey  = "managed_endpoint:controller"
	controllerLockTTL  = 30 * time.Second
	idleWindow         = 5 * time.Minute // no traffic for this long releases replicas above quota
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
	ticker := time.NewTicker(c.s.config.Fill.ReconcileInterval)
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

// reconcile is one pass: observe replicas, compute inventory, plan the fill
// across every endpoint, then grow, shrink and roll each one. A panic in one
// pass is reported as an error rather than taking the gateway down.
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

	// Every endpoint's placements compete for the same inventory, so the
	// fill plan is computed once across all of them.
	var targets []fillTarget
	for _, endpoint := range endpoints {
		if !endpoint.Enabled() {
			continue
		}
		for _, ft := range fleet.Placements(endpoint.Spec.ID) {
			if _, ok := endpoint.Spec.Gpu[ft.GPU]; !ok {
				continue // fleet.yaml names a GPU the app cannot run on; validation already flagged it
			}
			targets = append(targets, fillTarget{EndpointID: endpoint.Spec.ID, FleetTarget: ft, Demand: c.demand(ctx, endpoint, ft, live)})
		}
	}
	plans := planFill(inv.byType, targets)

	var errs []error
	for _, endpoint := range endpoints {
		if err := c.reconcileEndpoint(ctx, endpoint, fleet, plans, live, inv); err != nil {
			errs = append(errs, fmt.Errorf("endpoint %s: %w", endpoint.Spec.ID, err))
		}
	}
	return errors.Join(errs...)
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

// workerSlot is one worker's inventory of a single GPU type. Free is what the
// scheduler reports as unallocated; Held is what endpoint replicas occupy.
type workerSlot struct {
	WorkerID string
	PoolName string
	Locality string
	Total    uint32
	Free     uint32
	Held     uint32
	// MaxShare is the pool's cap on the fraction of its GPUs endpoints may
	// hold; zero means the cluster default.
	MaxShare float64
}

// gpuInventory is all eligible workers carrying one GPU type.
type gpuInventory struct {
	GPU          string
	Workers      []workerSlot
	clusterShare float64
}

func (w workerSlot) share(clusterShare float64) float64 {
	if w.MaxShare > 0 {
		return min(clusterShare, w.MaxShare)
	}
	return clusterShare
}

// allowance is the number of GPUs endpoints may hold after applying the
// cluster and per-pool share caps. Free GPUs plus those endpoints already
// hold count; GPUs held by serverless workloads never do.
func (g *gpuInventory) allowance() uint32 {
	var allowed uint32
	for pool := range g.pools() {
		allowed += g.poolAllowance(pool)
	}
	return allowed
}

func (g *gpuInventory) pools() map[string]struct{} {
	out := map[string]struct{}{}
	for _, w := range g.Workers {
		out[w.PoolName] = struct{}{}
	}
	return out
}

// poolAllowance is one pool's share cap in GPUs; poolHeld is what endpoints
// hold there. Placement spends each pool's own budget, so a pool capped at
// one GPU never hosts two replicas on allowance another pool contributed.
func (g *gpuInventory) poolAllowance(pool string) uint32 {
	var allowed float64
	for _, w := range g.Workers {
		if w.PoolName == pool {
			allowed += float64(w.Free+w.Held) * w.share(g.clusterShare)
		}
	}
	return uint32(math.Floor(allowed + 1e-9))
}

func (g *gpuInventory) poolHeld(pool string) uint32 {
	var held uint32
	for _, w := range g.Workers {
		if w.PoolName == pool {
			held += w.Held
		}
	}
	return held
}

// reserve picks the least-loaded worker that fits count GPUs within its
// pool's remaining budget, records the GPUs as held and returns the slot.
func (g *gpuInventory) reserve(count uint32) (workerSlot, bool) {
	count = max(count, 1)
	best := -1
	remaining := map[string]uint32{}
	for pool := range g.pools() {
		allowance := g.poolAllowance(pool)
		remaining[pool] = allowance - min(allowance, g.poolHeld(pool))
	}
	for i, w := range g.Workers {
		if w.Free < count || remaining[w.PoolName] < count {
			continue
		}
		if best < 0 || w.Free > g.Workers[best].Free || (w.Free == g.Workers[best].Free && w.Held < g.Workers[best].Held) {
			best = i
		}
	}
	if best < 0 {
		return workerSlot{}, false
	}
	w := &g.Workers[best]
	w.Free -= min(w.Free, count)
	w.Held += count
	return *w, true
}

// eligiblePool is a pool that opted into managed endpoints. It is known even
// when none of its workers currently has free capacity, so protected
// replicas that may provision a worker still land in a pool that agreed to
// host them.
type eligiblePool struct {
	Name     string
	Locality string
}

// clusterInventory is the GPU inventory endpoints may use, keyed by GPU key
// ("cpu" or a GPU type).
type clusterInventory struct {
	byType map[string]*gpuInventory
	// pools lists the endpoint-enabled pools per GPU key, regardless of
	// current worker capacity.
	pools map[string][]eligiblePool
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

// inventory reads worker state and folds in what replicas already hold.
func (c *controller) inventory(replicas []*types.EndpointReplica) (*clusterInventory, error) {
	workers, err := c.s.workers.GetAllWorkers()
	if err != nil {
		return nil, err
	}
	heldByWorker := map[string]uint32{}
	for _, r := range replicas {
		if r.WorkerID != "" && !r.Status.Terminal() {
			heldByWorker[r.WorkerID] += r.GPUCount
		}
	}

	inv := &clusterInventory{byType: map[string]*gpuInventory{}, pools: map[string][]eligiblePool{}}
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
		if w == nil || w.Status == types.WorkerStatusDisabled {
			continue
		}
		cfg, ok := c.poolConfig(w.PoolName)
		if !ok || !cfg.ManagedEndpoints.Enabled || w.TotalGpuCount == 0 || w.Gpu == "" {
			continue
		}
		key := types.GPUKey(w.Gpu)
		entry := inv.byType[key]
		if entry == nil {
			entry = &gpuInventory{GPU: key, clusterShare: c.s.config.Fill.MaxClusterShare}
			inv.byType[key] = entry
		}
		entry.Workers = append(entry.Workers, workerSlot{
			WorkerID: w.Id, PoolName: w.PoolName, Locality: localityOf(w.PoolName, cfg),
			Total: w.TotalGpuCount, Free: w.FreeGpuCount, Held: min(heldByWorker[w.Id], w.TotalGpuCount),
			MaxShare: cfg.ManagedEndpoints.MaxShare,
		})
	}
	return inv, nil
}

// place chooses a pool for a target, reserving the GPUs in the in-memory
// inventory. CPU targets go to the first endpoint-enabled CPU pool; GPU
// targets go to the least-loaded eligible worker. When no worker has room,
// a protected target (which may provision a new worker) falls back to an
// eligible pool; a replica is never submitted without a pool, so the
// scheduler cannot place it on a pool that did not opt in.
func (c *controller) place(inv *clusterInventory, target types.FleetTarget, protected bool) (eligiblePool, bool) {
	pools := inv.pools[target.GPU]
	if target.IsCPU() {
		if len(pools) == 0 {
			return eligiblePool{}, false
		}
		return pools[0], true
	}
	if entry := inv.byType[target.GPU]; entry != nil {
		if slot, ok := entry.reserve(target.Count); ok {
			return eligiblePool{Name: slot.PoolName, Locality: slot.Locality}, true
		}
	}
	if !protected || len(pools) == 0 {
		return eligiblePool{}, false
	}
	return pools[0], true
}

// --- fill planning -------------------------------------------------------------

// fillTarget is one (endpoint, gpu) unit of placement. Demand is the replica
// count the router asks for (queue pressure); it may exceed the fair-share
// quota when spare capacity exists.
type fillTarget struct {
	EndpointID string
	types.FleetTarget
	Demand uint32
}

func (t fillTarget) key() string { return t.EndpointID + "|" + t.GPU }

// fillPlan is the placement decision for one fillTarget: Quota is the
// fair-share allocation, Desired what the controller converges toward.
type fillPlan struct {
	Quota   uint32
	Desired uint32
}

// planFill divides each GPU type's allowance between the endpoints placed on
// it in proportion to their fleet shares, then clamps by min/max. Shares
// summing to more than one are normalized so an over-subscribed fleet.yaml
// degrades gracefully instead of exceeding the cluster cap.
func planFill(inventory map[string]*gpuInventory, targets []fillTarget) map[string]fillPlan {
	plans := make(map[string]fillPlan, len(targets))
	byType := map[string][]fillTarget{}
	for _, t := range targets {
		byType[t.GPU] = append(byType[t.GPU], t)
	}
	for gpu, group := range byType {
		var allowance uint32
		if inv := inventory[gpu]; inv != nil {
			allowance = inv.allowance()
		}
		var shareSum float64
		for _, t := range group {
			shareSum += math.Max(t.Share, 0)
		}
		norm := 1 / math.Max(shareSum, 1)
		for _, t := range group {
			var quota uint32
			if t.Share > 0 && allowance > 0 {
				quota = uint32(math.Floor(float64(allowance)*t.Share*norm/float64(max(t.Count, 1)) + 1e-9))
			}
			desired := max(quota, t.Demand, t.Min)
			if t.Max > 0 {
				desired = min(desired, t.Max)
			}
			plans[t.key()] = fillPlan{Quota: quota, Desired: desired}
		}
	}
	return plans
}

// replicaSet is the live replicas of the current version for one fillTarget,
// partitioned for scale decisions.
type replicaSet struct {
	Live      []*types.EndpointReplica // scheduling|loading|ready
	Ready     []*types.EndpointReplica // subset of Live that is ready
	Protected uint32
}

func partitionReplicas(replicas []*types.EndpointReplica, endpointID, gpu string, version uint) replicaSet {
	var set replicaSet
	for _, r := range replicas {
		if r.EndpointID != endpointID || r.GPU != gpu || r.Version != version || !r.Alive() {
			continue
		}
		set.Live = append(set.Live, r)
		if r.Status == types.ReplicaStatusReady {
			set.Ready = append(set.Ready, r)
		}
		if r.Protected {
			set.Protected++
		}
	}
	return set
}

// scaleDownCandidates orders live replicas so the least valuable are drained
// first: unprotected before protected, then not-yet-ready before ready, then
// the least loaded, then the newest.
func scaleDownCandidates(set replicaSet) []*types.EndpointReplica {
	out := slices.Clone(set.Live)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
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
	return out
}

// demand is the replica count recent traffic asks for: one more than serving
// when the last minute shows queueing at the router (hot endpoints grow past
// their fair share into spare capacity, still bounded by max), the current
// count while traffic flows, and zero once the endpoint has been idle for
// idleWindow so capacity acquired under load returns to the quota and min.
func (c *controller) demand(ctx context.Context, endpoint *types.ManagedEndpoint, ft types.FleetTarget, live []*types.EndpointReplica) uint32 {
	set := partitionReplicas(live, endpoint.Spec.ID, ft.GPU, endpoint.Version)
	ready := uint32(len(set.Ready))
	if ready == 0 {
		return 0
	}
	metrics, err := c.s.repo.GetRouteMetrics(ctx, endpoint.Spec.ID, ft.GPU, "", time.Minute)
	if err != nil || metrics == nil {
		return ready
	}
	if metrics.Requests == 0 {
		if idle, err := c.s.repo.GetRouteMetrics(ctx, endpoint.Spec.ID, ft.GPU, "", idleWindow); err == nil && idle != nil && idle.Requests == 0 {
			return 0
		}
		return ready
	}
	if meanQueueWait := time.Duration(metrics.QueueWaitSumMs/metrics.Requests) * time.Millisecond; meanQueueWait > c.s.config.Routing.MaxQueueWait/2 {
		return ready + 1
	}
	var inFlight, maxConcurrency int64
	for _, r := range set.Ready {
		inFlight += r.Capacity.InFlight
		maxConcurrency += r.Capacity.MaxConcurrency
	}
	if maxConcurrency > 0 && inFlight*10 >= maxConcurrency*8 {
		return ready + 1
	}
	return ready
}

// --- endpoints -----------------------------------------------------------------

// reconcileEndpoint converges one endpoint's replicas toward the fill plan
// and replaces replicas of older versions.
func (c *controller) reconcileEndpoint(ctx context.Context, endpoint *types.ManagedEndpoint, fleet *types.Fleet, plans map[string]fillPlan, live []*types.EndpointReplica, inv *clusterInventory) error {
	spec := &endpoint.Spec
	if !endpoint.Enabled() {
		for _, r := range live {
			if r.EndpointID == spec.ID {
				_ = c.drainReplica(ctx, r, spec.DrainSeconds, false, "endpoint retired")
			}
		}
		return nil
	}

	placed := map[string]bool{}
	for _, ft := range fleet.Placements(spec.ID) {
		placed[ft.GPU] = true
		plan := plans[fillTarget{EndpointID: spec.ID, FleetTarget: ft}.key()]
		set := partitionReplicas(live, spec.ID, ft.GPU, endpoint.Version)
		current := uint32(len(set.Live))
		switch {
		case current < plan.Desired:
			c.grow(ctx, endpoint, ft, plan.Desired-current, set.Protected, inv)
		case current > plan.Desired:
			excess := current - plan.Desired
			for _, r := range scaleDownCandidates(set) {
				if excess == 0 {
					break
				}
				if r.Protected && current-excess < ft.Min {
					continue
				}
				if err := c.drainReplica(ctx, r, spec.DrainSeconds, false, "scale down to fair share"); err == nil {
					excess--
				}
			}
		}
		c.retireStaleVersions(ctx, endpoint, ft.GPU, live)
	}
	// Replicas on GPU types fleet.yaml no longer places this endpoint on.
	for _, r := range live {
		if r.EndpointID == spec.ID && !placed[r.GPU] {
			_ = c.drainReplica(ctx, r, spec.DrainSeconds, false, "removed from fleet")
		}
	}
	return nil
}

// grow starts up to need replicas. Replicas needed to satisfy the fleet's
// min are protected (they may trigger provisioning); the rest are
// opportunistic and only land on free capacity.
func (c *controller) grow(ctx context.Context, endpoint *types.ManagedEndpoint, ft types.FleetTarget, need, protectedLive uint32, inv *clusterInventory) {
	protectedNeeded := ft.Min - min(ft.Min, protectedLive)
	backoff, _ := c.s.repo.InScheduleBackoff(ctx, endpoint.Spec.ID, ft.GPU)

	for i := uint32(0); i < min(need, maxStartsPerTick); i++ {
		protected := protectedNeeded > 0
		if !protected && backoff {
			return
		}
		pool, ok := c.place(inv, ft, protected)
		if !ok {
			if protected {
				log.Debug().Str("endpoint_id", endpoint.Spec.ID).Str("gpu", ft.GPU).Msg("managed endpoints: no eligible pool for protected replica")
			}
			return
		}
		if _, err := c.startReplica(ctx, startSpec{Endpoint: endpoint, Target: ft, Pool: pool, Protected: protected}); err != nil {
			log.Warn().Err(err).Str("endpoint_id", endpoint.Spec.ID).Str("gpu", ft.GPU).Msg("managed endpoints: start replica failed")
			return
		}
		if protected {
			protectedNeeded--
		}
	}
}

// retireStaleVersions drains replicas running an older version, one per GPU
// per tick, once the current version has something ready to take the
// traffic (or the old replica is not serving anyway).
func (c *controller) retireStaleVersions(ctx context.Context, endpoint *types.ManagedEndpoint, gpu string, live []*types.EndpointReplica) {
	currentReady := len(partitionReplicas(live, endpoint.Spec.ID, gpu, endpoint.Version).Ready)
	for _, r := range live {
		if r.EndpointID != endpoint.Spec.ID || r.GPU != gpu || r.Version == endpoint.Version || !r.Alive() {
			continue
		}
		if r.Status == types.ReplicaStatusReady && currentReady == 0 {
			continue
		}
		if err := c.drainReplica(ctx, r, endpoint.Spec.DrainSeconds, false, fmt.Sprintf("version %d retired", r.Version)); err == nil {
			return
		}
	}
}
