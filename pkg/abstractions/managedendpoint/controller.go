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
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// The controller is the reconciliation loop: observe replicas, compute the
// GPU inventory endpoints may occupy, divide it between endpoints by share,
// and start / drain replicas to converge. Exactly one gateway replica runs
// it at a time (Redis lock); others stand by and take over when it lapses.

const (
	controllerLockKey  = "managed_endpoint:controller"
	controllerLockTTL  = 30 * time.Second
	stubCacheTTL       = time.Minute
	terminalRetention  = 30 * time.Minute
	schedulingGrace    = 10 * time.Minute
	loadingGrace       = 30 * time.Minute
	containerLostGrace = 20 * time.Second
	maxStartsPerTick   = 8

	serviceReplicaPrefix = "service:"
	serviceDrainSeconds  = 30
)

// serviceReplicaID namespaces service replicas away from endpoint ids.
func serviceReplicaID(name string) string { return serviceReplicaPrefix + name }

type controller struct {
	s    *Service
	lock *common.RedisLock

	stubMu    sync.Mutex
	stubCache map[string]cachedStub

	// leaderSince is when this instance last became leader. Heartbeats
	// cannot arrive while no gateway is serving the harness RPC, so a
	// replica is only stale once it has been silent for the full window
	// *while we were listening*; otherwise every gateway deploy would fail
	// every serving replica.
	leaderSince atomic.Int64
}

type cachedStub struct {
	stub    *types.StubWithRelated
	config  *types.StubConfigV1
	fetched time.Time
}

func newController(s *Service) *controller {
	return &controller{s: s, lock: common.NewRedisLock(s.rdb), stubCache: map[string]cachedStub{}}
}

func (c *controller) run(ctx context.Context) {
	ticker := time.NewTicker(c.s.config.Fill.ReconcileInterval)
	defer ticker.Stop()

	leader := false
	for {
		select {
		case <-ctx.Done():
			if leader {
				_ = c.lock.Release(controllerLockKey)
			}
			return
		case <-ticker.C:
		}

		if !leader {
			if err := c.lock.Acquire(ctx, controllerLockKey, common.RedisLockOptions{TtlS: int(controllerLockTTL.Seconds()), Retries: 0}); err != nil {
				continue
			}
			leader = true
			c.leaderSince.Store(time.Now().UnixNano())
			log.Info().Msg("managed endpoints: controller leader acquired")
		} else if err := c.lock.Refresh(ctx, controllerLockKey, controllerLockTTL); err != nil {
			log.Warn().Err(err).Msg("managed endpoints: lost controller lock")
			leader = false
			continue
		}

		if err := c.reconcile(ctx); err != nil {
			log.Error().Err(err).Msg("managed endpoints: reconcile failed")
		}
	}
}

// silentFor reports how long a replica has gone without a heartbeat that we
// could have observed.
func (c *controller) silentFor(lastHeartbeat, now time.Time) time.Duration {
	if leader := time.Unix(0, c.leaderSince.Load()); leader.After(lastHeartbeat) {
		lastHeartbeat = leader
	}
	return now.Sub(lastHeartbeat)
}

// reconcile is one pass: observe replicas, compute inventory, fill services
// then endpoints, and drain what is no longer wanted. A panic in one pass
// is reported as an error rather than taking the gateway down.
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
	services, err := c.s.repo.ListServices(ctx)
	if err != nil {
		return err
	}
	endpoints, err := c.s.repo.ListEndpoints(ctx)
	if err != nil {
		return err
	}
	slices.SortFunc(endpoints, func(a, b *types.ManagedEndpoint) int { return strings.Compare(a.Spec.ID, b.Spec.ID) })

	var errs []error
	for _, service := range services {
		if err := c.reconcileService(ctx, service, live, inv); err != nil {
			errs = append(errs, fmt.Errorf("service %s: %w", service.Spec.Name, err))
		}
	}
	// Every enabled endpoint's targets compete for the same inventory, so the
	// fill plan is computed once across all of them.
	var targets []fillTarget
	for _, endpoint := range endpoints {
		if endpoint.Enabled() {
			for _, rt := range endpoint.Spec.Targets() {
				targets = append(targets, fillTarget{EndpointID: endpoint.Spec.ID, RoleTarget: rt, Demand: c.demand(ctx, endpoint, rt, live)})
			}
		}
	}
	plans := planFill(inv.byType, targets, c.s.config.Fill.MaxClusterShare)
	for _, endpoint := range endpoints {
		if err := c.reconcileEndpoint(ctx, endpoint, plans, live, inv); err != nil {
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

// endpointSpecFromStub extracts the endpoint spec a version's stub carries.
func (c *controller) endpointSpecFromStub(ctx context.Context, stubID string) (*types.ManagedEndpointSpec, error) {
	_, cfg, err := c.stub(ctx, stubID)
	if err != nil {
		return nil, err
	}
	if cfg == nil || cfg.ManagedEndpoint == nil || cfg.ManagedEndpoint.Endpoint == nil {
		return nil, fmt.Errorf("stub %s has no endpoint spec", stubID)
	}
	return cfg.ManagedEndpoint.Endpoint, nil
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
	GPU     string
	Workers []workerSlot
}

// allowance is the number of GPUs endpoints may hold after applying the
// cluster and per-pool share caps. Free GPUs plus those endpoints already
// hold count; GPUs held by serverless workloads never do.
func (g *gpuInventory) allowance(clusterShare float64) uint32 {
	var allowed float64
	for _, w := range g.Workers {
		share := clusterShare
		if w.MaxShare > 0 {
			share = min(share, w.MaxShare)
		}
		allowed += float64(w.Free+w.Held) * share
	}
	return uint32(math.Floor(allowed + 1e-9))
}

// reserve picks the least-loaded worker that fits count GPUs and passes
// accept, records the GPUs as held and returns the slot.
func (g *gpuInventory) reserve(count uint32, accept func(workerSlot) bool) (workerSlot, bool) {
	count = max(count, 1)
	best := -1
	for i, w := range g.Workers {
		if w.Free < count || !accept(w) {
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

// cpuInventoryKey is the inventory bucket for CPU-only targets and pools.
const cpuInventoryKey = "cpu"

// inventoryKey is the bucket a target draws from ("cpu" or a GPU type).
func inventoryKey(target types.GpuTarget) string {
	if target.IsCPU() {
		return cpuInventoryKey
	}
	return string(types.NormalizeGPUType(target.Type))
}

// eligiblePool is a pool that opted into managed endpoints. It is known even
// when none of its workers currently has free capacity, so protected
// replicas that may provision a worker still land in a pool that agreed to
// host them.
type eligiblePool struct {
	Name     string
	Locality string
}

// clusterInventory is the GPU inventory endpoints may use, keyed by GPU type.
type clusterInventory struct {
	byType     map[string]*gpuInventory
	localities map[string][]string // gpu type -> localities present
	cpuPools   []string
	// pools lists the endpoint-enabled pools per inventory key ("cpu" or GPU
	// type), regardless of current worker capacity.
	pools map[string][]eligiblePool
}

// fallbackPool picks an eligible pool for a target with no free capacity.
// A requested locality is honored in preference order; only a target that
// declared no locality may land in any eligible pool.
func (inv *clusterInventory) fallbackPool(gpuType string, localities []string) (eligiblePool, bool) {
	pools := inv.pools[gpuType]
	if len(localities) == 0 {
		if len(pools) == 0 {
			return eligiblePool{}, false
		}
		return pools[0], true
	}
	for _, locality := range localities {
		for _, p := range pools {
			if p.Locality == locality {
				return p, true
			}
		}
	}
	return eligiblePool{}, false
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

// poolLocality is localityOf for a pool looked up by name.
func (c *controller) poolLocality(name string) string {
	cfg, _ := c.poolConfig(name)
	return localityOf(name, cfg)
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

	inv := &clusterInventory{byType: map[string]*gpuInventory{}, localities: map[string][]string{}, pools: map[string][]eligiblePool{}}
	for name, cfg := range c.poolConfigs() {
		if !cfg.ManagedEndpoints.Enabled {
			continue
		}
		key := cpuInventoryKey
		if gpu := types.NormalizeGPUType(cfg.GPUType); gpu != "" && gpu != types.NO_GPU {
			key = string(gpu)
		}
		inv.pools[key] = append(inv.pools[key], eligiblePool{Name: name, Locality: localityOf(name, cfg)})
	}
	for _, pools := range inv.pools {
		slices.SortFunc(pools, func(a, b eligiblePool) int { return strings.Compare(a.Name, b.Name) })
	}
	for _, w := range workers {
		if w == nil || w.Status == types.WorkerStatusDisabled {
			continue
		}
		cfg, ok := c.poolConfig(w.PoolName)
		if !ok || !cfg.ManagedEndpoints.Enabled {
			continue
		}
		if w.TotalGpuCount == 0 || w.Gpu == "" {
			if !slices.Contains(inv.cpuPools, w.PoolName) {
				inv.cpuPools = append(inv.cpuPools, w.PoolName)
			}
			continue
		}
		gpuType := string(types.NormalizeGPUType(w.Gpu))
		locality := c.poolLocality(w.PoolName)
		entry := inv.byType[gpuType]
		if entry == nil {
			entry = &gpuInventory{GPU: gpuType}
			inv.byType[gpuType] = entry
		}
		entry.Workers = append(entry.Workers, workerSlot{
			WorkerID: w.Id, PoolName: w.PoolName, Locality: locality,
			Total: w.TotalGpuCount, Free: w.FreeGpuCount, Held: min(heldByWorker[w.Id], w.TotalGpuCount),
			MaxShare: cfg.ManagedEndpoints.MaxShare,
		})
		if !slices.Contains(inv.localities[gpuType], locality) {
			inv.localities[gpuType] = append(inv.localities[gpuType], locality)
		}
	}
	sort.Strings(inv.cpuPools)
	for _, l := range inv.localities {
		sort.Strings(l)
	}
	return inv, nil
}

// place chooses a pool (and its locality) for a target, reserving the GPUs
// in the in-memory inventory. CPU targets go to any endpoint-enabled CPU
// pool; GPU targets go to the least-loaded eligible worker. When no worker
// has room, a protected target (which may provision a new worker) falls
// back to an eligible pool honoring its localities; a managed replica is
// never submitted without a pool, so the scheduler cannot place it on a
// pool that did not opt in.
func (c *controller) place(inv *clusterInventory, target types.GpuTarget, localities []string, protected bool) (pool string, locality string, ok bool) {
	accept := func(w workerSlot) bool { return len(localities) == 0 || slices.Contains(localities, w.Locality) }
	if target.IsCPU() {
		for _, name := range inv.cpuPools {
			if loc := c.poolLocality(name); accept(workerSlot{Locality: loc}) {
				return name, loc, true
			}
		}
	} else if entry := inv.byType[inventoryKey(target)]; entry != nil {
		if slot, ok := entry.reserve(target.Count, accept); ok {
			return slot.PoolName, slot.Locality, true
		}
	}
	if !protected {
		return "", "", false
	}
	p, ok := inv.fallbackPool(inventoryKey(target), localities)
	return p.Name, p.Locality, ok
}

// --- fill planning -------------------------------------------------------------

// fillTarget is one (endpoint, role, gpu target) unit of placement. Demand is
// the replica count the router asks for (queue pressure); it may exceed the
// fair-share quota when spare capacity exists.
type fillTarget struct {
	EndpointID string
	types.RoleTarget
	Demand uint32
}

func (t fillTarget) key() string { return t.EndpointID + "|" + t.RoleTarget.Key() }

// gpuType is the inventory bucket the target draws from ("cpu" or "H100").
func (t fillTarget) gpuType() string { return inventoryKey(t.Target) }

// fillPlan is the placement decision for one fillTarget: Quota is the
// fair-share allocation, Desired what the controller converges toward.
type fillPlan struct {
	Quota   uint32
	Desired uint32
}

// planFill divides each GPU type's allowance between the targets that want it
// in proportion to their declared shares, then clamps by min/max replicas.
// Shares of targets on the same GPU type are normalized when they sum to more
// than one, so over-subscribed specs degrade gracefully instead of exceeding
// the cluster cap.
func planFill(inventory map[string]*gpuInventory, targets []fillTarget, clusterShare float64) map[string]fillPlan {
	plans := make(map[string]fillPlan, len(targets))
	byType := map[string][]fillTarget{}
	for _, t := range targets {
		byType[t.gpuType()] = append(byType[t.gpuType()], t)
	}
	for gpuType, group := range byType {
		var allowance uint32
		if inv := inventory[gpuType]; inv != nil {
			allowance = inv.allowance(clusterShare)
		}
		var shareSum float64
		for _, t := range group {
			shareSum += math.Max(t.Target.Share, 0)
		}
		norm := 1 / math.Max(shareSum, 1)
		for _, t := range group {
			var quota uint32
			if t.Target.Share > 0 && allowance > 0 {
				quota = uint32(math.Floor(float64(allowance)*t.Target.Share*norm/float64(max(t.Target.Count, 1)) + 1e-9))
			}
			desired := max(quota, t.Demand, t.Target.MinReplicas)
			if t.Target.MaxReplicas > 0 {
				desired = min(desired, t.Target.MaxReplicas)
			}
			plans[t.key()] = fillPlan{Quota: quota, Desired: desired}
		}
	}
	return plans
}

// replicaSet is the live replicas for one fillTarget, partitioned for
// scale decisions.
type replicaSet struct {
	Live      []*types.EndpointReplica // scheduling|loading|ready, serving-eligible
	Ready     []*types.EndpointReplica // subset of Live that is ready
	Protected uint32
}

func partitionReplicas(replicas []*types.EndpointReplica, endpointID, role, gpu string, version uint) replicaSet {
	var set replicaSet
	for _, r := range replicas {
		if r.EndpointID != endpointID || r.Role != role || r.GPU != gpu || r.Version != version || !r.Alive() || r.Tuning {
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

// demand asks for one more replica than currently serving when the recent
// route window shows queueing at the router, so hot endpoints grow past their
// fair share into spare capacity (still bounded by max_replicas).
func (c *controller) demand(ctx context.Context, endpoint *types.ManagedEndpoint, rt types.RoleTarget, live []*types.EndpointReplica) uint32 {
	set := partitionReplicas(live, endpoint.Spec.ID, rt.Role, rt.Target.Key(), endpoint.Version)
	ready := uint32(len(set.Ready))
	if ready == 0 {
		return 0
	}
	metrics, err := c.s.repo.GetRouteMetrics(ctx, endpoint.Spec.ID, rt.Target.Key(), 0, time.Minute)
	if err != nil || metrics == nil || metrics.Requests == 0 {
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

// endpointStartSpec is the launch spec for one replica of an endpoint target.
// Callers set placement (PoolName, Locality) and protection.
func (c *controller) endpointStartSpec(endpoint *types.ManagedEndpoint, rt types.RoleTarget, services map[string]string) startSpec {
	spec := &endpoint.Spec
	return startSpec{
		EndpointID: spec.ID, Version: endpoint.Version, StubID: endpoint.StubID, GitSHA: endpoint.GitSHA,
		Role: rt.Role, Target: rt.Target, Port: spec.Port, Harness: spec.Harness,
		Entrypoint: spec.Entrypoint, Services: services, KVCache: spec.KVCache,
		Evictable:    spec.Policy.Evictable && c.s.config.Preemption.Enabled,
		DrainSeconds: cmp.Or(spec.Policy.DrainSeconds, c.s.config.Preemption.DefaultDrainSeconds),
	}
}

// reconcileEndpoint converges one endpoint's replicas toward the fill plan.
func (c *controller) reconcileEndpoint(ctx context.Context, endpoint *types.ManagedEndpoint, plans map[string]fillPlan, live []*types.EndpointReplica, inv *clusterInventory) error {
	spec := &endpoint.Spec
	drainSeconds := spec.Policy.DrainSeconds

	if !endpoint.Enabled() {
		for _, r := range live {
			if r.EndpointID == spec.ID {
				_ = c.drainReplica(ctx, r, drainSeconds, false, "endpoint disabled")
			}
		}
		return nil
	}

	rollout, err := c.s.repo.GetRollout(ctx, spec.ID)
	if err != nil {
		return err
	}
	if rollout == nil {
		rollout = &types.RolloutState{EndpointID: spec.ID, ActiveVersion: endpoint.Version, Phase: types.RolloutPhaseIdle}
	}
	if err := c.stepRollout(ctx, endpoint, rollout, live, inv); err != nil {
		log.Warn().Err(err).Str("endpoint_id", spec.ID).Msg("managed endpoints: rollout step failed")
	}
	if err := c.ensureFleetRevisions(ctx, endpoint); err != nil {
		log.Warn().Err(err).Str("endpoint_id", spec.ID).Msg("managed endpoints: fleet config revisions")
	}

	// Keep what is running but do not grow until dependencies are up.
	services, missing := serviceAddresses(spec.Services, live)
	for _, rt := range spec.Targets() {
		plan := plans[fillTarget{EndpointID: spec.ID, RoleTarget: rt}.key()]
		set := partitionReplicas(live, spec.ID, rt.Role, rt.Target.Key(), endpoint.Version)
		current := uint32(len(set.Live))
		switch {
		case current < plan.Desired && len(missing) == 0:
			c.growTarget(ctx, endpoint, rt, plan.Desired-current, set.Protected, inv, services)
		case current > plan.Desired:
			excess := current - plan.Desired
			for _, r := range scaleDownCandidates(set) {
				if excess == 0 {
					break
				}
				if r.Protected && current-excess < rt.Target.MinReplicas {
					continue
				}
				if err := c.drainReplica(ctx, r, drainSeconds, false, "scale down to fair share"); err == nil {
					excess--
				}
			}
		}
		c.retireStaleVersions(ctx, endpoint, rollout, rt, live, drainSeconds)
	}
	return nil
}

// growTarget starts up to need replicas. Replicas needed to satisfy
// min_replicas are protected (they may trigger provisioning); the rest are
// opportunistic and only land on free capacity.
func (c *controller) growTarget(ctx context.Context, endpoint *types.ManagedEndpoint, rt types.RoleTarget, need, protectedLive uint32, inv *clusterInventory, services map[string]string) {
	protectedNeeded := rt.Target.MinReplicas - min(rt.Target.MinReplicas, protectedLive)
	backoff, _ := c.s.repo.InScheduleBackoff(ctx, endpoint.Spec.ID, rt.Key())

	for i := uint32(0); i < min(need, maxStartsPerTick); i++ {
		spec := c.endpointStartSpec(endpoint, rt, services)
		spec.Protected = protectedNeeded > 0
		if !spec.Protected && backoff {
			return
		}
		pool, locality, ok := c.place(inv, rt.Target, endpoint.Spec.Locality, spec.Protected)
		if !ok {
			if spec.Protected {
				log.Debug().Str("endpoint_id", endpoint.Spec.ID).Str("target", rt.Key()).Strs("locality", endpoint.Spec.Locality).
					Msg("managed endpoints: no eligible pool for protected replica")
			}
			return
		}
		spec.PoolName, spec.Locality = pool, locality
		if _, err := c.startReplica(ctx, spec); err != nil {
			log.Warn().Err(err).Str("endpoint_id", endpoint.Spec.ID).Str("target", rt.Key()).Msg("managed endpoints: start replica failed")
			return
		}
		if spec.Protected {
			protectedNeeded--
		}
	}
}

// retireStaleVersions drains replicas running versions that are neither
// active nor the current canary, one per target per tick, and only once the
// active version has something ready to take the traffic.
func (c *controller) retireStaleVersions(ctx context.Context, endpoint *types.ManagedEndpoint, rollout *types.RolloutState, rt types.RoleTarget, live []*types.EndpointReplica, drainSeconds uint32) {
	activeReady := len(partitionReplicas(live, endpoint.Spec.ID, rt.Role, rt.Target.Key(), endpoint.Version).Ready)
	for _, r := range live {
		if r.EndpointID != endpoint.Spec.ID || r.Role != rt.Role || r.GPU != rt.Target.Key() || !r.Alive() {
			continue
		}
		if r.Version == endpoint.Version || (rollout.CanaryVersion != 0 && r.Version == rollout.CanaryVersion) {
			continue
		}
		if r.Status == types.ReplicaStatusReady && activeReady == 0 {
			continue // keep serving the old version until the new one is up
		}
		if err := c.drainReplica(ctx, r, drainSeconds, false, fmt.Sprintf("version %d retired", r.Version)); err == nil {
			return
		}
	}
}

// ensureFleetRevisions seeds the git-sourced harness config for each target
// of one endpoint version. Fleet config streams are keyed by version, so
// this runs once per version and live fleet edits made afterwards stay in
// force for that version only; the next version starts from git again.
func (c *controller) ensureFleetRevisions(ctx context.Context, endpoint *types.ManagedEndpoint) error {
	if !endpoint.Spec.Harness {
		return nil
	}
	for _, rt := range endpoint.Spec.Targets() {
		key := fleetKey(rt.Role, rt.Target.Key(), endpoint.Version)
		latest, err := c.s.repo.LatestConfigRevision(ctx, endpoint.Spec.ID, types.ConfigScopeTarget, key)
		if err != nil {
			return err
		}
		if latest != nil {
			continue
		}
		config := rt.Target.Harness
		if config == nil {
			config = map[string]any{}
		}
		revision := &types.EndpointConfigRevision{
			EndpointID: endpoint.Spec.ID, Scope: types.ConfigScopeTarget, ScopeKey: key, Config: config,
			Author: fmt.Sprintf("git@v%d", endpoint.Version), Source: types.ConfigSourceGit,
		}
		if err := c.s.repo.CreateConfigRevision(ctx, revision); err != nil {
			return err
		}
		c.s.emit(types.EventEndpointConfig, types.EventEndpointSchema{
			EndpointID: endpoint.Spec.ID, Action: "config.fleet", Version: endpoint.Version,
			Role: rt.Role, GPU: rt.Target.Key(), Revision: revision.Revision,
			Data: map[string]any{"source": "git", "git_sha": endpoint.GitSHA},
		})
	}
	return nil
}

// serviceAddresses resolves the addresses of the shared services an endpoint
// depends on. Missing names are returned so fill can wait for them.
func serviceAddresses(names []string, live []*types.EndpointReplica) (map[string]string, []string) {
	if len(names) == 0 {
		return nil, nil
	}
	out := map[string]string{}
	var missing []string
	for _, name := range names {
		var addrs []string
		for _, r := range live {
			if r.EndpointID == serviceReplicaID(name) && r.Status == types.ReplicaStatusReady && r.Address != "" {
				addrs = append(addrs, r.Address)
			}
		}
		if len(addrs) == 0 {
			missing = append(missing, name)
			continue
		}
		sort.Strings(addrs)
		out[name] = strings.Join(addrs, ",")
	}
	return out, missing
}

// --- services ------------------------------------------------------------------

// reconcileService keeps a shared service at its replica count, per locality
// when requested. Service replicas are always protected.
func (c *controller) reconcileService(ctx context.Context, service *types.ManagedService, live []*types.EndpointReplica, inv *clusterInventory) error {
	spec := &service.Spec
	id := serviceReplicaID(spec.Name)

	if !service.Enabled() {
		for _, r := range live {
			if r.EndpointID == id {
				_ = c.drainReplica(ctx, r, serviceDrainSeconds, false, "service disabled")
			}
		}
		return nil
	}

	groups := []string{""}
	if spec.PerLocality {
		groups = c.serviceLocalities(spec, inv)
	}
	for _, locality := range groups {
		var set replicaSet
		for _, r := range live {
			if r.EndpointID == id && r.Version == service.Version && r.Alive() && (locality == "" || r.Locality == locality) {
				set.Live = append(set.Live, r)
				if r.Status == types.ReplicaStatusReady {
					set.Ready = append(set.Ready, r)
				}
			}
		}
		current := uint32(len(set.Live))
		switch {
		case current < spec.Replicas:
			var want []string
			if locality != "" {
				want = []string{locality}
			}
			for i := uint32(0); i < min(spec.Replicas-current, maxStartsPerTick); i++ {
				target, pool, loc, ok := c.placeService(inv, spec, want)
				if !ok {
					log.Debug().Str("service", spec.Name).Strs("locality", want).Msg("managed endpoints: no eligible pool for service replica")
					break
				}
				_, err := c.startReplica(ctx, startSpec{
					EndpointID: id, Version: service.Version, StubID: service.StubID, GitSHA: service.GitSHA,
					Role: types.ReplicaRoleServe, Target: target, Port: spec.Port, Locality: loc, PoolName: pool,
					Protected: true, Entrypoint: spec.Entrypoint,
				})
				if err != nil {
					log.Warn().Err(err).Str("service", spec.Name).Msg("managed endpoints: start service replica failed")
					break
				}
			}
		case current > spec.Replicas:
			for _, r := range scaleDownCandidates(set)[:current-spec.Replicas] {
				_ = c.drainReplica(ctx, r, serviceDrainSeconds, false, "service scale down")
			}
		}

		// Retire old versions once the new one is fully ready.
		if uint32(len(set.Ready)) >= spec.Replicas {
			for _, r := range live {
				if r.EndpointID == id && r.Version != service.Version && r.Alive() && (locality == "" || r.Locality == locality) {
					_ = c.drainReplica(ctx, r, serviceDrainSeconds, false, fmt.Sprintf("version %d retired", r.Version))
				}
			}
		}
	}
	return nil
}

// serviceLocalities lists every locality where at least one of the service's
// targets could run.
func (c *controller) serviceLocalities(spec *types.ManagedServiceSpec, inv *clusterInventory) []string {
	var out []string
	for _, t := range spec.Gpu {
		if t.IsCPU() {
			for _, name := range inv.cpuPools {
				out = append(out, c.poolLocality(name))
			}
			continue
		}
		out = append(out, inv.localities[string(types.NormalizeGPUType(t.Type))]...)
	}
	sort.Strings(out)
	if out = slices.Compact(out); len(out) == 0 {
		out = []string{""}
	}
	return out
}

// placeService picks the first target with free capacity; when none has,
// the first target with an eligible pool is used and the (protected) request
// may provision there. Without any eligible pool nothing is placed.
func (c *controller) placeService(inv *clusterInventory, spec *types.ManagedServiceSpec, localities []string) (types.GpuTarget, string, string, bool) {
	targets := spec.Gpu
	if len(targets) == 0 {
		targets = []types.GpuTarget{types.CPUTarget()}
	}
	for _, t := range targets {
		if pool, loc, ok := c.place(inv, t, localities, false); ok {
			return t, pool, loc, true
		}
	}
	for _, t := range targets {
		if pool, loc, ok := c.place(inv, t, localities, true); ok {
			return t, pool, loc, true
		}
	}
	return types.GpuTarget{}, "", "", false
}
