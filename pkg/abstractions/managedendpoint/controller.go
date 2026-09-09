package managedendpoint

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

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

// controller is the reconciliation loop. Exactly one gateway replica runs it
// at a time (Redis lock); others stand by and take over when the lock lapses.
type controller struct {
	s    *Service
	lock *common.RedisLock

	stubMu    sync.Mutex
	stubCache map[string]cachedStub

	// reconcileMu serializes on-demand reconciles (admin RPCs) with the loop.
	reconcileMu sync.Mutex

	// leaderSince is when this instance last became leader. Heartbeats
	// cannot arrive while no gateway is serving the harness RPC, so a
	// replica is only stale once it has been silent for the full window
	// *while we were listening*; otherwise every gateway deploy would fail
	// every serving replica.
	leaderSince atomic.Int64
}

// silentFor reports how long a replica has gone without a heartbeat that we
// could have observed.
func (c *controller) silentFor(lastHeartbeat time.Time, now time.Time) time.Duration {
	since := lastHeartbeat
	if leader := time.Unix(0, c.leaderSince.Load()); leader.After(since) {
		since = leader
	}
	return now.Sub(since)
}

type cachedStub struct {
	stub    *types.StubWithRelated
	config  *types.StubConfigV1
	fetched time.Time
}

func newController(s *Service) *controller {
	return &controller{
		s:         s,
		lock:      common.NewRedisLock(s.rdb),
		stubCache: map[string]cachedStub{},
	}
}

func (c *controller) run(ctx context.Context) {
	interval := c.s.config.Fill.ReconcileIntervalOrDefault()
	ticker := time.NewTicker(interval)
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

		if err := c.safeReconcile(ctx); err != nil {
			log.Error().Err(err).Msg("managed endpoints: reconcile failed")
		}
	}
}

// safeReconcile keeps a bug in one pass from taking the gateway down.
func (c *controller) safeReconcile(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("reconcile panicked: %v\n%s", r, debug.Stack())
		}
	}()
	return c.reconcile(ctx)
}

// reconcile is one pass: observe replicas, compute inventory, fill services
// then endpoints, and drain what is no longer wanted.
func (c *controller) reconcile(ctx context.Context) error {
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()

	replicas, err := c.s.repo.ListAllReplicas(ctx)
	if err != nil {
		return err
	}
	live := c.observeReplicas(ctx, replicas)

	inventory, err := c.inventory(ctx, live)
	if err != nil {
		return err
	}

	services, err := c.s.repo.ListServices(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, service := range services {
		if err := c.reconcileService(ctx, service, live, inventory); err != nil {
			errs = append(errs, fmt.Errorf("service %s: %w", service.Spec.Name, err))
		}
	}

	endpoints, err := c.s.repo.ListEndpoints(ctx)
	if err != nil {
		return err
	}
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].Spec.ID < endpoints[j].Spec.ID })

	targets, byEndpoint := c.collectTargets(ctx, endpoints, live)
	plans := planFill(inventory.byType, targets, c.s.config.Fill.MaxClusterShareOrDefault())

	for _, endpoint := range endpoints {
		if err := c.reconcileEndpoint(ctx, endpoint, byEndpoint[endpoint.Spec.ID], plans, live, inventory); err != nil {
			errs = append(errs, fmt.Errorf("endpoint %s: %w", endpoint.Spec.ID, err))
		}
	}
	return errors.Join(errs...)
}

// clusterInventory is the eligible GPU inventory keyed by normalized GPU type.
type clusterInventory struct {
	byType     map[string]gpuInventory
	localities map[string][]string // gpu type -> localities present
	cpuPools   []string
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

func poolLocality(name string, cfg types.WorkerPoolConfig) string {
	if strings.TrimSpace(cfg.Locality) != "" {
		return cfg.Locality
	}
	return name
}

// inventory reads worker state and folds in what replicas already hold.
func (c *controller) inventory(ctx context.Context, replicas []*types.EndpointReplica) (*clusterInventory, error) {
	workers, err := c.s.workers.GetAllWorkers()
	if err != nil {
		return nil, err
	}

	heldByWorker := map[string]uint32{}
	for _, r := range replicas {
		if r.WorkerID != "" && r.GPUCount > 0 && !r.Status.Terminal() {
			heldByWorker[r.WorkerID] += r.GPUCount
		}
	}

	inv := &clusterInventory{byType: map[string]gpuInventory{}, localities: map[string][]string{}}
	seenLocality := map[string]map[string]bool{}
	cpuPools := map[string]bool{}
	for _, w := range workers {
		if w == nil || w.Status == types.WorkerStatusDisabled {
			continue
		}
		cfg, ok := c.poolConfig(w.PoolName)
		if !ok || !cfg.ManagedEndpoints.Enabled {
			continue
		}
		locality := poolLocality(w.PoolName, cfg)
		if w.TotalGpuCount == 0 || w.Gpu == "" {
			cpuPools[w.PoolName] = true
			continue
		}
		gpuType := string(types.NormalizeGPUType(w.Gpu))
		held := heldByWorker[w.Id]
		if held > w.TotalGpuCount {
			held = w.TotalGpuCount
		}
		entry := inv.byType[gpuType]
		entry.GPU = gpuType
		entry.Workers = append(entry.Workers, workerSlot{
			WorkerID: w.Id,
			PoolName: w.PoolName,
			Locality: locality,
			Total:    w.TotalGpuCount,
			Free:     w.FreeGpuCount,
			Held:     held,
			MaxShare: cfg.ManagedEndpoints.MaxShare,
		})
		inv.byType[gpuType] = entry
		if seenLocality[gpuType] == nil {
			seenLocality[gpuType] = map[string]bool{}
		}
		if !seenLocality[gpuType][locality] {
			seenLocality[gpuType][locality] = true
			inv.localities[gpuType] = append(inv.localities[gpuType], locality)
		}
	}
	for name := range cpuPools {
		inv.cpuPools = append(inv.cpuPools, name)
	}
	sort.Strings(inv.cpuPools)
	for _, l := range inv.localities {
		sort.Strings(l)
	}
	return inv, nil
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
		return nil, nil, notFound("stub", stubID)
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

// collectTargets turns every enabled endpoint's active (and canary) versions
// into fill targets, attaching router demand.
func (c *controller) collectTargets(ctx context.Context, endpoints []*types.ManagedEndpoint, live []*types.EndpointReplica) ([]fillTarget, map[string]*endpointPlanInput) {
	var targets []fillTarget
	byEndpoint := map[string]*endpointPlanInput{}
	for _, endpoint := range endpoints {
		if !endpoint.Enabled {
			continue
		}
		input := &endpointPlanInput{endpoint: endpoint}
		byEndpoint[endpoint.Spec.ID] = input

		rollout, err := c.s.repo.GetRollout(ctx, endpoint.Spec.ID)
		if err != nil {
			log.Warn().Err(err).Str("endpoint_id", endpoint.Spec.ID).Msg("managed endpoints: rollout state unavailable")
		}
		input.rollout = rollout

		for _, rt := range endpoint.Spec.Targets() {
			demand := c.demand(ctx, endpoint, rt, live)
			targets = append(targets, fillTarget{
				EndpointID: endpoint.Spec.ID,
				Role:       rt.Role,
				GPU:        rt.Target.Key(),
				Type:       gpuTypeOf(rt.Target),
				Count:      rt.Target.Count,
				Share:      rt.Target.Share,
				Min:        rt.Target.MinReplicas,
				Max:        rt.Target.MaxReplicas,
				Demand:     demand,
			})
		}
	}
	return targets, byEndpoint
}

type endpointPlanInput struct {
	endpoint *types.ManagedEndpoint
	rollout  *types.RolloutState
}

func gpuTypeOf(t types.GpuTarget) string {
	if t.IsCPU() {
		return "cpu"
	}
	return string(types.NormalizeGPUType(t.Type))
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
	meanQueueWait := time.Duration(metrics.QueueWaitSumMs/metrics.Requests) * time.Millisecond
	if meanQueueWait > c.s.config.Routing.MaxQueueWaitOrDefault()/2 {
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
