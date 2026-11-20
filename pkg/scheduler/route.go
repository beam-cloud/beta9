package scheduler

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// A route is where a request may run: the pools that accept it, in the order
// they are tried. Placement picks workers from it and growth adds workers
// along it, so the two cannot disagree about which pools serve a request.
//
// A request that names a pool runs there. Any other request runs on the pools
// of the GPU type it asked for (CPU pools for CPU work), then on the pools
// that list its GPU type in alsoServes. A request that asks for several GPU
// types gets the pools of the type it lists first, then those of the next.
// Among pools that are otherwise equal a higher priority goes first, then
// the name.
type route struct {
	pools []routedPool
	// ranks is the rank of each pool in the route, and cannotRun for each
	// pool that was weighed and is not able or allowed to run the request.
	ranks map[string]int
	query routeQuery
}

// routeQuery is what routing reads from a request. It is worked out once per
// route, because the CPU tier is decoded from the stub's config.
type routeQuery struct {
	request *types.ContainerRequest
	// gpus is the GPU types the request asked for, most preferred first.
	gpus   []string
	anyGPU bool
	tier   types.CPUTier
	// buildPool is the pool reserved for image builds.
	buildPool string
}

func newRouteQuery(request *types.ContainerRequest, buildPool string) routeQuery {
	gpus := gpuRequestsForScheduling(request)
	return routeQuery{
		request:   request,
		gpus:      gpus,
		anyGPU:    slices.Contains(gpus, string(types.GPU_ANY)),
		tier:      request.CPUTier(),
		buildPool: buildPool,
	}
}

type routedPool struct {
	*WorkerPool
	// rank is 0 for a pool the request asked for. Pools that serve it only
	// through alsoServes are numbered from 1 in the order they are tried.
	rank int
	// fallback is set for a pool that serves the request only through
	// alsoServes.
	fallback bool
	// overflow is set for a Shared CPU pool taking work that does not share
	// CPU. Such pools come last (see types.CPUTier.Overflows).
	overflow bool
	// asked is where the pool's GPU type comes among those the request
	// asked for. It is 0 for a pool that only also serves the request.
	asked int
}

const cannotRun = -1

var errNoPoolForRequest = errors.New("no pool accepts the request")

// newRoute builds the route for a request from the pools that could serve it.
// It is a pure function of its arguments.
func newRoute(request *types.ContainerRequest, candidates []*WorkerPool, buildPool string) route {
	query := newRouteQuery(request, buildPool)
	r := route{
		ranks: make(map[string]int, len(candidates)),
		query: query,
	}

	for _, pool := range candidates {
		if !poolCanRun(pool, query) {
			r.ranks[pool.Name] = cannotRun
			continue
		}
		fallback, serves := poolServes(pool, query)
		if !serves {
			continue
		}
		routed := routedPool{WorkerPool: pool, fallback: fallback, overflow: query.tier.Overflows(pool.Config)}
		if !fallback {
			routed.asked = query.askedFor(pool.Config.GPUType)
		}
		r.pools = append(r.pools, routed)
	}

	slices.SortFunc(r.pools, func(a, b routedPool) int {
		return cmp.Or(
			compareBool(a.overflow, b.overflow),
			compareBool(a.fallback, b.fallback),
			cmp.Compare(a.asked, b.asked),
			cmp.Compare(b.Config.Priority, a.Config.Priority),
			strings.Compare(a.Name, b.Name),
		)
	})

	fallbacks := 0
	for i := range r.pools {
		if r.pools[i].fallback {
			fallbacks++
			r.pools[i].rank = fallbacks
		}
		r.ranks[r.pools[i].Name] = r.pools[i].rank
	}
	return r
}

// askedFor is where a GPU type comes among those the request asked for, in
// the order it listed them. "any" stands for every type the request did not
// list before it.
func (q routeQuery) askedFor(gpu string) int {
	return max(0, slices.IndexFunc(q.gpus, func(asked string) bool {
		return asked == gpu || asked == string(types.GPU_ANY)
	}))
}

// compareBool orders false before true.
func compareBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case b:
		return -1
	}
	return 1
}

// poolServes applies the routing rule: does the pool serve what the request
// asked for? fallback is set when it does so only through alsoServes.
func poolServes(pool *WorkerPool, query routeQuery) (fallback, ok bool) {
	config := pool.Config

	switch {
	case query.request.PoolSelector != "":
		return false, pool.Name == query.request.PoolSelector
	case !query.request.RequiresGPU():
		return false, config.GPUType == "" && !config.RequiresPoolSelector
	case query.anyGPU:
		return false, config.GPUType != "" && !config.RequiresPoolSelector
	case slices.Contains(query.gpus, config.GPUType) && !config.RequiresPoolSelector:
		return false, true
	}
	// A pool behind a selector can still take overflow: alsoServes is the
	// operator opting it in.
	return true, slices.ContainsFunc(query.gpus, config.Serves)
}

// poolCanRun reports whether the pool is able and allowed to run the request,
// whatever GPU type it serves.
func poolCanRun(pool *WorkerPool, query routeQuery) bool {
	config := pool.Config
	request := query.request
	runtime := poolRuntime(config)

	switch {
	case !runtimeMatchesCheckpoint(request, runtime), !runtimeAcceptsRequest(request, runtime):
		return false
	case !request.StorageAvailable() && config.AgentHosted():
		// Machines outside the cluster reach workspace files through storage
		// the workspace has to have.
		return false
	case config.Mode == types.PoolModeProvider && !isManagedRequest(request):
		// Machines a workspace contributes run only managed endpoint replicas.
		return false
	case isManagedRequest(request) && request.Preemptable && !config.AllowsManagedEndpointSpot():
		// A replica placed on Spot runs only in a Spot pool that takes them.
		return false
	case !request.Preemptable && config.Preemptable:
		return false
	case query.buildPool != "" && pool.Name == query.buildPool && !request.IsBuildRequest():
		// The image build pool is reserved for builds.
		return false
	case !query.tier.MatchesPool(config):
		return false
	}

	// A checkpoint restores only on the accelerator it was taken on. An agent
	// pool with no gpuType holds whatever its machines have, so its workers
	// are checked one by one instead.
	//
	// A deployment may start without its checkpoint. Placement tries that
	// when this route has no worker free (selectWorkerFromWorkers), which
	// brings in the pools of other accelerators. Waiting and growth do not:
	// a worker is added where the checkpoint restores.
	anyHardware := strings.TrimSpace(config.GPUType) == "" && config.AgentHosted()
	return anyHardware || acceleratorMatchesCheckpoint(request, config.GPUType)
}

func poolRuntime(config types.WorkerPoolConfig) string {
	if runtime := strings.TrimSpace(config.ContainerRuntime); runtime != "" {
		return runtime
	}
	return types.ContainerRuntimeRunc.String()
}

func isManagedRequest(request *types.ContainerRequest) bool {
	return request != nil && request.Stub.Type.IsManagedEndpoint()
}

// rankOf reports whether a worker's pool lets it take the request, and how
// far down the route that puts it: 0 for a worker with a GPU the request
// asked for, otherwise the rank of its pool.
//
// A pool that cannot run the request rules its workers out, and a pool that
// only also serves the request lets them in. Every other worker speaks for
// itself: it records, when it starts, whether its pool takes only requests
// that name it, and eligible checks the GPU it has. That is all there is to
// ask of a pool the request asked for, and it answers where the route
// cannot. An agent pool with no gpuType is in no GPU route, yet a machine of
// it has the GPU it has, and takes the requests for it. A replica can see a
// worker before it has loaded the worker's pool: a workspace's private pool
// loads on first use, and a pool created through the API reaches the other
// replicas a moment later. And a workspace's private pool may share its name
// with a shared pool, whose place in the route is not the private worker's.
func (r route) rankOf(worker *types.Worker) (rank int, ok bool) {
	name := workerPoolSelector(worker)
	rank = r.ranks[name]
	switch {
	case rank == cannotRun:
		return 0, false
	case r.query.request.PoolSelector != "":
		return 0, name == r.query.request.PoolSelector
	case rank == 0 || workspaceOwned(worker):
		return 0, !worker.RequiresPoolSelector
	case slices.Contains(r.query.gpus, worker.Gpu):
		// Overflow is running on another GPU than the one asked for. A pool
		// behind a selector that lists its own gpuType, or an agent pool
		// with a machine of the type, gives the request what it asked for.
		return 0, true
	}
	return rank, true
}

// workspaceOwned reports whether the worker is a workspace's own capacity:
// a machine it joined to a pool of its own. Capacity the control plane
// manages belongs to no workspace, whichever one it is accounted to.
func workspaceOwned(worker *types.Worker) bool {
	return worker.WorkspaceId != "" && !worker.ControlPlaneManaged
}

// SharedPools returns the pools that any workspace may be routed to. A
// workspace's private pool is registered under a workspace-scoped key and is
// reachable only by naming it.
func (m *WorkerPoolManager) SharedPools() []*WorkerPool {
	var pools []*WorkerPool
	m.poolMap.Range(func(key string, pool *WorkerPool) bool {
		if key == pool.Name {
			pools = append(pools, pool)
		}
		return true
	})
	return pools
}

// routeFor returns where the request may run. It reads only the request and
// the pools this replica has loaded, so every placement decision can call it.
func (s *Scheduler) routeFor(request *types.ContainerRequest) route {
	var candidates []*WorkerPool
	if s.workerPoolManager != nil {
		if request.PoolSelector == "" {
			candidates = s.workerPoolManager.SharedPools()
		} else if pool, ok := s.selectedPool(request); ok {
			candidates = []*WorkerPool{pool}
		}
	}
	return newRoute(request, candidates, s.config.ImageService.BuildContainerPoolSelector)
}

// selectedPool is the pool a request names: the workspace's own pool of that
// name if it has one, otherwise the shared one.
func (s *Scheduler) selectedPool(request *types.ContainerRequest) (*WorkerPool, bool) {
	if pool, ok := s.privateAgentPool(request.WorkspaceId, request.PoolSelector); ok {
		return pool, true
	}
	return s.workerPoolManager.GetPool(request.PoolSelector)
}

// growthRouteFor is routeFor for a request that is about to add a worker.
// Growth can be the first use of a workspace's private pool on this replica,
// so the pool is loaded from its stored state first.
func (s *Scheduler) growthRouteFor(request *types.ContainerRequest) (route, error) {
	if _, err := s.ensureAgentPoolForRequest(request); err != nil {
		return route{}, err
	}
	r := s.routeFor(request)
	if len(r.pools) == 0 {
		return route{}, errNoPoolForRequest
	}
	return r, nil
}

// eligible keeps the workers whose pool is in the route and whose GPU the
// request can use there.
func (r route) eligible(workers []*types.Worker) []*types.Worker {
	// A machine pin has already chosen the worker, whose pool may need a
	// selector the request does not carry.
	pinned := r.query.request.MachineId != ""

	filtered := []*types.Worker{}
	for _, worker := range workers {
		rank, inRoute := r.rankOf(worker)
		if (inRoute || pinned) && r.query.canUseGPU(worker.Gpu, rank > 0) {
			filtered = append(filtered, worker)
		}
	}
	return filtered
}

// canUseGPU reports whether the request can run on a worker with this GPU.
// fallback is set for a worker of a pool that only also serves the request,
// which it does whatever GPU it has.
func (q routeQuery) canUseGPU(gpu string, fallback bool) bool {
	switch {
	case !q.request.RequiresGPU(), fallback, slices.Contains(q.gpus, gpu):
		return true
	case !q.anyGPU:
		return false
	case q.request.PoolSelector != "":
		// Inside a named pool "any" is a true wildcard: its hardware may not
		// be in the GPU catalog.
		return true
	}
	_, known := gpuCatalog()[gpu]
	return known
}

// HasManagedPoolForGPU reports whether work that asks for the GPU type and
// names no pool has anywhere to run, counting pools that are scaled to zero.
// It applies the routing rule that scheduling uses, so a GPU type served only
// through alsoServes counts. gpuType may be types.GPU_ANY.
//
// A pool whose capacity can be taken back counts, though it runs only work
// that is preemptable. Provider pools never count: they only host managed
// endpoint replicas.
func (s *Scheduler) HasManagedPoolForGPU(gpuType string) bool {
	if s == nil || s.workerPoolManager == nil {
		return false
	}
	query := newRouteQuery(&types.ContainerRequest{Gpu: gpuType, GpuCount: 1}, "")
	for _, pool := range s.workerPoolManager.SharedPools() {
		_, serves := poolServes(pool, query)
		if serves && pool.Config.Mode != types.PoolModeProvider {
			return true
		}
	}
	return false
}

// What follows reads scheduling.failover.chains, which alsoServes on each pool
// replaced, so a config written the old way keeps routing for one release.
// It is deleted together with the old key.

// chainMigration is what a config's chains turn into.
type chainMigration struct {
	// alsoServes is the GPU types each pool takes over from the chains.
	alsoServes map[string][]string
	// write is the config that says the same thing the new way.
	write []string
	// reordered describes each chain whose pools are now tried in a different
	// order.
	reordered []string
	// notes are other things that behave differently.
	notes []string
}

// migrateFailoverChains converts the chains and works out what the operator
// needs to know. It is a pure function of the config.
func migrateFailoverChains(failover types.FailoverConfig, pools map[string]types.WorkerPoolConfig) chainMigration {
	migration := chainMigration{alsoServes: failover.AlsoServes()}
	if len(failover.Chains) == 0 {
		return migration
	}
	if !failover.Enabled {
		migration.notes = append(migration.notes,
			"scheduling.failover is disabled, so its chains have no effect; remove the section")
		return migration
	}

	for _, pool := range slices.Sorted(maps.Keys(migration.alsoServes)) {
		config, configured := pools[pool]
		gpus := migration.alsoServes[pool]
		if configured {
			gpus = slices.DeleteFunc(slices.Clone(gpus), func(gpu string) bool { return !chainAdds(config, gpu) })
		}
		if len(gpus) == 0 {
			continue
		}
		line := fmt.Sprintf("worker.pools.%s.alsoServes: [%s]", pool, strings.Join(gpus, ", "))
		switch {
		case !configured:
			line += "  (not in this config: set also_serves on it through the pools API)"
		case strings.TrimSpace(config.GPUType) == "":
			line += "  (a pool with no gpuType cannot list alsoServes: set its gpuType)"
		}
		migration.write = append(migration.write, line)
	}

	for _, gpu := range slices.Sorted(maps.Keys(failover.Chains)) {
		// Chains named their pools in the order to try them. Pools that also
		// serve a GPU type are tried by priority, then name.
		var was []string
		for _, pool := range failover.Chains[gpu].Pools {
			if config, configured := pools[pool]; configured && chainAdds(config, strings.TrimSpace(gpu)) {
				was = append(was, pool)
			}
		}
		now := slices.Clone(was)
		slices.SortStableFunc(now, func(a, b string) int {
			return cmp.Or(cmp.Compare(pools[b].Priority, pools[a].Priority), strings.Compare(a, b))
		})
		if !slices.Equal(was, now) {
			migration.reordered = append(migration.reordered, fmt.Sprintf(
				"%s fallbacks were tried in the order [%s] and are now tried by priority, then name: [%s]; set worker.pools.<name>.priority to keep the old order",
				gpu, strings.Join(was, ", "), strings.Join(now, ", ")))
		}
	}

	if len(failover.Chains) > 1 {
		migration.notes = append(migration.notes,
			"a request that lists several GPU types now uses the fallback pools of all of them; it used only those of the first listed type that had a chain")
	}
	return migration
}

// warn logs what replaces the chains, once, at startup.
func (m chainMigration) warn() {
	if len(m.alsoServes) > 0 {
		log.Warn().Strs("write_instead", m.write).
			Msg("scheduling.failover.chains is deprecated and stops being read in the next release; it was converted to alsoServes on each pool")
	}
	for _, change := range m.reordered {
		log.Warn().Msg("scheduling.failover.chains: " + change)
	}
	for _, note := range m.notes {
		log.Warn().Msg("scheduling.failover: " + note)
	}
}

// apply adds the GPU types a pool takes over from the chains to its
// alsoServes.
func (m chainMigration) apply(name string, config types.WorkerPoolConfig) types.WorkerPoolConfig {
	for _, gpu := range m.alsoServes[name] {
		if chainAdds(config, gpu) && !config.Serves(gpu) {
			config.AlsoServes = append(slices.Clone(config.AlsoServes), gpu)
		}
	}
	return config
}

// chainAdds reports whether a chain for the GPU type brings the pool requests
// it would not get otherwise. A pool of that type gets them already, unless
// it takes only requests that name it: the chain is then what lets the
// others in.
func chainAdds(config types.WorkerPoolConfig, gpu string) bool {
	return config.RequiresPoolSelector || !strings.EqualFold(config.GPUType, gpu)
}
