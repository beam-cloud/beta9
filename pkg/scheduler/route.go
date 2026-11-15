package scheduler

import (
	"cmp"
	"errors"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

// A route is where a request may run: the pools that accept it, in the order
// they are tried. Placement picks workers from it and growth adds workers
// along it, so the two cannot disagree about which pools serve a request.
//
// A request that names a pool runs there. Any other request runs on the pools
// of the GPU type it asked for (CPU pools for CPU work), then on the pools
// that list its GPU type in alsoServes. Within each group a higher priority
// goes first, then the name.
type route struct {
	pools []routedPool
	// considered is every pool that was weighed for the request, with its
	// rank, or notInRoute if the request may not run there.
	considered map[string]int
	query      routeQuery
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
	// preference orders pools within a group, highest first: the pool's
	// priority, less its GPU type's position in the request's list.
	preference int32
}

const notInRoute = -1

var errNoPoolForRequest = errors.New("no pool accepts the request")

// newRoute builds the route for a request from the pools that could serve it.
// It is a pure function of its arguments.
func newRoute(request *types.ContainerRequest, candidates []*WorkerPool, buildPool string) route {
	query := newRouteQuery(request, buildPool)
	r := route{
		considered: make(map[string]int, len(candidates)),
		query:      query,
	}

	for _, pool := range candidates {
		r.considered[pool.Name] = notInRoute

		fallback, ok := poolAccepts(pool, query)
		if !ok {
			continue
		}
		r.pools = append(r.pools, routedPool{
			WorkerPool: pool,
			fallback:   fallback,
			overflow:   query.tier.Overflows(pool.Config),
			preference: pool.Config.Priority - int32(gpuPriorityModifier(request, pool.Config.GPUType)),
		})
	}

	slices.SortFunc(r.pools, func(a, b routedPool) int {
		return cmp.Or(
			compareBool(a.overflow, b.overflow),
			compareBool(a.fallback, b.fallback),
			cmp.Compare(b.preference, a.preference),
			strings.Compare(a.Name, b.Name),
		)
	})

	fallbacks := 0
	for i := range r.pools {
		if r.pools[i].fallback {
			fallbacks++
			r.pools[i].rank = fallbacks
		}
		r.considered[r.pools[i].Name] = r.pools[i].rank
	}
	return r
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

// poolAccepts reports whether the request may run on the pool. fallback is
// set when the pool serves the request only through alsoServes.
func poolAccepts(pool *WorkerPool, query routeQuery) (fallback, ok bool) {
	fallback, ok = poolServes(pool, query)
	return fallback, ok && poolCanRun(pool, query)
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
	if checkpointAccelerator(request) == "" {
		return true
	}
	if strings.TrimSpace(config.GPUType) == "" && config.AgentHosted() {
		return true
	}
	return acceleratorMatchesCheckpoint(request, config.GPUType)
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

// rankOf reports whether a worker's pool is in the route, and its rank there.
//
// A worker records, when it starts, whether its pool takes only requests that
// name it. That record stands for the pool in two cases. A replica can see a
// worker before it has loaded the worker's pool: a workspace's private pool
// loads on first use, and a pool created through the API reaches the other
// replicas a moment later. And a workspace's private pool may share its name
// with a shared pool, whose place in the route is not the private worker's.
func (r route) rankOf(worker *types.Worker) (rank int, ok bool) {
	name := workerPoolSelector(worker)
	rank, loaded := r.considered[name]
	switch {
	case loaded && rank == notInRoute:
		return 0, false
	case r.query.request.PoolSelector != "":
		return 0, name == r.query.request.PoolSelector
	case rank > 0 && !workspaceOwned(worker):
		return rank, true
	}
	return 0, !worker.RequiresPoolSelector
}

// workspaceOwned reports whether the worker is a workspace's own capacity:
// a machine it joined to a pool of its own. Capacity the control plane
// manages belongs to no workspace, whichever one it is accounted to.
func workspaceOwned(worker *types.Worker) bool {
	return worker.WorkspaceId != "" && !worker.ControlPlaneManaged
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
	request, gpus, anyGPU := r.query.request, r.query.gpus, r.query.anyGPU
	var catalog map[string]int
	if anyGPU {
		catalog = types.GPUTypesToMap(types.AllGPUTypes())
	}

	filtered := []*types.Worker{}
	for _, worker := range workers {
		rank, inRoute := r.rankOf(worker)
		// A machine pin has already chosen the worker, whose pool may need a
		// selector the request does not carry.
		if !inRoute && request.MachineId == "" {
			continue
		}

		if request.RequiresGPU() {
			switch {
			case rank > 0:
				// The pool also serves this request, whatever GPU it has.
			case slices.Contains(gpus, worker.Gpu):
			case anyGPU && request.PoolSelector != "":
				// Inside a named pool "any" is a true wildcard: its hardware
				// may not be in the GPU catalog.
			case anyGPU:
				if _, known := catalog[worker.Gpu]; !known {
					continue
				}
			default:
				continue
			}
		}
		filtered = append(filtered, worker)
	}
	return filtered
}

// HasManagedPoolForGPU reports whether work that asks for the GPU type and
// names no pool has anywhere to run, counting pools that are scaled to zero.
// It applies the routing rule that scheduling uses, so a GPU type served only
// through alsoServes counts. gpuType may be types.GPU_ANY.
//
// Of what else a pool asks of a request, it knows the two things that are
// true of a kind of work rather than of one request. Capacity that can be
// taken back runs only work that is preemptable. Provider pools never count:
// they only host managed endpoint replicas.
func (s *Scheduler) HasManagedPoolForGPU(gpuType string, preemptable bool) bool {
	if s == nil || s.workerPoolManager == nil {
		return false
	}
	query := newRouteQuery(&types.ContainerRequest{Gpu: gpuType, GpuCount: 1}, "")
	for _, pool := range s.workerPoolManager.SharedPools() {
		_, serves := poolServes(pool, query)
		if serves && pool.Config.Mode != types.PoolModeProvider && (preemptable || !pool.Config.Preemptable) {
			return true
		}
	}
	return false
}
