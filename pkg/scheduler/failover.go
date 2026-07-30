package scheduler

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	// failoverDemandTTL is how long a demand record survives without being
	// rewritten. It spans several reconcile ticks, so a request that keeps
	// failing to place keeps demand alive, while a request that places lets it
	// lapse with no bookkeeping.
	failoverDemandTTL = 3 * time.Minute
)

// failoverChain is the resolved failover preference for one request: the GPU
// type whose chain matched, the failover pools in preference order, and
// whether the chain may end in on-demand hardware.
type failoverChain struct {
	gpu      string
	pools    []string
	onDemand bool
}

// contains reports whether a pool belongs to the chain.
func (c *failoverChain) contains(poolName string) bool {
	return c != nil && slices.Contains(c.pools, poolName)
}

// rank is the pool's position in the chain, used as a scoring penalty.
// Primary pools (not in the chain) rank 0.
func (c *failoverChain) rank(poolName string) int32 {
	if c == nil {
		return 0
	}
	for index, name := range c.pools {
		if name == poolName {
			return int32(index) + 1
		}
	}
	return 0
}

// failoverChainFor resolves the chain a request may fail over through, or nil
// when failover does not apply. Binding is narrow on purpose: only serverless
// GPU requests bind, and only to the first requested GPU type that has a
// chain. Selector-bound requests (private and marketplace pools) have their
// own fallback, and "any"-GPU requests already see every pool.
func (s *Scheduler) failoverChainFor(request *types.ContainerRequest) *failoverChain {
	config := s.config.Scheduling.Failover
	if !config.Enabled || len(config.Chains) == 0 || request == nil {
		return nil
	}
	if request.PoolSelector != "" || !request.RequiresGPU() {
		return nil
	}

	gpuRequests := gpuRequestsForScheduling(request)
	if slices.Contains(gpuRequests, string(types.GPU_ANY)) {
		return nil
	}

	for _, gpu := range gpuRequests {
		chain, ok := failoverChainForGPU(config.Chains, gpu)
		if !ok {
			continue
		}

		resolved := &failoverChain{gpu: gpu, onDemand: chain.OnDemand != nil}
		for _, poolName := range chain.Pools {
			// A chain pool that no longer exists is skipped rather than
			// failing the request: config and live pools drift independently.
			if _, ok := s.workerPoolManager.GetPool(poolName); ok {
				resolved.pools = append(resolved.pools, poolName)
			}
		}
		// Append the managed on-demand pool as the final failover target.
		if resolved.onDemand {
			poolName := types.FailoverOnDemandPoolName(gpu)
			if _, ok := s.workerPoolManager.GetPool(poolName); ok {
				resolved.pools = append(resolved.pools, poolName)
			}
		}
		if len(resolved.pools) == 0 && !resolved.onDemand {
			return nil
		}
		return resolved
	}

	return nil
}

func failoverChainForGPU(chains map[string]types.FailoverChain, gpu string) (types.FailoverChain, bool) {
	if chain, ok := chains[gpu]; ok {
		return chain, true
	}
	for name, chain := range chains {
		if strings.EqualFold(name, gpu) {
			return chain, true
		}
	}
	return types.FailoverChain{}, false
}

// failoverControllers returns the chain's pool controllers in preference
// order, to be appended after the request's primary controllers.
func (s *Scheduler) failoverControllers(chain *failoverChain, existing []WorkerPoolController) []WorkerPoolController {
	if chain == nil || len(chain.pools) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(existing))
	for _, controller := range existing {
		if controller != nil {
			seen[controller.Name()] = struct{}{}
		}
	}

	controllers := make([]WorkerPoolController, 0, len(chain.pools))
	for _, poolName := range chain.pools {
		pool, ok := s.workerPoolManager.GetPool(poolName)
		if !ok || pool.Controller == nil {
			continue
		}
		if _, duplicate := seen[pool.Controller.Name()]; duplicate {
			continue
		}
		controllers = append(controllers, pool.Controller)
	}
	return controllers
}

func controllerNames(controllers []WorkerPoolController) []string {
	names := make([]string, 0, len(controllers))
	for _, controller := range controllers {
		if controller != nil {
			names = append(names, controller.Name())
		}
	}
	return names
}

// noteFailoverDemand records that every eligible serverless pool refused this
// request, which is the only trigger for reserving on-demand hardware. The
// record is short-lived, so demand disappears on its own once requests place.
func (s *Scheduler) noteFailoverDemand(request *types.ContainerRequest, chain *failoverChain, pools []string) {
	if chain == nil || !chain.onDemand || s.computeRepo == nil {
		return
	}

	demand := &compute.FailoverDemand{
		GPU:       chain.gpu,
		Pools:     pools,
		GPUCount:  gpuCountForScheduling(request),
		CreatedAt: time.Now().UTC(),
	}
	if err := s.computeRepo.PushFailoverDemand(s.ctx, demand, failoverDemandTTL); err != nil {
		requestLog(log.Warn(), request).Err(err).Msg("failed to record on-demand failover demand")
		return
	}

	requestLog(log.Debug(), request).
		Str("gpu", chain.gpu).
		Msg("serverless capacity exhausted, requested on-demand failover capacity")
}

// emitContainerPlaced reports where a request landed, including whether it
// left its requested GPU type. Failover rate is the share of these events with
// failover=true.
func (s *Scheduler) emitContainerPlaced(worker *types.Worker, request *types.ContainerRequest, chain *failoverChain) {
	if s.pushComputeEvent == nil || worker == nil || request == nil {
		return
	}

	requestedGPUs := gpuRequestsForScheduling(request)
	failover := request.RequiresGPU() && worker.Gpu != "" && !slices.Contains(requestedGPUs, worker.Gpu)

	waitMs := int64(0)
	if !request.Timestamp.IsZero() {
		waitMs = time.Since(request.Timestamp).Milliseconds()
	}

	chainGPU := ""
	if chain != nil {
		chainGPU = chain.gpu
	}

	s.pushComputeEvent(types.EventComputeSchema{
		PoolName:    worker.PoolName,
		WorkerID:    worker.Id,
		MachineID:   worker.MachineId,
		ContainerID: request.ContainerId,
		Action:      types.EventComputeActionContainerPlaced,
		Attrs: map[string]string{
			types.EventComputeAttrChain:        chainGPU,
			types.EventComputeAttrFailover:     fmt.Sprintf("%t", failover),
			types.EventComputeAttrRequestedGPU: strings.Join(requestedGPUs, ","),
			types.EventComputeAttrPlacedGPU:    worker.Gpu,
			types.EventComputeAttrWaitMs:       fmt.Sprintf("%d", waitMs),
		},
	})
}
