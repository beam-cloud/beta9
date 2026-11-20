package scheduler

import (
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

// priceProvisionedSpot stamps pricing only after selecting the worker. The
// request's preferred GPU, pool selector and interruption policy do not tell
// us where it actually ran or what the machine cost.
func (s *Scheduler) priceProvisionedSpot(worker *types.Worker, requests []*types.ContainerRequest) error {
	if !s.config.Monitoring.ProvisionedSpotBilling || worker.MachineId == "" || worker.Gpu == "" || s.provisioner == nil {
		return nil
	}
	pool, ok := s.workerPoolManager.GetPool(worker.PoolName)
	if !ok || pool.Config.Mode != types.PoolModeExternal || !pool.Config.Buys() {
		return nil
	}

	// Existing external sandbox billing routes keep their current meter and
	// quote. The shared Spot meter is for Beam's serverless workload ledger.
	var billable []*types.ContainerRequest
	for _, request := range requests {
		if request.GpuCount > 0 && !request.Stub.Type.IsPlatformWorkload() && !s.externallyBilledSandbox(request) {
			billable = append(billable, request)
		}
	}
	if len(billable) == 0 {
		return nil
	}

	machine, err := s.provisioner.Machine(s.ctx, worker.MachineId)
	if err != nil {
		return fmt.Errorf("read Spot purchase for machine %s: %w", worker.MachineId, err)
	}
	if machine == nil || !machine.Spot {
		return nil
	}
	if !worker.ProvisionedSpotBilling {
		return fmt.Errorf("worker %s does not support provisioned Spot billing", worker.Id)
	}
	if s.spotBillableMicros == nil || machine.HourlyMicros <= 0 || machine.GPUCount == 0 || machine.MachineID != worker.MachineId {
		return fmt.Errorf("missing purchase pricing for Spot machine %s", worker.MachineId)
	}
	hourlyMicros := s.spotBillableMicros(machine.HourlyMicros)
	if hourlyMicros <= 0 {
		return fmt.Errorf("invalid billable price for Spot machine %s", worker.MachineId)
	}
	pricing := &types.ProvisionedSpotPricing{
		MachineID: machine.MachineID, GPUCount: machine.GPUCount, HourlyCostMicros: hourlyMicros,
	}
	for _, request := range billable {
		if request.GpuCount > pricing.GPUCount {
			return fmt.Errorf("container %s exceeds Spot machine GPU count", request.ContainerId)
		}
		request.ProvisionedSpot = pricing
	}
	return nil
}

func (s *Scheduler) externallyBilledSandbox(request *types.ContainerRequest) bool {
	if !request.Stub.Type.IsSandbox() {
		return false
	}
	billing := s.config.ManagedCompute.Billing
	for _, route := range billing.PoolRoutes {
		prefix := strings.TrimSpace(route.PoolNamePrefix)
		config := route.BillingConfig(billing)
		mode := strings.ToLower(strings.TrimSpace(config.Mode))
		if prefix != "" && strings.HasPrefix(request.Stub.Name, prefix) &&
			strings.TrimSpace(config.Endpoint) != "" && mode != "noop" && mode != "disabled" {
			return true
		}
	}
	return false
}
