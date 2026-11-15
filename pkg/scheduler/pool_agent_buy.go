package scheduler

import (
	"errors"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// A pool with a provisioning block adds a worker by buying a machine.
//
// The machine has a worker from the moment it is bought: a Pending one, of
// the machine's size, under the id the machine's worker will have. Requests
// wait for it and the sizer counts it, as they do for a Kubernetes worker
// that is starting. When the machine joins, the agent path takes that same
// worker over.
//
// Such a pool grows no other way. A machine that has joined already has its
// worker, so there is never one with room and no worker to give it.

// canBuyMachine reports whether a machine with room for a worker of this
// size can be bought now.
func (wpc *AgentWorkerPoolController) canBuyMachine(cpu int64, memory int64, gpuCount uint32) bool {
	return wpc.provisioner != nil && wpc.provisioner.CanProvision(wpc.ctx, wpc.poolName(), wpc.machineNeed(cpu, memory, gpuCount))
}

// buyMachine buys a machine with room for a worker of this size and returns
// the Pending worker that stands for it.
func (wpc *AgentWorkerPoolController) buyMachine(cpu int64, memory int64, gpuCount uint32) (*types.Worker, error) {
	if wpc.provisioner == nil {
		return nil, fmt.Errorf("pool %q buys its machines, but this gateway has nothing to buy them with", wpc.poolName())
	}
	machine, err := wpc.provisioner.Provision(wpc.ctx, wpc.poolName(), wpc.machineNeed(cpu, memory, gpuCount))
	if err != nil {
		return nil, err
	}
	worker := wpc.bootingMachineWorker(*machine)
	if err := wpc.workerRepo.AddWorker(worker); err != nil {
		// The machine is bought all the same. Calling this a failure would
		// have the pool asked again, and a second machine bought. The
		// worker is made again by keepBootingWorkers.
		log.Warn().Err(err).Str("pool_name", wpc.poolName()).Str("machine_id", machine.MachineID).
			Msg("bought a machine but could not add the worker that stands for it")
	}
	return worker, nil
}

// machineNeed is the machine that holds a worker of this size. A worker's
// CPU is counted in schedulable millicores, a machine's in physical ones.
func (wpc *AgentWorkerPoolController) machineNeed(cpu int64, memory int64, gpuCount uint32) MachineNeed {
	return MachineNeed{
		CPU:      wpc.workerPoolConfig.CPUShareMultiplier.Physical(cpu),
		Memory:   memory,
		GPUCount: gpuCount,
	}
}

// bootingMachineWorker is the Pending worker for a machine that has been
// bought and has not joined. It matches what the agent path will make of the
// machine once it has, so the worker carries over.
func (wpc *AgentWorkerPoolController) bootingMachineWorker(machine ProvisionedMachine) *types.Worker {
	gpu := ""
	if machine.GPUCount > 0 {
		gpu = wpc.workerPoolConfig.GPUType
	}
	spec := wpc.machineWorkerSpec(machine.MachineID, machine.CPU, machine.Memory, gpu, machine.GPUCount)
	spec.workspaceID = wpc.workspaceID
	spec.controlPlaneManaged = true
	spec.buildVersion = types.WorkerImageVersion("", wpc.config.Worker.ImageTag)
	return spec.worker()
}

// keepBootingWorkers keeps the Pending worker of every machine that is
// booting. A Pending worker expires after cleanupPendingWorkerAgeLimit,
// which a machine can take longer than to boot; without its worker the
// machine would stop counting as capacity and the pool would buy another.
//
// joined are the machines the agent path already looks after.
func (wpc *AgentWorkerPoolController) keepBootingWorkers(joined []*compute.AgentTokenState) error {
	if wpc.provisioner == nil {
		return nil
	}
	booting, err := wpc.provisioner.Booting(wpc.ctx, wpc.poolName())
	if err != nil {
		return err
	}

	hasJoined := map[string]bool{}
	for _, machine := range joined {
		if machine != nil {
			hasJoined[machine.MachineID] = true
		}
	}

	var errs []error
	for _, machine := range booting {
		if hasJoined[machine.MachineID] {
			continue
		}
		kept, err := wpc.workerRepo.KeepPendingWorker(compute.AgentMachineWorkerID(machine.MachineID))
		if err == nil && !kept {
			// The worker expired or Redis lost it: the machine is still coming.
			err = wpc.workerRepo.AddWorker(wpc.bootingMachineWorker(machine))
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
