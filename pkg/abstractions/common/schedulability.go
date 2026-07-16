package abstractions

import (
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

// GPUPoolChecker answers whether any serverless pool config supports a GPU
// type. Implemented by *scheduler.Scheduler.
type GPUPoolChecker interface {
	HasManagedPoolForGPU(gpuType string, allowMarketplace bool) bool
}

// StubSchedulable reports whether a stub's containers could ever be placed by
// the scheduler right now, based on registered pool configs (not live worker
// capacity). It is in-memory and cheap enough for the request path. When the
// stub is unschedulable it returns a user-facing reason.
//
// Pool-selector-bound stubs are always treated as schedulable: their pools
// are registered lazily during scheduling, so absence here proves nothing.
func StubSchedulable(s GPUPoolChecker, stubConfig *types.StubConfigV1) (bool, string) {
	if s == nil || stubConfig == nil {
		return true, ""
	}
	if stubConfig.PoolSelector() != "" || stubConfig.MachineID != "" {
		return true, ""
	}
	if !stubConfig.RequiresGPU() {
		return true, ""
	}

	unsupported := []string{}
	for _, gpu := range stubGPUTypes(stubConfig) {
		if s.HasManagedPoolForGPU(gpu, stubConfig.AllowMarketplace) {
			return true, ""
		}
		unsupported = append(unsupported, gpu)
	}
	if len(unsupported) == 0 {
		return true, ""
	}

	return false, fmt.Sprintf(
		"No compute capacity for GPU type %s. Reserve hardware with 'beta9 machine reserve --gpu %s', or redeploy with a different GPU.",
		strings.Join(unsupported, ", "),
		unsupported[0],
	)
}

func stubGPUTypes(stubConfig *types.StubConfigV1) []string {
	gpus := []string{}
	appendGPU := func(gpu types.GpuType) {
		if gpu == "" || gpu == types.NO_GPU {
			return
		}
		value := gpu.String()
		for _, existing := range gpus {
			if existing == value {
				return
			}
		}
		gpus = append(gpus, value)
	}
	for _, gpu := range stubConfig.Runtime.Gpus {
		appendGPU(gpu)
	}
	appendGPU(stubConfig.Runtime.Gpu)
	return gpus
}

// UnschedulableReason returns a non-empty user-facing reason when this
// instance's containers can never be placed by the scheduler and none are
// currently running or pending. Callers should fail incoming requests fast
// with this reason instead of holding them until the queue timeout: the
// moment capacity joins (a pool is added or a machine comes online) the
// check passes again with no redeploy needed.
func (i *AutoscaledInstance) UnschedulableReason() string {
	if i.Scheduler == nil {
		return ""
	}
	ok, reason := StubSchedulable(i.Scheduler, i.StubConfig)
	if ok {
		return ""
	}

	// Containers that are already up (scheduled before capacity went away)
	// can still serve requests; only fail fast when nothing is live.
	state, err := i.State()
	if err != nil || state.RunningContainers+state.PendingContainers > 0 {
		return ""
	}
	return reason
}
