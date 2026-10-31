package agent

import (
	"fmt"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type gpuDevice struct {
	ID            string
	UUID          string
	Name          string
	BusID         string
	VFIOFunctions []string
}

type workerGPUConfig struct {
	args []string
	env  map[string]string
}

func gpuPreflight() ([]gpuDevice, []check) {
	if vfio := vfioInventoryFromEnv(); vfio != nil {
		return vfio.preflight()
	}
	devices := detectNvidiaGPUDevices()
	var checks []check
	if len(devices) > 0 {
		nvidiaRuntime := commandExists("nvidia-ctk") || commandExists("nvidia-container-runtime")
		checks = append(checks, check{
			Name:     "nvidia-runtime",
			Ok:       nvidiaRuntime,
			Message:  "required to pin GPUs into worker containers",
			Severity: severity(nvidiaRuntime),
		})

		driverOK, driverMessage := nvidiaProcDriverStateOK(devices)
		checks = append(checks, check{
			Name:     "nvidia-driver-state",
			Ok:       driverOK,
			Message:  driverMessage,
			Severity: severity(driverOK),
		})

		if cudaOK, cudaMessage, ran := cudaDriverInitProbe(); ran {
			checks = append(checks, check{
				Name:     "cuda-driver-init",
				Ok:       cudaOK,
				Message:  cudaMessage,
				Severity: types.AgentPreflightSeverityInfo,
			})
		}
	}
	return devices, checks
}

func workerGPUOptions(slot *pb.AgentWorkerSlot) (workerGPUConfig, error) {
	vfio := vfioInventoryFromEnv()
	if slotContainerRuntime(slot) == types.ContainerRuntimeMicroVM.String() {
		return vfio.workerOptions(slot)
	}
	if slot.GpuCount == 0 {
		return workerGPUConfig{}, nil
	}
	if vfio != nil {
		return workerGPUConfig{}, fmt.Errorf("VFIO GPUs require the microvm runtime")
	}
	assignment := firstNonEmpty(slot.GpuAssignment, types.NvidiaVisibleDevicesAll)
	devices := types.NvidiaVisibleDevicesAll
	if slot.GpuAssignment != "" {
		devices = fmt.Sprintf("%q", "device="+slot.GpuAssignment)
	}
	return workerGPUConfig{
		args: []string{"--gpus", devices},
		env: map[string]string{
			types.NvidiaVisibleDevicesEnv: assignment,
			types.WorkerGPUDevicesEnv:     assignment,
		},
	}, nil
}
