package types

type ContainerRuntime string

const (
	ContainerRuntimeRunc   ContainerRuntime = "runc"
	ContainerRuntimeGvisor ContainerRuntime = "gvisor"
)

func (r ContainerRuntime) String() string {
	return string(r)
}

// MarketplaceContainerRuntimeForGPU returns the runtime used for public
// marketplace capacity for a GPU family. gVisor remains the default where its
// NVIDIA proxy supports the device; other GPUs fall back to runc and are
// advertised that way.
func MarketplaceContainerRuntimeForGPU(gpu string) string {
	if marketplaceGPUUsesGvisor(gpu) {
		return ContainerRuntimeGvisor.String()
	}
	return ContainerRuntimeRunc.String()
}

func marketplaceGPUUsesGvisor(gpu string) bool {
	switch NormalizeGPUType(gpu) {
	case GPU_T4, GPU_A10G, GPU_A100, GPU_A100_40, GPU_A100_80, GPU_L4, GPU_H100:
		return true
	default:
		return false
	}
}
