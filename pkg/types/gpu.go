package types

type GPUType string

func (g GPUType) String() string {
	return string(g)
}

const (
	GPU_A10G    GPUType = "A10G"
	GPU_A100_40 GPUType = "A100-40"
	GPU_A100_80 GPUType = "A100-80"
	GPU_L4      GPUType = "L4"
	GPU_T4      GPUType = "T4"
	GPU_H100    GPUType = "H100"
	GPU_A6000   GPUType = "A6000"
	GPU_RTX4090 GPUType = "RTX4090"

	NO_GPU GPUType = "NO_GPU"
)

func AllGPUTypes() []GPUType {
	return []GPUType{GPU_A10G, GPU_A100_40, GPU_A100_80, GPU_L4, GPU_T4, GPU_H100, GPU_A6000, GPU_RTX4090}
}
