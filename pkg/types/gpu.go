package types

import (
	"encoding/json"
	"strings"
)

type GpuType string

func (g *GpuType) UnmarshalJSON(data []byte) error {
	var gpuStr string
	err := json.Unmarshal(data, &gpuStr)
	if err == nil {
		*g = GpuType(gpuStr)
		return nil
	}

	var gpuInt int
	err = json.Unmarshal(data, &gpuInt)
	if err != nil {
		return err
	}

	if gpuInt == 0 {
		*g = GpuType("")
	} else if gpuInt > 0 {
		*g = GpuType("T4")
	}

	return nil
}

func (g *GpuType) MarshalJSON() ([]byte, error) {
	if *g == "" {
		return []byte("0"), nil
	}

	return json.Marshal(string(*g))
}

func (g GpuType) String() string {
	return string(g)
}

func GPUTypesFromString(gpu string) []GpuType {
	gpus := []GpuType{}
	gpuString := strings.Trim(gpu, " ")
	if len(gpuString) > 0 {
		for _, g := range strings.Split(gpuString, ",") {
			gpus = append(gpus, GpuType(g))
		}
	}

	return gpus
}

func GpuTypesToStrings(gpus []GpuType) []string {
	var gpuStrings []string
	for _, gpu := range gpus {
		gpuStrings = append(gpuStrings, string(gpu))
	}
	return gpuStrings
}

const (
	GPU_A10G    GpuType = "A10G"
	GPU_A100_40 GpuType = "A100-40"
	GPU_L4      GpuType = "L4"
	GPU_T4      GpuType = "T4"
	GPU_H100    GpuType = "H100"
	GPU_RTX4090 GpuType = "RTX4090"
	GPU_L40S    GpuType = "L40S"

	NO_GPU  GpuType = "NO_GPU"
	GPU_ANY GpuType = "any"
)

func AllGPUTypes() []GpuType {
	return []GpuType{GPU_A10G, GPU_A100_40, GPU_L4, GPU_T4, GPU_H100, GPU_RTX4090, GPU_L40S, GPU_ANY}
}

func GpuTypesToSlice(gpuTypes []GpuType) []string {
	gpuTypesSlice := []string{}
	for _, gpuType := range gpuTypes {
		gpuTypesSlice = append(gpuTypesSlice, string(gpuType))
	}
	return gpuTypesSlice
}

func GPUTypesToMap(gpuTypes []GpuType) map[string]int {
	gpuTypesMap := map[string]int{}
	for i, gpuType := range gpuTypes {
		gpuTypesMap[string(gpuType)] = i
	}
	return gpuTypesMap
}
