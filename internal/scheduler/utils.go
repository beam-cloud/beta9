package scheduler

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/beam-cloud/beam/internal/types"
)

func ParseCPU(cpu interface{}) (int64, error) {
	cpuValue, err := strconv.ParseInt(strings.TrimSuffix(fmt.Sprintf("%v", cpu), "m"), 10, 64)
	if err != nil {
		return 0, err
	}

	if cpuValue < 0 {
		return 0, errors.New("invalid cpu value")
	}

	return cpuValue, nil
}

func ParseMemory(memory interface{}) (int64, error) {
	memStr := fmt.Sprintf("%v", memory)
	unit := strings.TrimLeftFunc(memStr, func(r rune) bool { return r >= '0' && r <= '9' })
	valueStr := strings.TrimSuffix(memStr, unit)

	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return 0, err
	}

	var memoryValue int64
	switch unit {
	case "Ki":
		memoryValue = value * 1024 / (1024 * 1024)
	case "Mi":
		memoryValue = value
	case "Gi":
		memoryValue = value * 1024
	default:
		return 0, errors.New("invalid memory unit")
	}

	return memoryValue, nil
}

func ParseGPU(gpu interface{}) (uint, error) {
	gpuValue, err := strconv.ParseInt(fmt.Sprintf("%v", gpu), 10, 64)
	if err != nil {
		return 0, err
	}

	if gpuValue < 0 {
		return 0, errors.New("invalid gpu value")
	}

	return uint(gpuValue), nil
}

func ParseGPUType(gpu interface{}) (types.GPUType, error) {
	switch fmt.Sprintf("%v", gpu) {
	case string(types.GPU_A100_40):
		return types.GPU_A100_40, nil
	case string(types.GPU_A100_80):
		return types.GPU_A100_80, nil
	case string(types.GPU_A10G):
		return types.GPU_A10G, nil
	case string(types.GPU_L4):
		return types.GPU_L4, nil
	case string(types.GPU_T4):
		return types.GPU_T4, nil
	default:
		return types.GPUType(""), errors.New("invalid gpu type")
	}
}
