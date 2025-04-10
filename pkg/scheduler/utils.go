package scheduler

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	"k8s.io/apimachinery/pkg/api/resource"
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

func ParseGpuCount(gpuCount interface{}) (int64, error) {
	gpuCountValue, err := strconv.ParseInt(gpuCount.(string), 10, 64)
	if err != nil {
		return 0, err
	}

	if gpuCountValue < 0 {
		return 0, errors.New("invalid gpu_count value")
	}

	return gpuCountValue, nil
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
	case "":
		memoryValue = value
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

func ParseGPUType(gpu interface{}) (types.GpuType, error) {
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
	case string(types.GPU_H100):
		return types.GPU_H100, nil
	case string(types.GPU_A6000):
		return types.GPU_A6000, nil
	case string(types.GPU_RTX4090):
		return types.GPU_RTX4090, nil
	case string(types.GPU_L40S):
		return types.GPU_L40S, nil
	default:
		return types.GpuType(""), errors.New("invalid gpu type")
	}
}

func parseTmpSizeLimit(workerPoolTmpSizeLimit string, globalWorkerTmpSizeLimit string) resource.Quantity {
	defaultLimit := resource.MustParse("30Gi")

	// First try worker pool specific limit, then fall back to global config
	limitToUse := workerPoolTmpSizeLimit
	if limitToUse == "" {
		limitToUse = globalWorkerTmpSizeLimit
	}

	tmpSizeLimit, err := resource.ParseQuantity(limitToUse)
	if err != nil {
		log.Error().Err(err).Str("attempted_limit", limitToUse).Msg("failed to parse tmp size limit, using default")
		tmpSizeLimit = defaultLimit
	}
	return tmpSizeLimit
}
