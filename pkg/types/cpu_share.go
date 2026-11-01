package types

import (
	"fmt"
	"math"
)

const maxCPUShareMultiplier CPUShareMultiplier = 16

// CPUShareMultiplier is the number of vCPUs a pool schedules per physical CPU;
// zero means 1. CPU limits, memory and billing stay physical.
type CPUShareMultiplier float64

func (m CPUShareMultiplier) Validate() error {
	if m == 0 || (m >= 1 && m <= maxCPUShareMultiplier && float64(m) == float64(m.hundredths())/100) {
		return nil
	}
	return fmt.Errorf("cpu share multiplier must be between 1 and %g with at most two decimals, got %g", float64(maxCPUShareMultiplier), float64(m))
}

// Schedulable converts physical CPU millicores into schedulable millicores.
func (m CPUShareMultiplier) Schedulable(physical int64) int64 {
	return physical * m.hundredths() / 100
}

func (m CPUShareMultiplier) hundredths() int64 {
	if !(m >= 1 && m <= maxCPUShareMultiplier) {
		return 100
	}
	return int64(math.Round(float64(m) * 100))
}
