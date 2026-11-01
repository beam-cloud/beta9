package types

import (
	"fmt"
	"math"
)

// MaxCPUShareMultiplier bounds how far a pool may oversubscribe its CPUs.
const MaxCPUShareMultiplier CPUShareMultiplier = 16

// CPUShareMultiplier is the number of vCPUs a worker pool admits per physical
// CPU; zero means 1. It scales scheduler capacity only: host and container CPU
// limits, memory, and billing keep using physical CPUs and requested vCPUs.
type CPUShareMultiplier float64

// Validate accepts zero or a ratio between 1 and MaxCPUShareMultiplier with at
// most two decimals, the precision the capacity conversions use.
func (m CPUShareMultiplier) Validate() error {
	if m == 0 || (m >= 1 && m <= MaxCPUShareMultiplier && float64(m) == float64(m.hundredths())/100) {
		return nil
	}
	return fmt.Errorf("cpu share multiplier must be between 1 and %g with at most two decimals, got %g", float64(MaxCPUShareMultiplier), float64(m))
}

// Schedulable converts physical millicores into the millicores the scheduler
// admits.
func (m CPUShareMultiplier) Schedulable(physical int64) int64 {
	return physical * m.hundredths() / 100
}

// Physical is the fewest physical millicores whose schedulable capacity holds
// cpu.
func (m CPUShareMultiplier) Physical(cpu int64) int64 {
	ratio := m.hundredths()
	return (cpu*100 + ratio - 1) / ratio
}

// hundredths keeps the conversions in integers, so Physical never rounds a
// worker below the request that sized it.
func (m CPUShareMultiplier) hundredths() int64 {
	if !(m >= 1 && m <= MaxCPUShareMultiplier) {
		return 100
	}
	return int64(math.Round(float64(m) * 100))
}
