package types

import (
	"encoding/json"
	"errors"
	"fmt"
)

// CPUTierProtocolVersion is reported by workers that can run tiered work.
const CPUTierProtocolVersion uint32 = 1

// SpotDrainSeconds is the notice Spot work gets before it is stopped.
const SpotDrainSeconds = 30

var ErrCPUTierIneligible = errors.New("CPU tiers apply only to hosted CPU workloads")

type CPUTier string

const (
	CPUTierSpot        CPUTier = "spot"
	CPUTierShared      CPUTier = "shared"
	CPUTierPerformance CPUTier = "performance"
)

// CPUTierPolicy is a workspace's CPU tier enrollment, owned by the billing
// service: whether it may choose any tier, and the tier its workloads get when
// they choose none.
type CPUTierPolicy struct {
	Enabled        bool    `json:"enabled"`
	DefaultCPUTier CPUTier `json:"default_cpu_tier"`
}

// Resolve returns the requested tier, or the default when none is requested.
func (p CPUTierPolicy) Resolve(requested CPUTier) (CPUTier, error) {
	switch {
	case requested == "":
		return p.DefaultCPUTier, nil
	case !p.Enabled && requested != p.DefaultCPUTier:
		return "", fmt.Errorf("CPU tiers are not enabled for this workspace")
	}
	return requested, nil
}

// Validate checks that a workload's shape fits the tier.
func (t CPUTier) Validate(cpu, memory int64, nativeVM, gpu bool) error {
	switch {
	case t == "":
		return nil
	case t != CPUTierSpot && t != CPUTierShared && t != CPUTierPerformance:
		return fmt.Errorf("unknown CPU tier %q", t)
	case gpu:
		return ErrCPUTierIneligible
	case nativeVM && (cpu < 1000 || memory < 1024):
		return fmt.Errorf("CPU tier VMs require at least 1 vCPU and 1 GiB RAM")
	case t == CPUTierShared && (cpu > 8000 || memory > 16384):
		return fmt.Errorf("Shared supports at most 8 vCPU and 16 GiB RAM")
	}
	return nil
}

// ValidateCPUTier checks that the stub's resources fit its CPU tier.
func (c *StubConfigV1) ValidateCPUTier() error {
	return c.CPUTier.Validate(c.Runtime.Cpu, c.Runtime.Memory, c.UseVM, c.RequiresGPU())
}

// MatchesPool reports whether work on tier t may run in pool p. Untiered work
// stays on untagged pools. Shared and Performance need a hosted pool of their
// class with a 2.5x or 1x CPU share multiplier; Spot borrows from either.
func (t CPUTier) MatchesPool(p WorkerPoolConfig) bool {
	if t == "" {
		return p.CPUClass == ""
	}
	m := p.CPUShareMultiplier.hundredths()
	valid := p.HostedCPU() && (p.CPUClass == CPUTierShared && m == 250 || p.CPUClass == CPUTierPerformance && m == 100)
	return valid && (t == CPUTierSpot || t == p.CPUClass)
}

// HostedCPU reports whether the pool is Beam-hosted CPU capacity, the only
// capacity CPU tiers use.
func (p WorkerPoolConfig) HostedCPU() bool {
	return p.GPUType == "" && !p.AgentHosted()
}

// ValidateCPU checks the pool's CPU share multiplier and the CPU class it sells.
func (p WorkerPoolConfig) ValidateCPU() error {
	if err := p.CPUShareMultiplier.Validate(); err != nil {
		return err
	}
	if p.CPUClass != "" && !p.CPUClass.MatchesPool(p) {
		return fmt.Errorf("CPU class requires hosted CPU capacity with Shared 2.5x or Performance 1x")
	}
	return nil
}

func (r *ContainerRequest) CPUTier() CPUTier {
	var c struct {
		Tier CPUTier `json:"cpu_tier"`
	}
	if r != nil {
		_ = json.Unmarshal([]byte(r.Stub.Config), &c)
	}
	return c.Tier
}
