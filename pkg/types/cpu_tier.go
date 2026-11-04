package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
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

// CPUPriceBinding is set by the billing service, never accepted from a client.
// Hourly cents are strings so the persisted Stripe amounts retain their precision.
type CPUPriceBinding struct {
	Tier                  CPUTier `json:"tier"`
	Version               string  `json:"version"`
	CPUPriceID            string  `json:"cpu_price_id"`
	MemoryPriceID         string  `json:"memory_price_id"`
	CPUCentsPerHour       string  `json:"cpu_cents_per_hour"`
	MemoryCentsPerGiBHour string  `json:"memory_cents_per_gib_hour"`
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
	return p.GPUType == "" && !p.Mode.AgentHosted()
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

// CPUPricing returns the prices bound to a tiered request, or nil for an untiered one.
func (r *ContainerRequest) CPUPricing() (*CPUPriceBinding, error) {
	if r.CPUTier() == "" {
		return nil, nil
	}
	c, err := r.Stub.UnmarshalConfig()
	if err != nil {
		return nil, err
	}
	if err := c.CPUTier.Validate(r.Cpu, r.Memory, r.UseVM, r.RequiresGPU()); err != nil {
		return nil, err
	}
	if c.CPUPricing == nil || c.CPUPricing.Tier != c.CPUTier {
		return nil, fmt.Errorf("CPU tier requires a server-resolved price binding")
	}
	return c.CPUPricing, nil
}

// CostPerMs prices cpu millicores and memory MiB in cents per millisecond.
func (b *CPUPriceBinding) CostPerMs(cpu, memory int64) (float64, error) {
	c, okCPU := new(big.Rat).SetString(b.CPUCentsPerHour)
	m, okMemory := new(big.Rat).SetString(b.MemoryCentsPerGiBHour)
	if !okCPU || !okMemory || c.Sign() <= 0 || m.Sign() <= 0 || b.Version == "" || b.CPUPriceID == "" || b.MemoryPriceID == "" {
		return 0, fmt.Errorf("invalid CPU price binding")
	}
	c.Add(c.Mul(c, big.NewRat(cpu, 1000)), m.Mul(m, big.NewRat(memory, 1024)))
	cost, _ := c.Quo(c, big.NewRat(3600000, 1)).Float64()
	return cost, nil
}

// ValidateCPURevision rejects in-place changes to a stub's tier or a tiered
// stub's resources. Usage is priced from the stub, so these need a redeploy.
func (c *StubConfigV1) ValidateCPURevision(previous StubConfigV1) error {
	r, p := c.Runtime, previous.Runtime
	if c.CPUTier != previous.CPUTier || c.CPUTier != "" && (r.Cpu != p.Cpu || r.Memory != p.Memory || r.GpuCount != p.GpuCount) {
		return fmt.Errorf("redeploy to change CPU tier or resources; historical usage keeps its original configuration")
	}
	return c.CPUTier.Validate(r.Cpu, r.Memory, c.UseVM, c.RequiresGPU())
}
