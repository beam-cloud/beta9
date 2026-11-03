package types

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
)

const CPUTierProtocolVersion uint32 = 1

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

func (t CPUTier) Validate(cpu, memory int64, nativeVM, gpu bool) error {
	if t == "" {
		return nil
	}
	if t != CPUTierSpot && t != CPUTierShared && t != CPUTierPerformance {
		return fmt.Errorf("unknown CPU tier %q", t)
	}
	if gpu {
		return fmt.Errorf("CPU tiers apply only to hosted CPU workloads")
	}
	if nativeVM && (cpu < 1000 || memory < 1024) {
		return fmt.Errorf("CPU tier VMs require at least 1 vCPU and 1 GiB RAM")
	}
	if t == CPUTierShared && (cpu > 8000 || memory > 16384) {
		return fmt.Errorf("Shared supports at most 8 vCPU and 16 GiB RAM")
	}
	return nil
}

// MatchesPool is the only tier-to-pool policy. Untagged pools retain legacy
// placement; Spot inherits the multiplier of the capacity it borrows.
func (t CPUTier) MatchesPool(p WorkerPoolConfig) bool {
	if t == "" {
		return p.CPUClass == ""
	}
	if p.GPUType != "" || p.Mode.AgentHosted() {
		return false
	}
	multiplier := float64(p.CPUShareMultiplier)
	if multiplier == 0 {
		multiplier = 1
	}
	valid := p.CPUClass == CPUTierShared && multiplier == 2.5 || p.CPUClass == CPUTierPerformance && multiplier == 1
	return valid && (t == CPUTierSpot || t == p.CPUClass)
}

func (r *ContainerRequest) CPUConfig() (*StubConfigV1, error) {
	c, err := r.Stub.UnmarshalConfig()
	if err != nil {
		return nil, err
	}
	if err = c.CPUTier.Validate(r.Cpu, r.Memory, r.UseVM, r.RequiresGPU()); err != nil {
		return nil, err
	}
	if c.CPUTier != "" && (c.CPUPricing == nil || c.CPUPricing.Tier != c.CPUTier) {
		return nil, fmt.Errorf("CPU tier requires a server-resolved price binding")
	}
	return c, nil
}

func (r *ContainerRequest) CPUTier() CPUTier {
	var c struct {
		Tier CPUTier `json:"cpu_tier"`
	}
	_ = json.Unmarshal([]byte(r.Stub.Config), &c)
	return c.Tier
}

func (b *CPUPriceBinding) CostPerMs(cpu, memory int64) (float64, error) {
	a, ok := new(big.Rat).SetString(b.CPUCentsPerHour)
	m, valid := new(big.Rat).SetString(b.MemoryCentsPerGiBHour)
	if !ok || !valid || a.Sign() <= 0 || m.Sign() <= 0 || b.Version == "" || b.CPUPriceID == "" || b.MemoryPriceID == "" {
		return 0, fmt.Errorf("invalid CPU price binding")
	}
	a.Mul(a, big.NewRat(cpu, 1000))
	m.Mul(m, big.NewRat(memory, 1024))
	a.Add(a, m).Quo(a, big.NewRat(3600000, 1))
	f, _ := a.Float64()
	return f, nil
}

// Bound resource changes need a fresh stub, so historical duration cannot be
// replayed with a different shape or price. Replica/env edits keep the binding.
func (c *StubConfigV1) ValidateCPURevision(previous StubConfigV1) error {
	if (c.CPUTier != "" || previous.CPUTier != "") && (c.CPUTier != previous.CPUTier || !reflect.DeepEqual(c.CPUPricing, previous.CPUPricing) ||
		c.Runtime.Cpu != previous.Runtime.Cpu || c.Runtime.Memory != previous.Runtime.Memory || c.Runtime.GpuCount != previous.Runtime.GpuCount || !reflect.DeepEqual(c.Runtime.Gpus, previous.Runtime.Gpus)) {
		return fmt.Errorf("redeploy to change CPU tier or resources; historical usage keeps its original configuration")
	}
	return c.CPUTier.Validate(c.Runtime.Cpu, c.Runtime.Memory, c.UseVM, c.RequiresGPU())
}
