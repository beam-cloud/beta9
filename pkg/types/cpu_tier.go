package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// CPUTierProtocolVersion is reported by workers that can run tiered work.
const CPUTierProtocolVersion uint32 = 1

var ErrCPUTierIneligible = errors.New("CPU tiers apply only to hosted CPU workloads")

type CPUTier string

const (
	CPUTierSpot        CPUTier = "spot"
	CPUTierShared      CPUTier = "shared"
	CPUTierPerformance CPUTier = "performance"
)

// CPUTiers defines each CPU tier by name (worker.cpuTiers). A pool sells a
// tier by setting its cpuClass to the tier's name.
type CPUTiers map[CPUTier]CPUTierConfig

// CPUTierConfig defines a CPU tier. A tier either sells the capacity of its
// pools, scheduled at CPUShareMultiplier vCPUs per physical CPU, or borrows the
// idle capacity of the tiers in BorrowsFrom. Borrowed work never adds workers,
// waits for room, and gets DrainSeconds notice when the capacity is reclaimed.
type CPUTierConfig struct {
	CPUShareMultiplier CPUShareMultiplier `key:"cpuShareMultiplier" json:"cpu_share_multiplier,omitempty"`
	MaxCpu             int64              `key:"maxCpu" json:"max_cpu,omitempty"`       // Millicores per workload, or 0 for no limit
	MaxMemory          int64              `key:"maxMemory" json:"max_memory,omitempty"` // MiB per workload, or 0 for no limit
	BorrowsFrom        []CPUTier          `key:"borrowsFrom" json:"borrows_from,omitempty"`
	DrainSeconds       uint32             `key:"drainSeconds" json:"drain_seconds,omitempty"`
}

// Borrows reports whether the tier runs on other tiers' idle capacity.
func (c CPUTierConfig) Borrows() bool {
	return len(c.BorrowsFrom) > 0
}

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

// Validate checks that every tier sells at a valid CPU share multiplier or
// borrows only from tiers that sell.
func (ts CPUTiers) Validate() error {
	for name, tier := range ts {
		if err := tier.CPUShareMultiplier.Validate(); err != nil {
			return fmt.Errorf("CPU tier %q: %w", name, err)
		}
		for _, from := range tier.BorrowsFrom {
			if lender, ok := ts[from]; !ok || lender.Borrows() {
				return fmt.Errorf("CPU tier %q borrows from %q, which sells no capacity", name, from)
			}
		}
	}
	return nil
}

// ValidateWorkload checks that a workload's shape fits tier t.
func (ts CPUTiers) ValidateWorkload(t CPUTier, cpu, memory int64, nativeVM, gpu bool) error {
	tier, ok := ts[t]
	switch {
	case t == "":
		return nil
	case !ok:
		return fmt.Errorf("unknown CPU tier %q", t)
	case gpu:
		return ErrCPUTierIneligible
	case nativeVM && (cpu < 1000 || memory < 1024):
		return fmt.Errorf("CPU tier VMs require at least 1 vCPU and 1 GiB RAM")
	case tier.MaxCpu > 0 && cpu > tier.MaxCpu:
		return fmt.Errorf("The %s tier supports at most %g vCPU", t, float64(tier.MaxCpu)/1000)
	case tier.MaxMemory > 0 && memory > tier.MaxMemory:
		return fmt.Errorf("The %s tier supports at most %g GiB RAM", t, float64(tier.MaxMemory)/1024)
	}
	return nil
}

// ValidateCPUTier checks that the stub's resources fit its CPU tier.
func (c *StubConfigV1) ValidateCPUTier(tiers CPUTiers) error {
	return tiers.ValidateWorkload(c.CPUTier, c.Runtime.Cpu, c.Runtime.Memory, c.UseVM, c.RequiresGPU())
}

// NormalizePool returns pool p with the CPU share multiplier of the tier it
// sells. Only hosted CPU pools sell tiers, and only tiers that do not borrow;
// a pool may repeat its tier's multiplier but not change it.
func (ts CPUTiers) NormalizePool(p WorkerPoolConfig) (WorkerPoolConfig, error) {
	if err := p.CPUShareMultiplier.Validate(); err != nil || p.CPUClass == "" {
		return p, err
	}
	tier, ok := ts[p.CPUClass]
	switch {
	case !ok || tier.Borrows():
		return p, fmt.Errorf("CPU class %q is not a tier that sells capacity", p.CPUClass)
	case !p.HostedCPU():
		return p, fmt.Errorf("CPU class requires hosted CPU capacity")
	case p.CPUShareMultiplier != 0 && p.CPUShareMultiplier.hundredths() != tier.CPUShareMultiplier.hundredths():
		return p, fmt.Errorf("CPU class %q schedules at %gx", p.CPUClass, float64(tier.CPUShareMultiplier.hundredths())/100)
	}
	p.CPUShareMultiplier = tier.CPUShareMultiplier
	return p, nil
}

// MatchesPool reports whether work on tier t may run in pool p. Untiered work
// stays on untagged pools; tiered work runs on pools that sell its tier or one
// it borrows from.
func (ts CPUTiers) MatchesPool(t CPUTier, p WorkerPoolConfig) bool {
	if t == "" {
		return p.CPUClass == ""
	}
	_, err := ts.NormalizePool(p)
	return err == nil && p.CPUClass != "" && (p.CPUClass == t || slices.Contains(ts[t].BorrowsFrom, p.CPUClass))
}

// HostedCPU reports whether the pool is Beam-hosted CPU capacity, the only
// capacity CPU tiers use.
func (p WorkerPoolConfig) HostedCPU() bool {
	return p.GPUType == "" && !p.AgentHosted()
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

// Borrowed reports whether the request runs on capacity borrowed from other CPU
// tiers, the only tiered work the scheduler makes evictable.
func (r *ContainerRequest) Borrowed() bool {
	return r != nil && r.Evictable && r.CPUTier() != ""
}
