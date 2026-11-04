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

// cpuTiers defines the CPU tiers, which only hosted CPU workloads use. Shared
// and Performance sell the capacity of hosted CPU pools of their class (see
// CPUClass). Spot is the lowest tier and only runs when chosen: it borrows
// idle capacity from every hosted CPU pool, never adds workers, and gets
// drainSeconds notice when it is reclaimed.
var cpuTiers = map[CPUTier]struct {
	maxCpu, maxMemory int64 // Per workload, in millicores and MiB
	borrowsFrom       []CPUTier
	drainSeconds      uint32
}{
	CPUTierShared:      {maxCpu: 8000, maxMemory: 16384},
	CPUTierPerformance: {},
	CPUTierSpot:        {borrowsFrom: []CPUTier{CPUTierShared, CPUTierPerformance}, drainSeconds: 30},
}

// Borrows reports whether the tier runs on other tiers' idle capacity.
func (t CPUTier) Borrows() bool {
	return len(cpuTiers[t].borrowsFrom) > 0
}

// DrainSeconds is the notice work on a borrowing tier gets before it stops.
func (t CPUTier) DrainSeconds() uint32 {
	return cpuTiers[t].drainSeconds
}

// CPUTierPolicy is a workspace's CPU tier enrollment, owned by the billing
// service: whether it may choose any tier, and the tier its workloads get when
// they choose none.
type CPUTierPolicy struct {
	Enabled        bool    `json:"enabled"`
	DefaultCPUTier CPUTier `json:"default_cpu_tier"`
}

// Resolve returns the requested tier, or the default when none is requested.
// A policy can't make a borrowing tier the default.
func (p CPUTierPolicy) Resolve(requested CPUTier) (CPUTier, error) {
	switch {
	case requested == "" && p.DefaultCPUTier.Borrows():
		return "", fmt.Errorf("invalid CPU tier policy: %s must be chosen explicitly", p.DefaultCPUTier)
	case requested == "":
		return p.DefaultCPUTier, nil
	case !p.Enabled && requested != p.DefaultCPUTier:
		return "", fmt.Errorf("CPU tiers are not enabled for this workspace")
	}
	return requested, nil
}

// Validate checks that a workload's shape fits the tier.
func (t CPUTier) Validate(cpu, memory int64, nativeVM, gpu bool) error {
	tier, ok := cpuTiers[t]
	switch {
	case t == "":
		return nil
	case !ok:
		return fmt.Errorf("unknown CPU tier %q", t)
	case gpu:
		return ErrCPUTierIneligible
	case nativeVM && (cpu < 1000 || memory < 1024):
		return fmt.Errorf("CPU tier VMs require at least 1 vCPU and 1 GiB RAM")
	case tier.maxCpu > 0 && (cpu > tier.maxCpu || memory > tier.maxMemory):
		return fmt.Errorf("The %s tier supports at most %g vCPU and %g GiB RAM", t, float64(tier.maxCpu)/1000, float64(tier.maxMemory)/1024)
	}
	return nil
}

// ValidateCPUTier checks that the stub's resources fit its CPU tier.
func (c *StubConfigV1) ValidateCPUTier() error {
	return c.CPUTier.Validate(c.Runtime.Cpu, c.Runtime.Memory, c.UseVM, c.RequiresGPU())
}

// MatchesPool reports whether work on tier t may run in pool p. Untiered work
// stays off shared pools, which it never opted into; tiered work runs on pools
// that sell its tier or one it borrows from.
func (t CPUTier) MatchesPool(p WorkerPoolConfig) bool {
	class := p.CPUClass()
	if t == "" {
		return class != CPUTierShared
	}
	return class != "" && (class == t || slices.Contains(cpuTiers[t].borrowsFrom, class))
}

// CPUClass is the tier a pool sells. A hosted CPU pool that schedules more than
// one vCPU per physical CPU sells Shared, and any other hosted CPU pool sells
// Performance. GPU and agent pools sell none.
func (p WorkerPoolConfig) CPUClass() CPUTier {
	switch {
	case !p.HostedCPU():
		return ""
	case p.CPUShareMultiplier.hundredths() > 100:
		return CPUTierShared
	}
	return CPUTierPerformance
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

// Borrowed reports whether the request runs on borrowed capacity (Spot).
func (r *ContainerRequest) Borrowed() bool {
	return r.CPUTier().Borrows()
}
