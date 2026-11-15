package provisioner

import (
	"time"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/scheduler"
)

// MachineTag is the tag an instance bought for a pool carries at its
// provider. Its value is the machine's ID. An instance with the tag and no
// record is deleted, so nothing else may use the tag.
const MachineTag = "beam-pool-machine"

// Machine is the record of one machine bought for a pool. It is written
// before the provider is asked for the machine and removed once the provider
// has confirmed that the machine is gone, so everything that costs money has
// a record.
type Machine struct {
	// ID is the ID the machine joins its pool with, and its MachineTag.
	ID   string `json:"id"`
	Pool string `json:"pool"`
	// What it was bought as, and from which provider.
	offerKey
	// InstanceID is the provider's ID for the machine. It is empty until the
	// provider has answered the request to create it.
	InstanceID string `json:"instance_id,omitempty"`
	// TokenHash names the credential the machine joins with.
	TokenHash string `json:"token_hash,omitempty"`

	// What it has, and costs.
	GPUCount      uint32 `json:"gpu_count,omitempty"`
	CPUMillicores int64  `json:"cpu_millicores"`
	MemoryMB      int64  `json:"memory_mb"`
	HourlyMicros  int64  `json:"hourly_micros"`

	CreatedAt time.Time `json:"created_at"`
	// ReadyAt is when the machine's worker first became available. It is
	// zero while the machine boots.
	ReadyAt time.Time `json:"ready_at,omitzero"`
	// Until is when a provider that can delete a machine by itself deletes
	// this one, unless that is put off again. It is set, and put off, for
	// every machine: a provider that cannot does nothing with it.
	Until time.Time `json:"until,omitzero"`
	// BilledUntil is the time up to which the machine's cost has been added
	// to what was spent.
	BilledUntil time.Time `json:"billed_until,omitzero"`

	// IdleSince is since when the machine has had nothing to run.
	IdleSince time.Time `json:"idle_since,omitzero"`
	// DownSince is since when a machine that had joined has had no usable
	// worker.
	DownSince time.Time `json:"down_since,omitzero"`
	// PoolGoneSince is since when no gateway replica has had a pool that
	// buys this machine.
	PoolGoneSince time.Time `json:"pool_gone_since,omitzero"`

	// Release is set once the machine is to be given back. It is not unset.
	Release *Release `json:"release,omitempty"`
}

// Release is why a machine is given back, and how.
type Release struct {
	Reason string `json:"reason"`
	// DrainUntil is how long the machine's work is waited for before it is
	// given back anyway. Zero means it is given back at once.
	DrainUntil time.Time `json:"drain_until,omitzero"`
	// SoldOut is set when the provider could not start what it sold. What
	// the machine was bought as is then passed over for a while, like an
	// offer the provider refused to sell.
	SoldOut bool `json:"sold_out,omitempty"`
}

// newMachine is the record of a machine about to be bought from offer.
func newMachine(id, pool string, offer providers.Offer, now time.Time) Machine {
	return Machine{
		ID:            id,
		Pool:          pool,
		offerKey:      keyOf(offer),
		GPUCount:      offer.GPUCount,
		CPUMillicores: offer.CPUMillicores,
		MemoryMB:      offer.MemoryMB,
		HourlyMicros:  offer.HourlyCostMicros,
		CreatedAt:     now,
	}
}

// booting reports whether the machine has been bought and has not run a
// worker yet.
func (m Machine) booting() bool {
	return m.InstanceID != "" && m.ReadyAt.IsZero() && m.Release == nil
}

// workerID is the ID of the worker that stands for the machine.
func (m Machine) workerID() string {
	return compute.AgentMachineWorkerID(m.ID)
}

// billedFrom is the time from which the machine's cost has yet to be
// counted as spent.
func (m Machine) billedFrom() time.Time {
	if m.BilledUntil.IsZero() {
		return m.CreatedAt
	}
	return m.BilledUntil
}

// cost is what the machine cost from one time to another, in cents. A
// machine without an instance is not known to have been bought, and has cost
// nothing.
func (m Machine) cost(from, to time.Time) float64 {
	if m.InstanceID == "" || !to.After(from) {
		return 0
	}
	return float64(m.HourlyMicros) / microsPerCent * to.Sub(from).Hours()
}

// usableMemoryShare is how much of the memory a machine is sold with can be
// counted on. A machine reports less than its nominal size, because firmware
// and the kernel keep some.
const usableMemoryShare = 0.9

func usableMemoryMB(nominal int64) int64 {
	return int64(float64(nominal) * usableMemoryShare)
}

// provisioned is the machine as the scheduler is told about it.
func (m Machine) provisioned() scheduler.ProvisionedMachine {
	return scheduler.ProvisionedMachine{
		MachineID: m.ID,
		CPU:       m.CPUMillicores,
		Memory:    usableMemoryMB(m.MemoryMB),
		GPUCount:  m.GPUCount,
	}
}
