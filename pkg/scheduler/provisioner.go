package scheduler

import "context"

// Provisioner buys machines for pools that have a provisioning block. Such a
// pool adds a worker by buying a machine.
//
// It is declared here, next to the pool that calls it, so the scheduler
// depends on no provider code. pkg/provisioner implements it.
type Provisioner interface {
	// CanProvision reports whether the pool could buy a machine with room for
	// need right now. It is false when the pool's provider has no matching
	// stock, the pool is at maxNodes, a spend cap is reached, or a purchase
	// for the pool has just failed for any reason but lack of stock. Lack of
	// stock rules out only the offer that was asked for.
	CanProvision(ctx context.Context, pool string, need MachineNeed) bool

	// Provision buys one machine for the pool and returns it before it has
	// booted. The machine joins the pool on its own once it is up.
	Provision(ctx context.Context, pool string, need MachineNeed) (*ProvisionedMachine, error)

	// Booting returns the pool's machines that have been bought and have not
	// yet run a worker.
	Booting(ctx context.Context, pool string) ([]ProvisionedMachine, error)
}

// MachineNeed is the capacity a new machine must have.
type MachineNeed struct {
	CPU      int64 // physical millicores
	Memory   int64 // MiB
	GPUCount uint32
}

// ProvisionedMachine is a machine that has been bought and is booting.
type ProvisionedMachine struct {
	// MachineID is the ID the machine will join the pool with. It is chosen
	// before the purchase, so the worker that stands for the machine can be
	// created at once.
	MachineID string
	CPU       int64 // physical millicores
	Memory    int64 // MiB
	GPUCount  uint32
}
