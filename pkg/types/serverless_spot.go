package types

// ServerlessGPUPreemptable allows ordinary GPU Pods, Functions and TaskQueues to use
// shared Spot capacity, as Endpoints already do. Their request constructors
// apply this default; the scheduler still respects an explicit false value.
//
// Explicit pool or machine placement, reservations, VMs and managed endpoints
// keep their own interruption policy. This does not make a request Evictable:
// allowing a provider to reclaim its machine is separate from letting the
// scheduler stop a container to make room for another workload.
func (c *StubConfigV1) ServerlessGPUPreemptable() bool {
	return c != nil && c.RequiresGPU() && c.PoolSelector() == "" && c.MachineID == "" &&
		!c.Pool.RequiresReservation() && !c.UseVM && c.ManagedEndpoint == nil
}
