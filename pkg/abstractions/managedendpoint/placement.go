package managedendpoint

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// Placement decides, for one GPU type per tick, which replicas start, which
// give their GPU back, and which retire. config.yaml is the source of truth:
// minimums first in priority order, then requested serverless copies, then
// hot surplus up to each cap.

const (
	passMinimum    = iota // configured minimums, priority order
	passServerless        // one copy per requested serverless model
	passSurplus           // hot extras up to each cap
)

const (
	reasonIdle        = "on-demand endpoint idle"
	reasonOverCap     = "over config.yaml cap"
	reasonRetired     = "endpoint retired"
	reasonUnlisted    = "removed from config.yaml"
	reasonReclaimedBy = "gpu reclaimed for "
	rolloutReplace    = "replace"
)

// slot is one config.yaml entry on one GPU type and what runs for it now.
type slot struct {
	entry     types.FleetEntry
	endpoint  *types.ManagedEndpoint
	replicas  []*types.EndpointReplica // current version on this GPU
	demand    *endpointDemand          // serverless placements only
	started   uint32
	replacing bool // an older version is still alive on this GPU
	res       *replicaResources
	resRead   bool
}

func (s *slot) id() string { return s.entry.EndpointID }

func (s *slot) gpuCount(gpu string) uint32 {
	return max(s.endpoint.Spec.Gpu[gpu].Count, 1)
}

func (s *slot) count() uint32 {
	n := s.started
	for _, r := range s.replicas {
		if r.Alive() {
			n++
		}
	}
	return n
}

// surplus is how many replicas the slot could give back and still meet its minimum.
func (s *slot) surplus() uint32 {
	return s.count() - min(s.count(), s.entry.MinReplicas)
}

func (s *slot) starting() bool {
	if s.started > 0 {
		return true
	}
	for _, r := range s.replicas {
		if r.Alive() && !r.Serving() {
			return true
		}
	}
	return false
}

// requested notes that capacity is on its way, so no other GPU type starts a
// second copy for the same demand this tick.
func (s *slot) requested() {
	if s.demand != nil {
		s.demand.starting = true
	}
}

func (s *slot) limit() uint32 {
	if s.entry.MaxReplicas == 0 {
		return s.count() + maxStartsPerTick
	}
	return s.entry.MaxReplicas
}

// want is how many replicas this pass allows the slot; 0 when it does not apply.
func (s *slot) want(pass int) uint32 {
	switch {
	case pass == passMinimum:
		return s.entry.MinReplicas
	case s.entry.Serverless != (pass == passServerless):
		return 0
	case s.entry.Serverless:
		return s.serverlessWant()
	}
	return s.limit()
}

// serverlessWant is one more copy while requests exceed the capacity the
// running copies report. Capacity is learned from the engine heartbeat, so
// one copy comes up before demand is judged again. A serving old version may
// carry traffic while its replacement loads; that must not stall the rollout.
func (s *slot) serverlessWant() uint32 {
	d := s.demand
	if d == nil || d.starting {
		return 0
	}
	requested := d.active > d.capacity || d.pending && d.active >= d.capacity
	replacing := s.replacing && s.count() == 0 && d.warm
	if !requested && !replacing {
		return 0
	}
	return min(s.limit(), s.count()+1)
}

func (s *slot) idle() bool {
	return s.entry.Serverless && s.demand != nil && !s.demand.warm && s.demand.active == 0
}

// scaleDownOrder drains unprotected replicas first, then not ready, least loaded, and newest.
func scaleDownOrder(replicas []*types.EndpointReplica) {
	sort.SliceStable(replicas, func(i, j int) bool {
		a, b := replicas[i], replicas[j]
		if a.Protected != b.Protected {
			return !a.Protected
		}
		if a.Serving() != b.Serving() {
			return !a.Serving()
		}
		if a.Capacity.InFlight != b.Capacity.InFlight {
			return a.Capacity.InFlight < b.Capacity.InFlight
		}
		return a.StartedAt.After(b.StartedAt)
	})
}

// Fill ------------------------------------------------------------------------

// fill starts replicas for one GPU type. When no GPU is idle, at most one
// replica per tick is reclaimed for the slot that needs it; see reclaim.
func (c *controller) fill(ctx context.Context, gpu string, entries []types.FleetEntry, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica, inv *clusterInventory, demand map[string]*endpointDemand) {
	slots := c.slots(ctx, gpu, entries, endpoints, live, demand)
	protected := protectedCounts(gpu, live)
	reclaimed := false
	for pass := passMinimum; pass <= passSurplus; pass++ {
		for i, s := range slots {
			noRoom := c.grow(ctx, gpu, s, pass, protected, inv)
			if noRoom && !reclaimed && !s.starting() {
				reclaimed = c.reclaim(ctx, gpu, i, slots, live, inv, pass)
				if reclaimed {
					s.requested()
				}
			}
		}
		if pass == passMinimum && unmetMinimum(slots) {
			return // an unmet minimum holds the remaining capacity
		}
	}
}

// slots builds the GPU's placements and applies their scale-downs.
func (c *controller) slots(ctx context.Context, gpu string, entries []types.FleetEntry, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica, demand map[string]*endpointDemand) []*slot {
	var out []*slot
	for _, entry := range entries {
		endpoint := endpoints[entry.EndpointID]
		if endpoint == nil || !endpoint.Enabled() {
			continue
		}
		s := &slot{entry: entry, endpoint: endpoint, demand: demand[entry.EndpointID]}
		var all []*types.EndpointReplica
		for _, r := range live {
			if r.EndpointID != entry.EndpointID || r.GPU != gpu || !r.Alive() {
				continue
			}
			all = append(all, r)
			if r.Version == endpoint.Version {
				s.replicas = append(s.replicas, r)
			} else {
				s.replacing = true
			}
		}
		if s.idle() {
			c.scaleToZero(ctx, s, all)
		}
		c.trimToCap(ctx, s)
		out = append(out, s)
	}
	return out
}

// scaleToZero drains an idle serverless model once its startup and ready grace pass.
func (c *controller) scaleToZero(ctx context.Context, s *slot, replicas []*types.EndpointReplica) {
	now := time.Now()
	for _, r := range replicas {
		if !initialDemandGrace(r, now) {
			_ = c.drainReplica(ctx, r, s.endpoint.Spec.DrainSeconds, false, reasonIdle)
		}
	}
}

// trimToCap treats a lowered cap as an explicit scale-down, protected copies included.
func (c *controller) trimToCap(ctx context.Context, s *slot) {
	over := int(s.count()) - int(s.entry.MaxReplicas)
	if s.entry.MaxReplicas == 0 || over <= 0 {
		return
	}
	scaleDownOrder(s.replicas)
	for _, r := range s.replicas[:over] {
		_ = c.drainReplica(ctx, r, s.endpoint.Spec.DrainSeconds, false, reasonOverCap)
	}
}

func protectedCounts(gpu string, live []*types.EndpointReplica) map[string]uint32 {
	counts := map[string]uint32{}
	for _, r := range live {
		if r.GPU == gpu && r.Alive() && r.Protected {
			counts[r.EndpointID]++
		}
	}
	return counts
}

func unmetMinimum(slots []*slot) bool {
	for _, s := range slots {
		if s.count() < s.entry.MinReplicas {
			return true
		}
	}
	return false
}

// grow starts replicas for the slot until the pass is satisfied, the per-tick
// budget is spent, a start fails (which backs the endpoint off), or no GPU is
// idle, which is the only case reported.
func (c *controller) grow(ctx context.Context, gpu string, s *slot, pass int, protected map[string]uint32, inv *clusterInventory) (noRoom bool) {
	want := s.want(pass)
	if s.count() >= want || c.inBackoff(ctx, s.id(), gpu) {
		return false
	}
	for s.count() < want && s.started < maxStartsPerTick {
		pool, ok := inv.place(gpu, s.gpuCount(gpu))
		if !ok {
			return true
		}
		protect := c.protectNext(s, pass, protected)
		if _, err := c.startReplica(ctx, s.endpoint, gpu, pool, protect); err != nil {
			log.Warn().Err(err).Str("endpoint_id", s.id()).Str("gpu", gpu).Msg("managed endpoints: start replica failed")
			_ = c.s.repo.SetScheduleBackoff(ctx, s.id(), gpu, c.s.config.Reconcile.FailureBackoff)
			return false
		}
		s.started++
		s.requested()
		if protect {
			protected[s.id()]++
		}
	}
	return false
}

func (c *controller) inBackoff(ctx context.Context, endpointID, gpu string) bool {
	backoff, err := c.s.repo.InScheduleBackoff(ctx, endpointID, gpu)
	return err != nil || backoff
}

// protectNext decides whether the slot's next replica is non-evictable: every
// hot copy when cluster preemption is off, otherwise only the copies covering
// a preemption:false minimum. An old version keeps its protected slot while a
// replacement loads; protect() transfers it once the new copy is ready.
func (c *controller) protectNext(s *slot, pass int, protected map[string]uint32) bool {
	if s.entry.Serverless {
		return false
	}
	if !c.s.config.Preemption.Enabled {
		return true
	}
	return pass == passMinimum && s.entry.ProtectMinimum && protected[s.id()] < s.entry.MinReplicas
}

// Reclaim ---------------------------------------------------------------------

type victim struct {
	replica  *types.EndpointReplica
	owner    *slot
	released replicaResources
}

// reclaim drains one replica so slots[index] can start. The victims on a
// single worker must free room for the whole request, or nothing is drained.
func (c *controller) reclaim(ctx context.Context, gpu string, index int, slots []*slot, live []*types.EndpointReplica, inv *clusterInventory, pass int) bool {
	if inv == nil || c.s.containers == nil || gpu == types.CPUInventoryKey {
		return false
	}
	s := slots[index]
	res := c.resources(ctx, s)
	if res == nil {
		return false // never destroy a replica for an unknown request shape
	}
	if releasePending(live, s) {
		return false // wait for the release already under way
	}
	if s.entry.Serverless && c.idleAlternative(ctx, gpu, s, live, inv) {
		return false // idle capacity of another configured GPU type comes first
	}
	need := s.gpuCount(gpu)
	byWorker := c.victims(ctx, gpu, index, slots, live, inv, pass, need)
	for _, id := range slices.Sorted(maps.Keys(byWorker)) {
		if !inv.roomAfter(gpu, inv.workers[id], byWorker[id], need, *res) {
			continue
		}
		first := byWorker[id][0]
		reason := reasonReclaimedBy + s.id()
		return c.drainReplica(ctx, first.replica, first.owner.endpoint.Spec.DrainSeconds, false, reason) == nil
	}
	return false
}

// resources is the request shape a replica of the slot reserves, or nil when unknown.
func (c *controller) resources(ctx context.Context, s *slot) *replicaResources {
	if s.resRead {
		return s.res
	}
	s.resRead = true
	if c.s.backend == nil {
		return nil
	}
	stub, err := c.s.backend.GetStubByExternalId(ctx, s.endpoint.StubID)
	if err != nil || stub == nil || stub.ExternalId != s.endpoint.StubID {
		return nil
	}
	config, err := stub.UnmarshalConfig()
	if err != nil || config == nil {
		return nil
	}
	if res, ok := reservedReplicaResources(config.Runtime.Cpu, config.Runtime.Memory); ok {
		s.res = &res
	}
	return s.res
}

func settling(r *types.EndpointReplica) bool {
	return r.Status == types.ReplicaStatusDraining || r.Status == types.ReplicaStatusEvicting
}

// releasePending reports whether a replica is already draining for this slot.
func releasePending(live []*types.EndpointReplica, s *slot) bool {
	for _, r := range live {
		if settling(r) && r.StatusReason == reasonReclaimedBy+s.id() {
			return true
		}
	}
	return false
}

// idleAlternative reports whether another GPU type configured for the
// serverless slot has idle room, is under its cap and is not backing off.
func (c *controller) idleAlternative(ctx context.Context, gpu string, s *slot, live []*types.EndpointReplica, inv *clusterInventory) bool {
	for other, placement := range s.demand.gpus {
		if other == gpu {
			continue
		}
		if placement.MaxReplicas > 0 && aliveOn(live, s.id(), other) >= placement.MaxReplicas {
			continue
		}
		if !inv.canPlace(other, s.gpuCount(other)) {
			continue
		}
		if !c.inBackoff(ctx, s.id(), other) {
			return true
		}
	}
	return false
}

func aliveOn(live []*types.EndpointReplica, endpointID, gpu string) uint32 {
	var n uint32
	for _, r := range live {
		if r.EndpointID == endpointID && r.GPU == gpu && r.Alive() {
			n++
		}
	}
	return n
}

// victims lists the replicas the slot at index may take, grouped by worker,
// in the order they would be drained: least priority owner first, then the
// owner's scale-down order.
func (c *controller) victims(ctx context.Context, gpu string, index int, slots []*slot, live []*types.EndpointReplica, inv *clusterInventory, pass int, need uint32) map[string][]victim {
	busy := map[string]bool{} // workers with a drain in flight; their GPUs are already spoken for
	for _, r := range live {
		if settling(r) {
			busy[r.WorkerID] = true
		}
	}
	byWorker := map[string][]victim{}
	for i := len(slots) - 1; i >= 0; i-- {
		if !donates(slots, i, index, pass) {
			continue
		}
		owner := slots[i]
		candidates := slices.Clone(owner.replicas)
		scaleDownOrder(candidates)
		for _, r := range candidates {
			w := inv.workers[r.WorkerID]
			if busy[r.WorkerID] || w == nil || w.total < need {
				continue
			}
			state, ok := c.evictable(r, w, gpu)
			if !ok {
				continue
			}
			released, ok := reservedReplicaResources(state.Cpu, state.Memory)
			if !ok {
				continue
			}
			byWorker[r.WorkerID] = append(byWorker[r.WorkerID], victim{r, owner, released})
		}
	}
	return byWorker
}

// donates reports whether slots[owner] may give a replica to slots[index] in
// this pass. Only surplus is ever taken. A minimum may take from any slot; a
// requested serverless copy takes hot surplus only; hot surplus takes only
// lower-priority hot surplus, never a serverless copy (idle ones drain on
// their own).
func donates(slots []*slot, owner, index, pass int) bool {
	if owner == index || slots[owner].surplus() == 0 {
		return false
	}
	if slots[owner].entry.Serverless && pass != passMinimum {
		return false
	}
	if pass == passSurplus && owner < index {
		return false
	}
	return true
}

// evictable returns the running, evictable container behind a replica on the
// given worker, if draining it would free that worker's GPUs.
func (c *controller) evictable(r *types.EndpointReplica, w *inventoryWorker, gpu string) (*types.ContainerState, bool) {
	if !r.Alive() || r.Protected {
		return nil, false
	}
	if w.gpu != gpu || w.pool != r.PoolName {
		return nil, false
	}
	state, err := c.s.containers.GetContainerState(r.ContainerID)
	if err != nil || state == nil {
		return nil, false
	}
	if state.WorkerId != r.WorkerID || state.Status != types.ContainerStatusRunning {
		return nil, false
	}
	if !state.Evictable || state.Evicting {
		return nil, false
	}
	return state, true
}

// roomAfter reports whether draining the victims, in order and within each
// owner's surplus, leaves the worker with room for the request and the pool
// above its floor.
func (inv *clusterInventory) roomAfter(gpu string, w *inventoryWorker, victims []victim, need uint32, res replicaResources) bool {
	free := w.free
	pool := inv.poolFree(gpu, w.pool)
	cpu, memory := w.cpu, w.memory
	taken := map[*slot]uint32{}
	for _, v := range victims {
		if taken[v.owner] >= v.owner.surplus() {
			continue
		}
		taken[v.owner]++
		gpus := max(v.replica.GPUCount, 1)
		free += gpus
		pool += gpus
		cpu += v.released.cpu
		memory += v.released.memory
		fits := free >= need && pool >= inv.floors[w.pool]+need && cpu >= res.cpu && memory >= res.memory
		if fits {
			return true
		}
	}
	return false
}

// Retire ----------------------------------------------------------------------

// rollout is what one endpoint's replicas look like on one GPU type.
type rollout struct {
	serving         uint32 // any version
	currentServing  uint32
	currentStarting bool
	settling        bool // a drain is in flight
}

// covered: a current copy serves, and the GPU keeps its minimum without one more stale copy.
func (g *rollout) covered(minimum uint32) bool {
	return g.currentServing > 0 && g.serving-1 >= max(minimum, 1)
}

// retire drains what config.yaml no longer wants: every replica of a disabled
// endpoint or an unlisted GPU type, and stale versions once the GPU is
// covered. One serving replica per endpoint per tick keeps rollouts rolling.
func (c *controller) retire(ctx context.Context, endpoint *types.ManagedEndpoint, fleet *types.Fleet, live []*types.EndpointReplica, blocked map[group]bool) {
	placements := fleet.Placements(endpoint.Spec.ID)
	if !endpoint.Enabled() {
		placements = nil
	}
	rollouts := map[string]*rollout{}
	var stale []*types.EndpointReplica
	for _, r := range live {
		if r.EndpointID != endpoint.Spec.ID || blocked[group{r.EndpointID, r.GPU}] {
			continue
		}
		g := rollouts[r.GPU]
		if g == nil {
			g = &rollout{}
			rollouts[r.GPU] = g
		}
		_, placed := placements[r.GPU]
		current := placed && r.Version == endpoint.Version
		switch {
		case settling(r):
			g.settling = true
		case !r.Alive():
		case current && r.Serving():
			g.serving++
			g.currentServing++
		case current:
			g.currentStarting = true
		default:
			if r.Serving() {
				g.serving++
			}
			stale = append(stale, r)
		}
	}
	scaleDownOrder(stale)
	for _, r := range stale {
		reason, now := retirement(endpoint, placements, rollouts, r)
		if !now {
			continue
		}
		serving := r.Serving()
		if err := c.drainReplica(ctx, r, endpoint.Spec.DrainSeconds, false, reason); err == nil && serving && endpoint.Enabled() {
			return
		}
	}
}

// retirement says why a stale replica may go now, or that it must wait.
func retirement(endpoint *types.ManagedEndpoint, placements map[string]types.FleetPlacement, rollouts map[string]*rollout, r *types.EndpointReplica) (reason string, now bool) {
	placement, placed := placements[r.GPU]
	g := rollouts[r.GPU]
	switch {
	case !endpoint.Enabled():
		return reasonRetired, true
	case !placed:
		// A GPU move is a replacement: the old type serves until a new one does.
		if r.Serving() && len(placements) > 0 && !anyCurrentServing(rollouts) {
			return "", false
		}
		return reasonUnlisted, true
	case !r.Serving() || g.covered(placement.MinReplicas):
		return fmt.Sprintf("version %d retired", r.Version), true
	case endpoint.Spec.Rollout == rolloutReplace && !g.currentStarting && !g.settling:
		// The app accepts downtime to free the GPU for its replacement.
		return fmt.Sprintf("version %d retired (making room for version %d)", r.Version, endpoint.Version), true
	}
	return "", false
}

func anyCurrentServing(rollouts map[string]*rollout) bool {
	for _, g := range rollouts {
		if g.currentServing > 0 {
			return true
		}
	}
	return false
}
