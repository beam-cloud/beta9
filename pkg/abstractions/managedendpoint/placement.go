package managedendpoint

import (
	"math"
	"sort"

	"github.com/beam-cloud/beta9/pkg/types"
)

// Placement is a pure model of how much of the cluster's GPU inventory
// managed endpoints may occupy and how that allowance is divided between
// endpoints. It is deliberately free of I/O so it can be tested directly.

// workerSlot is one worker's inventory of a single GPU type.
type workerSlot struct {
	WorkerID string
	PoolName string
	Locality string
	// Total is the worker's GPU count; Free is what the scheduler reports as
	// unallocated right now; Held is what managed endpoint replicas occupy.
	Total uint32
	Free  uint32
	Held  uint32
	// MaxShare is the pool's cap on the fraction of its GPUs endpoints may
	// hold; zero means "use the cluster default".
	MaxShare float64
}

// gpuInventory is all eligible workers carrying one GPU type.
type gpuInventory struct {
	GPU     string
	Workers []workerSlot
}

// capacity is the number of GPUs endpoints could occupy if nothing else
// wanted them: free GPUs plus those endpoints already hold. Serverless
// workloads' GPUs never count.
func (g gpuInventory) capacity() uint32 {
	var total uint32
	for _, w := range g.Workers {
		total += w.Free + w.Held
	}
	return total
}

// allowance is the number of GPUs endpoints may hold after applying the
// cluster and per-pool share caps.
func (g gpuInventory) allowance(clusterShare float64) uint32 {
	var allowed float64
	for _, w := range g.Workers {
		share := clusterShare
		if w.MaxShare > 0 && w.MaxShare < share {
			share = w.MaxShare
		}
		allowed += float64(w.Free+w.Held) * share
	}
	return uint32(math.Floor(allowed + 1e-9))
}

// free is the number of GPUs available right now.
func (g gpuInventory) free() uint32 {
	var total uint32
	for _, w := range g.Workers {
		total += w.Free
	}
	return total
}

// pickWorker returns the index of the worker with the most free GPUs that can
// fit a request for count GPUs and passes accept (nil accepts all). Preferring
// the least-loaded worker spreads replicas across the fleet.
func (g gpuInventory) pickWorker(count uint32, accept func(workerSlot) bool) (int, bool) {
	if count == 0 {
		count = 1
	}
	best := -1
	for i, w := range g.Workers {
		if w.Free < count || (accept != nil && !accept(w)) {
			continue
		}
		if best < 0 || w.Free > g.Workers[best].Free || (w.Free == g.Workers[best].Free && w.Held < g.Workers[best].Held) {
			best = i
		}
	}
	return best, best >= 0
}

// reserve records that count GPUs on worker i are now held by a replica so
// later picks in the same tick see the reduced free capacity.
func (g *gpuInventory) reserve(i int, count uint32) {
	if i < 0 || i >= len(g.Workers) {
		return
	}
	if count == 0 {
		count = 1
	}
	w := &g.Workers[i]
	if w.Free >= count {
		w.Free -= count
	} else {
		w.Free = 0
	}
	w.Held += count
}

// fillTarget is one (endpoint, role, gpu target) unit of placement.
type fillTarget struct {
	EndpointID string
	Role       string
	GPU        string // target key, e.g. H100x2
	Type       string // normalized GPU type, e.g. H100
	Count      uint32
	Share      float64
	Min        uint32
	Max        uint32 // 0 = unbounded
	// Demand is the replica count the router asks for (queue pressure); it
	// may exceed the fair-share quota when spare capacity exists.
	Demand uint32
}

func (t fillTarget) key() string { return t.EndpointID + "|" + targetKey(t.Role, t.GPU) }

// fillPlan is the placement decision for one fillTarget.
type fillPlan struct {
	// Quota is the fair-share allocation in replicas.
	Quota uint32
	// Desired is what the controller converges toward this tick.
	Desired uint32
}

// planFill divides each GPU type's allowance between the targets that want it
// in proportion to their declared shares, then clamps by min/max replicas.
// Shares of targets on the same GPU type are normalized when they sum to more
// than one, so over-subscribed specs degrade gracefully instead of exceeding
// the cluster cap.
func planFill(inventory map[string]gpuInventory, targets []fillTarget, clusterShare float64) map[string]fillPlan {
	plans := make(map[string]fillPlan, len(targets))

	byType := map[string][]int{}
	for i, t := range targets {
		byType[t.Type] = append(byType[t.Type], i)
	}

	for gpuType, idxs := range byType {
		var allowance uint32
		if inv, ok := inventory[gpuType]; ok {
			allowance = inv.allowance(clusterShare)
		}

		var shareSum float64
		for _, i := range idxs {
			shareSum += math.Max(targets[i].Share, 0)
		}
		norm := 1.0
		if shareSum > 1 {
			norm = 1 / shareSum
		}

		for _, i := range idxs {
			t := targets[i]
			count := t.Count
			if count == 0 {
				count = 1
			}
			var quota uint32
			if t.Share > 0 && allowance > 0 {
				quota = uint32(math.Floor(float64(allowance)*t.Share*norm/float64(count) + 1e-9))
			}
			desired := quota
			if t.Demand > desired {
				desired = t.Demand
			}
			if desired < t.Min {
				desired = t.Min
			}
			if t.Max > 0 && desired > t.Max {
				desired = t.Max
			}
			plans[t.key()] = fillPlan{Quota: quota, Desired: desired}
		}
	}
	return plans
}

// replicaSet is the live replicas for one fillTarget, partitioned for
// scale decisions.
type replicaSet struct {
	Live      []*types.EndpointReplica // scheduling|loading|ready, serving-eligible
	Ready     []*types.EndpointReplica // subset of Live that is ready
	Protected uint32
}

func partitionReplicas(replicas []*types.EndpointReplica, endpointID, role, gpu string, version uint) replicaSet {
	var set replicaSet
	for _, r := range replicas {
		if r.EndpointID != endpointID || r.Role != role || r.GPU != gpu || r.Version != version {
			continue
		}
		if r.Status.Terminal() || r.Status == types.ReplicaStatusDraining || r.Status == types.ReplicaStatusEvicting {
			continue
		}
		if r.Tuning || r.Candidate {
			continue
		}
		set.Live = append(set.Live, r)
		if r.Status == types.ReplicaStatusReady {
			set.Ready = append(set.Ready, r)
		}
		if r.Protected {
			set.Protected++
		}
	}
	return set
}

// scaleDownCandidates orders live replicas so the least valuable are drained
// first: unprotected before protected, then not-yet-ready before ready, then
// the least loaded, then the newest.
func scaleDownCandidates(set replicaSet) []*types.EndpointReplica {
	out := append([]*types.EndpointReplica(nil), set.Live...)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Protected != b.Protected {
			return !a.Protected
		}
		aReady, bReady := a.Status == types.ReplicaStatusReady, b.Status == types.ReplicaStatusReady
		if aReady != bReady {
			return !aReady
		}
		if a.Capacity.InFlight != b.Capacity.InFlight {
			return a.Capacity.InFlight < b.Capacity.InFlight
		}
		return a.StartedAt.After(b.StartedAt)
	})
	return out
}
