package managedendpoint

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

// protectedReplicas selects the copies that cover each preemption:false
// minimum: serving current first, then serving old (so a rollout keeps its
// minimum until the replacement is ready), then existing roles to avoid churn.
// With cluster preemption off, every hot replica is protected.
func protectedReplicas(fleet *types.Fleet, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica, preemptionEnabled bool) map[string]bool {
	desired := map[string]bool{}
	rank := func(endpoint *types.ManagedEndpoint, r *types.EndpointReplica) int {
		n := 0
		if r.Serving() {
			n += 8
		}
		if r.Version == endpoint.Version {
			n += 1
			if r.Serving() {
				n += 4
			}
		}
		if r.Protected {
			n += 2
		}
		return n
	}
	if !preemptionEnabled {
		for _, r := range live {
			if r.Alive() && !fleet.Placements(r.EndpointID)[r.GPU].Serverless {
				desired[r.ID] = true
			}
		}
		return desired
	}
	for id, endpoint := range endpoints {
		if !endpoint.Enabled() {
			continue
		}
		for gpu, placement := range fleet.Placements(id) {
			if !placement.ProtectsMinimum() || placement.MinReplicas == 0 {
				continue
			}
			var candidates []*types.EndpointReplica
			for _, r := range live {
				if r.EndpointID == id && r.GPU == gpu && r.Alive() {
					candidates = append(candidates, r)
				}
			}
			slices.SortFunc(candidates, func(a, b *types.EndpointReplica) int {
				return cmp.Or(cmp.Compare(rank(endpoint, b), rank(endpoint, a)), a.StartedAt.Compare(b.StartedAt), strings.Compare(a.ID, b.ID))
			})
			for _, r := range candidates[:min(int(placement.MinReplicas), len(candidates))] {
				desired[r.ID] = true
			}
		}
	}
	return desired
}

// protect applies protectedReplicas: promote before demoting so a policy
// change or rollout never drops below the serving minimum. The repository
// change fences scheduler eviction without restarting the engine. A failed
// group keeps its roles and is skipped by fill and retire this tick.
func (c *controller) protect(ctx context.Context, fleet *types.Fleet, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica) (map[group]bool, error) {
	desired := protectedReplicas(fleet, endpoints, live, c.s.config.Preemption.Enabled)
	blocked := map[group]bool{}
	var errs []error
	for _, want := range []bool{true, false} {
		for _, r := range live {
			g := group{r.EndpointID, r.GPU}
			if blocked[g] || !r.Alive() {
				continue
			}
			if desired[r.ID] != want || r.Protected == want {
				continue // not this phase, or already right
			}
			updated, err := c.s.repo.SetReplicaProtection(ctx, r.ID, want)
			if err != nil {
				blocked[g] = true
				errs = append(errs, fmt.Errorf("replica %s: set protection: %w", r.ID, err))
				continue
			}
			*r = *updated
		}
	}
	return blocked, errors.Join(errs...)
}
