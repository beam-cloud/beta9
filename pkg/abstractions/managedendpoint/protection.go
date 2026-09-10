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

// protectedReplicas selects the copies that cover each configured minimum.
// Keep a ready old version protected until its replacement is ready; prefer
// current versions once ready, then retain existing roles to avoid churn.
func protectedReplicas(fleet *types.Fleet, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica, preemptionEnabled bool) map[string]bool {
	desired := make(map[string]bool)
	if !preemptionEnabled {
		// The cluster override also covers copies awaiting retirement after a
		// config change. Intentional retirement does not require demoting them.
		for _, replica := range live {
			if replica.Alive() && !fleet.Placements(replica.EndpointID)[replica.GPU].Serverless {
				desired[replica.ID] = true
			}
		}
		return desired
	}
	for id, endpoint := range endpoints {
		if !endpoint.Enabled() {
			continue
		}
		for gpu, placement := range fleet.Placements(id) {
			count := placement.MinReplicas
			if placement.Serverless || placement.Preemption == nil || *placement.Preemption {
				count = 0
			}
			if count == 0 {
				continue
			}
			var candidates []*types.EndpointReplica
			for _, replica := range live {
				if replica.EndpointID == id && replica.GPU == gpu && replica.Alive() {
					candidates = append(candidates, replica)
				}
			}
			slices.SortFunc(candidates, func(a, b *types.EndpointReplica) int {
				rank := func(r *types.EndpointReplica) int {
					n := 0
					if r.Serving() {
						n += 8
						if r.Version == endpoint.Version {
							n += 4
						}
					}
					if r.Version == endpoint.Version {
						n++
					}
					if r.Protected {
						n += 2
					}
					return n
				}
				return cmp.Or(cmp.Compare(rank(b), rank(a)), a.StartedAt.Compare(b.StartedAt), strings.Compare(a.ID, b.ID))
			})
			for _, replica := range candidates[:min(int(count), len(candidates))] {
				desired[replica.ID] = true
			}
		}
	}
	return desired
}

type protectionGroup struct{ endpointID, gpu string }

// Promote before demoting so policy changes and rollouts keep the serving
// minimum reserved. Repository changes fence concurrent scheduler eviction
// and update worker capacity without restarting the engine.
// A failed group keeps its existing protected roles and waits for observation
// to refresh its assignment. Other endpoint/GPU groups can still reconcile.
func (c *controller) reconcileProtection(ctx context.Context, fleet *types.Fleet, endpoints map[string]*types.ManagedEndpoint, live []*types.EndpointReplica) (map[protectionGroup]bool, error) {
	desired := protectedReplicas(fleet, endpoints, live, c.s.config.Preemption.Enabled)
	blocked := make(map[protectionGroup]bool)
	var errs []error
	for _, protect := range []bool{true, false} {
		for _, replica := range live {
			group := protectionGroup{replica.EndpointID, replica.GPU}
			if blocked[group] || !replica.Alive() || desired[replica.ID] != protect || replica.Protected == protect {
				continue
			}
			updated, err := c.s.repo.SetReplicaProtection(ctx, replica.ID, protect)
			if err != nil {
				blocked[group] = true
				errs = append(errs, fmt.Errorf("replica %s: set protection: %w", replica.ID, err))
				continue
			}
			*replica = *updated
		}
	}
	return blocked, errors.Join(errs...)
}
