package managedendpoint

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// Rollouts: a new version deployed from git becomes a canary. The controller
// keeps canary replicas up, bakes them under real traffic and promotes or
// rolls back on route metrics. Admins can decide early, pin a version, or
// re-activate a retired one.

// canaryStartGrace bounds how long a canary may sit without a ready replica
// before the version is rolled back.
const canaryStartGrace = 30 * time.Minute

func findVersion(versions []*types.EndpointVersion, version uint) *types.EndpointVersion {
	if i := slices.IndexFunc(versions, func(v *types.EndpointVersion) bool { return v.Version == version }); i >= 0 {
		return versions[i]
	}
	return nil
}

func readyCount(replicas []*types.EndpointReplica) int {
	n := 0
	for _, r := range replicas {
		if r.Status == types.ReplicaStatusReady {
			n++
		}
	}
	return n
}

// stepRollout advances a baking canary: it keeps canary replicas up, starts
// the bake clock when the first one is ready, and after the bake window
// promotes or rolls back based on route metrics. Admin pins suspend automatic
// promotion.
func (c *controller) stepRollout(ctx context.Context, endpoint *types.ManagedEndpoint, rollout *types.RolloutState, live []*types.EndpointReplica) error {
	if rollout.CanaryVersion == 0 || rollout.Phase != types.RolloutPhaseBaking {
		return nil
	}
	id := endpoint.Spec.ID
	versions, err := c.s.repo.ListVersions(ctx, id)
	if err != nil {
		return err
	}
	canary := findVersion(versions, rollout.CanaryVersion)
	if canary == nil {
		return c.finishRollout(ctx, endpoint, rollout, false, "canary version record missing")
	}
	canarySpec, err := c.endpointSpecFromStub(ctx, canary.StubID)
	if err != nil {
		return c.finishRollout(ctx, endpoint, rollout, false, err.Error())
	}
	canaryEndpoint := &types.ManagedEndpoint{Spec: *canarySpec, StubID: canary.StubID, Version: canary.Version, GitSHA: canary.GitSHA}

	// Keep canary replicas up: one per target, bounded by the configured total.
	var canaryReplicas []*types.EndpointReplica
	for _, r := range live {
		if r.EndpointID == id && r.Version == rollout.CanaryVersion && !r.Tuning {
			canaryReplicas = append(canaryReplicas, r)
		}
	}
	wanted := c.s.config.Rollout.CanaryReplicas
	if services, missing := serviceAddresses(canarySpec.Services, live); uint32(len(canaryReplicas)) < wanted && len(missing) == 0 {
		for _, rt := range canarySpec.Targets() {
			if uint32(len(canaryReplicas)) >= wanted {
				break
			}
			if len(partitionReplicas(canaryReplicas, id, rt.Role, rt.Target.Key(), rollout.CanaryVersion).Live) > 0 {
				continue
			}
			spec := c.endpointStartSpec(canaryEndpoint, rt, services)
			spec.Protected, spec.Evictable = true, false
			replica, err := c.startReplica(ctx, spec)
			if err != nil {
				log.Warn().Err(err).Str("endpoint_id", id).Uint("version", rollout.CanaryVersion).Msg("managed endpoints: start canary failed")
				break
			}
			canaryReplicas = append(canaryReplicas, replica)
		}
	}
	// Canary replicas follow their own version's fleet config.
	if err := c.ensureFleetRevisions(ctx, canaryEndpoint); err != nil {
		log.Warn().Err(err).Str("endpoint_id", id).Msg("managed endpoints: canary config revisions")
	}

	now := time.Now()
	if rollout.BakeStartedAt.IsZero() {
		if readyCount(canaryReplicas) > 0 {
			rollout.BakeStartedAt, rollout.LastDecision, rollout.LastDecisionAt = now, "canary ready; baking", now
			return c.s.repo.SaveRollout(ctx, rollout)
		}
		if now.Sub(canary.CreatedAt) > canaryStartGrace {
			return c.finishRollout(ctx, endpoint, rollout, false, "canary never became ready")
		}
		return nil
	}
	window := time.Duration(c.s.config.Rollout.BakeSeconds) * time.Second
	if now.Sub(rollout.BakeStartedAt) < window || (rollout.PinnedVersion != 0 && rollout.PinnedVersion != rollout.CanaryVersion) {
		return nil
	}

	activeMetrics, err := c.s.repo.GetRouteMetrics(ctx, id, "", rollout.ActiveVersion, window)
	if err != nil {
		return err
	}
	canaryMetrics, err := c.s.repo.GetRouteMetrics(ctx, id, "", rollout.CanaryVersion, window)
	if err != nil {
		return err
	}
	var activeReplicas []*types.EndpointReplica
	for _, r := range live {
		if r.EndpointID == id && r.Version == rollout.ActiveVersion && r.Status == types.ReplicaStatusReady {
			activeReplicas = append(activeReplicas, r)
		}
	}
	promote, reason := evaluateRollout(activeMetrics, canaryMetrics, activeReplicas, canaryReplicas, c.s.config.Rollout.Thresholds)
	return c.finishRollout(ctx, endpoint, rollout, promote, reason)
}

// finishRollout decides a baking canary: promote makes it the active
// version; otherwise the canary is rolled back and its replicas drained.
func (c *controller) finishRollout(ctx context.Context, endpoint *types.ManagedEndpoint, rollout *types.RolloutState, promote bool, reason string) error {
	canaryVersion := rollout.CanaryVersion
	versions, err := c.s.repo.ListVersions(ctx, endpoint.Spec.ID)
	if err != nil {
		return err
	}
	canary := findVersion(versions, canaryVersion)
	if promote && canary != nil {
		return c.switchVersion(ctx, endpoint, rollout, versions, canary, "rollout.promoted", "promoted: "+reason)
	}
	if canary != nil {
		canary.State = types.VersionStateRolledBack
		if err := c.s.repo.SaveVersion(ctx, canary); err != nil {
			return err
		}
	}
	if replicas, err := c.s.repo.ListReplicas(ctx, endpoint.Spec.ID); err == nil {
		for _, r := range replicas {
			if r.Version == canaryVersion && !r.Status.Terminal() {
				_ = c.drainReplica(ctx, r, endpoint.Spec.Policy.DrainSeconds, false, "canary rolled back")
			}
		}
	}
	rollout.CanaryVersion, rollout.BakeStartedAt, rollout.Phase = 0, time.Time{}, types.RolloutPhaseRolledBack
	rollout.LastDecision, rollout.LastDecisionAt = "rolled back: "+reason, time.Now()
	if err := c.s.repo.SaveRollout(ctx, rollout); err != nil {
		return err
	}
	c.s.emit(types.EventEndpointRollout, types.EventEndpointSchema{
		EndpointID: endpoint.Spec.ID, Action: "rollout.rolled_back", Version: canaryVersion, Message: reason,
		Data: map[string]any{"previous_version": rollout.ActiveVersion, "git_sha": endpoint.GitSHA},
	})
	log.Info().Str("endpoint_id", endpoint.Spec.ID).Uint("version", canaryVersion).Str("reason", reason).Msg("managed endpoints: canary rolled back")
	return nil
}

// activateVersion makes an already-deployed version the active one without a
// bake (manual promote of a retired version, or rollback). The controller's
// version retirement then replaces replicas gradually.
func (c *controller) activateVersion(ctx context.Context, endpoint *types.ManagedEndpoint, rollout *types.RolloutState, versions []*types.EndpointVersion, version uint, reason string) error {
	target := findVersion(versions, version)
	if target == nil {
		return fmt.Errorf("version %d not found", version)
	}
	return c.switchVersion(ctx, endpoint, rollout, versions, target, "rollout.activated", reason)
}

// switchVersion swaps the endpoint onto target's spec/stub, marks target
// active and the previously active version retired, and clears any canary.
func (c *controller) switchVersion(ctx context.Context, endpoint *types.ManagedEndpoint, rollout *types.RolloutState, versions []*types.EndpointVersion, target *types.EndpointVersion, action, decision string) error {
	spec, err := c.endpointSpecFromStub(ctx, target.StubID)
	if err != nil {
		return err
	}
	now := time.Now()
	previous := endpoint.Version
	endpoint.Spec, endpoint.StubID, endpoint.Version, endpoint.GitSHA = *spec, target.StubID, target.Version, target.GitSHA
	endpoint.Status, endpoint.UpdatedAt = types.EndpointStatusActive, now
	if err := c.s.repo.SaveEndpoint(ctx, endpoint); err != nil {
		return err
	}
	for _, v := range versions {
		switch {
		case v.Version == target.Version:
			v.State = types.VersionStateActive
		case v.State == types.VersionStateActive:
			v.State = types.VersionStateRetired
		case rollout.CanaryVersion != 0 && v.Version == rollout.CanaryVersion:
			v.State = types.VersionStateRolledBack
		default:
			continue
		}
		if err := c.s.repo.SaveVersion(ctx, v); err != nil {
			return err
		}
	}
	rollout.ActiveVersion, rollout.CanaryVersion, rollout.BakeStartedAt, rollout.Phase = target.Version, 0, time.Time{}, types.RolloutPhaseIdle
	rollout.LastDecision, rollout.LastDecisionAt = decision, now
	if err := c.s.repo.SaveRollout(ctx, rollout); err != nil {
		return err
	}
	c.s.emit(types.EventEndpointRollout, types.EventEndpointSchema{
		EndpointID: endpoint.Spec.ID, Action: action, Version: target.Version, Message: decision,
		Data: map[string]any{"previous_version": previous, "git_sha": endpoint.GitSHA},
	})
	log.Info().Str("endpoint_id", endpoint.Spec.ID).Uint("version", target.Version).Str("reason", decision).Msg("managed endpoints: version activated")
	return nil
}

// evaluateRollout decides whether a baked canary is at least as good as the
// active version. It is conservative: with no traffic on either side a
// healthy canary is promoted; with traffic, any threshold regression rolls
// back.
func evaluateRollout(active, canary *types.RouteMetrics, activeReplicas, canaryReplicas []*types.EndpointReplica, th types.RolloutThresholds) (bool, string) {
	if readyCount(canaryReplicas) == 0 {
		return false, "no ready canary replica at end of bake"
	}
	if canary == nil || canary.Requests == 0 {
		return true, "canary healthy with no traffic during bake"
	}
	if canary.Requests >= 20 && canary.ErrorRate() > th.ErrorRate {
		return false, fmt.Sprintf("canary error rate %.2f%% exceeds %.2f%%", canary.ErrorRate()*100, th.ErrorRate*100)
	}
	if active == nil || active.Requests == 0 {
		return true, "canary healthy; active version had no traffic to compare"
	}
	if a, b := float64(active.MeanTTFTMs()), float64(canary.MeanTTFTMs()); a > 0 && b > a*(1+th.TTFT) {
		return false, fmt.Sprintf("canary TTFT %.0fms regressed vs %.0fms", b, a)
	}
	aTPOT, aTPS := meanEngine(activeReplicas)
	bTPOT, bTPS := meanEngine(canaryReplicas)
	if aTPOT > 0 && bTPOT > aTPOT*(1+th.TPOT) {
		return false, fmt.Sprintf("canary TPOT %.1fms regressed vs %.1fms", bTPOT, aTPOT)
	}
	if aTPS > 0 && bTPS > 0 && bTPS < aTPS*(1-th.Throughput) {
		return false, fmt.Sprintf("canary decode throughput %.0f tok/s regressed vs %.0f", bTPS, aTPS)
	}
	return true, fmt.Sprintf("canary within thresholds over %d requests", canary.Requests)
}

// meanEngine averages engine-reported TPOT and decode throughput over the
// ready replicas.
func meanEngine(replicas []*types.EndpointReplica) (tpotMs float64, tokensPerSec float64) {
	var n, tpot, tps float64
	for _, r := range replicas {
		if r.Status == types.ReplicaStatusReady {
			n++
			tpot += float64(r.Capacity.TPOTMs)
			tps += float64(r.Capacity.DecodeTokensPerSec)
		}
	}
	if n == 0 {
		return 0, 0
	}
	return tpot / n, tps / n
}
