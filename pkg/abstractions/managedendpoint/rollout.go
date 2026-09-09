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

// requiredRoles lists the roles a version must run for the endpoint to
// serve: "serve" for a monolithic spec, prefill and decode when
// disaggregated. Order follows Targets().
func requiredRoles(spec *types.ManagedEndpointSpec) []string {
	var roles []string
	for _, rt := range spec.Targets() {
		if !slices.Contains(roles, rt.Role) {
			roles = append(roles, rt.Role)
		}
	}
	return roles
}

// roleReplicas filters replicas to one role, alive ones only.
func roleReplicas(replicas []*types.EndpointReplica, role string) []*types.EndpointReplica {
	var out []*types.EndpointReplica
	for _, r := range replicas {
		if r.Role == role && r.Alive() {
			out = append(out, r)
		}
	}
	return out
}

// missingReadyRole returns the first required role with no ready replica.
func missingReadyRole(replicas []*types.EndpointReplica, roles []string) (string, bool) {
	for _, role := range roles {
		if readyCount(roleReplicas(replicas, role)) == 0 {
			return role, true
		}
	}
	return "", false
}

// growCanaryRole starts canary replicas for one role until wanted are alive
// and returns the ones it started. Targets with the fewest canaries are
// tried first; a target with no eligible pool is skipped, and the loop ends
// once every target has been skipped in a row or a start fails.
func (c *controller) growCanaryRole(ctx context.Context, canary *types.ManagedEndpoint, rollout *types.RolloutState, role string, replicas []*types.EndpointReplica, wanted uint32, inv *clusterInventory, services map[string]string) (started []*types.EndpointReplica) {
	var targets []types.RoleTarget
	for _, rt := range canary.Spec.Targets() {
		if rt.Role == role {
			targets = append(targets, rt)
		}
	}
	perTarget := map[string]int{}
	for _, r := range replicas {
		perTarget[r.GPU]++
	}
	slices.SortStableFunc(targets, func(a, b types.RoleTarget) int { return perTarget[a.Target.Key()] - perTarget[b.Target.Key()] })

	have, skipped := uint32(len(replicas)), 0
	for i := 0; have < wanted && len(targets) > 0 && skipped < len(targets); i++ {
		rt := targets[i%len(targets)]
		spec := c.endpointStartSpec(canary, rt, services)
		spec.Protected, spec.Evictable = true, false
		var ok bool
		if spec.PoolName, spec.Locality, ok = c.place(inv, rt.Target, canary.Spec.Locality, true); !ok {
			skipped++
			log.Debug().Str("endpoint_id", canary.Spec.ID).Uint("version", rollout.CanaryVersion).Str("target", rt.Key()).
				Msg("managed endpoints: no eligible pool for canary replica")
			continue
		}
		replica, err := c.startReplica(ctx, spec)
		if err != nil {
			log.Warn().Err(err).Str("endpoint_id", canary.Spec.ID).Uint("version", rollout.CanaryVersion).Msg("managed endpoints: start canary failed")
			break
		}
		started = append(started, replica)
		have, skipped = have+1, 0
	}
	return started
}

// stepRollout advances a baking canary: it keeps canary replicas up for
// every required role, starts the bake clock once each role has a ready
// replica, and after the bake window promotes or rolls back based on route
// metrics. Admin pins suspend automatic promotion.
func (c *controller) stepRollout(ctx context.Context, endpoint *types.ManagedEndpoint, rollout *types.RolloutState, live []*types.EndpointReplica, inv *clusterInventory) error {
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

	// Keep canary replicas up: CanaryReplicas per required role, so a
	// disaggregated endpoint never bakes (or promotes) with a role missing.
	var canaryReplicas []*types.EndpointReplica
	for _, r := range live {
		if r.EndpointID == id && r.Version == rollout.CanaryVersion && !r.Tuning {
			canaryReplicas = append(canaryReplicas, r)
		}
	}
	roles := requiredRoles(canarySpec)
	wanted := c.s.config.Rollout.CanaryReplicas
	if services, missing := serviceAddresses(canarySpec.Services, live); len(missing) == 0 {
		for _, role := range roles {
			canaryReplicas = append(canaryReplicas, c.growCanaryRole(ctx, canaryEndpoint, rollout, role, roleReplicas(canaryReplicas, role), wanted, inv, services)...)
		}
	}
	// Canary replicas follow their own version's fleet config.
	if err := c.ensureFleetRevisions(ctx, canaryEndpoint); err != nil {
		log.Warn().Err(err).Str("endpoint_id", id).Msg("managed endpoints: canary config revisions")
	}

	now := time.Now()
	if rollout.BakeStartedAt.IsZero() {
		if _, missing := missingReadyRole(canaryReplicas, roles); !missing {
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
	if role, missing := missingReadyRole(canaryReplicas, roles); missing {
		return c.finishRollout(ctx, endpoint, rollout, false, fmt.Sprintf("no ready %s canary replica at end of bake", role))
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
	promote, reason := evaluateRollout(activeMetrics, canaryMetrics, activeReplicas, canaryReplicas, c.s.config.Rollout)
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
	// A rollback is only recorded once the canary replicas are drained; if
	// they cannot be listed the rollout stays baking and is retried.
	replicas, err := c.s.repo.ListReplicas(ctx, endpoint.Spec.ID)
	if err != nil {
		return err
	}
	if canary != nil {
		canary.State = types.VersionStateRolledBack
		if err := c.s.repo.SaveVersion(ctx, canary); err != nil {
			return err
		}
	}
	for _, r := range replicas {
		if r.Version == canaryVersion && !r.Status.Terminal() {
			_ = c.drainReplica(ctx, r, endpoint.Spec.Policy.DrainSeconds, false, "canary rolled back")
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
// back. The error rate is only judged once the canary served at least
// rollout.MinCanaryRequests, so a handful of early failures cannot decide
// the rollout.
func evaluateRollout(active, canary *types.RouteMetrics, activeReplicas, canaryReplicas []*types.EndpointReplica, rollout types.ManagedEndpointsRolloutConfig) (bool, string) {
	th := rollout.Thresholds
	if readyCount(canaryReplicas) == 0 {
		return false, "no ready canary replica at end of bake"
	}
	if canary == nil || canary.Requests == 0 {
		return true, "canary healthy with no traffic during bake"
	}
	if canary.Requests >= int64(rollout.MinCanaryRequests) && canary.ErrorRate() > th.ErrorRate {
		return false, fmt.Sprintf("canary error rate %.2f%% exceeds %.2f%% over %d requests (min sample %d)",
			canary.ErrorRate()*100, th.ErrorRate*100, canary.Requests, rollout.MinCanaryRequests)
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
