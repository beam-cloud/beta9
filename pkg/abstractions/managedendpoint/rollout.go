package managedendpoint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	rolloutPhaseIdle       = "idle"
	rolloutPhaseBaking     = "baking"
	rolloutPhaseRolledBack = "rolled_back"

	// canaryStartGrace bounds how long a canary may sit without a ready
	// replica before the version is rolled back.
	canaryStartGrace = 30 * time.Minute
)

// stepRollout advances a baking canary: it keeps canary replicas up, starts
// the bake clock when the first one is ready, and after the bake window
// promotes or rolls back based on route metrics. Admin pins suspend automatic
// promotion.
func (c *controller) stepRollout(ctx context.Context, endpoint *types.ManagedEndpoint, rollout *types.RolloutState, live []*types.EndpointReplica) error {
	if rollout.CanaryVersion == 0 || rollout.Phase != rolloutPhaseBaking {
		return nil
	}
	spec := &endpoint.Spec

	versions, err := c.s.repo.ListVersions(ctx, spec.ID)
	if err != nil {
		return err
	}
	var canary *types.EndpointVersion
	for _, v := range versions {
		if v.Version == rollout.CanaryVersion {
			canary = v
		}
	}
	if canary == nil {
		return c.finishRollout(ctx, endpoint, rollout, false, "canary version record missing")
	}
	_, canaryConfig, err := c.stub(ctx, canary.StubID)
	if err != nil {
		return err
	}
	if canaryConfig == nil || canaryConfig.ManagedEndpoint == nil || canaryConfig.ManagedEndpoint.Endpoint == nil {
		return c.finishRollout(ctx, endpoint, rollout, false, "canary stub has no endpoint spec")
	}
	canarySpec := canaryConfig.ManagedEndpoint.Endpoint

	// Keep canary replicas up: one per target, bounded by the configured total.
	var canaryReplicas []*types.EndpointReplica
	for _, r := range live {
		if r.EndpointID == spec.ID && r.Version == rollout.CanaryVersion && !r.Tuning {
			canaryReplicas = append(canaryReplicas, r)
		}
	}
	wanted := c.s.config.Rollout.CanaryReplicasOrDefault()
	if uint32(len(canaryReplicas)) < wanted {
		services, missing := c.serviceAddresses(ctx, canarySpec.Services, live)
		if len(missing) == 0 {
			for _, rt := range canarySpec.Targets() {
				if uint32(len(canaryReplicas)) >= wanted {
					break
				}
				if hasReplicaFor(canaryReplicas, rt) {
					continue
				}
				replica, err := c.startReplica(ctx, startSpec{
					EndpointID: spec.ID,
					Version:    rollout.CanaryVersion,
					StubID:     canary.StubID,
					Role:       rt.Role,
					Target:     rt.Target,
					Port:       canarySpec.Port,
					PoolName:   "",
					Protected:  true,
					Harness:    canarySpec.Harness.Enabled,
					Entrypoint: canarySpec.Entrypoint,
					Services:   services,
					KVCache:    canarySpec.KVCache,
					GitSHA:     canary.GitSHA,
				})
				if err != nil {
					log.Warn().Err(err).Str("endpoint_id", spec.ID).Uint("version", rollout.CanaryVersion).Msg("managed endpoints: start canary failed")
					break
				}
				canaryReplicas = append(canaryReplicas, replica)
			}
		}
	}

	if err := c.ensureCanaryRevisions(ctx, spec.ID, rollout.CanaryVersion, canarySpec); err != nil {
		log.Warn().Err(err).Str("endpoint_id", spec.ID).Msg("managed endpoints: canary config revisions")
	}

	readyCanaries := 0
	for _, r := range canaryReplicas {
		if r.Status == types.ReplicaStatusReady {
			readyCanaries++
		}
	}
	now := time.Now()
	if readyCanaries > 0 && rollout.BakeStartedAt.IsZero() {
		rollout.BakeStartedAt = now
		rollout.LastDecision = "canary ready; baking"
		rollout.LastDecisionAt = now
		return c.s.repo.SaveRollout(ctx, rollout)
	}
	if rollout.BakeStartedAt.IsZero() {
		if now.Sub(canary.CreatedAt) > canaryStartGrace {
			return c.finishRollout(ctx, endpoint, rollout, false, "canary never became ready")
		}
		return nil
	}
	if now.Sub(rollout.BakeStartedAt) < c.s.config.Rollout.BakeDuration() {
		return nil
	}
	if rollout.PinnedVersion != 0 && rollout.PinnedVersion != rollout.CanaryVersion {
		return nil
	}

	window := c.s.config.Rollout.BakeDuration()
	activeMetrics, err := c.s.repo.GetRouteMetrics(ctx, spec.ID, "", rollout.ActiveVersion, window)
	if err != nil {
		return err
	}
	canaryMetrics, err := c.s.repo.GetRouteMetrics(ctx, spec.ID, "", rollout.CanaryVersion, window)
	if err != nil {
		return err
	}
	var activeReplicas []*types.EndpointReplica
	for _, r := range live {
		if r.EndpointID == spec.ID && r.Version == rollout.ActiveVersion && r.Status == types.ReplicaStatusReady {
			activeReplicas = append(activeReplicas, r)
		}
	}
	promote, reason := evaluateRollout(activeMetrics, canaryMetrics, activeReplicas, canaryReplicas, c.s.config.Rollout.Thresholds)
	return c.finishRollout(ctx, endpoint, rollout, promote, reason)
}

func hasReplicaFor(replicas []*types.EndpointReplica, rt types.RoleTarget) bool {
	for _, r := range replicas {
		if r.Role == rt.Role && r.GPU == rt.Target.Key() {
			return true
		}
	}
	return false
}

// ensureCanaryRevisions publishes the canary's harness config under the
// canary version's author so canary replicas do not pick up the active
// fleet's tuning while baking.
func (c *controller) ensureCanaryRevisions(ctx context.Context, endpointID string, version uint, spec *types.ManagedEndpointSpec) error {
	if !spec.Harness.Enabled {
		return nil
	}
	canaryEndpoint := &types.ManagedEndpoint{Spec: *spec, Version: version}
	return c.ensureFleetRevisions(ctx, canaryEndpoint)
}

// finishRollout promotes the canary (swapping the endpoint's spec/stub) or
// rolls it back (draining canary replicas).
func (c *controller) finishRollout(ctx context.Context, endpoint *types.ManagedEndpoint, rollout *types.RolloutState, promote bool, reason string) error {
	now := time.Now()
	canaryVersion := rollout.CanaryVersion
	previous := rollout.ActiveVersion

	versions, err := c.s.repo.ListVersions(ctx, endpoint.Spec.ID)
	if err != nil {
		return err
	}
	var canary *types.EndpointVersion
	for _, v := range versions {
		if v.Version == canaryVersion {
			canary = v
		}
	}

	if promote && canary != nil {
		_, cfg, err := c.stub(ctx, canary.StubID)
		if err != nil {
			return err
		}
		if cfg == nil || cfg.ManagedEndpoint == nil || cfg.ManagedEndpoint.Endpoint == nil {
			return errors.New("canary stub has no endpoint spec")
		}
		endpoint.Spec = *cfg.ManagedEndpoint.Endpoint
		endpoint.StubID = canary.StubID
		endpoint.Version = canary.Version
		endpoint.GitSHA = canary.GitSHA
		endpoint.Status = types.EndpointStatusActive
		endpoint.UpdatedAt = now
		if err := c.s.repo.SaveEndpoint(ctx, endpoint); err != nil {
			return err
		}
		for _, v := range versions {
			switch {
			case v.Version == canaryVersion:
				v.State = types.VersionStateActive
			case v.State == types.VersionStateActive:
				v.State = types.VersionStateRetired
			default:
				continue
			}
			if err := c.s.repo.SaveVersion(ctx, v); err != nil {
				return err
			}
		}
		rollout.ActiveVersion = canaryVersion
		rollout.LastDecision = "promoted: " + reason
	} else {
		if canary != nil {
			canary.State = types.VersionStateRolledBack
			if err := c.s.repo.SaveVersion(ctx, canary); err != nil {
				return err
			}
		}
		replicas, err := c.s.repo.ListReplicas(ctx, endpoint.Spec.ID)
		if err == nil {
			for _, r := range replicas {
				if r.Version == canaryVersion && !r.Status.Terminal() {
					_ = c.drainReplica(ctx, r, endpoint.Spec.Policy.DrainSeconds, false, "canary rolled back")
				}
			}
		}
		rollout.LastDecision = "rolled back: " + reason
	}

	rollout.CanaryVersion = 0
	rollout.BakeStartedAt = time.Time{}
	rollout.Phase = rolloutPhaseIdle
	if !promote {
		rollout.Phase = rolloutPhaseRolledBack
	}
	rollout.LastDecisionAt = now
	if err := c.s.repo.SaveRollout(ctx, rollout); err != nil {
		return err
	}

	action := "rollout.promoted"
	if !promote {
		action = "rollout.rolled_back"
	}
	c.s.emit(types.EventEndpointRollout, types.EventEndpointSchema{
		EndpointID: endpoint.Spec.ID,
		Action:     action,
		Version:    canaryVersion,
		Message:    reason,
		Data:       map[string]any{"previous_version": previous, "git_sha": endpoint.GitSHA},
	})
	log.Info().Str("endpoint_id", endpoint.Spec.ID).Uint("version", canaryVersion).Bool("promoted", promote).Str("reason", reason).Msg("managed endpoints: rollout decided")
	return nil
}

// activateVersion makes an already-deployed version the active one without a
// bake (manual promote of a retired version, or rollback). The controller's
// version retirement then replaces replicas gradually.
func (c *controller) activateVersion(ctx context.Context, endpoint *types.ManagedEndpoint, rollout *types.RolloutState, versions []*types.EndpointVersion, version uint, reason string) error {
	var target *types.EndpointVersion
	for _, v := range versions {
		if v.Version == version {
			target = v
		}
	}
	if target == nil {
		return fmt.Errorf("version %d not found", version)
	}
	_, cfg, err := c.stub(ctx, target.StubID)
	if err != nil {
		return err
	}
	if cfg == nil || cfg.ManagedEndpoint == nil || cfg.ManagedEndpoint.Endpoint == nil {
		return fmt.Errorf("version %d stub has no endpoint spec", version)
	}

	now := time.Now()
	previous := endpoint.Version
	endpoint.Spec = *cfg.ManagedEndpoint.Endpoint
	endpoint.StubID = target.StubID
	endpoint.Version = target.Version
	endpoint.GitSHA = target.GitSHA
	endpoint.Status = types.EndpointStatusActive
	endpoint.UpdatedAt = now
	if err := c.s.repo.SaveEndpoint(ctx, endpoint); err != nil {
		return err
	}
	for _, v := range versions {
		switch {
		case v.Version == version:
			v.State = types.VersionStateActive
		case v.State == types.VersionStateActive:
			v.State = types.VersionStateRetired
		case v.Version == rollout.CanaryVersion && rollout.CanaryVersion != 0:
			v.State = types.VersionStateRolledBack
		default:
			continue
		}
		if err := c.s.repo.SaveVersion(ctx, v); err != nil {
			return err
		}
	}
	rollout.ActiveVersion = version
	rollout.CanaryVersion = 0
	rollout.BakeStartedAt = time.Time{}
	rollout.Phase = rolloutPhaseIdle
	rollout.LastDecision = reason
	rollout.LastDecisionAt = now
	if err := c.s.repo.SaveRollout(ctx, rollout); err != nil {
		return err
	}
	c.s.emit(types.EventEndpointRollout, types.EventEndpointSchema{
		EndpointID: endpoint.Spec.ID, Action: "rollout.activated", Version: version, Message: reason,
		Data: map[string]any{"previous_version": previous, "git_sha": endpoint.GitSHA},
	})
	return nil
}

// evaluateRollout decides whether a baked canary is at least as good as the
// active version. It is conservative: with no traffic on either side a
// healthy canary is promoted; with traffic, any threshold regression rolls
// back.
func evaluateRollout(active, canary *types.RouteMetrics, activeReplicas, canaryReplicas []*types.EndpointReplica, th types.RolloutThresholds) (bool, string) {
	errorRate := th.ErrorRate
	if errorRate <= 0 {
		errorRate = 0.02
	}
	ttftRegress := th.TTFT
	if ttftRegress <= 0 {
		ttftRegress = 0.25
	}
	tpotRegress := th.TPOT
	if tpotRegress <= 0 {
		tpotRegress = 0.25
	}
	throughputRegress := th.Throughput
	if throughputRegress <= 0 {
		throughputRegress = 0.25
	}

	readyCanaries := 0
	for _, r := range canaryReplicas {
		if r.Status == types.ReplicaStatusReady {
			readyCanaries++
		}
	}
	if readyCanaries == 0 {
		return false, "no ready canary replica at end of bake"
	}
	if canary == nil || canary.Requests == 0 {
		return true, "canary healthy with no traffic during bake"
	}

	if canary.Requests >= 20 && canary.ErrorRate() > errorRate {
		return false, fmt.Sprintf("canary error rate %.2f%% exceeds %.2f%%", canary.ErrorRate()*100, errorRate*100)
	}
	if active == nil || active.Requests == 0 {
		return true, "canary healthy; active version had no traffic to compare"
	}

	if active.TTFTCount > 0 && canary.TTFTCount > 0 {
		a, b := float64(active.MeanTTFTMs()), float64(canary.MeanTTFTMs())
		if a > 0 && b > a*(1+ttftRegress) {
			return false, fmt.Sprintf("canary TTFT %.0fms regressed vs %.0fms", b, a)
		}
	}

	aTPOT, aTPS := meanEngine(activeReplicas)
	bTPOT, bTPS := meanEngine(canaryReplicas)
	if aTPOT > 0 && bTPOT > aTPOT*(1+tpotRegress) {
		return false, fmt.Sprintf("canary TPOT %.1fms regressed vs %.1fms", bTPOT, aTPOT)
	}
	if aTPS > 0 && bTPS > 0 && bTPS < aTPS*(1-throughputRegress) {
		return false, fmt.Sprintf("canary decode throughput %.0f tok/s regressed vs %.0f", bTPS, aTPS)
	}
	return true, fmt.Sprintf("canary within thresholds over %d requests", canary.Requests)
}

func meanEngine(replicas []*types.EndpointReplica) (tpotMs float64, tokensPerSec float64) {
	var n, tpot, tps float64
	for _, r := range replicas {
		if r.Status != types.ReplicaStatusReady {
			continue
		}
		n++
		tpot += float64(r.Capacity.TPOTMs)
		tps += float64(r.Capacity.DecodeTokensPerSec)
	}
	if n == 0 {
		return 0, 0
	}
	return tpot / n, tps / n
}
