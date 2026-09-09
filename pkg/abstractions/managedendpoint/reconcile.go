package managedendpoint

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	serviceReplicaPrefix = "service:"
	serviceDrainSeconds  = 30
)

// serviceReplicaID namespaces service replicas away from endpoint ids.
func serviceReplicaID(name string) string { return serviceReplicaPrefix + name }

func isServiceReplica(replica *types.EndpointReplica) bool {
	return replica != nil && strings.HasPrefix(replica.EndpointID, serviceReplicaPrefix)
}

// evictionDrainSeconds is the grace an evicted replica of spec gets to finish
// in-flight work: the endpoint's own drain policy, or the cluster default.
func (c *controller) evictionDrainSeconds(spec *types.ManagedEndpointSpec) uint32 {
	if spec != nil && spec.Policy.DrainSeconds > 0 {
		return spec.Policy.DrainSeconds
	}
	return c.s.config.Preemption.DefaultDrainSeconds
}

// reconcileEndpoint converges one endpoint's replicas toward the fill plan.
func (c *controller) reconcileEndpoint(ctx context.Context, endpoint *types.ManagedEndpoint, input *endpointPlanInput, plans map[string]fillPlan, live []*types.EndpointReplica, inv *clusterInventory) error {
	spec := &endpoint.Spec
	drainSeconds := spec.Policy.DrainSeconds

	if !endpoint.Enabled || input == nil {
		for _, r := range live {
			if r.EndpointID == spec.ID {
				_ = c.drainReplica(ctx, r, drainSeconds, false, "endpoint disabled")
			}
		}
		return nil
	}

	rollout := input.rollout
	if rollout == nil {
		rollout = &types.RolloutState{EndpointID: spec.ID, ActiveVersion: endpoint.Version, Phase: rolloutPhaseIdle}
	}
	if err := c.stepRollout(ctx, endpoint, rollout, live); err != nil {
		log.Warn().Err(err).Str("endpoint_id", spec.ID).Msg("managed endpoints: rollout step failed")
	}

	if err := c.ensureFleetRevisions(ctx, endpoint); err != nil {
		log.Warn().Err(err).Str("endpoint_id", spec.ID).Msg("managed endpoints: fleet config revisions")
	}

	services, missing := c.serviceAddresses(ctx, spec.Services, live)
	if len(missing) > 0 {
		// Keep what is running but do not grow until dependencies are up.
		log.Debug().Str("endpoint_id", spec.ID).Strs("missing_services", missing).Msg("managed endpoints: waiting on services")
	}

	for _, rt := range spec.Targets() {
		plan := plans[fillTarget{EndpointID: spec.ID, Role: rt.Role, GPU: rt.Target.Key()}.key()]
		set := partitionReplicas(live, spec.ID, rt.Role, rt.Target.Key(), endpoint.Version)
		current := uint32(len(set.Live))

		switch {
		case current < plan.Desired && len(missing) == 0:
			c.growTarget(ctx, endpoint, rt, plan, set, inv, services)
		case current > plan.Desired:
			excess := current - plan.Desired
			for _, r := range scaleDownCandidates(set) {
				if excess == 0 {
					break
				}
				if r.Protected && uint32(len(set.Live))-excess < rt.Target.MinReplicas {
					continue
				}
				if err := c.drainReplica(ctx, r, drainSeconds, false, "scale down to fair share"); err == nil {
					excess--
				}
			}
		}

		c.retireStaleVersions(ctx, endpoint, rollout, rt, live, drainSeconds)
	}
	return nil
}

// growTarget starts replicas up to the plan. Replicas needed to satisfy
// min_replicas are protected (they may trigger provisioning); the rest are
// opportunistic and only land on free capacity.
func (c *controller) growTarget(ctx context.Context, endpoint *types.ManagedEndpoint, rt types.RoleTarget, plan fillPlan, set replicaSet, inv *clusterInventory, services map[string]string) {
	spec := &endpoint.Spec
	need := plan.Desired - uint32(len(set.Live))
	if need > maxStartsPerTick {
		need = maxStartsPerTick
	}
	protectedNeeded := uint32(0)
	if rt.Target.MinReplicas > set.Protected {
		protectedNeeded = rt.Target.MinReplicas - set.Protected
	}
	backoff, _ := c.s.repo.InScheduleBackoff(ctx, spec.ID, rt.Key())

	for i := uint32(0); i < need; i++ {
		protected := protectedNeeded > 0
		if !protected && backoff {
			return
		}
		pool, locality, ok := c.place(inv, rt.Target, spec.Locality)
		if !ok && !protected {
			return
		}
		_, err := c.startReplica(ctx, startSpec{
			EndpointID:   spec.ID,
			Version:      endpoint.Version,
			StubID:       endpoint.StubID,
			Role:         rt.Role,
			Target:       rt.Target,
			Port:         spec.Port,
			Locality:     locality,
			PoolName:     pool,
			Protected:    protected,
			Harness:      spec.Harness.Enabled,
			Entrypoint:   spec.Entrypoint,
			Services:     services,
			KVCache:      spec.KVCache,
			Evictable:    spec.Policy.Evictable && c.s.config.Preemption.Enabled,
			DrainSeconds: c.evictionDrainSeconds(spec),
			GitSHA:       endpoint.GitSHA,
		})
		if err != nil {
			log.Warn().Err(err).Str("endpoint_id", spec.ID).Str("target", rt.Key()).Msg("managed endpoints: start replica failed")
			return
		}
		if protected {
			protectedNeeded--
		}
	}
}

// place chooses a pool (and its locality) for a target, reserving the GPUs
// in the in-memory inventory. CPU targets go to any endpoint-enabled CPU
// pool; GPU targets go to the least-loaded eligible worker.
func (c *controller) place(inv *clusterInventory, target types.GpuTarget, localities []string) (pool string, locality string, ok bool) {
	accept := func(w workerSlot) bool {
		if len(localities) == 0 {
			return true
		}
		for _, l := range localities {
			if l == w.Locality {
				return true
			}
		}
		return false
	}
	if target.IsCPU() {
		for _, name := range inv.cpuPools {
			cfg, _ := c.poolConfig(name)
			loc := poolLocality(name, cfg)
			if accept(workerSlot{PoolName: name, Locality: loc}) {
				return name, loc, true
			}
		}
		return "", "", false
	}
	gpuType := string(types.NormalizeGPUType(target.Type))
	entry, ok := inv.byType[gpuType]
	if !ok {
		return "", "", false
	}
	i, ok := entry.pickWorker(target.Count, accept)
	if !ok {
		return "", "", false
	}
	entry.reserve(i, target.Count)
	inv.byType[gpuType] = entry
	return entry.Workers[i].PoolName, entry.Workers[i].Locality, true
}

// retireStaleVersions drains replicas running versions that are neither
// active nor the current canary, one per target per tick, and only once the
// active version has something ready to take the traffic.
func (c *controller) retireStaleVersions(ctx context.Context, endpoint *types.ManagedEndpoint, rollout *types.RolloutState, rt types.RoleTarget, live []*types.EndpointReplica, drainSeconds uint32) {
	activeReady := len(partitionReplicas(live, endpoint.Spec.ID, rt.Role, rt.Target.Key(), endpoint.Version).Ready)
	for _, r := range live {
		if r.EndpointID != endpoint.Spec.ID || r.Role != rt.Role || r.GPU != rt.Target.Key() {
			continue
		}
		if r.Version == endpoint.Version || (rollout.CanaryVersion != 0 && r.Version == rollout.CanaryVersion) {
			continue
		}
		if r.Status == types.ReplicaStatusDraining || r.Status == types.ReplicaStatusEvicting {
			continue
		}
		if r.Status == types.ReplicaStatusReady && activeReady == 0 {
			// Keep serving the old version until the new one is up.
			continue
		}
		if err := c.drainReplica(ctx, r, drainSeconds, false, fmt.Sprintf("version %d retired", r.Version)); err == nil {
			return
		}
	}
}

// ensureFleetRevisions seeds the git-sourced harness config for each target
// of one endpoint version. Fleet config streams are keyed by version, so
// this runs once per version and live fleet edits made afterwards stay in
// force for that version only; the next version starts from git again.
func (c *controller) ensureFleetRevisions(ctx context.Context, endpoint *types.ManagedEndpoint) error {
	if !endpoint.Spec.Harness.Enabled {
		return nil
	}
	author := gitAuthor(endpoint.Version)
	for _, rt := range endpoint.Spec.Targets() {
		key := fleetKey(rt.Role, rt.Target.Key(), endpoint.Version)
		latest, err := c.s.repo.LatestConfigRevision(ctx, endpoint.Spec.ID, types.ConfigScopeTarget, key)
		if err != nil {
			return err
		}
		if latest != nil {
			continue
		}
		config := rt.Target.Harness
		if config == nil {
			config = map[string]any{}
		}
		revision := &types.EndpointConfigRevision{
			EndpointID: endpoint.Spec.ID,
			Scope:      types.ConfigScopeTarget,
			ScopeKey:   key,
			Config:     config,
			Author:     author,
			Source:     types.ConfigSourceGit,
		}
		if err := c.s.repo.CreateConfigRevision(ctx, revision); err != nil {
			return err
		}
		c.s.emit(types.EventEndpointConfig, types.EventEndpointSchema{
			EndpointID: endpoint.Spec.ID, Action: "config.fleet", Version: endpoint.Version,
			Role: rt.Role, GPU: rt.Target.Key(), Revision: revision.Revision,
			Data: map[string]any{"source": "git", "git_sha": endpoint.GitSHA},
		})
	}
	return nil
}

func gitAuthor(version uint) string { return fmt.Sprintf("git@v%d", version) }

// serviceAddresses resolves the addresses of the shared services an endpoint
// depends on. Missing names are returned so fill can wait for them.
func (c *controller) serviceAddresses(ctx context.Context, names []string, live []*types.EndpointReplica) (map[string]string, []string) {
	if len(names) == 0 {
		return nil, nil
	}
	out := map[string]string{}
	var missing []string
	for _, name := range names {
		var addrs []string
		for _, r := range live {
			if r.EndpointID == serviceReplicaID(name) && r.Status == types.ReplicaStatusReady && r.Address != "" {
				addrs = append(addrs, r.Address)
			}
		}
		if len(addrs) == 0 {
			missing = append(missing, name)
			continue
		}
		sort.Strings(addrs)
		out[name] = strings.Join(addrs, ",")
	}
	return out, missing
}

// reconcileService keeps a shared service at its replica count, per locality
// when requested. Service replicas are always protected.
func (c *controller) reconcileService(ctx context.Context, service *types.ManagedService, live []*types.EndpointReplica, inv *clusterInventory) error {
	spec := &service.Spec
	id := serviceReplicaID(spec.Name)

	if !service.Enabled {
		for _, r := range live {
			if r.EndpointID == id {
				_ = c.drainReplica(ctx, r, serviceDrainSeconds, false, "service disabled")
			}
		}
		return nil
	}

	groups := []string{""}
	if spec.PerLocality {
		groups = c.serviceLocalities(spec, inv)
	}

	for _, locality := range groups {
		var set replicaSet
		for _, r := range live {
			if r.EndpointID != id || r.Version != service.Version || r.Status.Terminal() {
				continue
			}
			if r.Status == types.ReplicaStatusDraining || r.Status == types.ReplicaStatusEvicting {
				continue
			}
			if locality != "" && r.Locality != locality {
				continue
			}
			set.Live = append(set.Live, r)
			if r.Status == types.ReplicaStatusReady {
				set.Ready = append(set.Ready, r)
			}
		}
		current := uint32(len(set.Live))
		switch {
		case current < spec.Replicas:
			var want []string
			if locality != "" {
				want = []string{locality}
			}
			for i := current; i < spec.Replicas && i-current < maxStartsPerTick; i++ {
				target, pool, loc := c.placeService(inv, spec, want)
				if _, err := c.startReplica(ctx, startSpec{
					EndpointID: id,
					Version:    service.Version,
					StubID:     service.StubID,
					Role:       types.ReplicaRoleServe,
					Target:     target,
					Port:       spec.Port,
					Locality:   loc,
					PoolName:   pool,
					Protected:  true,
					Entrypoint: spec.Entrypoint,
					GitSHA:     service.GitSHA,
				}); err != nil {
					log.Warn().Err(err).Str("service", spec.Name).Msg("managed endpoints: start service replica failed")
					break
				}
			}
		case current > spec.Replicas:
			for _, r := range scaleDownCandidates(set)[:current-spec.Replicas] {
				_ = c.drainReplica(ctx, r, serviceDrainSeconds, false, "service scale down")
			}
		}

		// Retire old versions once the new one is fully ready.
		if uint32(len(set.Ready)) >= spec.Replicas {
			for _, r := range live {
				if r.EndpointID == id && r.Version != service.Version && r.Status != types.ReplicaStatusDraining && (locality == "" || r.Locality == locality) {
					_ = c.drainReplica(ctx, r, serviceDrainSeconds, false, fmt.Sprintf("version %d retired", r.Version))
				}
			}
		}
	}
	return nil
}

// serviceLocalities lists every locality where at least one of the service's
// targets could run.
func (c *controller) serviceLocalities(spec *types.ManagedServiceSpec, inv *clusterInventory) []string {
	seen := map[string]bool{}
	var out []string
	add := func(l string) {
		if !seen[l] {
			seen[l] = true
			out = append(out, l)
		}
	}
	for _, t := range spec.Gpu {
		if t.IsCPU() {
			for _, name := range inv.cpuPools {
				cfg, _ := c.poolConfig(name)
				add(poolLocality(name, cfg))
			}
			continue
		}
		for _, l := range inv.localities[string(types.NormalizeGPUType(t.Type))] {
			add(l)
		}
	}
	sort.Strings(out)
	if len(out) == 0 {
		out = []string{""}
	}
	return out
}

// placeService picks the first target with free capacity; when none has,
// the first target is used and the (protected) request may provision.
func (c *controller) placeService(inv *clusterInventory, spec *types.ManagedServiceSpec, localities []string) (types.GpuTarget, string, string) {
	targets := spec.Gpu
	if len(targets) == 0 {
		targets = []types.GpuTarget{types.CPUTarget()}
	}
	for _, t := range targets {
		if pool, loc, ok := c.place(inv, t, localities); ok {
			return t, pool, loc
		}
	}
	loc := ""
	if len(localities) > 0 {
		loc = localities[0]
	}
	return targets[0], "", loc
}

// liveAuthor tags a live fleet edit with the version it was made against so
// the next git version replaces it.
func liveAuthor(version uint, who string) string {
	return fmt.Sprintf("live@v%d:%s", version, who)
}
