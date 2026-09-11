package managedendpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v2"
)

// The endpoints repo deploys itself: its CI runs `beta9 endpoints deploy`,
// which deploys every app through the ordinary stub RPCs and then calls
// ApplyRepo with config.yaml and each app's outcome. Pull requests call the
// same RPC as a dry run, so a commit is validated against this cluster's GPU
// types, pools and registry before it merges.

const imageCheckTimeout = 15 * time.Second

// ApplyRepo validates one commit of the endpoints repo and, unless dry_run,
// records deploy outcomes, applies config.yaml and retires endpoints whose
// app directory is gone. Validation problems are returned in errors.
func (s *Service) ApplyRepo(ctx context.Context, in *pb.ApplyRepoRequest) (*pb.ApplyRepoResponse, error) {
	out := &pb.ApplyRepoResponse{}
	return admin(s, ctx, out, func() error {
		state, err := s.applyRepo(ctx, in, out)
		if state != nil {
			out.State = gitopsToProto(state)
		}
		if err != nil {
			return err
		}
		if len(out.Errors) > 0 {
			return fmt.Errorf("%d problem(s) found", len(out.Errors))
		}
		return nil
	})
}

func (s *Service) applyRepo(ctx context.Context, in *pb.ApplyRepoRequest, out *pb.ApplyRepoResponse) (*types.GitOpsState, error) {
	fail := func(format string, args ...any) { out.Errors = append(out.Errors, fmt.Sprintf(format, args...)) }
	warn := func(format string, args ...any) { out.Warnings = append(out.Warnings, fmt.Sprintf(format, args...)) }

	previous, err := s.repo.GetGitOpsState(ctx)
	if err != nil {
		return nil, err
	}
	if previous == nil {
		previous = &types.GitOpsState{}
	}
	endpoints, err := s.repo.ListEndpoints(ctx)
	if err != nil {
		return nil, err
	}
	active := map[string]*types.ManagedEndpoint{}
	for _, e := range endpoints {
		if e.Enabled() {
			active[e.Spec.ID] = e
		}
	}

	now := time.Now()
	state := &types.GitOpsState{RepoURL: in.RepoUrl, Ref: in.Ref, LastSHA: in.Sha, LastRunAt: now, PerEndpoint: map[string]types.GitOpsEndpointState{}}
	specs := map[string]*types.ManagedEndpointSpec{} // what the repo declares at this commit
	present := map[string]bool{}
	var failed []string
	for _, e := range in.Endpoints {
		entry := types.GitOpsEndpointState{Path: e.Path, ID: e.Id, Status: types.GitOpsStatusApplied, StubID: e.StubId, Version: uint(e.Version), UpdatedAt: now}
		if entry.ID == "" {
			entry.ID = previous.PerEndpoint[e.Path].ID // a broken import keeps its endpoint until the directory is gone
		}
		spec, specErr := parseSpec(e.SpecJson)
		switch {
		case e.Error != "":
			entry.Status, entry.Error = types.GitOpsStatusFailed, e.Error
		case specErr != nil:
			entry.Status, entry.Error = types.GitOpsStatusFailed, "spec: "+specErr.Error()
		case spec.ID != e.Path:
			entry.Status, entry.Error = types.GitOpsStatusFailed, fmt.Sprintf("id %q must equal its directory %q", spec.ID, e.Path)
		default:
			entry.ID = spec.ID
			specs[spec.ID] = spec
			if in.DryRun {
				s.checkSpec(ctx, e, spec, fail, warn)
			}
		}
		if entry.Error != "" {
			fail("%s: %s", e.Path, entry.Error)
			failed = append(failed, e.Path+": "+entry.Error)
		}
		if entry.ID != "" {
			present[entry.ID] = true
			if existing := active[entry.ID]; existing != nil && specs[entry.ID] == nil {
				specs[entry.ID] = &existing.Spec
			}
		}
		state.PerEndpoint[e.Path] = entry
	}

	fleet, err := parseFleet(in.ConfigYaml)
	if err != nil {
		state.FleetError = "config.yaml: " + err.Error()
		fail("%s", state.FleetError)
	} else {
		for _, id := range slices.Sorted(maps.Keys(fleet.Endpoints)) {
			entry := fleet.Endpoints[id]
			spec := specs[id]
			if spec == nil {
				fail("config.yaml: %s is not an app in the repo", id)
				continue
			}
			for _, gpu := range slices.Sorted(maps.Keys(entry.GPUs)) {
				if _, ok := spec.Gpu[gpu]; !ok {
					fail("config.yaml: %s does not declare gpu %q in its app", id, gpu)
				} else if in.DryRun && entry.Enabled && len(s.controller.pools(gpu)) == 0 {
					warn("config.yaml: %s: no pool on this cluster hosts %s", id, gpu)
				}
			}
			if entry.OpenRouter != nil {
				if err := entry.OpenRouter.ValidateFor(spec); err != nil {
					fail("config.yaml: %s: openrouter: %v", id, err)
				}
			}
		}
	}
	if in.DryRun {
		return nil, nil
	}

	// Deploys already registered their stubs; stamp the commit on them.
	for id, endpoint := range active {
		if entry := state.PerEndpoint[id]; present[id] && entry.Status == types.GitOpsStatusApplied && endpoint.GitSHA != in.Sha {
			endpoint.GitSHA, endpoint.UpdatedAt = in.Sha, now
			if err := s.repo.SaveEndpoint(ctx, endpoint); err != nil {
				return state, err
			}
		}
	}
	// An endpoint whose directory is gone is retired; the controller drains it.
	for id, endpoint := range active {
		if present[id] {
			continue
		}
		endpoint.Status, endpoint.UpdatedAt = types.EndpointStatusRetired, now
		if err := s.repo.SaveEndpoint(ctx, endpoint); err != nil {
			return state, err
		}
		delete(active, id)
		state.PerEndpoint[id] = types.GitOpsEndpointState{Path: id, ID: id, Status: types.GitOpsStatusRetired, UpdatedAt: now}
		s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{EndpointID: id, Action: "gitops.retired", Message: "removed from repo", Data: map[string]any{"sha": in.Sha}})
	}
	if fleet != nil {
		known := map[string]*types.ManagedEndpointSpec{}
		for id, e := range active {
			known[id] = &e.Spec
		}
		fleet.GitSHA = in.Sha
		if dropped := fleet.Prune(known); len(dropped) > 0 {
			state.FleetError = "skipped: " + strings.Join(dropped, "; ")
		}
		if err := s.repo.SaveFleet(ctx, fleet); err != nil {
			return state, err
		}
		s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{Action: "gitops.fleet", Message: in.Sha, Data: map[string]any{"fleet": fleet.Endpoints}})
	}
	state.LastError = strings.Join(failed, "\n")
	if err := s.repo.SaveGitOpsState(ctx, state); err != nil {
		return state, err
	}
	s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{Action: "gitops.applied", Message: in.Sha, Data: map[string]any{"sha": in.Sha, "apps": len(in.Endpoints), "failed": len(failed)}})
	log.Info().Str("sha", in.Sha).Int("apps", len(in.Endpoints)).Int("failed", len(failed)).Msg("managed endpoints: repo applied")
	return state, nil
}

// checkSpec is the pre-merge check of one app: known GPU types with a pool on
// this cluster, and a reachable image.
func (s *Service) checkSpec(ctx context.Context, e *pb.RepoEndpoint, spec *types.ManagedEndpointSpec, fail, warn func(string, ...any)) {
	for _, gpu := range slices.Sorted(maps.Keys(spec.Gpu)) {
		if gpu != types.CPUInventoryKey && len(s.controller.pools(gpu)) == 0 {
			warn("%s: no pool on this cluster hosts %s", e.Path, gpu)
		}
	}
	if e.Image == "" {
		return
	}
	ref, err := name.ParseReference(e.Image)
	if err != nil {
		fail("%s: image %q: %v", e.Path, e.Image, err)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, imageCheckTimeout)
	defer cancel()
	_, err = remote.Head(ref, remote.WithContext(ctx))
	var terr *transport.Error
	switch {
	case err == nil:
	case errors.As(err, &terr) && (terr.StatusCode == http.StatusUnauthorized || terr.StatusCode == http.StatusForbidden):
		warn("%s: image %s needs registry credentials; not verified", e.Path, e.Image)
	default:
		fail("%s: image %s: %v", e.Path, e.Image, err)
	}
}

func parseSpec(raw string) (*types.ManagedEndpointSpec, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("missing")
	}
	spec := &types.ManagedEndpointSpec{}
	if err := json.Unmarshal([]byte(raw), spec); err != nil {
		return nil, err
	}
	spec.Normalize()
	return spec, spec.Validate()
}

// parseFleet reads config.yaml: endpoint id -> {enabled, gpus: {<gpu>: placement}}.
func parseFleet(text string) (*types.Fleet, error) {
	if strings.TrimSpace(text) == "" {
		return nil, errors.New("an endpoint mapping is required (use {} to disable all endpoints)")
	}
	fleet := &types.Fleet{Endpoints: map[string]types.FleetEndpoint{}}
	decoder := yaml.NewDecoder(strings.NewReader(text))
	decoder.SetStrict(true)
	if err := decoder.Decode(&fleet.Endpoints); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("exactly one YAML document is required")
	}
	if fleet.Endpoints == nil {
		return nil, errors.New("an endpoint mapping is required (use {} to disable all endpoints)")
	}
	ids := map[string]bool{}
	for id, endpoint := range fleet.Endpoints {
		key := strings.ToLower(strings.TrimSpace(id))
		if key == "" || ids[key] {
			return nil, fmt.Errorf("empty or duplicate endpoint %q", id)
		}
		ids[key] = true
		gpus := map[string]bool{}
		for gpu := range endpoint.GPUs {
			key := types.GPUKey(gpu)
			if gpus[key] {
				return nil, fmt.Errorf("%s: duplicate GPU %q", id, gpu)
			}
			gpus[key] = true
		}
	}
	fleet.Normalize()
	return fleet, fleet.Validate()
}

// Endpoint states shown to operators. Deploy problems come from the repo's
// last CI run; everything after that from the fleet and the replicas.
const (
	StateDeployFailed       = "deploy_failed"
	StateRetired            = "retired"
	StateDisabled           = "disabled" // deployed but not placed by config.yaml
	StateWaitingForCapacity = "waiting_for_capacity"
	StateLoading            = "loading"
	StateReady              = "ready"
	StateFailed             = "failed"
)

// endpointState derives one word and one reason for an endpoint.
func endpointState(e *types.ManagedEndpoint, fleet *types.Fleet, gitops *types.GitOpsState, replicas []*types.EndpointReplica) (string, string) {
	if e.Status == types.EndpointStatusRetired {
		return StateRetired, "removed from the repo"
	}
	if gitops != nil {
		if entry, ok := gitops.PerEndpoint[e.Spec.ID]; ok && entry.Status == types.GitOpsStatusFailed {
			return StateDeployFailed, entry.Error
		}
	}
	var placements map[string]types.FleetPlacement
	if fleet != nil {
		placements = fleet.Placements(e.Spec.ID)
	}
	if len(placements) == 0 {
		return StateDisabled, "not enabled in config.yaml"
	}
	var want uint32
	var gpus []string
	for gpu, p := range placements {
		want += p.MinReplicas
		gpus = append(gpus, gpu)
	}
	slices.Sort(gpus)
	var ready, alive uint32
	var lastFailure *types.EndpointReplica
	for _, r := range replicas {
		if r.EndpointID != e.Spec.ID {
			continue
		}
		switch {
		case r.Status == types.ReplicaStatusReady:
			ready++
			alive++
		case r.Alive():
			alive++
		case r.Status == types.ReplicaStatusFailed && (lastFailure == nil || r.EndedAt.After(lastFailure.EndedAt)):
			lastFailure = r
		}
	}
	switch {
	case ready > 0:
		return StateReady, fmt.Sprintf("%d/%d replicas ready", ready, alive)
	case alive > 0:
		return StateLoading, fmt.Sprintf("%d replica(s) starting", alive)
	case lastFailure != nil:
		return StateFailed, fmt.Sprintf("last replica failed: %s", lastFailure.StatusReason)
	case want == 0:
		return StateWaitingForCapacity, "minReplicas is 0; fills spare " + strings.Join(gpus, ", ") + " capacity only"
	}
	return StateWaitingForCapacity, "no idle " + strings.Join(gpus, ", ") + " in an opted-in pool"
}
