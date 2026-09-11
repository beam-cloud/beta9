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

// review is one commit of the repo checked against this cluster.
type review struct {
	in      *pb.ApplyRepoRequest
	out     *pb.ApplyRepoResponse
	now     time.Time
	state   *types.GitOpsState
	specs   map[string]*types.ManagedEndpointSpec // what the repo declares at this commit
	present map[string]bool                       // endpoint ids with an app directory
	fleet   *types.Fleet
	failed  []string
}

func (r *review) fail(format string, args ...any) {
	r.out.Errors = append(r.out.Errors, fmt.Sprintf(format, args...))
}

func (r *review) warn(format string, args ...any) {
	r.out.Warnings = append(r.out.Warnings, fmt.Sprintf(format, args...))
}

func (s *Service) applyRepo(ctx context.Context, in *pb.ApplyRepoRequest, out *pb.ApplyRepoResponse) (*types.GitOpsState, error) {
	previous, err := s.repo.GetGitOpsState(ctx)
	if err != nil {
		return nil, err
	}
	if previous == nil {
		previous = &types.GitOpsState{}
	}
	active, err := s.activeEndpoints(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	r := &review{
		in:      in,
		out:     out,
		now:     now,
		state:   &types.GitOpsState{RepoURL: in.RepoUrl, Ref: in.Ref, LastSHA: in.Sha, LastRunAt: now, PerEndpoint: map[string]types.GitOpsEndpointState{}},
		specs:   map[string]*types.ManagedEndpointSpec{},
		present: map[string]bool{},
	}
	for _, e := range in.Endpoints {
		s.reviewApp(ctx, r, e, previous, active)
	}
	s.reviewFleet(r)
	if in.DryRun {
		return nil, s.announce(ctx, previous, in)
	}
	return r.state, s.commitRepo(ctx, r, active)
}

// announce remembers a deploy that has started: `deploy` names its commit in
// its dry run, `validate` does not. The commit stays pending until it applies,
// so a CI run that dies in between is visible.
func (s *Service) announce(ctx context.Context, state *types.GitOpsState, in *pb.ApplyRepoRequest) error {
	if in.Sha == "" {
		return nil
	}
	state.PendingSHA, state.PendingAt = in.Sha, time.Now()
	return s.repo.SaveGitOpsState(ctx, state)
}

func (s *Service) activeEndpoints(ctx context.Context) (map[string]*types.ManagedEndpoint, error) {
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
	return active, nil
}

// reviewApp records one app directory's outcome and what it declares.
func (s *Service) reviewApp(ctx context.Context, r *review, e *pb.RepoEndpoint, previous *types.GitOpsState, active map[string]*types.ManagedEndpoint) {
	entry := types.GitOpsEndpointState{Path: e.Path, ID: e.Id, Status: types.GitOpsStatusApplied, StubID: e.StubId, Version: uint(e.Version), UpdatedAt: r.now}
	if entry.ID == "" {
		entry.ID = previous.PerEndpoint[e.Path].ID // a broken import keeps its endpoint until the directory is gone
	}
	spec, err := appSpec(e)
	if err != nil {
		entry.Status, entry.Error = types.GitOpsStatusFailed, err.Error()
		r.fail("%s: %s", e.Path, entry.Error)
		r.failed = append(r.failed, e.Path+": "+entry.Error)
	} else {
		entry.ID = spec.ID
		r.specs[spec.ID] = spec
		if r.in.DryRun {
			s.checkSpec(ctx, e, spec, r.fail, r.warn)
		}
	}
	if entry.ID != "" {
		r.present[entry.ID] = true
		if existing := active[entry.ID]; existing != nil && r.specs[entry.ID] == nil {
			r.specs[entry.ID] = &existing.Spec // config.yaml is checked against what still runs
		}
	}
	r.state.PerEndpoint[e.Path] = entry
}

// appSpec is what one app directory declares, or why it cannot be applied.
func appSpec(e *pb.RepoEndpoint) (*types.ManagedEndpointSpec, error) {
	if e.Error != "" {
		return nil, errors.New(e.Error)
	}
	spec, err := parseSpec(e.SpecJson)
	if err != nil {
		return nil, fmt.Errorf("spec: %w", err)
	}
	if spec.ID != e.Path {
		return nil, fmt.Errorf("id %q must equal its directory %q", spec.ID, e.Path)
	}
	return spec, nil
}

// reviewFleet parses config.yaml and checks every placement against the apps.
func (s *Service) reviewFleet(r *review) {
	fleet, err := parseFleet(r.in.ConfigYaml)
	if err != nil {
		r.state.FleetError = "config.yaml: " + err.Error()
		r.fail("%s", r.state.FleetError)
		return
	}
	r.fleet = fleet
	for _, id := range slices.Sorted(maps.Keys(fleet.Endpoints)) {
		entry := fleet.Endpoints[id]
		spec := r.specs[id]
		if spec == nil {
			r.fail("config.yaml: %s is not an app in the repo", id)
			continue
		}
		for _, gpu := range slices.Sorted(maps.Keys(entry.GPUs)) {
			_, declared := spec.Gpu[gpu]
			switch {
			case !declared:
				r.fail("config.yaml: %s does not declare gpu %q in its app", id, gpu)
			case r.in.DryRun && entry.Enabled && len(s.controller.pools(gpu)) == 0:
				r.warn("config.yaml: %s: no pool on this cluster hosts %s", id, gpu)
			}
		}
		if entry.OpenRouter != nil {
			if err := entry.OpenRouter.ValidateFor(spec); err != nil {
				r.fail("config.yaml: %s: openrouter: %v", id, err)
			}
		}
	}
}

// commitRepo records the deploy: the commit is stamped on every deployed
// endpoint, endpoints whose directory is gone are retired (the controller
// drains them), and config.yaml becomes the fleet.
func (s *Service) commitRepo(ctx context.Context, r *review, active map[string]*types.ManagedEndpoint) error {
	for id, endpoint := range active {
		switch {
		case !r.present[id]:
			if err := s.retireEndpoint(ctx, r, endpoint); err != nil {
				return err
			}
			delete(active, id)
		case r.state.PerEndpoint[id].Status == types.GitOpsStatusApplied && endpoint.GitSHA != r.in.Sha:
			endpoint.GitSHA, endpoint.UpdatedAt = r.in.Sha, r.now
			if err := s.repo.SaveEndpoint(ctx, endpoint); err != nil {
				return err
			}
		}
	}
	if r.fleet != nil {
		if err := s.saveFleet(ctx, r, active); err != nil {
			return err
		}
	}
	r.state.LastError = strings.Join(r.failed, "\n")
	if err := s.repo.SaveGitOpsState(ctx, r.state); err != nil {
		return err
	}
	s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{Action: "gitops.applied", Message: r.in.Sha, Data: map[string]any{"sha": r.in.Sha, "apps": len(r.in.Endpoints), "failed": len(r.failed)}})
	log.Info().Str("sha", r.in.Sha).Int("apps", len(r.in.Endpoints)).Int("failed", len(r.failed)).Msg("managed endpoints: repo applied")
	return nil
}

func (s *Service) retireEndpoint(ctx context.Context, r *review, endpoint *types.ManagedEndpoint) error {
	id := endpoint.Spec.ID
	endpoint.Status, endpoint.UpdatedAt = types.EndpointStatusRetired, r.now
	if err := s.repo.SaveEndpoint(ctx, endpoint); err != nil {
		return err
	}
	r.state.PerEndpoint[id] = types.GitOpsEndpointState{Path: id, ID: id, Status: types.GitOpsStatusRetired, UpdatedAt: r.now}
	s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{EndpointID: id, Action: "gitops.retired", Message: "removed from repo", Data: map[string]any{"sha": r.in.Sha}})
	return nil
}

// saveFleet applies config.yaml, minus placements of endpoints that no longer run.
func (s *Service) saveFleet(ctx context.Context, r *review, active map[string]*types.ManagedEndpoint) error {
	known := map[string]*types.ManagedEndpointSpec{}
	for id, e := range active {
		known[id] = &e.Spec
	}
	r.fleet.GitSHA = r.in.Sha
	if dropped := r.fleet.Prune(known); len(dropped) > 0 {
		r.state.FleetError = "skipped: " + strings.Join(dropped, "; ")
	}
	if err := s.repo.SaveFleet(ctx, r.fleet); err != nil {
		return err
	}
	s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{Action: "gitops.fleet", Message: r.in.Sha, Data: map[string]any{"fleet": r.fleet.Endpoints}})
	return nil
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
	StateIdle               = "idle" // serverless placement, scaled to zero
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
	serverless := false
	for gpu, p := range placements {
		want += p.MinReplicas
		gpus = append(gpus, gpu)
		serverless = serverless || p.Serverless
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
	case serverless:
		return StateIdle, "scaled to zero; the first request starts a replica"
	case want == 0:
		return StateWaitingForCapacity, "minReplicas is 0; fills spare " + strings.Join(gpus, ", ") + " capacity only"
	}
	return StateWaitingForCapacity, "no idle " + strings.Join(gpus, ", ") + " in an opted-in pool"
}
