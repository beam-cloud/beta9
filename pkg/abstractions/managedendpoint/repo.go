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

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v2"
)

// The endpoints repo deploys itself: its CI runs `beta9 endpoints deploy`,
// which deploys every app through the ordinary stub RPCs (DeployStub
// registers the app) and then calls ApplyRepo with config.yaml and each app's
// outcome: config.yaml publishes each app and places it on GPU types. Pull
// requests call the same RPC as a dry run.

const (
	imageCheckTimeout = 15 * time.Second
	applyLockKey      = "managed_endpoint:apply"
	applyLockTTL      = time.Minute
)

// tokenPricedKinds report token usage in their responses and may be priced per token.
var tokenPricedKinds = []types.EndpointKind{
	types.EndpointKindLLM, types.EndpointKindEmbedding, types.EndpointKindCustom, types.EndpointKindDecision,
}

// ApplyRepo validates one commit of the endpoints repo and, unless dry_run,
// records deploy outcomes, applies config.yaml and retires apps whose
// directory is gone. Validation problems are returned in errors. Commits
// apply one at a time across gateways; a concurrent CI run waits its turn.
func (s *Service) ApplyRepo(ctx context.Context, in *pb.ApplyRepoRequest) (*pb.ApplyRepoResponse, error) {
	out := &pb.ApplyRepoResponse{}
	return admin(s, ctx, out, func() error {
		opts := common.RedisLockOptions{TtlS: int(applyLockTTL.Seconds()), Retries: int(applyLockTTL / time.Second), RetryInterval: time.Second}
		return s.lock.WithLease(ctx, applyLockKey, opts, func(ctx context.Context) error {
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
	})
}

// review is one commit checked against this cluster.
type review struct {
	in      *pb.ApplyRepoRequest
	out     *pb.ApplyRepoResponse
	now     time.Time
	state   *types.GitOpsState
	apps    map[string]*types.ManagedEndpoint // what the repo declares at this commit
	present map[string]bool                   // endpoint ids with an app directory
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
	all, err := s.repo.ListEndpoints(ctx)
	if err != nil {
		return nil, err
	}
	active := map[string]*types.ManagedEndpoint{}
	for _, app := range all {
		if app.Enabled() {
			active[app.Spec.ID] = app
		}
	}
	now := time.Now()
	r := &review{
		in:      in,
		out:     out,
		now:     now,
		state:   &types.GitOpsState{RepoURL: in.RepoUrl, Ref: in.Ref, LastSHA: in.Sha, LastRunAt: now, PerEndpoint: map[string]types.GitOpsEndpointState{}},
		apps:    map[string]*types.ManagedEndpoint{},
		present: map[string]bool{},
	}
	for _, e := range in.Endpoints {
		s.reviewApp(ctx, r, e, previous, active)
	}
	s.reviewFleet(r)
	if !in.DryRun {
		return r.state, s.commitRepo(ctx, r, active)
	}
	// `deploy` names its commit in its dry run, `validate` does not. The
	// commit stays pending until it applies, so a CI run that dies in between
	// is visible.
	if in.Sha == "" {
		return nil, nil
	}
	previous.PendingSHA, previous.PendingAt = in.Sha, now
	return nil, s.repo.SaveGitOpsState(ctx, previous)
}

// reviewApp records one app directory's outcome and what it declares. A
// deployed app is what DeployStub registered; a dry run checks the spec CI read.
func (s *Service) reviewApp(ctx context.Context, r *review, e *pb.RepoEndpoint, previous *types.GitOpsState, active map[string]*types.ManagedEndpoint) {
	entry := types.GitOpsEndpointState{Path: e.Path, ID: e.Id, Status: types.GitOpsStatusApplied, StubID: e.StubId, Version: uint(e.Version), UpdatedAt: r.now}
	if entry.ID == "" {
		entry.ID = previous.PerEndpoint[e.Path].ID // a broken import keeps its app until the directory is gone
	}
	app, err := s.declaredApp(ctx, r, e, active)
	if err != nil {
		entry.Status, entry.Error = types.GitOpsStatusFailed, err.Error()
		r.fail("%s: %s", e.Path, entry.Error)
		r.failed = append(r.failed, e.Path+": "+entry.Error)
	} else {
		entry.ID = app.Spec.ID
		r.apps[app.Spec.ID] = app
		if r.in.DryRun {
			s.checkSpec(ctx, r, e, &app.Spec)
		}
	}
	if entry.ID != "" {
		r.present[entry.ID] = true
		if existing := active[entry.ID]; existing != nil && r.apps[entry.ID] == nil {
			r.apps[entry.ID] = existing // config.yaml is checked against what still runs
		}
	}
	r.state.PerEndpoint[e.Path] = entry
}

// declaredApp is the app one directory declares, or why it cannot be applied.
// On a deploy it is what DeployStub registered; a dry run checks the spec CI
// read from app.py (the ManagedEndpoint spec()).
func (s *Service) declaredApp(ctx context.Context, r *review, e *pb.RepoEndpoint, active map[string]*types.ManagedEndpoint) (*types.ManagedEndpoint, error) {
	if e.Error != "" {
		return nil, errors.New(e.Error)
	}
	if e.Id != e.Path {
		return nil, fmt.Errorf("id %q must equal its directory %q", e.Id, e.Path)
	}
	if !r.in.DryRun {
		app, err := s.repo.GetEndpoint(ctx, e.Id)
		if err != nil {
			return nil, err
		}
		if app == nil || app.StubID != e.StubId {
			return nil, fmt.Errorf("stub %s was not registered as %s; deploy it with this cluster's gateway", e.StubId, e.Id)
		}
		return app, nil
	}
	if strings.TrimSpace(e.SpecJson) == "" {
		return nil, errors.New("spec: missing")
	}
	app := &types.ManagedEndpoint{StubID: e.StubId, Version: uint(e.Version), Status: types.EndpointStatusActive}
	if err := json.Unmarshal([]byte(e.SpecJson), &app.Spec); err != nil {
		return nil, fmt.Errorf("spec: %w", err)
	}
	app.Spec.Normalize()
	if err := app.Spec.Validate(); err != nil {
		return nil, fmt.Errorf("spec: %w", err)
	}
	return app, nil
}

// reviewFleet parses config.yaml and checks every entry against the app it
// publishes: token prices need an engine that reports tokens, OpenRouter
// metadata must fit the engine kind, and placements need declared GPUs.
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
		app := r.apps[id]
		if app == nil {
			r.fail("config.yaml: %s is not an app in the repo", id)
			continue
		}
		if entry.Enabled && entry.Pricing.PerToken() && !slices.Contains(tokenPricedKinds, app.Spec.Kind) {
			r.fail("config.yaml: %s: token pricing needs an engine reporting usage (%v); %s apps are priced per request", id, tokenPricedKinds, app.Spec.Kind)
		}
		if entry.Enabled && entry.OpenRouter != nil {
			if err := entry.OpenRouter.ValidateFor(app.Spec.Kind, entry.Catalog, entry.Pricing); err != nil {
				r.fail("config.yaml: %s: openrouter: %v", id, err)
			}
		}
		for _, gpu := range slices.Sorted(maps.Keys(entry.GPUs)) {
			_, declared := app.Spec.Gpu[gpu]
			switch {
			case !declared:
				r.fail("config.yaml: %s does not declare gpu %q in its app", id, gpu)
			case r.in.DryRun && entry.Enabled && len(s.controller.pools(gpu)) == 0:
				r.warn("config.yaml: %s: no pool on this cluster hosts %s", id, gpu)
			}
		}
	}
}

// commitRepo records the deploy: the commit and publication are stamped on
// every deployed app, apps whose directory is gone are retired (the
// controller drains them), and config.yaml becomes the fleet.
func (s *Service) commitRepo(ctx context.Context, r *review, active map[string]*types.ManagedEndpoint) error {
	for id, app := range active {
		if r.present[id] {
			continue
		}
		app.Status, app.UpdatedAt = types.EndpointStatusRetired, r.now
		if err := s.repo.SaveEndpoint(ctx, app); err != nil {
			return err
		}
		delete(active, id)
		r.state.PerEndpoint[id] = types.GitOpsEndpointState{Path: id, ID: id, Status: types.GitOpsStatusRetired, UpdatedAt: r.now}
		s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{EndpointID: id, Action: "gitops.retired", Message: "removed from repo", Data: map[string]any{"sha": r.in.Sha}})
	}
	if r.fleet != nil {
		// Placements of apps that no longer run are dropped, never blocking the rest.
		r.fleet.GitSHA = r.in.Sha
		if dropped := r.fleet.Prune(active); len(dropped) > 0 {
			r.state.FleetError = "skipped: " + strings.Join(dropped, "; ")
		}
		if err := s.repo.SaveFleet(ctx, r.fleet); err != nil {
			return err
		}
		s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{Action: "gitops.fleet", Message: r.in.Sha, Data: map[string]any{"fleet": r.fleet.Endpoints}})
	}
	for id, app := range active {
		if r.state.PerEndpoint[id].Status != types.GitOpsStatusApplied {
			continue
		}
		publication, published := app.Publication, app.Published
		if r.fleet != nil {
			e, ok := r.fleet.Endpoints[id]
			publication, published = e.Publication, ok && e.Enabled
		}
		if app.GitSHA != r.in.Sha || app.Published != published || mustJSON(app.Publication) != mustJSON(publication) {
			app.GitSHA, app.Publication, app.Published, app.UpdatedAt = r.in.Sha, publication, published, r.now
			if err := s.repo.SaveEndpoint(ctx, app); err != nil {
				return err
			}
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

// checkSpec is the pre-merge check of one app: known GPU types with a pool on
// this cluster, and a reachable image.
func (s *Service) checkSpec(ctx context.Context, r *review, e *pb.RepoEndpoint, spec *types.ManagedEndpointSpec) {
	for _, gpu := range slices.Sorted(maps.Keys(spec.Gpu)) {
		if gpu != types.CPUInventoryKey && len(s.controller.pools(gpu)) == 0 {
			r.warn("%s: no pool on this cluster hosts %s", e.Path, gpu)
		}
	}
	if e.Image == "" {
		return
	}
	ref, err := name.ParseReference(e.Image)
	if err != nil {
		r.fail("%s: image %q: %v", e.Path, e.Image, err)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, imageCheckTimeout)
	defer cancel()
	_, err = remote.Head(ref, remote.WithContext(ctx))
	var terr *transport.Error
	switch {
	case err == nil:
	case errors.As(err, &terr) && (terr.StatusCode == http.StatusUnauthorized || terr.StatusCode == http.StatusForbidden):
		r.warn("%s: image %s needs registry credentials; not verified", e.Path, e.Image)
	default:
		r.fail("%s: image %s: %v", e.Path, e.Image, err)
	}
}

// parseFleet reads config.yaml: endpoint id -> {enabled, gpus, catalog, public, allowed_workspaces, pricing, openrouter}.
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
