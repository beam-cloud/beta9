package types

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Managed endpoints are inference endpoints owned by the platform, not by a
// user workspace. A git repository is the source of truth: each app.py there
// declares one endpoint (ManagedEndpointSpec, deployed as a stub in the admin
// workspace) and fleet.yaml declares how spare GPU capacity is divided between
// endpoints (Fleet). The controller fills replicas accordingly and the /v1
// route serves them with an OpenAI/OpenRouter-compatible API.

const (
	StubTypeManagedEndpoint           string = "managed_endpoint"
	StubTypeManagedEndpointDeployment string = "managed_endpoint/deployment"
)

func (t StubType) IsManagedEndpoint() bool { return t.Kind() == StubTypeManagedEndpoint }

type EndpointKind string

const (
	EndpointKindLLM       EndpointKind = "llm"
	EndpointKindEmbedding EndpointKind = "embedding"
	EndpointKindImage     EndpointKind = "image"
	EndpointKindCustom    EndpointKind = "custom"
)

// EndpointRoute is an OpenAI-style route suffix under /v1 that an endpoint serves.
type EndpointRoute string

const (
	EndpointRouteChatCompletions  EndpointRoute = "chat/completions"
	EndpointRouteCompletions      EndpointRoute = "completions"
	EndpointRouteEmbeddings       EndpointRoute = "embeddings"
	EndpointRouteImageGenerations EndpointRoute = "images/generations"
	EndpointRouteImageEdits       EndpointRoute = "images/edits"
	EndpointRouteInvoke           EndpointRoute = "invoke"
)

// kindRoutes lists the routes each kind may declare. The routes a kind serves
// by default are the leading ones: both completion routes for an LLM, the
// first route for everything else.
var kindRoutes = map[EndpointKind][]EndpointRoute{
	EndpointKindLLM:       {EndpointRouteChatCompletions, EndpointRouteCompletions, EndpointRouteEmbeddings, EndpointRouteImageGenerations},
	EndpointKindEmbedding: {EndpointRouteEmbeddings},
	EndpointKindImage:     {EndpointRouteImageGenerations, EndpointRouteImageEdits},
	EndpointKindCustom:    {EndpointRouteInvoke},
}

func defaultRoutes(kind EndpointKind) []EndpointRoute {
	routes := kindRoutes[kind]
	n := 1
	if kind == EndpointKindLLM {
		n = 2
	}
	return routes[:min(n, len(routes))] // unknown kinds get none; Validate rejects them
}

// CPUInventoryKey is the GPU key for CPU-only placement.
const CPUInventoryKey = "cpu"

// GPUKey canonicalizes a GPU type name into the key used by specs, fleet.yaml
// and inventory: "h100" -> "H100", "" / "cpu" -> "cpu".
func GPUKey(gpu string) string {
	gpu = strings.TrimSpace(gpu)
	if gpu == "" || strings.EqualFold(gpu, CPUInventoryKey) || NormalizeGPUType(gpu) == NO_GPU {
		return CPUInventoryKey
	}
	return string(NormalizeGPUType(gpu))
}

// GpuSpec is how an endpoint runs on one GPU type: engine args appended to the
// entrypoint (restart-class settings) and the harness seed (live settings).
type GpuSpec struct {
	// Count is GPUs per replica (tensor parallelism); ignored for cpu.
	Count      uint32         `json:"count,omitempty"`
	EngineArgs []string       `json:"engine_args,omitempty"`
	Harness    map[string]any `json:"harness,omitempty"`
}

// Catalog is the public listing metadata for an endpoint (OpenRouter shape).
type Catalog struct {
	Name                string   `json:"name,omitempty"`
	Description         string   `json:"description,omitempty"`
	HFID                string   `json:"hf_id,omitempty"`
	ContextLength       uint32   `json:"context_length,omitempty"`
	MaxCompletionTokens uint32   `json:"max_completion_tokens,omitempty"`
	Tokenizer           string   `json:"tokenizer,omitempty"`
	InstructType        string   `json:"instruct_type,omitempty"`
	Modalities          []string `json:"modalities,omitempty"`
	SupportedParameters []string `json:"supported_parameters,omitempty"`
	Public              bool     `json:"public"`
	AllowedWorkspaces   []string `json:"allowed_workspaces,omitempty"`
	Free                bool     `json:"free,omitempty"`
}

// Pricing holds USD decimal strings per billable unit. Empty means free for
// that dimension. Amounts are parsed with arbitrary precision at deploy time.
type Pricing struct {
	PromptTokens       string `json:"prompt_tokens,omitempty"`
	CompletionTokens   string `json:"completion_tokens,omitempty"`
	CachedPromptTokens string `json:"cached_prompt_tokens,omitempty"`
	Request            string `json:"request,omitempty"`
	Image              string `json:"image,omitempty"`
}

// IsZero reports whether no dimension is priced.
func (p Pricing) IsZero() bool { return p == Pricing{} }

// Validate checks that every set dimension is a non-negative USD decimal.
func (p Pricing) Validate() error {
	for name, value := range map[string]string{
		"prompt_tokens": p.PromptTokens, "completion_tokens": p.CompletionTokens,
		"cached_prompt_tokens": p.CachedPromptTokens, "request": p.Request, "image": p.Image,
	} {
		if _, err := PricingRat(value); err != nil {
			return fmt.Errorf("pricing.%s: %w", name, err)
		}
	}
	return nil
}

var pricingPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]{1,18})?$`)

// PricingRat parses a pricing dimension into an exact rational; empty is zero.
func PricingRat(value string) (*big.Rat, error) {
	if value == "" {
		return new(big.Rat), nil
	}
	rat, ok := new(big.Rat).SetString(value)
	if !ok || !pricingPattern.MatchString(value) {
		return nil, fmt.Errorf("%q is not a non-negative decimal amount", value)
	}
	return rat, nil
}

// ManagedEndpointSpec is the repo contract: everything an endpoint app declares.
// Where replicas run is not part of it; see Fleet.
type ManagedEndpointSpec struct {
	ID      string       `json:"id"`
	Kind    EndpointKind `json:"kind"`
	Engine  string       `json:"engine,omitempty"`
	Port    uint32       `json:"port"`
	Health  string       `json:"health,omitempty"`
	Metrics string       `json:"metrics,omitempty"`
	// Gpu maps a GPU key ("H100", "cpu") to how the engine runs there. An
	// endpoint may only be placed on GPU types it lists.
	Gpu     map[string]GpuSpec `json:"gpu"`
	Routes  []EndpointRoute    `json:"routes,omitempty"`
	Pricing Pricing            `json:"pricing"`
	Catalog Catalog            `json:"catalog"`
	// Harness enables the in-engine harness (live knobs over EndpointHarnessService).
	Harness bool `json:"harness"`
	// DrainSeconds is the grace an evicted or retired replica gets to finish
	// in-flight requests before the worker kills it.
	DrainSeconds uint32   `json:"drain_seconds"`
	Entrypoint   []string `json:"entrypoint,omitempty"`
}

// ManagedEndpointStubConfig is embedded in StubConfigV1 for managed stubs.
type ManagedEndpointStubConfig struct {
	Endpoint *ManagedEndpointSpec `json:"endpoint,omitempty"`
	// GitSHA is the source revision the deployer built this stub from.
	GitSHA string `json:"git_sha,omitempty"`
}

// ManagedEndpointValidation configures spec validation. Empty allow lists
// allow everything.
type ManagedEndpointValidation struct {
	AllowedEngines []string
	AllowedKinds   []EndpointKind
}

var endpointIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*(/[a-z0-9][a-z0-9._-]*)?$`)

func cleanPath(p string) string { return "/" + strings.TrimPrefix(strings.TrimSpace(p), "/") }

// Normalize fills defaults so downstream code can rely on a canonical spec.
func (s *ManagedEndpointSpec) Normalize() {
	s.ID = strings.ToLower(strings.TrimSpace(s.ID))
	s.Engine = strings.ToLower(strings.TrimSpace(s.Engine))
	if s.Kind == "" {
		s.Kind = EndpointKindCustom
	}
	if s.Port == 0 {
		s.Port = 8000
	}
	s.Health = cleanPath(cmp.Or(s.Health, "health"))
	if s.Metrics != "" {
		s.Metrics = cleanPath(s.Metrics)
	}
	if len(s.Routes) == 0 {
		s.Routes = defaultRoutes(s.Kind)
	}
	for i := range s.Routes {
		s.Routes[i] = EndpointRoute(strings.Trim(strings.TrimSpace(string(s.Routes[i])), "/"))
	}
	if s.DrainSeconds == 0 {
		s.DrainSeconds = 5
	}
	gpu := make(map[string]GpuSpec, len(s.Gpu))
	for key, spec := range s.Gpu {
		key = GPUKey(key)
		if key == CPUInventoryKey {
			spec.Count = 0
		} else {
			spec.Count = max(spec.Count, 1)
		}
		gpu[key] = spec
	}
	if len(gpu) == 0 {
		gpu[CPUInventoryKey] = GpuSpec{}
	}
	s.Gpu = gpu
	if s.Catalog.Name == "" {
		s.Catalog.Name = s.ID
	}
}

// Validate checks the spec against platform policy. Call Normalize first.
func (s *ManagedEndpointSpec) Validate(policy ManagedEndpointValidation) error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if !endpointIDPattern.MatchString(s.ID) {
		fail("id %q must look like vendor/slug (lowercase, [a-z0-9._-])", s.ID)
	}
	allowed, validKind := kindRoutes[s.Kind]
	switch {
	case !validKind:
		fail("kind %q is not one of llm, embedding, image, custom", s.Kind)
	case len(policy.AllowedKinds) > 0 && !slices.Contains(policy.AllowedKinds, s.Kind):
		fail("kind %q is not enabled on this cluster", s.Kind)
	}
	if s.Engine != "" && len(policy.AllowedEngines) > 0 && !slices.ContainsFunc(policy.AllowedEngines, func(e string) bool { return strings.EqualFold(e, s.Engine) }) {
		fail("engine %q is not in the allowed engine list", s.Engine)
	}
	if s.Port == 0 || s.Port > 65535 {
		fail("port %d is invalid", s.Port)
	}
	if len(s.Entrypoint) == 0 {
		fail("entrypoint is required")
	}
	for _, route := range s.Routes {
		if validKind && !slices.Contains(allowed, route) {
			fail("route %q is not valid for kind %q", route, s.Kind)
		}
	}
	if err := s.Pricing.Validate(); err != nil {
		errs = append(errs, err)
	}
	if s.Kind == EndpointKindImage && s.Pricing.Image == "" && s.Pricing.Request == "" && !s.Catalog.Free {
		fail("image endpoints must price per image or per request, or be marked free")
	}
	for key, spec := range s.Gpu {
		if key != CPUInventoryKey && !KnownGPUType(GpuType(key)) {
			fail("gpu %q is not a known GPU type", key)
		}
		if spec.Count > 8 {
			fail("gpu %q count %d exceeds 8", key, spec.Count)
		}
	}
	return errors.Join(errs...)
}

// ServesRoute reports whether the endpoint declares a route.
func (s *ManagedEndpointSpec) ServesRoute(route EndpointRoute) bool {
	return slices.Contains(s.Routes, route)
}

// --- Fleet ---------------------------------------------------------------------

// Fleet is fleet.yaml: for each GPU key, the endpoints that fill its idle GPUs
// in priority order. Idle GPUs go to the first entry until it reaches its cap
// (none by default), then to the next; when a higher entry is short of GPUs
// the entries below it give theirs back. It is the only thing that decides
// where replicas run.
type Fleet struct {
	GitSHA    string                  `json:"git_sha,omitempty"`
	Priority  map[string][]FleetEntry `json:"priority"`
	UpdatedAt time.Time               `json:"updated_at"`
}

// FleetEntry is one endpoint in a GPU type's priority list. Max is the most
// replicas it runs on that type; 0 means every idle GPU it can get.
type FleetEntry struct {
	EndpointID string `json:"endpoint_id"`
	Max        uint32 `json:"max,omitempty"`
}

const maxFleetReplicas = 64

// Normalize canonicalizes GPU keys and endpoint ids and drops empty lists.
func (f *Fleet) Normalize() {
	out := make(map[string][]FleetEntry, len(f.Priority))
	for gpu, entries := range f.Priority {
		var kept []FleetEntry
		for _, e := range entries {
			if e.EndpointID = strings.ToLower(strings.TrimSpace(e.EndpointID)); e.EndpointID != "" {
				kept = append(kept, e)
			}
		}
		if len(kept) > 0 {
			out[GPUKey(gpu)] = kept
		}
	}
	f.Priority = out
}

// Validate checks GPU keys, caps and duplicates. CPU entries must be capped:
// there is no GPU inventory to run out of. Call Normalize first.
func (f *Fleet) Validate() error {
	var errs []error
	for gpu, entries := range f.Priority {
		if gpu != CPUInventoryKey && !KnownGPUType(GpuType(gpu)) {
			errs = append(errs, fmt.Errorf("%s is not a known GPU type", gpu))
		}
		seen := map[string]bool{}
		for _, e := range entries {
			if seen[e.EndpointID] {
				errs = append(errs, fmt.Errorf("%s: %s is listed twice", gpu, e.EndpointID))
			}
			seen[e.EndpointID] = true
			if e.Max > maxFleetReplicas {
				errs = append(errs, fmt.Errorf("%s: %s max %d exceeds %d", gpu, e.EndpointID, e.Max, maxFleetReplicas))
			}
			if gpu == CPUInventoryKey && e.Max == 0 {
				errs = append(errs, fmt.Errorf("cpu: %s needs a replica count", e.EndpointID))
			}
		}
	}
	return errors.Join(errs...)
}

// Prune drops entries for endpoints that are not deployed or do not declare
// the GPU type in their app, so one broken deploy never blocks the rest of the
// fleet. It returns one message per dropped entry.
func (f *Fleet) Prune(endpoints map[string]*ManagedEndpointSpec) []string {
	var dropped []string
	for gpu, entries := range f.Priority {
		kept := entries[:0]
		for _, e := range entries {
			spec, ok := endpoints[e.EndpointID]
			if !ok {
				dropped = append(dropped, fmt.Sprintf("%s is not a deployed endpoint", e.EndpointID))
				continue
			}
			if spec != nil {
				if _, ok := spec.Gpu[gpu]; !ok {
					dropped = append(dropped, fmt.Sprintf("%s does not declare gpu %q in its app", e.EndpointID, gpu))
					continue
				}
			}
			kept = append(kept, e)
		}
		if len(kept) == 0 {
			delete(f.Priority, gpu)
		} else {
			f.Priority[gpu] = kept
		}
	}
	slices.Sort(dropped)
	return slices.Compact(dropped)
}

// GPUs returns the GPU keys with a priority list, sorted.
func (f *Fleet) GPUs() []string {
	out := make([]string, 0, len(f.Priority))
	for gpu := range f.Priority {
		out = append(out, gpu)
	}
	slices.Sort(out)
	return out
}

// Lists reports whether the endpoint is in the GPU type's priority list.
func (f *Fleet) Lists(endpointID, gpu string) bool {
	return slices.ContainsFunc(f.Priority[gpu], func(e FleetEntry) bool { return e.EndpointID == endpointID })
}

// Placements returns one endpoint's (gpu, max) pairs, GPU keys sorted.
func (f *Fleet) Placements(endpointID string) []FleetTarget {
	var out []FleetTarget
	for gpu, entries := range f.Priority {
		for _, e := range entries {
			if e.EndpointID == endpointID {
				out = append(out, FleetTarget{GPU: gpu, Max: e.Max})
			}
		}
	}
	slices.SortFunc(out, func(a, b FleetTarget) int { return strings.Compare(a.GPU, b.GPU) })
	return out
}

// FleetTarget is one endpoint's place on one GPU type: the type and its
// replica cap there (0 = every idle GPU).
type FleetTarget struct {
	GPU string
	Max uint32
}

func (t FleetTarget) IsCPU() bool { return t.GPU == CPUInventoryKey }

// --- Metering ------------------------------------------------------------------

// MeterBucket is one closed minute of usage for one kind, not yet delivered to
// the billing meter. Rows are per workspace and model.
type MeterBucket struct {
	Key   string
	Kind  UsageKind
	Start time.Time
	Rows  []MeterRow
}

type MeterRow struct {
	WorkspaceID string
	Model       string
	Usage       Usage
}

// --- Registry ------------------------------------------------------------------

type EndpointStatus string

const (
	EndpointStatusActive  EndpointStatus = "active"
	EndpointStatusRetired EndpointStatus = "retired"
)

// ManagedEndpoint is the registry record for a deployed endpoint: the spec of
// its current version and which stub serves it. A new deploy bumps Version;
// the controller replaces replicas of older versions.
type ManagedEndpoint struct {
	Spec      ManagedEndpointSpec `json:"spec"`
	StubID    string              `json:"stub_id"`
	Version   uint                `json:"version"`
	GitSHA    string              `json:"git_sha,omitempty"`
	Status    EndpointStatus      `json:"status"`
	CreatedAt time.Time           `json:"created_at"`
	UpdatedAt time.Time           `json:"updated_at"`
}

// Enabled reports whether the endpoint may be filled and routed to.
func (e *ManagedEndpoint) Enabled() bool { return e.Status == EndpointStatusActive }

// --- Replicas ------------------------------------------------------------------

type ReplicaStatus string

const (
	ReplicaStatusScheduling ReplicaStatus = "scheduling"
	ReplicaStatusLoading    ReplicaStatus = "loading"
	ReplicaStatusReady      ReplicaStatus = "ready"
	ReplicaStatusDraining   ReplicaStatus = "draining"
	ReplicaStatusEvicting   ReplicaStatus = "evicting"
	ReplicaStatusEvicted    ReplicaStatus = "evicted"
	ReplicaStatusFailed     ReplicaStatus = "failed"
	ReplicaStatusStopped    ReplicaStatus = "stopped"
)

// Terminal reports whether a replica in this status will never serve again.
func (s ReplicaStatus) Terminal() bool {
	return s == ReplicaStatusEvicted || s == ReplicaStatusFailed || s == ReplicaStatusStopped
}

// ReplicaCapacity is the harness- or probe-reported serving capacity.
type ReplicaCapacity struct {
	InFlight            int64 `json:"in_flight"`
	MaxConcurrency      int64 `json:"max_concurrency"`
	Running             int64 `json:"running"`
	Waiting             int64 `json:"waiting"`
	KVCacheFreeMilli    int64 `json:"kv_cache_free_milli"`
	DecodeTokensPerSec  int64 `json:"decode_tokens_per_sec"`
	PromptTokensPerSec  int64 `json:"prompt_tokens_per_sec"`
	TTFTMs              int64 `json:"ttft_ms"`
	TPOTMs              int64 `json:"tpot_ms"`
	PrefixCacheHitMilli int64 `json:"prefix_cache_hit_milli"`
}

// ReplicaConfig is the live harness config of one replica: what an admin last
// asked for (Revision/Config) and what the engine reported back. Live config
// dies with the replica; durable settings belong in the repo.
type ReplicaConfig struct {
	Revision uint64          `json:"revision"`
	Config   json.RawMessage `json:"config,omitempty"`
	// Author is the caller's label for the change; Actor is the authenticated
	// token that made it.
	Author string    `json:"author,omitempty"`
	Actor  string    `json:"actor,omitempty"`
	SetAt  time.Time `json:"set_at,omitempty"`
	// AckedRevision, Applied, Error and Effective describe the engine's
	// answer to the most recent revision it processed.
	AckedRevision uint64          `json:"acked_revision"`
	Applied       bool            `json:"applied"`
	Error         string          `json:"error,omitempty"`
	Effective     json.RawMessage `json:"effective,omitempty"`
	AckedAt       time.Time       `json:"acked_at,omitempty"`
}

// Acked reports whether the engine has answered the current revision.
func (c ReplicaConfig) Acked() bool { return c.AckedRevision >= c.Revision }

// Ack records the engine's answer to revision. Only revisions that were
// issued and not already superseded by a later answer are accepted, so a
// buggy or restarting harness cannot make future revisions look acknowledged.
func (c *ReplicaConfig) Ack(revision uint64, applied bool, errMsg string, effective json.RawMessage, now time.Time) bool {
	if revision > c.Revision || revision < c.AckedRevision {
		return false
	}
	c.AckedRevision, c.Applied, c.Error, c.AckedAt = revision, applied, errMsg, now
	c.Effective = nil
	if json.Valid(effective) {
		c.Effective = effective
	}
	return true
}

// EndpointReplica is one running container serving an endpoint on one GPU type.
type EndpointReplica struct {
	ID          string `json:"id"`
	EndpointID  string `json:"endpoint_id"`
	Version     uint   `json:"version"`
	GPU         string `json:"gpu"`
	GPUCount    uint32 `json:"gpu_count"`
	Locality    string `json:"locality,omitempty"`
	PoolName    string `json:"pool_name,omitempty"`
	ContainerID string `json:"container_id"`
	WorkerID    string `json:"worker_id,omitempty"`
	MachineID   string `json:"machine_id,omitempty"`
	// ProviderWorkspaceID is set when the replica runs on a workspace's
	// contributed (provider pool) machine; that workspace earns a share of
	// the revenue routed to the replica.
	ProviderWorkspaceID string        `json:"provider_workspace_id,omitempty"`
	Address             string        `json:"address,omitempty"`
	Status              ReplicaStatus `json:"status"`
	StatusReason        string        `json:"status_reason,omitempty"`
	// SecretHash is the SHA-256 of the per-replica secret handed to the
	// container as BEAM_REPLICA_SECRET; harness RPCs must present it.
	SecretHash     string `json:"secret_hash,omitempty"`
	HarnessEnabled bool   `json:"harness_enabled"`
	// Probe is the port and paths of the deployment this replica runs,
	// snapshotted at start: an old version keeps its own contract while a
	// newer one is rolling out.
	Probe        ReplicaProbe    `json:"probe"`
	Config       ReplicaConfig   `json:"config"`
	Capacity     ReplicaCapacity `json:"capacity"`
	Capabilities json.RawMessage `json:"capabilities,omitempty"`
	// EngineMetrics is the engine status the harness attached to its last heartbeat.
	EngineMetrics json.RawMessage `json:"engine_metrics,omitempty"`
	StartedAt     time.Time       `json:"started_at"`
	// LoadingSince is when the current loading phase began: container start,
	// or the moment a ready engine went back to loading (reload, failing
	// health check). The loading grace runs from here, not from StartedAt.
	LoadingSince  time.Time `json:"loading_since,omitempty"`
	ReadyAt       time.Time `json:"ready_at,omitempty"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	EndedAt       time.Time `json:"ended_at,omitempty"`
	DrainDeadline time.Time `json:"drain_deadline,omitempty"`
}

// ReplicaProbe is the immutable probe contract of one deployment version.
type ReplicaProbe struct {
	Port   uint32 `json:"port"`
	Health string `json:"health,omitempty"`
	// Metrics is the Prometheus path to scrape for capacity; empty for
	// engines that expose none (non-LLM kinds).
	Metrics string `json:"metrics,omitempty"`
}

// Serving reports whether the replica may receive traffic.
func (r *EndpointReplica) Serving() bool { return r != nil && r.Status == ReplicaStatusReady }

// EnterLoading moves the replica into loading and starts a fresh loading
// phase unless it is already in one.
func (r *EndpointReplica) EnterLoading(now time.Time, reason string) {
	if r.Status != ReplicaStatusLoading {
		r.LoadingSince = now
	}
	r.Status = ReplicaStatusLoading
	r.StatusReason = reason
}

// LoadingFor is how long the current loading phase has lasted.
func (r *EndpointReplica) LoadingFor(now time.Time) time.Duration {
	if r.LoadingSince.IsZero() {
		return now.Sub(r.StartedAt)
	}
	return now.Sub(r.LoadingSince)
}

// Alive reports whether the replica is scheduling, loading or ready, i.e.
// counts toward a target's live set (draining and evicting replicas do not).
func (r *EndpointReplica) Alive() bool {
	return r.Status == ReplicaStatusScheduling || r.Status == ReplicaStatusLoading || r.Status == ReplicaStatusReady
}

// --- GitOps ------------------------------------------------------------------------

type GitOpsStatus string

const (
	GitOpsStatusApplied GitOpsStatus = "applied"
	GitOpsStatusFailed  GitOpsStatus = "failed"
	GitOpsStatusRetired GitOpsStatus = "retired"
)

// GitOpsEndpointState is the apply state of one repo directory.
type GitOpsEndpointState struct {
	Path       string       `json:"path"`
	ID         string       `json:"id"`
	AppliedSHA string       `json:"applied_sha,omitempty"`
	Status     GitOpsStatus `json:"status"`
	Error      string       `json:"error,omitempty"`
	StubID     string       `json:"stub_id,omitempty"`
	Version    uint         `json:"version,omitempty"`
	UpdatedAt  time.Time    `json:"updated_at"`
}

// GitOpsState is the reconciler's view of the endpoints repo.
type GitOpsState struct {
	RepoURL   string    `json:"repo_url"`
	Ref       string    `json:"ref"`
	LastSHA   string    `json:"last_sha,omitempty"`
	TargetSHA string    `json:"target_sha,omitempty"`
	LastRunAt time.Time `json:"last_run_at,omitempty"`
	LastError string    `json:"last_error,omitempty"`
	// FleetError is why fleet.yaml was not applied, or which placements were
	// skipped ("skipped: ...") when it was. FleetSHA is the commit whose
	// fleet.yaml was last applied (or rejected as invalid); it trails LastSHA
	// only while a fleet write is still to be retried.
	FleetError  string                         `json:"fleet_error,omitempty"`
	FleetSHA    string                         `json:"fleet_sha,omitempty"`
	Running     bool                           `json:"running"`
	PerEndpoint map[string]GitOpsEndpointState `json:"per_endpoint"`
	UpdatedAt   time.Time                      `json:"updated_at"`

	// In-flight deployer run. RunID ties the deployer's report back to this
	// state; ContainerID and TokenID are cleaned up when the run finishes.
	RunID       string    `json:"run_id,omitempty"`
	ContainerID string    `json:"container_id,omitempty"`
	TokenID     string    `json:"token_id,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
}

// GitOpsReport is what the deployer posts back after applying one SHA: one
// result per app directory found in the repo plus the parsed fleet.yaml.
type GitOpsReport struct {
	RunID   string               `json:"run_id"`
	SHA     string               `json:"sha"`
	Error   string               `json:"error,omitempty"`
	Results []GitOpsDeployResult `json:"results"`
	// FleetYAML is the raw fleet.yaml ({gpu: [endpoint | {endpoint: max}]}); empty
	// when the repo has none (nothing is placed until it does).
	FleetYAML string `json:"fleet_yaml,omitempty"`
}

// GitOpsDeployResult is the outcome for one app directory. ID is empty when
// the directory's app failed to import.
type GitOpsDeployResult struct {
	Path    string `json:"path"`
	ID      string `json:"id"`
	OK      bool   `json:"ok"`
	Skipped bool   `json:"skipped"` // unchanged since the last applied SHA
	Error   string `json:"error,omitempty"`
	StubID  string `json:"stub_id,omitempty"`
	Version uint   `json:"version,omitempty"`
}

// --- Traffic -------------------------------------------------------------------------

// RouteSample is one completed /v1 request observed by the router.
type RouteSample struct {
	EndpointID string `json:"endpoint_id"`
	GPU        string `json:"gpu"`
	ReplicaID  string `json:"replica_id"`
	// ConfigRevision is the live config the replica had acknowledged when it
	// served the request, so a tuning experiment is measured per revision.
	ConfigRevision   uint64        `json:"config_revision,omitempty"`
	StatusCode       int           `json:"status_code"`
	PromptTokens     int64         `json:"prompt_tokens"`
	CompletionTokens int64         `json:"completion_tokens"`
	Images           int64         `json:"images"`
	CostMicroUSD     int64         `json:"cost_micro_usd"`
	Duration         time.Duration `json:"duration"`
	TTFT             time.Duration `json:"ttft"`
	QueueWait        time.Duration `json:"queue_wait"`
	At               time.Time     `json:"at"`
}

// Failed reports whether the sample counts as an error.
func (s RouteSample) Failed() bool { return s.StatusCode >= 500 || s.StatusCode == 0 }

// RouteMetrics aggregates RouteSamples over a window.
type RouteMetrics struct {
	EndpointID       string        `json:"endpoint_id"`
	GPU              string        `json:"gpu,omitempty"`
	ReplicaID        string        `json:"replica_id,omitempty"`
	ConfigRevision   uint64        `json:"config_revision,omitempty"`
	Window           time.Duration `json:"window"`
	Requests         int64         `json:"requests"`
	Errors           int64         `json:"errors"`
	PromptTokens     int64         `json:"prompt_tokens"`
	CompletionTokens int64         `json:"completion_tokens"`
	Images           int64         `json:"images"`
	CostMicroUSD     int64         `json:"cost_micro_usd"`
	DurationSumMs    int64         `json:"duration_sum_ms"`
	TTFTSumMs        int64         `json:"ttft_sum_ms"`
	TTFTCount        int64         `json:"ttft_count"`
	QueueWaitSumMs   int64         `json:"queue_wait_sum_ms"`
}

func (m RouteMetrics) ErrorRate() float64 {
	if m.Requests == 0 {
		return 0
	}
	return float64(m.Errors) / float64(m.Requests)
}

func (m RouteMetrics) MeanTTFTMs() int64 {
	if m.TTFTCount == 0 {
		return 0
	}
	return m.TTFTSumMs / m.TTFTCount
}

// MeanTPOTMs approximates time per output token from duration minus TTFT.
func (m RouteMetrics) MeanTPOTMs() int64 {
	if m.CompletionTokens == 0 {
		return 0
	}
	return max(m.DurationSumMs-m.TTFTSumMs, 0) / m.CompletionTokens
}

// --- Usage and earnings ------------------------------------------------------------

// UsageKind separates what a workspace spent calling models from what it
// earned serving them on contributed machines.
type UsageKind string

const (
	UsageSpend  UsageKind = "spend"
	UsageEarned UsageKind = "earned"
)

// Usage is what one workspace consumed on (or earned from) one model: the
// daily counters behind the usage page and provider payouts.
type Usage struct {
	Requests         int64 `json:"requests"`
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	Images           int64 `json:"images"`
	MicroUSD         int64 `json:"micro_usd"`
}

func (u *Usage) Add(o Usage) {
	u.Requests += o.Requests
	u.PromptTokens += o.PromptTokens
	u.CompletionTokens += o.CompletionTokens
	u.Images += o.Images
	u.MicroUSD += o.MicroUSD
}

// UsageReport is a workspace's usage (or provider earnings) over a range of days.
type UsageReport struct {
	Total    Usage            `json:"total"`
	PerModel map[string]Usage `json:"per_model"`
	PerDay   map[string]Usage `json:"per_day"`
}
