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

// Managed endpoints are platform-owned inference endpoints deployed from a git
// repository: app.py declares a ManagedEndpointSpec, fleet.yaml a Fleet.

const (
	StubTypeManagedEndpoint           string = "managed_endpoint"
	StubTypeManagedEndpointDeployment string = "managed_endpoint/deployment"
	StubTypePlatformDeployer          string = "platform_deployer"
)

func (t StubType) IsManagedEndpoint() bool { return t.Kind() == StubTypeManagedEndpoint }

// IsPlatformWorkload identifies service-owned containers, not paying callers.
// Platform deployer stubs can only be created internally by the control plane.
func (t StubType) IsPlatformWorkload() bool {
	return t.IsManagedEndpoint() || t.Kind() == StubTypePlatformDeployer
}

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

// kindRoutes lists the routes each kind may declare; defaults are the leading
// ones (both completion routes for an LLM, the first for everything else).
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
	return routes[:min(n, len(routes))]
}

// CPUInventoryKey is the GPU key for CPU-only placement.
const CPUInventoryKey = "cpu"

// GPUKey canonicalizes a GPU type name: "h100" -> "H100", "" -> "cpu".
func GPUKey(gpu string) string {
	gpu = strings.TrimSpace(gpu)
	if gpu == "" || strings.EqualFold(gpu, CPUInventoryKey) || NormalizeGPUType(gpu) == NO_GPU {
		return CPUInventoryKey
	}
	return string(NormalizeGPUType(gpu))
}

// GpuSpec is how an endpoint runs on one GPU type.
type GpuSpec struct {
	Count      uint32         `json:"count,omitempty"` // GPUs per replica
	EngineArgs []string       `json:"engine_args,omitempty"`
	Config     map[string]any `json:"config,omitempty"`
}

// Catalog contains display metadata; pricing and access belong to the endpoint.
type Catalog struct {
	Name          string `json:"name,omitempty"`
	Description   string `json:"description,omitempty"`
	ContextLength uint32 `json:"context_length,omitempty"`
}

// Pricing holds USD decimal strings per billable unit; empty means free.
type Pricing struct {
	PromptTokens       string `json:"prompt_tokens,omitempty"`
	CompletionTokens   string `json:"completion_tokens,omitempty"`
	CachedPromptTokens string `json:"cached_prompt_tokens,omitempty"`
	Request            string `json:"request,omitempty"`
	Image              string `json:"image,omitempty"`
}

func (p Pricing) IsZero() bool {
	// Prices are validated decimal strings; explicitly declaring "0" is free too.
	for _, value := range []string{p.PromptTokens, p.CompletionTokens, p.CachedPromptTokens, p.Request, p.Image} {
		if strings.Trim(value, "0.") != "" {
			return false
		}
	}
	return true
}

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

// ManagedEndpointSpec is everything an endpoint's app.py declares. Where
// replicas run is Fleet's decision.
type ManagedEndpointSpec struct {
	ID                string             `json:"id"`
	Kind              EndpointKind       `json:"kind"`
	Engine            string             `json:"engine,omitempty"`
	Port              uint32             `json:"port"`
	Health            string             `json:"health,omitempty"`
	Metrics           string             `json:"metrics,omitempty"`
	Gpu               map[string]GpuSpec `json:"gpu"` // GPU key -> how the engine runs there
	Routes            []EndpointRoute    `json:"routes,omitempty"`
	Pricing           Pricing            `json:"pricing"`
	Catalog           Catalog            `json:"catalog"`
	Public            bool               `json:"public"`
	AllowedWorkspaces []string           `json:"allowed_workspaces,omitempty"`
	Protected         bool               `json:"protected,omitempty"` // serverless work cannot evict its replicas; a change rolls them
	Rollout           string             `json:"rollout,omitempty"`   // wait_for_capacity (default) or replace (allows downtime)
	DrainSeconds      uint32             `json:"drain_seconds"`       // grace on eviction or retirement; 0 is immediate
	Entrypoint        []string           `json:"entrypoint,omitempty"`
}

// ManagedEndpointStubConfig is embedded in StubConfigV1 for managed stubs.
type ManagedEndpointStubConfig struct {
	Endpoint *ManagedEndpointSpec `json:"endpoint,omitempty"`
	GitSHA   string               `json:"git_sha,omitempty"`
}

var endpointIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*(/[a-z0-9][a-z0-9._-]*)?$`)

func cleanPath(p string) string { return "/" + strings.TrimPrefix(strings.TrimSpace(p), "/") }

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

// Validate checks a normalized spec.
func (s *ManagedEndpointSpec) Validate() error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if !endpointIDPattern.MatchString(s.ID) {
		fail("id %q must look like vendor/slug (lowercase, [a-z0-9._-])", s.ID)
	}
	allowed, validKind := kindRoutes[s.Kind]
	if !validKind {
		fail("kind %q is not one of llm, embedding, image, custom", s.Kind)
	}
	if s.Port == 0 || s.Port > 65535 {
		fail("port %d is invalid", s.Port)
	}
	if len(s.Entrypoint) == 0 {
		fail("entrypoint is required")
	}
	if s.Rollout != "" && s.Rollout != "wait_for_capacity" && s.Rollout != "replace" {
		fail("rollout %q must be wait_for_capacity or replace", s.Rollout)
	}
	for _, route := range s.Routes {
		if validKind && !slices.Contains(allowed, route) {
			fail("route %q is not valid for kind %q", route, s.Kind)
		}
	}
	if err := s.Pricing.Validate(); err != nil {
		errs = append(errs, err)
	}
	if s.Kind == EndpointKindImage && s.Pricing.Image == "" && s.Pricing.Request == "" {
		fail("image endpoints must set a per-image or per-request price (use \"0\" for free)")
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

func (s *ManagedEndpointSpec) ServesRoute(route EndpointRoute) bool {
	return slices.Contains(s.Routes, route)
}

// Fleet is fleet.yaml: for each endpoint, whether it runs and which GPU types
// it fills, with a priority among the endpoints on that type and an optional
// replica cap. It is the only thing that decides where replicas run.
type Fleet struct {
	GitSHA    string                   `json:"git_sha,omitempty"`
	Endpoints map[string]FleetEndpoint `json:"endpoints"`
	UpdatedAt time.Time                `json:"updated_at"`
}

type FleetEndpoint struct {
	Enabled bool                      `json:"enabled" yaml:"enabled"`
	GPUs    map[string]FleetPlacement `json:"gpus" yaml:"gpus"`
}

// FleetPlacement is one endpoint on one GPU type. Priority 1 fills first;
// MaxReplicas 0 means every idle GPU.
type FleetPlacement struct {
	Priority    uint32 `json:"priority" yaml:"priority"`
	MaxReplicas uint32 `json:"max_replicas,omitempty" yaml:"maxReplicas"`
}

// FleetEntry is an endpoint's place in one GPU type's priority order.
type FleetEntry struct {
	EndpointID  string
	Priority    uint32
	MaxReplicas uint32
}

const maxFleetReplicas = 64

func (f *Fleet) Normalize() {
	out := make(map[string]FleetEndpoint, len(f.Endpoints))
	for id, e := range f.Endpoints {
		gpus := make(map[string]FleetPlacement, len(e.GPUs))
		for gpu, p := range e.GPUs {
			gpus[GPUKey(gpu)] = p
		}
		e.GPUs = gpus
		if id = strings.ToLower(strings.TrimSpace(id)); id != "" {
			out[id] = e
		}
	}
	f.Endpoints = out
}

// Validate checks GPU keys, priorities and caps. Call Normalize first.
func (f *Fleet) Validate() error {
	var errs []error
	for id, e := range f.Endpoints {
		for gpu, p := range e.GPUs {
			if gpu != CPUInventoryKey && !KnownGPUType(GpuType(gpu)) {
				errs = append(errs, fmt.Errorf("%s: %s is not a known GPU type", id, gpu))
			}
			if p.Priority == 0 {
				errs = append(errs, fmt.Errorf("%s: %s: priority is required (1 fills first)", id, gpu))
			}
			if p.MaxReplicas > maxFleetReplicas {
				errs = append(errs, fmt.Errorf("%s: %s: maxReplicas %d exceeds %d", id, gpu, p.MaxReplicas, maxFleetReplicas))
			}
			if gpu == CPUInventoryKey && p.MaxReplicas == 0 {
				errs = append(errs, fmt.Errorf("%s: cpu needs maxReplicas", id))
			}
		}
	}
	return errors.Join(errs...)
}

// Prune drops endpoints that are not deployed and GPU types their app does
// not declare, so one broken deploy never blocks the rest of the fleet.
func (f *Fleet) Prune(endpoints map[string]*ManagedEndpointSpec) []string {
	var dropped []string
	for id, e := range f.Endpoints {
		spec, ok := endpoints[id]
		if !ok {
			dropped = append(dropped, fmt.Sprintf("%s is not a deployed endpoint", id))
			delete(f.Endpoints, id)
			continue
		}
		for gpu := range e.GPUs {
			if _, ok := spec.Gpu[gpu]; !ok {
				dropped = append(dropped, fmt.Sprintf("%s does not declare gpu %q in its app", id, gpu))
				delete(e.GPUs, gpu)
			}
		}
	}
	slices.Sort(dropped)
	return dropped
}

// GPUs returns the GPU keys any enabled endpoint fills, sorted.
func (f *Fleet) GPUs() []string {
	seen := map[string]bool{}
	for _, e := range f.Endpoints {
		if e.Enabled {
			for gpu := range e.GPUs {
				seen[gpu] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for gpu := range seen {
		out = append(out, gpu)
	}
	slices.Sort(out)
	return out
}

// Entries returns the enabled endpoints on a GPU type in priority order.
func (f *Fleet) Entries(gpu string) []FleetEntry {
	var out []FleetEntry
	for id, e := range f.Endpoints {
		if p, ok := e.GPUs[gpu]; ok && e.Enabled {
			out = append(out, FleetEntry{EndpointID: id, Priority: p.Priority, MaxReplicas: p.MaxReplicas})
		}
	}
	slices.SortFunc(out, func(a, b FleetEntry) int {
		return cmp.Or(cmp.Compare(a.Priority, b.Priority), strings.Compare(a.EndpointID, b.EndpointID))
	})
	return out
}

// Placements returns the GPU types an enabled endpoint fills.
func (f *Fleet) Placements(endpointID string) map[string]FleetPlacement {
	if e, ok := f.Endpoints[endpointID]; ok && e.Enabled {
		return e.GPUs
	}
	return nil
}

// MeterBucket is one closed minute of usage not yet delivered to the billing meter.
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

type EndpointStatus string

const (
	EndpointStatusActive  EndpointStatus = "active"
	EndpointStatusRetired EndpointStatus = "retired"
)

// ManagedEndpoint is the registry record of a deployed endpoint. A new deploy
// bumps Version; the controller replaces replicas of older versions.
type ManagedEndpoint struct {
	Spec      ManagedEndpointSpec `json:"spec"`
	StubID    string              `json:"stub_id"`
	Version   uint                `json:"version"`
	GitSHA    string              `json:"git_sha,omitempty"`
	Status    EndpointStatus      `json:"status"`
	CreatedAt time.Time           `json:"created_at"`
	UpdatedAt time.Time           `json:"updated_at"`
}

func (e *ManagedEndpoint) Enabled() bool { return e.Status == EndpointStatusActive }

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

// ReplicaConfig is the live harness config of one replica: the revision last
// asked for and the engine's answer to it. It dies with the replica.
type ReplicaConfig struct {
	Revision      uint64          `json:"revision"`
	Config        json.RawMessage `json:"config,omitempty"`
	Author        string          `json:"author,omitempty"` // caller's label
	Actor         string          `json:"actor,omitempty"`  // authenticated token
	SetAt         time.Time       `json:"set_at,omitempty"`
	AckedRevision uint64          `json:"acked_revision"`
	Applied       bool            `json:"applied"`
	Error         string          `json:"error,omitempty"`
	Effective     json.RawMessage `json:"effective,omitempty"`
	AckedAt       time.Time       `json:"acked_at,omitempty"`
}

func (c ReplicaConfig) Acked() bool { return c.AckedRevision >= c.Revision }

// Ack records the engine's answer to an issued, not yet superseded revision.
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
	ID                  string          `json:"id"`
	EndpointID          string          `json:"endpoint_id"`
	Version             uint            `json:"version"`
	GPU                 string          `json:"gpu"`
	GPUCount            uint32          `json:"gpu_count"`
	Locality            string          `json:"locality,omitempty"`
	PoolName            string          `json:"pool_name,omitempty"`
	ContainerID         string          `json:"container_id"`
	WorkerID            string          `json:"worker_id,omitempty"`
	MachineID           string          `json:"machine_id,omitempty"`
	ProviderWorkspaceID string          `json:"provider_workspace_id,omitempty"` // set on contributed machines; earns a revenue share
	Address             string          `json:"address,omitempty"`
	Status              ReplicaStatus   `json:"status"`
	StatusReason        string          `json:"status_reason,omitempty"`
	SecretHash          string          `json:"secret_hash,omitempty"` // SHA-256 of BEAM_REPLICA_SECRET
	HarnessEnabled      bool            `json:"harness_enabled"`
	EngineReady         bool            `json:"engine_ready,omitempty"` // harness readiness; HTTP health must also pass before serving
	Probe               ReplicaProbe    `json:"probe"`                  // snapshotted at start; an old version keeps its own contract
	Config              ReplicaConfig   `json:"config"`
	Capacity            ReplicaCapacity `json:"capacity"`
	Capabilities        json.RawMessage `json:"capabilities,omitempty"`
	EngineMetrics       json.RawMessage `json:"engine_metrics,omitempty"`
	StartedAt           time.Time       `json:"started_at"`
	LoadingSince        time.Time       `json:"loading_since,omitempty"` // start of the current loading phase; the grace runs from here
	ReadyAt             time.Time       `json:"ready_at,omitempty"`
	LastHeartbeat       time.Time       `json:"last_heartbeat"`
	EndedAt             time.Time       `json:"ended_at,omitempty"`
	DrainDeadline       time.Time       `json:"drain_deadline,omitempty"`
}

// ReplicaProbe is the probe contract of one deployment version.
type ReplicaProbe struct {
	Port    uint32 `json:"port"`
	Health  string `json:"health,omitempty"`
	Metrics string `json:"metrics,omitempty"` // Prometheus path; empty for non-LLM engines
}

func (r *EndpointReplica) Serving() bool { return r != nil && r.Status == ReplicaStatusReady }

// EnterLoading starts a fresh loading phase unless one is already running.
func (r *EndpointReplica) EnterLoading(now time.Time, reason string) {
	if r.Status != ReplicaStatusLoading {
		r.LoadingSince = now
	}
	r.Status = ReplicaStatusLoading
	r.StatusReason = reason
}

func (r *EndpointReplica) LoadingFor(now time.Time) time.Duration {
	if r.LoadingSince.IsZero() {
		return now.Sub(r.StartedAt)
	}
	return now.Sub(r.LoadingSince)
}

// Alive reports whether the replica counts toward the live set (draining and evicting do not).
func (r *EndpointReplica) Alive() bool {
	return r.Status == ReplicaStatusScheduling || r.Status == ReplicaStatusLoading || r.Status == ReplicaStatusReady
}

type GitOpsStatus string

const (
	GitOpsStatusApplied GitOpsStatus = "applied"
	GitOpsStatusFailed  GitOpsStatus = "failed"
	GitOpsStatusRetired GitOpsStatus = "retired"
)

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
	RepoURL     string                         `json:"repo_url"`
	Ref         string                         `json:"ref"`
	LastSHA     string                         `json:"last_sha,omitempty"`
	TargetSHA   string                         `json:"target_sha,omitempty"`
	LastRunAt   time.Time                      `json:"last_run_at,omitempty"`
	LastError   string                         `json:"last_error,omitempty"`
	FleetError  string                         `json:"fleet_error,omitempty"` // why fleet.yaml was rejected, or "skipped: ..." entries
	FleetSHA    string                         `json:"fleet_sha,omitempty"`   // trails LastSHA only while a fleet write is retried
	Running     bool                           `json:"running"`
	PerEndpoint map[string]GitOpsEndpointState `json:"per_endpoint"`
	UpdatedAt   time.Time                      `json:"updated_at"`

	// In-flight deployer run.
	RunID       string    `json:"run_id,omitempty"`
	ContainerID string    `json:"container_id,omitempty"`
	TokenID     string    `json:"token_id,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
}

// GitOpsReport is what the deployer posts back after applying one SHA.
type GitOpsReport struct {
	RunID     string               `json:"run_id"`
	SHA       string               `json:"sha"`
	Error     string               `json:"error,omitempty"`
	Results   []GitOpsDeployResult `json:"results"`
	FleetYAML string               `json:"fleet_yaml,omitempty"` // raw fleet.yaml; empty when the repo has none
}

// GitOpsDeployResult is the outcome for one app directory; ID is empty when
// the app failed to import.
type GitOpsDeployResult struct {
	Path    string `json:"path"`
	ID      string `json:"id"`
	OK      bool   `json:"ok"`
	Skipped bool   `json:"skipped"` // unchanged since the last applied SHA
	Error   string `json:"error,omitempty"`
	StubID  string `json:"stub_id,omitempty"`
	Version uint   `json:"version,omitempty"`
}

// RouteSample is one completed /v1 request.
type RouteSample struct {
	EndpointID       string        `json:"endpoint_id"`
	GPU              string        `json:"gpu"`
	ReplicaID        string        `json:"replica_id"`
	ConfigRevision   uint64        `json:"config_revision,omitempty"` // live config acknowledged when the request was served
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

func (m RouteMetrics) MeanTTFTMs() int64 {
	if m.TTFTCount == 0 {
		return 0
	}
	return m.TTFTSumMs / m.TTFTCount
}

func (m RouteMetrics) MeanTPOTMs() int64 {
	if m.CompletionTokens == 0 {
		return 0
	}
	return max(m.DurationSumMs-m.TTFTSumMs, 0) / m.CompletionTokens
}

// UsageKind separates what a workspace spent calling models from what it
// earned serving them on contributed machines.
type UsageKind string

const (
	UsageSpend  UsageKind = "spend"
	UsageEarned UsageKind = "earned"
)

// Usage is what one workspace consumed on (or earned from) one model.
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

type UsageReport struct {
	Total    Usage            `json:"total"`
	PerModel map[string]Usage `json:"per_model"`
	PerDay   map[string]Usage `json:"per_day"`
}
