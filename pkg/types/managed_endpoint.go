package types

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Managed endpoints are Beam deployments the platform publishes under /v1.
// app.py declares how the app runs (ManagedEndpointSpec, on the stub);
// config.yaml declares whether it is published, to whom, at what price and on
// which GPUs (Fleet). ManagedEndpoint is the applied record the gateway serves
// from, and every request or task against it settles as one Charge.

const (
	StubTypeManagedEndpoint           string = "managed_endpoint" // platform-run model server
	StubTypeManagedEndpointDeployment string = "managed_endpoint/deployment"
	StubTypePlatformDeployer          string = "platform_deployer" // CI job that deploys hosted apps
)

func (t StubType) IsManagedEndpoint() bool { return t.Kind() == StubTypeManagedEndpoint }

// IsPlatformWorkload: runs in the platform workspace, exempt from customer credit and pool quotas.
func (t StubType) IsPlatformWorkload() bool {
	return t.IsManagedEndpoint() || t == StubType(StubTypePlatformDeployer)
}

// PlatformWorkload is IsPlatformWorkload for a stub, including hosted
// deployments of ordinary kinds whose containers the fleet controller runs.
func (s *Stub) PlatformWorkload() bool {
	return s.Type.IsPlatformWorkload() || StubConfigIsHosted(s.Config)
}

// StubConfigIsHosted reports whether a stub's config carries a hosted
// declaration: its containers run for the platform, not the customer.
func StubConfigIsHosted(config string) bool {
	marker := struct {
		ManagedEndpoint *struct{} `json:"managed_endpoint"`
	}{}
	return json.Unmarshal([]byte(config), &marker) == nil && marker.ManagedEndpoint != nil
}

// ManagedEndpointStubConfig is the hosted declaration on a stub's config.
type ManagedEndpointStubConfig struct {
	Endpoint *ManagedEndpointSpec `json:"endpoint,omitempty"`
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

// EndpointKind is the engine kind a model server declares.
type EndpointKind string

const (
	EndpointKindLLM       EndpointKind = "llm"
	EndpointKindEmbedding EndpointKind = "embedding"
	EndpointKindImage     EndpointKind = "image"
	EndpointKindCustom    EndpointKind = "custom"
)

var EndpointKinds = []EndpointKind{EndpointKindLLM, EndpointKindEmbedding, EndpointKindImage, EndpointKindCustom}

// EndpointRoute is an OpenAI-style route suffix under /v1 that the hosted
// layer knows. Task queues serve only tasks; other deployments any
// synchronous route.
type EndpointRoute string

const (
	EndpointRouteChatCompletions  EndpointRoute = "chat/completions"
	EndpointRouteCompletions      EndpointRoute = "completions"
	EndpointRouteEmbeddings       EndpointRoute = "embeddings"
	EndpointRouteImageGenerations EndpointRoute = "images/generations"
	EndpointRouteImageEdits       EndpointRoute = "images/edits"
	EndpointRouteAudioSpeech      EndpointRoute = "audio/speech"
	EndpointRouteInvoke           EndpointRoute = "invoke"
	EndpointRouteTasks            EndpointRoute = "tasks"
)

// Async reports whether the route queues work instead of answering inline.
func (r EndpointRoute) Async() bool { return r == EndpointRouteTasks }

// ModelScoped reports whether the route is addressed as /models/{id}/{route}
// rather than by its OpenAI path.
func (r EndpointRoute) ModelScoped() bool { return r == EndpointRouteInvoke || r == EndpointRouteTasks }

// Rollout says how a new version replaces the old: wait for spare capacity
// (default) or replace running replicas in place.
type Rollout string

const (
	RolloutWaitForCapacity Rollout = "wait_for_capacity"
	RolloutReplace         Rollout = "replace"
)

// EngineVLLM is the engine whose streaming usage reporting the gateway tunes.
const EngineVLLM = "vllm"

// GpuSpec is how a model server runs on one GPU type.
type GpuSpec struct {
	Count      uint32         `json:"count,omitempty"` // GPUs per replica
	EngineArgs []string       `json:"engine_args,omitempty"`
	Config     map[string]any `json:"config,omitempty"`
}

// ManagedEndpointSpec is the hosting metadata a stub carries; only the platform
// publishing path may attach it. Model servers (managed_endpoint stubs) also
// declare how the engine runs; other deployments only carry the id.
type ManagedEndpointSpec struct {
	ID           string             `json:"id"`
	Kind         EndpointKind       `json:"kind,omitempty"`
	Engine       string             `json:"engine,omitempty"`
	Port         uint32             `json:"port,omitempty"`
	Health       string             `json:"health,omitempty"`
	Metrics      string             `json:"metrics,omitempty"` // Prometheus path scraped for LLM engines without a harness
	Gpu          map[string]GpuSpec `json:"gpu,omitempty"`
	Rollout      Rollout            `json:"rollout,omitempty"`
	DrainSeconds uint32             `json:"drain_seconds"`
	Entrypoint   []string           `json:"entrypoint,omitempty"`
}

var endpointIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*(/[a-z0-9][a-z0-9._-]*)?$`)

func (s *ManagedEndpointSpec) Normalize(modelServer bool) {
	s.ID = strings.ToLower(strings.TrimSpace(s.ID))
	if !modelServer {
		return
	}
	s.Kind = cmp.Or(EndpointKind(strings.ToLower(strings.TrimSpace(string(s.Kind)))), EndpointKindCustom)
	s.Engine = strings.ToLower(strings.TrimSpace(s.Engine))
	s.Port = cmp.Or(s.Port, 8000)
	s.Health = "/" + strings.TrimPrefix(strings.TrimSpace(cmp.Or(s.Health, "health")), "/")
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
}

func (s *ManagedEndpointSpec) Validate(modelServer bool) error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if !endpointIDPattern.MatchString(s.ID) {
		fail("id %q must look like vendor/slug (lowercase, [a-z0-9._-])", s.ID)
	}
	if !modelServer {
		return errors.Join(errs...)
	}
	if !slices.Contains(EndpointKinds, s.Kind) {
		fail("kind %q is not one of %v", s.Kind, EndpointKinds)
	}
	if s.Port > 65535 {
		fail("port %d is invalid", s.Port)
	}
	if len(s.Entrypoint) == 0 {
		fail("entrypoint is required")
	}
	if s.Rollout != "" && s.Rollout != RolloutWaitForCapacity && s.Rollout != RolloutReplace {
		fail("rollout %q must be %s or %s", s.Rollout, RolloutWaitForCapacity, RolloutReplace)
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

// Catalog is the display metadata of a published app.
type Catalog struct {
	Name          string `json:"name,omitempty" yaml:"name"`
	Description   string `json:"description,omitempty" yaml:"description"`
	ContextLength uint32 `json:"context_length,omitempty" yaml:"context_length"`
}

// Publication is what config.yaml declares about one app besides placement.
type Publication struct {
	Catalog           Catalog             `json:"catalog" yaml:"catalog"`
	Public            bool                `json:"public" yaml:"public"`
	AllowedWorkspaces []string            `json:"allowed_workspaces,omitempty" yaml:"allowed_workspaces"`
	Pricing           Pricing             `json:"pricing" yaml:"pricing"`
	OpenRouter        *OpenRouterMetadata `json:"openrouter,omitempty" yaml:"openrouter"`
}

// Allows reports whether a workspace may discover and call the app.
func (p *Publication) Allows(workspaceID, workspaceName string) bool {
	return p.Public || slices.Contains(p.AllowedWorkspaces, workspaceID) || slices.Contains(p.AllowedWorkspaces, workspaceName)
}

type EndpointStatus string

const (
	EndpointStatusActive  EndpointStatus = "active"
	EndpointStatusRetired EndpointStatus = "retired"
)

// ManagedEndpoint is the publication record of one deployed app: its current stub
// and version, what config.yaml published, and how model servers run.
type ManagedEndpoint struct {
	StubID   string         `json:"stub_id"`
	StubType StubType       `json:"stub_type"`
	Version  uint           `json:"version"`
	GitSHA   string         `json:"git_sha,omitempty"`
	Status   EndpointStatus `json:"status"`
	// Published is set when config.yaml enables the app; only published apps
	// are routable and their Publication is what config.yaml last applied.
	Published bool `json:"published"`
	Publication
	Spec      ManagedEndpointSpec `json:"spec"`
	CreatedAt time.Time           `json:"created_at"`
	UpdatedAt time.Time           `json:"updated_at"`
}

// Enabled: deployed and not retired. Callable: enabled and published under /v1.
func (e *ManagedEndpoint) Enabled() bool     { return e.Status == EndpointStatusActive }
func (e *ManagedEndpoint) Callable() bool    { return e.Enabled() && e.Published }
func (e *ManagedEndpoint) ModelServer() bool { return e.StubType.IsManagedEndpoint() }

// HostedDemandKey holds the container count an ordinary deployment's
// autoscaler wants for a hosted app; the fleet controller reads it instead of
// letting that autoscaler start containers.
func HostedDemandKey(id string) string { return "managed_endpoint:want:" + id }

// Fleet is config.yaml: for each app, whether it is published and which GPU
// types it fills, with a priority among the apps on that type and an optional
// replica cap. It is the only thing that decides where replicas run.
type Fleet struct {
	GitSHA    string                   `json:"git_sha,omitempty"`
	Endpoints map[string]FleetEndpoint `json:"endpoints"`
	UpdatedAt time.Time                `json:"updated_at"`
}

type FleetEndpoint struct {
	Enabled     bool                      `json:"enabled" yaml:"enabled"`
	GPUs        map[string]FleetPlacement `json:"gpus" yaml:"gpus"`
	Publication `yaml:",inline"`
}

// FleetPlacement is one app on one GPU type. Minimums fill first, in priority
// order, then spare capacity fills up to MaxReplicas (0 is uncapped).
// Disabling preemption protects only the minimum; extras stay evictable.
// Serverless placements start replicas only while work waits for them.
type FleetPlacement struct {
	Priority    uint32 `json:"priority" yaml:"priority"`
	MinReplicas uint32 `json:"min_replicas,omitempty" yaml:"minReplicas"`
	MaxReplicas uint32 `json:"max_replicas,omitempty" yaml:"maxReplicas"`
	Preemption  *bool  `json:"preemption,omitempty" yaml:"preemption"`
	Serverless  bool   `json:"serverless,omitempty" yaml:"serverless"`
}

// ProtectsMinimum: a hot minimum that ordinary serverless workloads may not preempt.
func (p FleetPlacement) ProtectsMinimum() bool {
	return !p.Serverless && p.Preemption != nil && !*p.Preemption
}

// YAML otherwise truncates fractional replica counts when decoding into uint32.
func (p *FleetPlacement) UnmarshalYAML(unmarshal func(any) error) error {
	var fields map[string]any
	if err := unmarshal(&fields); err != nil {
		return err
	}
	for _, name := range []string{"priority", "minReplicas", "maxReplicas"} {
		if value, exists := fields[name]; exists {
			switch value.(type) {
			case int, int64, uint64:
			default:
				return fmt.Errorf("%s must be an integer", name)
			}
		}
	}
	for _, name := range []string{"preemption", "serverless"} {
		if value, exists := fields[name]; exists {
			if _, ok := value.(bool); !ok {
				return fmt.Errorf("%s must be a boolean", name)
			}
		}
	}
	type plain FleetPlacement
	return unmarshal((*plain)(p))
}

// FleetEntry is an app's place in one GPU type's priority order.
type FleetEntry struct {
	EndpointID     string
	Priority       uint32
	MinReplicas    uint32
	MaxReplicas    uint32
	ProtectMinimum bool
	Serverless     bool
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
		e.Catalog.Name = cmp.Or(e.Catalog.Name, id)
		if id = strings.ToLower(strings.TrimSpace(id)); id != "" {
			out[id] = e
		}
	}
	f.Endpoints = out
}

// Validate checks publication, GPU keys, priorities and caps. Call Normalize first.
func (f *Fleet) Validate() error {
	var errs []error
	fail := func(id, format string, args ...any) {
		errs = append(errs, fmt.Errorf(id+": "+format, args...))
	}
	for id, e := range f.Endpoints {
		if e.Enabled {
			if err := e.Pricing.Validate(); err != nil {
				fail(id, "%v", err)
			}
		}
		if e.OpenRouter != nil {
			if err := e.OpenRouter.Validate(); err != nil {
				fail(id, "openrouter: %v", err)
			}
		}
		for gpu, p := range e.GPUs {
			switch {
			case gpu != CPUInventoryKey && !KnownGPUType(GpuType(gpu)):
				fail(id, "%s is not a known GPU type", gpu)
			case p.Priority == 0:
				fail(id, "%s: priority is required (1 fills first)", gpu)
			case p.Serverless && p.MinReplicas != 0:
				fail(id, "%s: serverless requires minReplicas: 0", gpu)
			case p.Serverless && p.Preemption != nil && !*p.Preemption:
				fail(id, "%s: serverless replicas must allow preemption", gpu)
			case p.MaxReplicas > maxFleetReplicas || p.MinReplicas > maxFleetReplicas:
				fail(id, "%s: replica counts exceed %d", gpu, maxFleetReplicas)
			case p.MaxReplicas > 0 && p.MinReplicas > p.MaxReplicas:
				fail(id, "%s: minReplicas must not exceed maxReplicas", gpu)
			case gpu == CPUInventoryKey && p.MaxReplicas == 0:
				fail(id, "cpu needs maxReplicas")
			}
		}
	}
	return errors.Join(errs...)
}

// Prune drops apps that are not deployed and GPU types their app does not
// declare, so one broken deploy never blocks the rest of the fleet.
func (f *Fleet) Prune(apps map[string]*ManagedEndpoint) []string {
	var dropped []string
	for id, e := range f.Endpoints {
		app, ok := apps[id]
		if !ok {
			dropped = append(dropped, fmt.Sprintf("%s is not a deployed app", id))
			delete(f.Endpoints, id)
			continue
		}
		for gpu := range e.GPUs {
			if _, ok := app.Spec.Gpu[gpu]; !ok {
				dropped = append(dropped, fmt.Sprintf("%s does not declare gpu %q in its app", id, gpu))
				delete(e.GPUs, gpu)
			}
		}
	}
	slices.Sort(dropped)
	return dropped
}

// GPUs returns the GPU keys any enabled app fills, sorted.
func (f *Fleet) GPUs() []string {
	seen := map[string]bool{}
	for _, e := range f.Endpoints {
		if e.Enabled {
			for gpu := range e.GPUs {
				seen[gpu] = true
			}
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// Entries returns the enabled apps on a GPU type in priority order.
func (f *Fleet) Entries(gpu string) []FleetEntry {
	var out []FleetEntry
	for id, e := range f.Endpoints {
		if p, ok := e.GPUs[gpu]; ok && e.Enabled {
			out = append(out, FleetEntry{EndpointID: id, Priority: p.Priority, MinReplicas: p.MinReplicas, MaxReplicas: p.MaxReplicas, ProtectMinimum: p.ProtectsMinimum(), Serverless: p.Serverless})
		}
	}
	slices.SortFunc(out, func(a, b FleetEntry) int {
		return cmp.Or(cmp.Compare(a.Priority, b.Priority), strings.Compare(a.EndpointID, b.EndpointID))
	})
	return out
}

// Serverless reports whether requests may start on-demand replicas of an app.
func (f *Fleet) Serverless(id string) bool {
	for _, placement := range f.Placements(id) {
		if placement.Serverless {
			return true
		}
	}
	return false
}

// Placements returns the GPU types an enabled app fills.
func (f *Fleet) Placements(id string) map[string]FleetPlacement {
	if e, ok := f.Endpoints[id]; ok && e.Enabled {
		return e.GPUs
	}
	return nil
}

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

// ReplicaCapacity is the harness-reported serving capacity.
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

// EndpointReplica is one container the fleet controller runs for a hosted app
// on one GPU type: a model server, or a runner of an ordinary deployment.
type EndpointReplica struct {
	ID                  string          `json:"id"`
	EndpointID          string          `json:"endpoint_id"`
	Version             uint            `json:"version"`
	GPU                 string          `json:"gpu"`
	GPUCount            uint32          `json:"gpu_count"`
	Protected           bool            `json:"protected"` // this replica belongs to the protected minimum
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
	Probe               ReplicaProbe    `json:"probe"`                  // snapshotted at start; Port 0 means "ready when running"
	Config              ReplicaConfig   `json:"config"`
	Capacity            ReplicaCapacity `json:"capacity"`
	Capabilities        json.RawMessage `json:"capabilities,omitempty"`
	EngineMetrics       json.RawMessage `json:"engine_metrics,omitempty"`
	StartedAt           time.Time       `json:"started_at"`
	LoadingSince        time.Time       `json:"loading_since,omitempty"`
	ReadyAt             time.Time       `json:"ready_at,omitempty"`
	LastHeartbeat       time.Time       `json:"last_heartbeat"`
	EndedAt             time.Time       `json:"ended_at,omitempty"`
	DrainDeadline       time.Time       `json:"drain_deadline,omitempty"`
}

// ReplicaProbe is the readiness contract of one deployment version.
type ReplicaProbe struct {
	Port    uint32 `json:"port"`
	Health  string `json:"health,omitempty"`
	Metrics string `json:"metrics,omitempty"`
}

func (r *EndpointReplica) Serving() bool { return r != nil && r.Status == ReplicaStatusReady }

// EnterLoading starts a fresh loading phase unless one is already running.
func (r *EndpointReplica) EnterLoading(now time.Time, reason string) {
	if r.Status != ReplicaStatusLoading {
		r.LoadingSince = now
	}
	r.Status, r.StatusReason = ReplicaStatusLoading, reason
}

func (r *EndpointReplica) LoadingFor(now time.Time) time.Duration {
	return now.Sub(cmp.Or(r.LoadingSince, r.StartedAt))
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

// GitOpsEndpointState is the outcome of the last deploy of one app directory.
type GitOpsEndpointState struct {
	Path      string       `json:"path"`
	ID        string       `json:"id,omitempty"`
	Status    GitOpsStatus `json:"status"`
	Error     string       `json:"error,omitempty"`
	StubID    string       `json:"stub_id,omitempty"`
	Version   uint         `json:"version,omitempty"`
	UpdatedAt time.Time    `json:"updated_at"`
}

// GitOpsState is what the last CI run of the endpoints repo reported.
type GitOpsState struct {
	RepoURL     string                         `json:"repo_url"`
	Ref         string                         `json:"ref"`
	LastSHA     string                         `json:"last_sha,omitempty"`
	LastRunAt   time.Time                      `json:"last_run_at,omitempty"`
	LastError   string                         `json:"last_error,omitempty"`  // failed app deploys, one per line
	FleetError  string                         `json:"fleet_error,omitempty"` // why config.yaml was rejected
	PerEndpoint map[string]GitOpsEndpointState `json:"per_endpoint"`          // by app path
	PendingSHA  string                         `json:"pending_sha,omitempty"` // announced by a deploy that has not applied yet
	PendingAt   time.Time                      `json:"pending_at,omitempty"`
}

// RouteMetrics are routing diagnostics aggregated over minute buckets; they
// are derived from settled charges but are not the billing record.
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
	CostMicroUSD     int64         `json:"cost_micro_usd"`
	DurationSumMs    int64         `json:"duration_sum_ms"`
	TTFTSumMs        int64         `json:"ttft_sum_ms"`
	TTFTCount        int64         `json:"ttft_count"`
	QueueWaitSumMs   int64         `json:"queue_wait_sum_ms"`
}

func (m *RouteMetrics) Fields() map[string]*int64 {
	return map[string]*int64{
		"requests": &m.Requests, "errors": &m.Errors, "prompt_tokens": &m.PromptTokens, "completion_tokens": &m.CompletionTokens,
		"cost_micro_usd": &m.CostMicroUSD, "duration_sum_ms": &m.DurationSumMs,
		"ttft_sum_ms": &m.TTFTSumMs, "ttft_count": &m.TTFTCount, "queue_wait_sum_ms": &m.QueueWaitSumMs,
	}
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

// MaxUsageCounter fits exactly in Redis Lua numbers and JavaScript clients.
const MaxUsageCounter int64 = 1<<53 - 1

// Pricing has exactly two forms: a flat price per successful request or task,
// or per-token prices. Free offerings declare an explicit "0"; an empty
// Pricing is unpriced and rejected at publication.
type Pricing struct {
	Request            string `json:"request,omitempty" yaml:"request"`
	PromptTokens       string `json:"prompt_tokens,omitempty" yaml:"prompt_tokens"`
	CompletionTokens   string `json:"completion_tokens,omitempty" yaml:"completion_tokens"`
	CachedPromptTokens string `json:"cached_prompt_tokens,omitempty" yaml:"cached_prompt_tokens"`
}

var pricingPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]{1,18})?$`)

// PricingRat parses one price into an exact rational; empty is zero.
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

func (p Pricing) PerRequest() bool { return p.Request != "" }
func (p Pricing) PerToken() bool   { return p.PromptTokens != "" || p.CompletionTokens != "" }

// Free reports whether every declared price is zero.
func (p Pricing) Free() bool {
	for _, value := range []string{p.Request, p.PromptTokens, p.CompletionTokens, p.CachedPromptTokens} {
		if rat, err := PricingRat(value); err != nil || rat.Sign() != 0 {
			return false
		}
	}
	return true
}

func (p Pricing) Validate() error {
	for name, value := range map[string]string{"request": p.Request, "prompt_tokens": p.PromptTokens, "completion_tokens": p.CompletionTokens, "cached_prompt_tokens": p.CachedPromptTokens} {
		if _, err := PricingRat(value); err != nil {
			return fmt.Errorf("pricing.%s: %w", name, err)
		}
	}
	switch {
	case p.PerRequest() && (p.PerToken() || p.CachedPromptTokens != ""):
		return errors.New("pricing: request and token prices are mutually exclusive")
	case p.PerToken() && (p.PromptTokens == "" || p.CompletionTokens == ""):
		return errors.New("pricing: token pricing needs both prompt_tokens and completion_tokens (use \"0\" for free)")
	case !p.PerRequest() && !p.PerToken():
		return errors.New("pricing: declare request or prompt_tokens/completion_tokens (use \"0\" for free)")
	}
	return nil
}

// Work is what an app reported for one completed request or task. It is
// never estimated: token-priced work without a usage object is not billed.
type Work struct {
	Requests         int64 `json:"requests"`
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	CachedTokens     int64 `json:"cached_tokens"` // part of PromptTokens, billed at the cache rate
}

func (w Work) Valid() bool {
	for _, n := range []int64{w.Requests, w.PromptTokens, w.CompletionTokens, w.CachedTokens} {
		if n < 0 || n > MaxUsageCounter {
			return false
		}
	}
	return w.CachedTokens <= w.PromptTokens
}

// Cost is the exact micro-USD price of Work: the total and its four components.
type Cost struct {
	MicroUSD           int64 `json:"micro_usd"`
	PromptMicroUSD     int64 `json:"prompt_micro_usd"` // uncached input
	CompletionMicroUSD int64 `json:"completion_micro_usd"`
	CachedMicroUSD     int64 `json:"cached_micro_usd"`
	RequestMicroUSD    int64 `json:"request_micro_usd"`
}

// Usage is Work and Cost summed over many charges (a day, a model, a report).
type Usage struct {
	Work
	Cost
}

func (u *Usage) Add(o Usage) {
	for i, field := range u.Fields() {
		*field += *o.Fields()[i]
	}
}

// Fields lists every counter by its wire name, in a fixed order.
func (u *Usage) Fields() []*int64 {
	return []*int64{&u.Requests, &u.PromptTokens, &u.CompletionTokens, &u.CachedTokens,
		&u.MicroUSD, &u.PromptMicroUSD, &u.CompletionMicroUSD, &u.CachedMicroUSD, &u.RequestMicroUSD}
}

var UsageFieldNames = []string{"requests", "prompt_tokens", "completion_tokens", "cached_tokens",
	"micro_usd", "prompt_micro_usd", "completion_micro_usd", "cached_micro_usd", "request_micro_usd"}

type UsageReport struct {
	Total    Usage            `json:"total"`
	PerModel map[string]Usage `json:"per_model"`
	PerDay   map[string]Usage `json:"per_day"`
}

// UsageKind separates what a workspace spent calling apps from what it earned
// serving them on contributed machines.
type UsageKind string

const (
	UsageSpend  UsageKind = "spend"
	UsageEarned UsageKind = "earned"
)

// Price computes the exact cost of w under p. The total is rounded once to
// micro-USD; the micro-dollars lost to flooring each component go to the
// components with the largest fractions, so the breakdown reconciles.
func (p Pricing) Price(w Work) (Cost, error) {
	if !w.Valid() {
		return Cost{}, errors.New("invalid usage")
	}
	var cost Cost
	type line struct {
		price    string
		quantity int64
		cost     *int64
		fraction *big.Rat
	}
	lines := []*line{
		{p.PromptTokens, w.PromptTokens - w.CachedTokens, &cost.PromptMicroUSD, nil},
		{p.CompletionTokens, w.CompletionTokens, &cost.CompletionMicroUSD, nil},
		{cmp.Or(p.CachedPromptTokens, p.PromptTokens), w.CachedTokens, &cost.CachedMicroUSD, nil},
		{p.Request, w.Requests, &cost.RequestMicroUSD, nil},
	}
	total := new(big.Rat)
	for _, l := range lines {
		amount := new(big.Rat)
		if l.price != "" && l.quantity > 0 {
			rate, err := PricingRat(l.price)
			if err != nil {
				return Cost{}, err
			}
			amount.Mul(rate, big.NewRat(l.quantity*1_000_000, 1))
		}
		whole := new(big.Int).Quo(amount.Num(), amount.Denom())
		if !whole.IsInt64() {
			return Cost{}, errors.New("cost overflows micro-USD")
		}
		*l.cost = whole.Int64()
		l.fraction = new(big.Rat).Sub(amount, new(big.Rat).SetInt(whole))
		total.Add(total, amount)
	}
	// Round half up, then reconcile the components with the total.
	rounded := new(big.Int).Quo(new(big.Int).Add(new(big.Int).Mul(total.Num(), big.NewInt(2)), total.Denom()), new(big.Int).Mul(total.Denom(), big.NewInt(2)))
	if !rounded.IsInt64() || rounded.Int64() > MaxUsageCounter {
		return Cost{}, errors.New("cost exceeds exact counter limit")
	}
	cost.MicroUSD = rounded.Int64()
	floored := cost.PromptMicroUSD + cost.CompletionMicroUSD + cost.CachedMicroUSD + cost.RequestMicroUSD
	slices.SortStableFunc(lines, func(a, b *line) int { return b.fraction.Cmp(a.fraction) })
	for i := int64(0); i < cost.MicroUSD-floored; i++ {
		*lines[i].cost++
	}
	return cost, nil
}

// ChargeStatus is the settlement state of a Charge.
type ChargeStatus string

const (
	ChargeOpen    ChargeStatus = "open"    // accepted; work not finished yet (queued tasks)
	ChargeSettled ChargeStatus = "settled" // priced and counted, or a completed unbilled request
	ChargeVoid    ChargeStatus = "void"    // failed, cancelled, expired or preempted; never billed
)

// Void closes a charge without billing it.
func (c *Charge) Void(reason string, now time.Time) {
	c.Status, c.Error, c.SettledAt = ChargeVoid, reason, now
}

// Charge is the authoritative record of one request or task against a hosted
// app. Its ID is the request id, or the task id for queued work, so retries
// and duplicate completions settle the same charge once. The caller, the app
// version and the price are snapshotted when the work is accepted.
type Charge struct {
	ID          string        `json:"id"`
	Status      ChargeStatus  `json:"status"`
	WorkspaceID string        `json:"workspace_id"` // the caller, never the execution workspace
	TokenID     string        `json:"token_id,omitempty"`
	AppID       string        `json:"app_id"`
	Version     uint          `json:"version"`
	StubType    string        `json:"stub_type,omitempty"`
	Route       EndpointRoute `json:"route,omitempty"`
	Pricing     Pricing       `json:"pricing"`
	Work        Work          `json:"work"`
	Cost        Cost          `json:"cost"`
	// Provider attribution: set when a workspace-contributed machine served
	// the work; ProviderShareMicroUSD is its cut of Cost.MicroUSD.
	ProviderWorkspaceID   string `json:"provider_workspace_id,omitempty"`
	ProviderShareMicroUSD int64  `json:"provider_share_micro_usd,omitempty"`
	// Diagnostics of the serving attempt (not billing inputs).
	ReplicaID      string    `json:"replica_id,omitempty"`
	ContainerID    string    `json:"container_id,omitempty"`
	MachineID      string    `json:"machine_id,omitempty"`
	GPU            string    `json:"gpu,omitempty"`
	ConfigRevision uint64    `json:"config_revision,omitempty"`
	StatusCode     int       `json:"status_code,omitempty"`
	Stream         bool      `json:"stream,omitempty"`
	DurationMs     int64     `json:"duration_ms,omitempty"`
	TTFTMs         int64     `json:"ttft_ms,omitempty"`
	QueueWaitMs    int64     `json:"queue_wait_ms,omitempty"`
	Error          string    `json:"error,omitempty"`
	AcceptedAt     time.Time `json:"accepted_at"`
	SettledAt      time.Time `json:"settled_at,omitempty"`
}

// Usage is the charge as one row of the usage counters.
func (c *Charge) Usage() Usage { return Usage{Work: c.Work, Cost: c.Cost} }

// Settle prices the reported work as one completed request and marks the
// charge settled. Flat-priced work ignores any reported tokens.
func (c *Charge) Settle(w Work, now time.Time) error {
	if c.Pricing.PerRequest() {
		w = Work{}
	}
	w.Requests = 1
	cost, err := c.Pricing.Price(w)
	if err != nil {
		return err
	}
	c.Work, c.Cost, c.Status, c.SettledAt = w, cost, ChargeSettled, now
	return nil
}
