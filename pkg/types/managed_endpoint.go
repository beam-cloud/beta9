package types

import (
	"bytes"
	"cmp"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v2"
	sigyaml "sigs.k8s.io/yaml"
)

// Managed endpoints are Beam deployments the platform publishes under /v1.
// app.py declares how the app runs (ManagedEndpointSpec, on the stub);
// config.yaml declares whether it is published, to whom, at what price and on
// which GPUs (Fleet). ManagedEndpoint is the applied record the gateway serves
// from, and every request against it settles as one Charge.

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

// ManagedEndpointStubConfig is the spec a managed endpoint stub carries.
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

// EndpointRoute is an OpenAI-style route suffix under /v1.
type EndpointRoute string

const (
	EndpointRouteChatCompletions  EndpointRoute = "chat/completions"
	EndpointRouteCompletions      EndpointRoute = "completions"
	EndpointRouteEmbeddings       EndpointRoute = "embeddings"
	EndpointRouteImageGenerations EndpointRoute = "images/generations"
	EndpointRouteImageEdits       EndpointRoute = "images/edits"
	EndpointRouteAudioSpeech      EndpointRoute = "audio/speech"
	EndpointRouteInvoke           EndpointRoute = "invoke"
)

// ModelScoped reports whether the route is addressed as /models/{id}/{route}
// rather than by its OpenAI path.
func (r EndpointRoute) ModelScoped() bool { return r == EndpointRouteInvoke }

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

// ManagedEndpointSpec is how app.py says a model server runs; only the
// platform publishing path may attach it to a stub.
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

func (s *ManagedEndpointSpec) Normalize() {
	s.ID = strings.ToLower(strings.TrimSpace(s.ID))
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

func (s *ManagedEndpointSpec) Validate() error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if !endpointIDPattern.MatchString(s.ID) {
		fail("id %q must look like vendor/slug (lowercase, [a-z0-9._-])", s.ID)
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

// ManagedEndpoint is the record of one deployed app: its current stub and
// version, how it runs, and what config.yaml published.
type ManagedEndpoint struct {
	StubID  string         `json:"stub_id"`
	Version uint           `json:"version"`
	GitSHA  string         `json:"git_sha,omitempty"`
	Status  EndpointStatus `json:"status"`
	// Published is set when config.yaml enables the app; only published apps
	// are routable and their Publication is what config.yaml last applied.
	Published bool `json:"published"`
	Publication
	Spec      ManagedEndpointSpec `json:"spec"`
	CreatedAt time.Time           `json:"created_at"`
	UpdatedAt time.Time           `json:"updated_at"`
}

// Enabled: deployed and not retired. Callable: enabled and published under /v1.
func (e *ManagedEndpoint) Enabled() bool  { return e.Status == EndpointStatusActive }
func (e *ManagedEndpoint) Callable() bool { return e.Enabled() && e.Published }

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

// EndpointReplica is one running container serving an endpoint on one GPU type.
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

// MeterRow is one aggregate returned by the billing meter: a window for one
// subject, split by the requested dimensions.
type MeterRow struct {
	WindowStart time.Time
	Value       float64
	GroupBy     map[string]string
}

// MaxUsageCounter fits exactly in a float64 (the meter) and in JavaScript clients.
const MaxUsageCounter int64 = 1<<53 - 1

// Pricing has exactly two forms: a flat price per successful request,
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

// Work is what an app reported for one completed request. It is
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

// EndpointUsageMeter names the billing meter that sums one Usage field over
// endpoint_usage events, grouped by workspace_id, endpoint_id and kind.
func EndpointUsageMeter(field string) string { return "endpoint_usage_" + field }

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
			// Valid counters can exceed MaxInt64/1e6; scale only after
			// converting to arbitrary precision, before applying the rate.
			quantity := new(big.Int).Mul(big.NewInt(l.quantity), big.NewInt(1_000_000))
			amount.Mul(rate, new(big.Rat).SetInt(quantity))
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
	ChargeSettled ChargeStatus = "settled" // priced and counted, or a completed unbilled request
	ChargeVoid    ChargeStatus = "void"    // failed or preempted; never billed
)

// Charge is the authoritative record of one request against a managed endpoint,
// keyed by request id so a duplicate completion settles the same charge
// once. The caller, the app version and the price are snapshotted when the
// request is accepted.
type Charge struct {
	ID          string        `json:"id"`
	Status      ChargeStatus  `json:"status"`
	WorkspaceID string        `json:"workspace_id"` // the caller, never the platform workspace
	TokenID     string        `json:"token_id,omitempty"`
	AppID       string        `json:"app_id"`
	Version     uint          `json:"version"`
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

// Usage is the charge as what the caller was metered for.
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

// Void closes a charge without billing it.
func (c *Charge) Void(reason string, now time.Time) {
	c.Status, c.Error, c.SettledAt = ChargeVoid, reason, now
}

// OpenRouterMetadata opts an endpoint into the provider catalog. It lives in
// config.yaml, separately from the SDK/runtime spec, so correcting provider
// metadata neither rebuilds the image nor replaces serving replicas.
// Prices and context length remain authoritative in the endpoint's app.
type OpenRouterMetadata struct {
	HuggingFaceID       string                 `json:"hugging_face_id,omitempty"`
	Quantization        string                 `json:"quantization,omitempty"`
	MaxOutputTokens     uint32                 `json:"max_output_tokens,omitempty"`
	SupportedParameters map[string]any         `json:"supported_parameters,omitempty"`
	Streaming           *bool                  `json:"streaming,omitempty"`
	Datacenters         []OpenRouterDatacenter `json:"datacenters,omitempty"`
}

type OpenRouterDatacenter struct {
	CountryCode string `json:"country_code"`
}

// Decode through JSON to preserve strict scalar types and descriptor maps:
// yaml.v2 otherwise truncates floats into integer fields and coerces strings.
func (m *OpenRouterMetadata) UnmarshalYAML(unmarshal func(any) error) error {
	var raw any
	if err := unmarshal(&raw); err != nil {
		return err
	}
	encoded, err := yaml.Marshal(raw)
	if err != nil {
		return err
	}
	encoded, err = sigyaml.YAMLToJSONStrict(encoded)
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return err
	}
	for name, value := range fields {
		if bytes.Equal(value, []byte("null")) {
			return fmt.Errorf("%s cannot be null; omit undeclared fields", name)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	type plain OpenRouterMetadata
	return decoder.Decode((*plain)(m))
}

// Source: https://openrouter.ai/docs/assets/provider-monitor-schema-v2.openapi.json
// Retrieved 2026-09-10, version 2.4. Keep the original schema intact so provider
// validation follows its closed fields and capability descriptors exactly.
//
//go:embed schemas/provider-monitor-schema-v2.openapi.json
var openRouterSchemaJSON []byte

var openRouterSchema = sync.OnceValues(func() (*jsonschema.Schema, error) {
	var document any
	if err := json.Unmarshal(openRouterSchemaJSON, &document); err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	const location = "https://openrouter.ai/docs/assets/provider-monitor-schema-v2.openapi.json"
	if err := compiler.AddResource(location, document); err != nil {
		return nil, err
	}
	return compiler.Compile(location + "#/components/schemas/ModelDocumentV2")
})

// ValidateOpenRouterDocument validates one model entry, not the OpenAI list
// envelope. Compilation happens once; inference and scheduling never call it.
func ValidateOpenRouterDocument(document any) error {
	schema, err := openRouterSchema()
	if err != nil {
		return err
	}
	// Callers may use typed metadata, uint32 limits, and decimal string prices.
	encoded, err := json.Marshal(document)
	if err != nil {
		return err
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	return schema.Validate(value)
}

func (m *OpenRouterMetadata) Validate() error {
	document := map[string]any{
		"schema_version": "2.4", "id": "validation", "name": "validation",
		"input_modalities":  []any{map[string]any{"type": "text"}},
		"output_modalities": []any{m.Output("text")},
	}
	if m.Quantization != "" {
		document["quantization"] = m.Quantization
	}
	if m.Datacenters != nil {
		document["datacenters"] = m.Datacenters
	}
	return ValidateOpenRouterDocument(document)
}

// Output supplies the declared features for the endpoint's single output.
// An empty descriptor map makes no claims about optional feature support.
func (m *OpenRouterMetadata) Output(kind string) map[string]any {
	parameters := m.SupportedParameters
	if parameters == nil {
		parameters = map[string]any{}
	}
	output := map[string]any{"type": kind, "supported_parameters": parameters}
	if m.MaxOutputTokens > 0 {
		output["max_length"] = map[string]any{"value": m.MaxOutputTokens, "unit": "token"}
	}
	if m.Streaming != nil {
		output["streaming"] = *m.Streaming
	}
	return output
}

// ValidateFor checks the metadata against the published app: token pricing
// belongs to text models, embeddings price only their input, and LLM output
// limits must fit the declared context length.
func (m *OpenRouterMetadata) ValidateFor(kind EndpointKind, catalog Catalog, pricing Pricing) error {
	switch kind {
	case EndpointKindLLM:
		if catalog.ContextLength == 0 || m.MaxOutputTokens == 0 || m.MaxOutputTokens > catalog.ContextLength {
			return fmt.Errorf("max_output_tokens must be positive and no greater than context_length")
		}
	case EndpointKindEmbedding, EndpointKindImage:
		if m.MaxOutputTokens != 0 {
			return fmt.Errorf("max_output_tokens is only supported for text outputs")
		}
		if kind == EndpointKindEmbedding && m.Streaming != nil {
			return fmt.Errorf("streaming is not supported for embeddings")
		}
		if kind == EndpointKindEmbedding && pricing.CompletionTokens != "" {
			return fmt.Errorf("embedding token pricing belongs to the input")
		}
		if kind == EndpointKindImage && pricing.PerToken() {
			return fmt.Errorf("image outputs require request pricing, not token pricing")
		}
	default:
		return fmt.Errorf("kind %q has no OpenRouter modality adapter", kind)
	}
	return nil
}
