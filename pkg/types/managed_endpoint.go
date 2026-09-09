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

// Managed endpoints are inference endpoints owned by the platform (not by a
// user workspace). Their specs live in a git repository as beta9 apps
// (ManagedEndpoint / ManagedService in the SDK), are deployed as stubs in the
// system workspace, filled opportunistically onto spare GPU capacity and served
// through the OpenRouter-compatible /v1 route.

const (
	StubTypeManagedEndpoint           string = "managed_endpoint"
	StubTypeManagedEndpointDeployment string = "managed_endpoint/deployment"
	StubTypeManagedService            string = "managed_service"
	StubTypeManagedServiceDeployment  string = "managed_service/deployment"
)

func (t StubType) IsManagedEndpoint() bool { return t.Kind() == StubTypeManagedEndpoint }
func (t StubType) IsManagedService() bool  { return t.Kind() == StubTypeManagedService }
func (t StubType) IsManaged() bool         { return t.IsManagedEndpoint() || t.IsManagedService() }

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
	if kind == EndpointKindLLM {
		return kindRoutes[kind][:2]
	}
	return kindRoutes[kind][:1]
}

// GpuTarget is one hardware shape an endpoint may run on, with the tuned
// engine args (restart-class) and harness knobs (live) for that shape.
type GpuTarget struct {
	Type        string         `json:"type"`
	Count       uint32         `json:"count"`
	MinReplicas uint32         `json:"min_replicas"`
	MaxReplicas uint32         `json:"max_replicas"`
	Share       float64        `json:"share"`
	EngineArgs  []string       `json:"engine_args,omitempty"`
	Harness     map[string]any `json:"harness,omitempty"`
}

// IsCPU reports whether the target describes CPU-only placement. A GPU type
// with an omitted count is still a GPU target; Normalize defaults it to one.
func (t GpuTarget) IsCPU() bool {
	return strings.TrimSpace(t.Type) == "" || NormalizeGPUType(t.Type) == NO_GPU
}

// Key uniquely identifies a target within an endpoint ("H100x2", "cpu").
func (t GpuTarget) Key() string {
	if t.IsCPU() {
		return "cpu"
	}
	return fmt.Sprintf("%sx%d", NormalizeGPUType(t.Type), t.Count)
}

// CPUTarget is the implicit target for endpoints declaring no GPUs.
func CPUTarget() GpuTarget { return GpuTarget{Type: string(NO_GPU), MaxReplicas: 1} }

// RoleTarget pairs a replica role with a GPU target.
type RoleTarget struct {
	Role   string
	Target GpuTarget
}

func (rt RoleTarget) Key() string { return rt.Role + ":" + rt.Target.Key() }

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

// ReplicaPolicy controls how replicas of an endpoint behave under preemption.
type ReplicaPolicy struct {
	Evictable    bool    `json:"evictable"`
	DrainSeconds uint32  `json:"drain_seconds"`
	SpareShare   float64 `json:"spare_share"`
}

// KVCacheSpec opts an endpoint into a shared KV store within a locality.
type KVCacheSpec struct {
	Connector   string         `json:"connector,omitempty"`
	Service     string         `json:"service,omitempty"`
	MinReplicas uint32         `json:"min_replicas,omitempty"`
	Extra       map[string]any `json:"extra,omitempty"`
}

const (
	ReplicaRoleServe   = "serve"
	ReplicaRolePrefill = "prefill"
	ReplicaRoleDecode  = "decode"
)

// ManagedEndpointSpec is the repo contract: everything an endpoint app declares.
type ManagedEndpointSpec struct {
	ID      string          `json:"id"`
	Kind    EndpointKind    `json:"kind"`
	Engine  string          `json:"engine,omitempty"`
	Port    uint32          `json:"port"`
	Health  string          `json:"health,omitempty"`
	Metrics string          `json:"metrics,omitempty"`
	Gpu     []GpuTarget     `json:"gpu"`
	Routes  []EndpointRoute `json:"routes,omitempty"`
	Pricing Pricing         `json:"pricing"`
	Catalog Catalog         `json:"catalog"`
	Policy  ReplicaPolicy   `json:"policy"`
	// Harness enables the in-engine harness (live knobs over EndpointHarnessService).
	Harness bool         `json:"harness"`
	KVCache *KVCacheSpec `json:"kv_cache,omitempty"`
	// Topology maps prefill/decode roles to the GPU targets each may run on.
	// Empty means a monolithic endpoint served from Gpu.
	Topology   map[string][]GpuTarget `json:"topology,omitempty"`
	Services   []string               `json:"services,omitempty"`
	Locality   []string               `json:"locality,omitempty"`
	Entrypoint []string               `json:"entrypoint,omitempty"`
}

// ManagedServiceSpec is a protected shared-infrastructure service (e.g. a
// Mooncake master) with the same placement shape but no public surface.
type ManagedServiceSpec struct {
	Name        string      `json:"name"`
	Port        uint32      `json:"port"`
	Health      string      `json:"health,omitempty"`
	Gpu         []GpuTarget `json:"gpu"`
	Replicas    uint32      `json:"replicas"`
	PerLocality bool        `json:"per_locality"`
	Entrypoint  []string    `json:"entrypoint,omitempty"`
}

// ManagedEndpointStubConfig is embedded in StubConfigV1 for managed stubs.
type ManagedEndpointStubConfig struct {
	Endpoint *ManagedEndpointSpec `json:"endpoint,omitempty"`
	Service  *ManagedServiceSpec  `json:"service,omitempty"`
	// GitSHA is the source revision the deployer built this stub from.
	GitSHA string `json:"git_sha,omitempty"`
}

// ManagedEndpointValidation configures spec validation. Empty allow lists
// allow everything.
type ManagedEndpointValidation struct {
	AllowedEngines []string
	AllowedKinds   []EndpointKind
	// KnownServices resolves spec.Services references; nil skips the check.
	KnownServices map[string]struct{}
}

var (
	endpointIDPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*(/[a-z0-9][a-z0-9._-]*)?$`)
	serviceNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
)

func cleanPath(p string) string { return "/" + strings.TrimPrefix(strings.TrimSpace(p), "/") }

// Disaggregated reports whether the endpoint splits prefill and decode.
func (s *ManagedEndpointSpec) Disaggregated() bool { return len(s.Topology) > 0 }

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
	if s.Policy.DrainSeconds == 0 {
		s.Policy.DrainSeconds = 5
	}
	if s.Policy.SpareShare <= 0 {
		s.Policy.SpareShare = 0.2
	}
	if len(s.Gpu) == 0 {
		s.Gpu = []GpuTarget{CPUTarget()}
	}
	normalizeTargets(s.Gpu, s.Policy.SpareShare)
	for role := range s.Topology {
		normalizeTargets(s.Topology[role], s.Policy.SpareShare)
	}
	if s.KVCache != nil {
		if s.KVCache.MinReplicas == 0 {
			s.KVCache.MinReplicas = 2
		}
		if s.KVCache.Service != "" && !slices.Contains(s.Services, s.KVCache.Service) {
			s.Services = append(s.Services, s.KVCache.Service)
		}
	}
	if s.Catalog.Name == "" {
		s.Catalog.Name = s.ID
	}
}

func normalizeTargets(targets []GpuTarget, spareShare float64) {
	for i := range targets {
		t := &targets[i]
		if t.IsCPU() {
			t.Type, t.Count = string(NO_GPU), 0
		} else {
			t.Type = string(NormalizeGPUType(t.Type))
			t.Count = max(t.Count, 1)
		}
		if t.MaxReplicas == 0 {
			t.MaxReplicas = max(t.MinReplicas, 1)
		}
		if t.Share <= 0 {
			t.Share = spareShare
		}
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
	if s.Policy.SpareShare <= 0 || s.Policy.SpareShare > 1 {
		fail("policy.spare_share %v must be in (0, 1]", s.Policy.SpareShare)
	}
	errs = append(errs, validateTargets("gpu", s.Gpu)...)
	if s.Disaggregated() {
		for _, role := range []string{ReplicaRolePrefill, ReplicaRoleDecode} {
			if len(s.Topology[role]) == 0 {
				fail("topology.%s is required for disaggregated mode", role)
			}
		}
		for role, targets := range s.Topology {
			if role != ReplicaRolePrefill && role != ReplicaRoleDecode {
				fail("topology.%s: unknown role", role)
			}
			errs = append(errs, validateTargets("topology."+role, targets)...)
		}
		if s.KVCache == nil {
			fail("disaggregated topology requires kv_cache")
		}
	}
	if s.KVCache != nil && s.KVCache.Connector == "" {
		fail("kv_cache.connector is required")
	}
	for _, service := range s.Services {
		if _, ok := policy.KnownServices[service]; policy.KnownServices != nil && !ok {
			fail("service %q is not deployed", service)
		}
	}
	return errors.Join(errs...)
}

func validateTargets(field string, targets []GpuTarget) []error {
	var errs []error
	seen := map[string]bool{}
	for i, t := range targets {
		fail := func(format string, args ...any) {
			errs = append(errs, fmt.Errorf("%s[%d]: %s", field, i, fmt.Sprintf(format, args...)))
		}
		switch {
		case GpuType(t.Type) == GPU_ANY:
			// Placement indexes inventory by concrete GPU type; "any" never fills.
			fail("gpu type %q is not allowed; list concrete types as alternatives", t.Type)
		case !t.IsCPU() && !KnownGPUType(GpuType(t.Type)):
			fail("unknown gpu type %q", t.Type)
		}
		if t.Count > 8 {
			fail("count %d exceeds 8", t.Count)
		}
		if t.MinReplicas > t.MaxReplicas {
			fail("min_replicas %d > max_replicas %d", t.MinReplicas, t.MaxReplicas)
		}
		if t.Share <= 0 || t.Share > 1 {
			fail("share %v must be in (0, 1]", t.Share)
		}
		if seen[t.Key()] {
			fail("duplicate target %s", t.Key())
		}
		seen[t.Key()] = true
	}
	return errs
}

// Targets returns every (role, target) placement for the endpoint, roles in
// sorted order.
func (s *ManagedEndpointSpec) Targets() []RoleTarget {
	if !s.Disaggregated() {
		return roleTargets(ReplicaRoleServe, s.Gpu)
	}
	var out []RoleTarget
	for _, role := range slices.Sorted(maps.Keys(s.Topology)) {
		out = append(out, roleTargets(role, s.Topology[role])...)
	}
	return out
}

func roleTargets(role string, targets []GpuTarget) []RoleTarget {
	out := make([]RoleTarget, 0, len(targets))
	for _, t := range targets {
		out = append(out, RoleTarget{Role: role, Target: t})
	}
	return out
}

// ServesRoute reports whether the endpoint declares a route.
func (s *ManagedEndpointSpec) ServesRoute(route EndpointRoute) bool {
	return slices.Contains(s.Routes, route)
}

// Normalize fills service defaults. Every target runs exactly Replicas
// protected replicas.
func (s *ManagedServiceSpec) Normalize() {
	s.Name = strings.ToLower(strings.TrimSpace(s.Name))
	if s.Port == 0 {
		s.Port = 8000
	}
	if s.Health != "" {
		s.Health = cleanPath(s.Health)
	}
	s.Replicas = max(s.Replicas, 1)
	if len(s.Gpu) == 0 {
		s.Gpu = []GpuTarget{CPUTarget()}
	}
	normalizeTargets(s.Gpu, 1)
	for i := range s.Gpu {
		s.Gpu[i].Share, s.Gpu[i].MinReplicas, s.Gpu[i].MaxReplicas = 1, s.Replicas, s.Replicas
	}
}

// Validate checks a service spec. Call Normalize first.
func (s *ManagedServiceSpec) Validate() error {
	var errs []error
	if !serviceNamePattern.MatchString(s.Name) {
		errs = append(errs, fmt.Errorf("service name %q must be lowercase [a-z0-9-]", s.Name))
	}
	if len(s.Entrypoint) == 0 {
		errs = append(errs, errors.New("entrypoint is required"))
	}
	if s.Port == 0 || s.Port > 65535 {
		errs = append(errs, fmt.Errorf("port %d is invalid", s.Port))
	}
	return errors.Join(append(errs, validateTargets("gpu", s.Gpu)...)...)
}

// --- Registry ------------------------------------------------------------------

type EndpointStatus string

const (
	EndpointStatusActive   EndpointStatus = "active"
	EndpointStatusDisabled EndpointStatus = "disabled"
	EndpointStatusRetired  EndpointStatus = "retired"
)

// ManagedRecord is the registry bookkeeping shared by deployed endpoints and
// services: which stub serves the current version and whether it is live.
type ManagedRecord struct {
	StubID    string         `json:"stub_id"`
	Version   uint           `json:"version"`
	GitSHA    string         `json:"git_sha,omitempty"`
	Status    EndpointStatus `json:"status"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

// Enabled reports whether the record may be filled and routed to.
func (r ManagedRecord) Enabled() bool { return r.Status == EndpointStatusActive }

// ManagedEndpoint is the registry record for a deployed endpoint.
type ManagedEndpoint struct {
	Spec ManagedEndpointSpec `json:"spec"`
	ManagedRecord
}

// ManagedService is the registry record for a deployed shared service.
type ManagedService struct {
	Spec ManagedServiceSpec `json:"spec"`
	ManagedRecord
}

type EndpointVersionState string

const (
	VersionStateCanary     EndpointVersionState = "canary"
	VersionStateActive     EndpointVersionState = "active"
	VersionStateRetired    EndpointVersionState = "retired"
	VersionStateRolledBack EndpointVersionState = "rolled_back"
)

// EndpointVersion tracks one deployed stub version of an endpoint.
type EndpointVersion struct {
	EndpointID string               `json:"endpoint_id"`
	Version    uint                 `json:"version"`
	StubID     string               `json:"stub_id"`
	GitSHA     string               `json:"git_sha,omitempty"`
	State      EndpointVersionState `json:"state"`
	CreatedAt  time.Time            `json:"created_at"`
	UpdatedAt  time.Time            `json:"updated_at"`
}

type RolloutPhase string

const (
	RolloutPhaseIdle       RolloutPhase = "idle"
	RolloutPhaseBaking     RolloutPhase = "baking"
	RolloutPhaseRolledBack RolloutPhase = "rolled_back"
)

// RolloutState describes the active canary/promotion for an endpoint.
type RolloutState struct {
	EndpointID     string       `json:"endpoint_id"`
	ActiveVersion  uint         `json:"active_version"`
	CanaryVersion  uint         `json:"canary_version,omitempty"`
	PinnedVersion  uint         `json:"pinned_version,omitempty"`
	Phase          RolloutPhase `json:"phase"`
	BakeStartedAt  time.Time    `json:"bake_started_at,omitempty"`
	LastDecision   string       `json:"last_decision,omitempty"`
	LastDecisionAt time.Time    `json:"last_decision_at,omitempty"`
	UpdatedAt      time.Time    `json:"updated_at"`
}

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
	// KVTransfer is the connector's own transfer stats (opaque JSON).
	KVTransfer json.RawMessage `json:"kv_transfer,omitempty"`
}

// EndpointReplica is one running container serving an endpoint role/target.
type EndpointReplica struct {
	ID          string `json:"id"`
	EndpointID  string `json:"endpoint_id"`
	Version     uint   `json:"version"`
	Role        string `json:"role"`
	GPU         string `json:"gpu"`
	GPUCount    uint32 `json:"gpu_count"`
	Locality    string `json:"locality"`
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
	// Protected replicas satisfy min_replicas: they may trigger provisioning
	// and are never evictable. Everything else is opportunistic.
	Protected bool `json:"protected"`
	// Tuning replicas are dedicated to live tuning and take no public traffic.
	Tuning bool `json:"tuning"`
	// SecretHash is the SHA-256 of the per-replica secret handed to the
	// container as BEAM_REPLICA_SECRET; harness RPCs must present it.
	SecretHash     string          `json:"secret_hash,omitempty"`
	HarnessEnabled bool            `json:"harness_enabled"`
	ConfigRevision uint64          `json:"config_revision"`
	Capacity       ReplicaCapacity `json:"capacity"`
	Capabilities   json.RawMessage `json:"capabilities,omitempty"`
	StartedAt      time.Time       `json:"started_at"`
	ReadyAt        time.Time       `json:"ready_at,omitempty"`
	LastHeartbeat  time.Time       `json:"last_heartbeat"`
	EndedAt        time.Time       `json:"ended_at,omitempty"`
	DrainDeadline  time.Time       `json:"drain_deadline,omitempty"`
}

// Serving reports whether the replica may receive traffic.
func (r *EndpointReplica) Serving() bool {
	return r != nil && r.Status == ReplicaStatusReady && !r.Tuning
}

// Alive reports whether the replica is scheduling, loading or ready, i.e.
// counts toward a target's live set (draining and evicting replicas do not).
func (r *EndpointReplica) Alive() bool {
	return r.Status == ReplicaStatusScheduling || r.Status == ReplicaStatusLoading || r.Status == ReplicaStatusReady
}

// --- Live config -----------------------------------------------------------------

type ConfigRevisionScope string

const (
	ConfigScopeTarget  ConfigRevisionScope = "target"
	ConfigScopeReplica ConfigRevisionScope = "replica"
)

type ConfigRevisionSource string

const (
	ConfigSourceGit  ConfigRevisionSource = "git"
	ConfigSourceLive ConfigRevisionSource = "live"
)

// EndpointConfigRevision is one version of the live harness config for a
// GPU target (fleet) or a single replica (tuning).
type EndpointConfigRevision struct {
	Revision   uint64               `json:"revision"`
	EndpointID string               `json:"endpoint_id"`
	Scope      ConfigRevisionScope  `json:"scope"`
	ScopeKey   string               `json:"scope_key"`
	Config     map[string]any       `json:"config"`
	Author     string               `json:"author,omitempty"`
	Source     ConfigRevisionSource `json:"source"`
	CreatedAt  time.Time            `json:"created_at"`
}

// ConfigAck is a replica's report on applying a config revision.
type ConfigAck struct {
	ReplicaID string          `json:"replica_id"`
	Revision  uint64          `json:"revision"`
	Applied   bool            `json:"applied"`
	Error     string          `json:"error,omitempty"`
	Effective json.RawMessage `json:"effective,omitempty"`
	At        time.Time       `json:"at"`
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
	Kind       string       `json:"kind"` // "endpoint" | "service"
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
// result per app directory found in the repo.
type GitOpsReport struct {
	RunID   string               `json:"run_id"`
	SHA     string               `json:"sha"`
	Error   string               `json:"error,omitempty"`
	Results []GitOpsDeployResult `json:"results"`
}

// GitOpsDeployResult is the outcome for one app directory. ID is empty when
// the directory's app failed to import.
type GitOpsDeployResult struct {
	Path    string `json:"path"`
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	OK      bool   `json:"ok"`
	Skipped bool   `json:"skipped"` // unchanged since the last applied SHA
	Error   string `json:"error,omitempty"`
	StubID  string `json:"stub_id,omitempty"`
	Version uint   `json:"version,omitempty"`
}

// --- Traffic -------------------------------------------------------------------------

// RouteSample is one completed /v1 request observed by the router.
type RouteSample struct {
	EndpointID       string        `json:"endpoint_id"`
	GPU              string        `json:"gpu"`
	ReplicaID        string        `json:"replica_id"`
	Version          uint          `json:"version"`
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

// Failed reports whether the sample counts as an error for rollout decisions.
func (s RouteSample) Failed() bool { return s.StatusCode >= 500 || s.StatusCode == 0 }

// RouteMetrics aggregates RouteSamples over a window.
type RouteMetrics struct {
	EndpointID       string        `json:"endpoint_id"`
	GPU              string        `json:"gpu,omitempty"`
	Version          uint          `json:"version,omitempty"`
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

// ProviderEarnings is what a workspace earned from requests served by its
// contributed machines.
type ProviderEarnings struct {
	Requests         int64 `json:"requests"`
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	Images           int64 `json:"images"`
	EarningsMicroUSD int64 `json:"earnings_micro_usd"`
}

func (e *ProviderEarnings) Add(o ProviderEarnings) {
	e.Requests += o.Requests
	e.PromptTokens += o.PromptTokens
	e.CompletionTokens += o.CompletionTokens
	e.Images += o.Images
	e.EarningsMicroUSD += o.EarningsMicroUSD
}

type ProviderEarningsReport struct {
	Total      ProviderEarnings            `json:"total"`
	PerMachine map[string]ProviderEarnings `json:"per_machine"`
	PerDay     map[string]ProviderEarnings `json:"per_day"`
}
