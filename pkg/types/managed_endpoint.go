package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"sort"
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

func (t StubType) IsManagedEndpoint() bool {
	return t.Kind() == StubTypeManagedEndpoint
}

func (t StubType) IsManagedService() bool {
	return t.Kind() == StubTypeManagedService
}

func (t StubType) IsManaged() bool {
	return t.IsManagedEndpoint() || t.IsManagedService()
}

type EndpointKind string

const (
	EndpointKindLLM       EndpointKind = "llm"
	EndpointKindEmbedding EndpointKind = "embedding"
	EndpointKindImage     EndpointKind = "image"
	EndpointKindCustom    EndpointKind = "custom"
)

func AllEndpointKinds() []EndpointKind {
	return []EndpointKind{EndpointKindLLM, EndpointKindEmbedding, EndpointKindImage, EndpointKindCustom}
}

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

// DefaultRoutesForKind returns the routes an endpoint of a kind serves when
// the spec does not name them explicitly.
func DefaultRoutesForKind(kind EndpointKind) []EndpointRoute {
	switch kind {
	case EndpointKindLLM:
		return []EndpointRoute{EndpointRouteChatCompletions, EndpointRouteCompletions}
	case EndpointKindEmbedding:
		return []EndpointRoute{EndpointRouteEmbeddings}
	case EndpointKindImage:
		return []EndpointRoute{EndpointRouteImageGenerations}
	default:
		return []EndpointRoute{EndpointRouteInvoke}
	}
}

// AllowedRoutesForKind returns every route an endpoint of a kind may declare.
func AllowedRoutesForKind(kind EndpointKind) []EndpointRoute {
	switch kind {
	case EndpointKindLLM:
		return []EndpointRoute{EndpointRouteChatCompletions, EndpointRouteCompletions, EndpointRouteEmbeddings, EndpointRouteImageGenerations}
	case EndpointKindEmbedding:
		return []EndpointRoute{EndpointRouteEmbeddings}
	case EndpointKindImage:
		return []EndpointRoute{EndpointRouteImageGenerations, EndpointRouteImageEdits}
	default:
		return []EndpointRoute{EndpointRouteInvoke}
	}
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

// IsCPU reports whether the target describes CPU-only placement.
func (t GpuTarget) IsCPU() bool {
	normalized := NormalizeGPUType(t.Type)
	return t.Type == "" || normalized == NO_GPU || t.Count == 0
}

// Key uniquely identifies a target within an endpoint ("H100x2", "cpu").
func (t GpuTarget) Key() string {
	if t.IsCPU() {
		return "cpu"
	}
	return fmt.Sprintf("%sx%d", NormalizeGPUType(t.Type), t.Count)
}

// CPUTarget is the implicit target for endpoints declaring no GPUs.
func CPUTarget() GpuTarget {
	return GpuTarget{Type: string(NO_GPU), Count: 0, MinReplicas: 0, MaxReplicas: 1}
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
func (p Pricing) IsZero() bool {
	return p == Pricing{}
}

var pricingPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]{1,18})?$`)

// Validate checks that every set dimension is a non-negative USD decimal.
func (p Pricing) Validate() error {
	for name, value := range map[string]string{
		"prompt_tokens":        p.PromptTokens,
		"completion_tokens":    p.CompletionTokens,
		"cached_prompt_tokens": p.CachedPromptTokens,
		"request":              p.Request,
		"image":                p.Image,
	} {
		if value == "" {
			continue
		}
		if !pricingPattern.MatchString(value) {
			return fmt.Errorf("pricing.%s: %q is not a non-negative decimal amount", name, value)
		}
	}
	return nil
}

// Rat parses a pricing dimension into an exact rational; empty is zero.
func PricingRat(value string) (*big.Rat, error) {
	if strings.TrimSpace(value) == "" {
		return new(big.Rat), nil
	}
	rat, ok := new(big.Rat).SetString(value)
	if !ok || rat.Sign() < 0 {
		return nil, fmt.Errorf("invalid pricing amount %q", value)
	}
	return rat, nil
}

// ReplicaPolicy controls how replicas of an endpoint behave under preemption.
type ReplicaPolicy struct {
	Evictable       bool    `json:"evictable"`
	DrainSeconds    uint32  `json:"drain_seconds"`
	KeepWarmSeconds uint32  `json:"keep_warm_seconds"`
	SpareShare      float64 `json:"spare_share"`
}

// HarnessSpec enables the in-engine harness (live knobs over EndpointHarnessService).
type HarnessSpec struct {
	Enabled bool `json:"enabled"`
}

// KVCacheSpec opts an endpoint into a shared KV store within a locality.
type KVCacheSpec struct {
	Connector   string         `json:"connector,omitempty"`
	Service     string         `json:"service,omitempty"`
	MinReplicas uint32         `json:"min_replicas,omitempty"`
	Extra       map[string]any `json:"extra,omitempty"`
}

type TopologyMode string

const (
	TopologyMonolithic    TopologyMode = "monolithic"
	TopologyDisaggregated TopologyMode = "disaggregated"
)

const (
	ReplicaRoleServe   = "serve"
	ReplicaRolePrefill = "prefill"
	ReplicaRoleDecode  = "decode"
)

// TopologySpec describes prefill/decode disaggregation. Roles maps a role to
// the GPU targets that role may run on.
type TopologySpec struct {
	Mode  TopologyMode           `json:"mode,omitempty"`
	Roles map[string][]GpuTarget `json:"roles,omitempty"`
}

func (t *TopologySpec) EffectiveMode() TopologyMode {
	if t == nil || t.Mode == "" {
		return TopologyMonolithic
	}
	return t.Mode
}

// ManagedEndpointSpec is the repo contract: everything an endpoint app declares.
type ManagedEndpointSpec struct {
	ID         string          `json:"id"`
	Kind       EndpointKind    `json:"kind"`
	Engine     string          `json:"engine,omitempty"`
	Port       uint32          `json:"port"`
	Health     string          `json:"health,omitempty"`
	Metrics    string          `json:"metrics,omitempty"`
	Gpu        []GpuTarget     `json:"gpu"`
	Routes     []EndpointRoute `json:"routes,omitempty"`
	Pricing    Pricing         `json:"pricing"`
	Catalog    Catalog         `json:"catalog"`
	Policy     ReplicaPolicy   `json:"policy"`
	Harness    HarnessSpec     `json:"harness"`
	KVCache    *KVCacheSpec    `json:"kv_cache,omitempty"`
	Topology   *TopologySpec   `json:"topology,omitempty"`
	Services   []string        `json:"services,omitempty"`
	Locality   []string        `json:"locality,omitempty"`
	Entrypoint []string        `json:"entrypoint,omitempty"`
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

var endpointIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*(/[a-z0-9][a-z0-9._-]*)?$`)
var serviceNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// ManagedEndpointValidation configures spec validation.
type ManagedEndpointValidation struct {
	AllowedEngines []string
	AllowedKinds   []EndpointKind
	// KnownServices resolves spec.Services references.
	KnownServices map[string]struct{}
}

func (v ManagedEndpointValidation) engineAllowed(engine string) bool {
	if len(v.AllowedEngines) == 0 {
		return true
	}
	for _, allowed := range v.AllowedEngines {
		if strings.EqualFold(allowed, engine) {
			return true
		}
	}
	return false
}

func (v ManagedEndpointValidation) kindAllowed(kind EndpointKind) bool {
	if len(v.AllowedKinds) == 0 {
		return true
	}
	for _, allowed := range v.AllowedKinds {
		if allowed == kind {
			return true
		}
	}
	return false
}

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
	if s.Health == "" {
		s.Health = "/health"
	}
	s.Health = "/" + strings.TrimPrefix(strings.TrimSpace(s.Health), "/")
	if s.Metrics != "" {
		s.Metrics = "/" + strings.TrimPrefix(strings.TrimSpace(s.Metrics), "/")
	}
	if len(s.Routes) == 0 {
		s.Routes = DefaultRoutesForKind(s.Kind)
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
	if s.Topology != nil {
		for role := range s.Topology.Roles {
			normalizeTargets(s.Topology.Roles[role], s.Policy.SpareShare)
		}
	}
	if s.KVCache != nil && s.KVCache.MinReplicas == 0 {
		s.KVCache.MinReplicas = 2
	}
	if s.Catalog.Name == "" {
		s.Catalog.Name = s.ID
	}
}

func normalizeTargets(targets []GpuTarget, spareShare float64) {
	for i := range targets {
		t := &targets[i]
		if t.IsCPU() {
			t.Type = string(NO_GPU)
			t.Count = 0
		} else {
			t.Type = string(NormalizeGPUType(t.Type))
			if t.Count == 0 {
				t.Count = 1
			}
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
	if !endpointIDPattern.MatchString(s.ID) {
		errs = append(errs, fmt.Errorf("id %q must look like vendor/slug (lowercase, [a-z0-9._-])", s.ID))
	}
	validKind := false
	for _, kind := range AllEndpointKinds() {
		if kind == s.Kind {
			validKind = true
		}
	}
	if !validKind {
		errs = append(errs, fmt.Errorf("kind %q is not one of llm, embedding, image, custom", s.Kind))
	} else if !policy.kindAllowed(s.Kind) {
		errs = append(errs, fmt.Errorf("kind %q is not enabled on this cluster", s.Kind))
	}
	if s.Engine != "" && !policy.engineAllowed(s.Engine) {
		errs = append(errs, fmt.Errorf("engine %q is not in the allowed engine list", s.Engine))
	}
	if s.Port == 0 || s.Port > 65535 {
		errs = append(errs, fmt.Errorf("port %d is invalid", s.Port))
	}
	if len(s.Entrypoint) == 0 {
		errs = append(errs, errors.New("entrypoint is required"))
	}
	if validKind {
		allowed := AllowedRoutesForKind(s.Kind)
		for _, route := range s.Routes {
			ok := false
			for _, a := range allowed {
				if a == route {
					ok = true
				}
			}
			if !ok {
				errs = append(errs, fmt.Errorf("route %q is not valid for kind %q", route, s.Kind))
			}
		}
	}
	if err := s.Pricing.Validate(); err != nil {
		errs = append(errs, err)
	}
	if s.Kind == EndpointKindImage && s.Pricing.Image == "" && s.Pricing.Request == "" && !s.Catalog.Free {
		errs = append(errs, errors.New("image endpoints must price per image or per request, or be marked free"))
	}
	errs = append(errs, validateTargets("gpu", s.Gpu)...)
	if s.Topology != nil && s.Topology.EffectiveMode() == TopologyDisaggregated {
		for _, role := range []string{ReplicaRolePrefill, ReplicaRoleDecode} {
			targets, ok := s.Topology.Roles[role]
			if !ok || len(targets) == 0 {
				errs = append(errs, fmt.Errorf("topology.roles.%s is required for disaggregated mode", role))
				continue
			}
			errs = append(errs, validateTargets("topology.roles."+role, targets)...)
		}
		for role := range s.Topology.Roles {
			if role != ReplicaRolePrefill && role != ReplicaRoleDecode {
				errs = append(errs, fmt.Errorf("topology.roles.%s: unknown role", role))
			}
		}
		if s.KVCache == nil {
			errs = append(errs, errors.New("disaggregated topology requires kv_cache"))
		}
	}
	if s.KVCache != nil {
		if s.KVCache.Connector == "" {
			errs = append(errs, errors.New("kv_cache.connector is required"))
		}
		if s.KVCache.Service != "" {
			s.Services = appendUnique(s.Services, s.KVCache.Service)
		}
	}
	for _, service := range s.Services {
		if policy.KnownServices != nil {
			if _, ok := policy.KnownServices[service]; !ok {
				errs = append(errs, fmt.Errorf("service %q is not deployed", service))
			}
		}
	}
	if s.Policy.SpareShare <= 0 || s.Policy.SpareShare > 1 {
		errs = append(errs, fmt.Errorf("policy.spare_share %v must be in (0, 1]", s.Policy.SpareShare))
	}
	return errors.Join(errs...)
}

func validateTargets(field string, targets []GpuTarget) []error {
	var errs []error
	seen := map[string]struct{}{}
	for i, t := range targets {
		prefix := fmt.Sprintf("%s[%d]", field, i)
		if !t.IsCPU() && !KnownGPUType(GpuType(t.Type)) {
			errs = append(errs, fmt.Errorf("%s: unknown gpu type %q", prefix, t.Type))
		}
		if t.Count > 8 {
			errs = append(errs, fmt.Errorf("%s: count %d exceeds 8", prefix, t.Count))
		}
		if t.MinReplicas > t.MaxReplicas {
			errs = append(errs, fmt.Errorf("%s: min_replicas %d > max_replicas %d", prefix, t.MinReplicas, t.MaxReplicas))
		}
		if t.Share <= 0 || t.Share > 1 {
			errs = append(errs, fmt.Errorf("%s: share %v must be in (0, 1]", prefix, t.Share))
		}
		key := t.Key()
		if _, dup := seen[key]; dup {
			errs = append(errs, fmt.Errorf("%s: duplicate target %s", prefix, key))
		}
		seen[key] = struct{}{}
	}
	return errs
}

func appendUnique(values []string, value string) []string {
	for _, v := range values {
		if v == value {
			return values
		}
	}
	return append(values, value)
}

// Targets returns every (role, target) placement for the endpoint.
func (s *ManagedEndpointSpec) Targets() []RoleTarget {
	if s.Topology != nil && s.Topology.EffectiveMode() == TopologyDisaggregated {
		roles := make([]string, 0, len(s.Topology.Roles))
		for role := range s.Topology.Roles {
			roles = append(roles, role)
		}
		sort.Strings(roles)
		var out []RoleTarget
		for _, role := range roles {
			for _, t := range s.Topology.Roles[role] {
				out = append(out, RoleTarget{Role: role, Target: t})
			}
		}
		return out
	}
	out := make([]RoleTarget, 0, len(s.Gpu))
	for _, t := range s.Gpu {
		out = append(out, RoleTarget{Role: ReplicaRoleServe, Target: t})
	}
	return out
}

// RoleTarget pairs a replica role with a GPU target.
type RoleTarget struct {
	Role   string
	Target GpuTarget
}

func (rt RoleTarget) Key() string {
	return rt.Role + ":" + rt.Target.Key()
}

// ServesRoute reports whether the endpoint declares a route.
func (s *ManagedEndpointSpec) ServesRoute(route EndpointRoute) bool {
	for _, r := range s.Routes {
		if r == route {
			return true
		}
	}
	return false
}

// Normalize fills service defaults.
func (s *ManagedServiceSpec) Normalize() {
	s.Name = strings.ToLower(strings.TrimSpace(s.Name))
	if s.Port == 0 {
		s.Port = 8000
	}
	if s.Health != "" {
		s.Health = "/" + strings.TrimPrefix(strings.TrimSpace(s.Health), "/")
	}
	if s.Replicas == 0 {
		s.Replicas = 1
	}
	if len(s.Gpu) == 0 {
		s.Gpu = []GpuTarget{CPUTarget()}
	}
	normalizeTargets(s.Gpu, 1)
	for i := range s.Gpu {
		s.Gpu[i].Share = 1
		s.Gpu[i].MinReplicas = s.Replicas
		s.Gpu[i].MaxReplicas = s.Replicas
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
	errs = append(errs, validateTargets("gpu", s.Gpu)...)
	return errors.Join(errs...)
}

// --- Runtime state -------------------------------------------------------

type EndpointStatus string

const (
	EndpointStatusActive   EndpointStatus = "active"
	EndpointStatusDisabled EndpointStatus = "disabled"
	EndpointStatusRetired  EndpointStatus = "retired"
)

// ManagedEndpoint is the registry record for a deployed endpoint version.
type ManagedEndpoint struct {
	Spec      ManagedEndpointSpec `json:"spec"`
	StubID    string              `json:"stub_id"`
	Version   uint                `json:"version"`
	GitSHA    string              `json:"git_sha,omitempty"`
	Enabled   bool                `json:"enabled"`
	Status    EndpointStatus      `json:"status"`
	CreatedAt time.Time           `json:"created_at"`
	UpdatedAt time.Time           `json:"updated_at"`
}

// ManagedService is the registry record for a deployed shared service.
type ManagedService struct {
	Spec      ManagedServiceSpec `json:"spec"`
	StubID    string             `json:"stub_id"`
	Version   uint               `json:"version"`
	GitSHA    string             `json:"git_sha,omitempty"`
	Enabled   bool               `json:"enabled"`
	Status    EndpointStatus     `json:"status"`
	CreatedAt time.Time          `json:"created_at"`
	UpdatedAt time.Time          `json:"updated_at"`
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

// Terminal reports whether a replica in this status will never serve again.
func (s ReplicaStatus) Terminal() bool {
	switch s {
	case ReplicaStatusEvicted, ReplicaStatusFailed, ReplicaStatusStopped:
		return true
	}
	return false
}

// KVTransferStats are reported by harnesses using a KV connector.
type KVTransferStats struct {
	TransferBytes      int64   `json:"transfer_bytes,omitempty"`
	StoreHitRateMilli  int64   `json:"store_hit_rate_milli,omitempty"`
	RemotePrefillCount int64   `json:"remote_prefill_count,omitempty"`
	StoreUsageMilli    int64   `json:"store_usage_milli,omitempty"`
	Extra              float64 `json:"extra,omitempty"`
}

// ReplicaCapacity is the harness- or probe-reported serving capacity.
type ReplicaCapacity struct {
	InFlight            int64            `json:"in_flight"`
	MaxConcurrency      int64            `json:"max_concurrency"`
	Running             int64            `json:"running"`
	Waiting             int64            `json:"waiting"`
	KVCacheFreeMilli    int64            `json:"kv_cache_free_milli"`
	DecodeTokensPerSec  int64            `json:"decode_tokens_per_sec"`
	PromptTokensPerSec  int64            `json:"prompt_tokens_per_sec"`
	TTFTMs              int64            `json:"ttft_ms"`
	TPOTMs              int64            `json:"tpot_ms"`
	PrefixCacheHitMilli int64            `json:"prefix_cache_hit_milli"`
	KVTransfer          *KVTransferStats `json:"kv_transfer,omitempty"`
}

// EndpointReplica is one running container serving an endpoint role/target.
type EndpointReplica struct {
	ID             string          `json:"id"`
	EndpointID     string          `json:"endpoint_id"`
	Version        uint            `json:"version"`
	Role           string          `json:"role"`
	GPU            string          `json:"gpu"`
	GPUCount       uint32          `json:"gpu_count"`
	Locality       string          `json:"locality"`
	PoolName       string          `json:"pool_name,omitempty"`
	ContainerID    string          `json:"container_id"`
	WorkerID       string          `json:"worker_id,omitempty"`
	Address        string          `json:"address,omitempty"`
	Status         ReplicaStatus   `json:"status"`
	Protected      bool            `json:"protected"`
	Candidate      bool            `json:"candidate"`
	Tuning         bool            `json:"tuning"`
	HarnessEnabled bool            `json:"harness_enabled"`
	ConfigRevision uint64          `json:"config_revision"`
	Capacity       ReplicaCapacity `json:"capacity"`
	Capabilities   json.RawMessage `json:"capabilities,omitempty"`
	StartedAt      time.Time       `json:"started_at"`
	ReadyAt        time.Time       `json:"ready_at,omitempty"`
	LastHeartbeat  time.Time       `json:"last_heartbeat"`
	StatusReason   string          `json:"status_reason,omitempty"`
}

// Serving reports whether the replica may receive traffic.
func (r *EndpointReplica) Serving() bool {
	return r != nil && r.Status == ReplicaStatusReady && !r.Tuning && !r.Candidate
}

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
// GPU target (fleet) or a single replica (experiment).
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

type EndpointVersionState string

const (
	VersionStateCanary     EndpointVersionState = "canary"
	VersionStateActive     EndpointVersionState = "active"
	VersionStateRetired    EndpointVersionState = "retired"
	VersionStateRolledBack EndpointVersionState = "rolled_back"
	VersionStatePinned     EndpointVersionState = "pinned"
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

// RolloutState describes the active canary/promotion for an endpoint.
type RolloutState struct {
	EndpointID     string    `json:"endpoint_id"`
	ActiveVersion  uint      `json:"active_version"`
	CanaryVersion  uint      `json:"canary_version,omitempty"`
	PinnedVersion  uint      `json:"pinned_version,omitempty"`
	Phase          string    `json:"phase"` // idle|baking|promoting|rolled_back
	BakeStartedAt  time.Time `json:"bake_started_at,omitempty"`
	LastDecision   string    `json:"last_decision,omitempty"`
	LastDecisionAt time.Time `json:"last_decision_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// EndpointCandidate is a replica held out of serving for validation.
type EndpointCandidate struct {
	ReplicaID  string    `json:"replica_id"`
	EndpointID string    `json:"endpoint_id"`
	Version    uint      `json:"version"`
	Reason     string    `json:"reason"`
	CreatedAt  time.Time `json:"created_at"`
}

// ExperimentStep is one applied config + its observed metrics/bench.
type ExperimentStep struct {
	Revision      uint64          `json:"revision"`
	Config        map[string]any  `json:"config"`
	Applied       bool            `json:"applied"`
	Error         string          `json:"error,omitempty"`
	Bench         json.RawMessage `json:"bench,omitempty"`
	EngineMetrics ReplicaCapacity `json:"engine_metrics"`
	At            time.Time       `json:"at"`
}

type ExperimentOutcome string

const (
	ExperimentOutcomeRunning ExperimentOutcome = "running"
	ExperimentOutcomeKeep    ExperimentOutcome = "keep"
	ExperimentOutcomeDiscard ExperimentOutcome = "discard"
	ExperimentOutcomeFailed  ExperimentOutcome = "failed"
)

// Experiment is a live-tuning session on one dedicated replica.
type Experiment struct {
	ID               string            `json:"id"`
	EndpointID       string            `json:"endpoint_id"`
	GPU              string            `json:"gpu"`
	Role             string            `json:"role"`
	ReplicaID        string            `json:"replica_id"`
	BaselineRevision uint64            `json:"baseline_revision"`
	CurrentRevision  uint64            `json:"current_revision"`
	Steps            []ExperimentStep  `json:"steps"`
	Outcome          ExperimentOutcome `json:"outcome"`
	Notes            string            `json:"notes,omitempty"`
	Author           string            `json:"author,omitempty"`
	Budget           string            `json:"budget,omitempty"`
	StartedAt        time.Time         `json:"started_at"`
	EndedAt          time.Time         `json:"ended_at,omitempty"`
}

// GitOpsEndpointState is the apply state of one repo directory.
type GitOpsEndpointState struct {
	Path       string    `json:"path"`
	ID         string    `json:"id"`
	Kind       string    `json:"kind"` // endpoint|service
	AppliedSHA string    `json:"applied_sha,omitempty"`
	Status     string    `json:"status"` // pending|applied|failed|retired
	Error      string    `json:"error,omitempty"`
	StubID     string    `json:"stub_id,omitempty"`
	Version    uint      `json:"version,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
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
}

// EndpointUsage is one billable request record for the /v1 route.
type EndpointUsage struct {
	EndpointID       string `json:"endpoint_id"`
	WorkspaceID      string `json:"workspace_id"`
	TokenID          string `json:"token_id,omitempty"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
	CachedTokens     int64  `json:"cached_tokens"`
	Requests         int64  `json:"requests"`
	Images           int64  `json:"images"`
	CostMicroUSD     int64  `json:"cost_micro_usd"`
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
func (s RouteSample) Failed() bool {
	return s.StatusCode >= 500 || s.StatusCode == 0
}

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
	decode := m.DurationSumMs - m.TTFTSumMs
	if decode < 0 {
		decode = 0
	}
	return decode / m.CompletionTokens
}

// TokensPerSecond is aggregate completion throughput over the window.
func (m RouteMetrics) TokensPerSecond() int64 {
	if m.Window <= 0 {
		return 0
	}
	return int64(float64(m.CompletionTokens) / m.Window.Seconds())
}
