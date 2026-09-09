package managedendpoint

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

// OpenRouter-shaped listings. Paths and field names follow OpenRouter's API
// so its SDKs and provider tooling work unchanged.

const providerSchemaVersion = "2.4"

func (r *router) visibleEndpoints(ctx context.Context, authInfo *auth.AuthInfo) ([]*types.ManagedEndpoint, error) {
	endpoints, err := r.s.repo.ListEndpoints(ctx)
	if err != nil {
		return nil, err
	}
	var out []*types.ManagedEndpoint
	for _, endpoint := range endpoints {
		if !endpoint.Enabled || !r.allowed(ctx, endpoint, authInfo) {
			continue
		}
		out = append(out, endpoint)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Spec.ID < out[j].Spec.ID })
	return out, nil
}

func modalities(spec *types.ManagedEndpointSpec) (input []string, output []string) {
	switch spec.Kind {
	case types.EndpointKindLLM:
		input, output = []string{"text"}, []string{"text"}
		for _, m := range spec.Catalog.Modalities {
			if m == "image" || m == "vision" {
				input = append(input, "image")
			}
			if m == "audio" {
				input = append(input, "audio")
			}
		}
	case types.EndpointKindEmbedding:
		input, output = []string{"text"}, []string{"embeddings"}
	case types.EndpointKindImage:
		input, output = []string{"text"}, []string{"image"}
		if spec.ServesRoute(types.EndpointRouteImageEdits) {
			input = append(input, "image")
		}
	default:
		input, output = []string{"text"}, []string{"text"}
	}
	return input, output
}

func modalityString(input, output []string) string {
	return strings.Join(input, "+") + "->" + strings.Join(output, "+")
}

func (r *router) modelEntry(endpoint *types.ManagedEndpoint) map[string]any {
	spec := &endpoint.Spec
	input, output := modalities(spec)
	name := spec.Catalog.Name
	if name == "" {
		name = spec.ID
	}
	pricing := map[string]any{
		"prompt":     pricingString(spec.Pricing.PromptTokens),
		"completion": pricingString(spec.Pricing.CompletionTokens),
		"request":    pricingString(spec.Pricing.Request),
		"image":      pricingString(spec.Pricing.Image),
	}
	if spec.Pricing.CachedPromptTokens != "" {
		pricing["input_cache_read"] = spec.Pricing.CachedPromptTokens
	}
	entry := map[string]any{
		"id":             spec.ID,
		"canonical_slug": spec.ID,
		"name":           name,
		"created":        endpoint.CreatedAt.Unix(),
		"description":    spec.Catalog.Description,
		"context_length": spec.Catalog.ContextLength,
		"architecture": map[string]any{
			"modality":          modalityString(input, output),
			"input_modalities":  input,
			"output_modalities": output,
			"tokenizer":         spec.Catalog.Tokenizer,
			"instruct_type":     nilIfEmpty(spec.Catalog.InstructType),
		},
		"pricing": pricing,
		"top_provider": map[string]any{
			"context_length":        spec.Catalog.ContextLength,
			"max_completion_tokens": nilIfZero(spec.Catalog.MaxCompletionTokens),
			"is_moderated":          false,
		},
		"per_request_limits":   nil,
		"supported_parameters": nonNil(spec.Catalog.SupportedParameters),
		"hugging_face_id":      spec.Catalog.HFID,
		"owned_by":             providerName,
		"object":               "model",
	}
	return entry
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nilIfZero(n uint32) any {
	if n == 0 {
		return nil
	}
	return n
}

func nonNil(list []string) []string {
	if list == nil {
		return []string{}
	}
	return list
}

func (r *router) handleListModels(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	endpoints, err := r.visibleEndpoints(ctx.Request().Context(), cc.AuthInfo)
	if err != nil {
		return writeRouteError(ctx, &routeError{http.StatusServiceUnavailable, "registry_unavailable", "endpoint registry unavailable"})
	}
	if ctx.QueryParam("format") == "openrouter-provider" {
		return r.providerDocument(ctx, endpoints)
	}
	data := make([]map[string]any, 0, len(endpoints))
	for _, endpoint := range endpoints {
		data = append(data, r.modelEntry(endpoint))
	}
	return ctx.JSON(http.StatusOK, map[string]any{"object": "list", "data": data})
}

// providerDocument renders the OpenRouter provider listing: one model per
// endpoint with readiness, capacity and datacenters derived from localities.
func (r *router) providerDocument(ctx echo.Context, endpoints []*types.ManagedEndpoint) error {
	replicas, _ := r.s.repo.ListAllReplicas(ctx.Request().Context())
	models := make([]map[string]any, 0, len(endpoints))
	for _, endpoint := range endpoints {
		spec := &endpoint.Spec
		input, output := modalities(spec)
		ready, datacenters, maxConcurrency := replicaSummary(replicas, spec.ID, endpoint.Version)
		models = append(models, map[string]any{
			"id":                    spec.ID,
			"name":                  firstNonEmpty(spec.Catalog.Name, spec.ID),
			"hugging_face_id":       spec.Catalog.HFID,
			"is_ready":              ready > 0,
			"description":           spec.Catalog.Description,
			"context_length":        spec.Catalog.ContextLength,
			"max_completion_tokens": spec.Catalog.MaxCompletionTokens,
			"quantization":          "",
			"modalities": map[string]any{
				"input":  input,
				"output": output,
			},
			"pricing": map[string]any{
				"prompt":           pricingString(spec.Pricing.PromptTokens),
				"completion":       pricingString(spec.Pricing.CompletionTokens),
				"input_cache_read": pricingString(spec.Pricing.CachedPromptTokens),
				"request":          pricingString(spec.Pricing.Request),
				"image":            pricingString(spec.Pricing.Image),
			},
			"capacity": map[string]any{
				"ready_replicas":  ready,
				"max_concurrency": maxConcurrency,
			},
			"supported_parameters": nonNil(spec.Catalog.SupportedParameters),
			"datacenters":          datacenters,
			"endpoints": map[string]any{
				"chat_completions":  route(spec, types.EndpointRouteChatCompletions, r.prefix),
				"completions":       route(spec, types.EndpointRouteCompletions, r.prefix),
				"embeddings":        route(spec, types.EndpointRouteEmbeddings, r.prefix),
				"image_generations": route(spec, types.EndpointRouteImageGenerations, r.prefix),
			},
		})
	}
	return ctx.JSON(http.StatusOK, map[string]any{
		"schema_version": providerSchemaVersion,
		"provider":       providerName,
		"models":         models,
	})
}

func route(spec *types.ManagedEndpointSpec, route types.EndpointRoute, prefix string) any {
	if !spec.ServesRoute(route) {
		return nil
	}
	return prefix + "/" + string(route)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func replicaSummary(replicas []*types.EndpointReplica, endpointID string, version uint) (ready int, datacenters []string, maxConcurrency int64) {
	seen := map[string]bool{}
	for _, r := range replicas {
		if r.EndpointID != endpointID || !r.Serving() {
			continue
		}
		ready++
		maxConcurrency += r.Capacity.MaxConcurrency
		if r.Locality != "" && !seen[r.Locality] {
			seen[r.Locality] = true
			datacenters = append(datacenters, r.Locality)
		}
	}
	sort.Strings(datacenters)
	if datacenters == nil {
		datacenters = []string{}
	}
	return ready, datacenters, maxConcurrency
}

// handleModelEndpoints lists one entry per (gpu target, locality) with status
// and recent latency, mirroring OpenRouter's /models/:author/:slug/endpoints.
func (r *router) handleModelEndpoints(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	id := ctx.Param("slug")
	if author := ctx.Param("author"); author != "" {
		id = author + "/" + id
	}
	rctx := ctx.Request().Context()
	endpoint, err := r.s.repo.GetEndpoint(rctx, id)
	if err != nil {
		return writeRouteError(ctx, &routeError{http.StatusServiceUnavailable, "registry_unavailable", "endpoint registry unavailable"})
	}
	if endpoint == nil || !endpoint.Enabled || !r.allowed(rctx, endpoint, cc.AuthInfo) {
		return writeRouteError(ctx, &routeError{http.StatusNotFound, "model_not_found", "model not found"})
	}
	replicas, _ := r.s.repo.ListReplicas(rctx, endpoint.Spec.ID)

	type groupKey struct{ gpu, locality string }
	groups := map[groupKey][]*types.EndpointReplica{}
	for _, replica := range replicas {
		if replica.Status.Terminal() || replica.Tuning || replica.Candidate {
			continue
		}
		key := groupKey{replica.GPU, replica.Locality}
		groups[key] = append(groups[key], replica)
	}
	for _, rt := range endpoint.Spec.Targets() {
		key := groupKey{rt.Target.Key(), ""}
		if _, ok := groups[key]; !ok {
			hasAny := false
			for k := range groups {
				if k.gpu == key.gpu {
					hasAny = true
				}
			}
			if !hasAny {
				groups[key] = nil
			}
		}
	}

	entries := make([]map[string]any, 0, len(groups))
	for key, list := range groups {
		ready := 0
		for _, replica := range list {
			if replica.Status == types.ReplicaStatusReady {
				ready++
			}
		}
		metrics, _ := r.s.repo.GetRouteMetrics(rctx, endpoint.Spec.ID, key.gpu, 0, 15*time.Minute)
		status := 0
		if ready == 0 {
			status = -1
		}
		entry := map[string]any{
			"name":                  endpoint.Spec.ID + " | " + key.gpu,
			"provider_name":         providerName,
			"tag":                   key.gpu,
			"gpu":                   key.gpu,
			"locality":              key.locality,
			"status":                status,
			"ready_replicas":        ready,
			"total_replicas":        len(list),
			"context_length":        endpoint.Spec.Catalog.ContextLength,
			"max_completion_tokens": nilIfZero(endpoint.Spec.Catalog.MaxCompletionTokens),
			"quantization":          nil,
			"supported_parameters":  nonNil(endpoint.Spec.Catalog.SupportedParameters),
			"pricing": map[string]any{
				"prompt":     pricingString(endpoint.Spec.Pricing.PromptTokens),
				"completion": pricingString(endpoint.Spec.Pricing.CompletionTokens),
				"request":    pricingString(endpoint.Spec.Pricing.Request),
				"image":      pricingString(endpoint.Spec.Pricing.Image),
			},
			"uptime_last_30m": nil,
		}
		if metrics != nil && metrics.Requests > 0 {
			entry["latency_ms"] = metrics.MeanTTFTMs()
			entry["requests_15m"] = metrics.Requests
			entry["error_rate_15m"] = metrics.ErrorRate()
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i]["name"].(string) < entries[j]["name"].(string)
	})

	model := r.modelEntry(endpoint)
	model["endpoints"] = entries
	return ctx.JSON(http.StatusOK, map[string]any{"data": model})
}

// handleGeneration returns the metered record of one request by id.
func (r *router) handleGeneration(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	id := strings.TrimSpace(ctx.QueryParam("id"))
	if id == "" {
		return writeRouteError(ctx, &routeError{http.StatusBadRequest, "missing_id", "id query parameter is required"})
	}
	record, err := r.s.repo.GetGeneration(ctx.Request().Context(), id)
	if err != nil || record == nil {
		return writeRouteError(ctx, &routeError{http.StatusNotFound, "generation_not_found", "generation not found"})
	}
	if record.WorkspaceID != cc.AuthInfo.Workspace.ExternalId && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return writeRouteError(ctx, &routeError{http.StatusNotFound, "generation_not_found", "generation not found"})
	}
	return ctx.JSON(http.StatusOK, map[string]any{"data": map[string]any{
		"id":                       record.RequestID,
		"model":                    record.Model,
		"provider_name":            providerName,
		"created_at":               record.Timestamp.UTC().Format(time.RFC3339Nano),
		"streamed":                 record.Stream,
		"generation_time":          record.DurationMs,
		"latency":                  record.TTFTMs,
		"tokens_prompt":            record.PromptTokens,
		"tokens_completion":        record.CompletionTokens,
		"native_tokens_prompt":     record.PromptTokens,
		"native_tokens_completion": record.CompletionTokens,
		"native_tokens_cached":     record.CachedTokens,
		"num_media_generation":     record.Images,
		"total_cost":               costUSD(record.CostMicroUSD),
		"usage":                    costUSD(record.CostMicroUSD),
		"cache_discount":           nil,
		"finish_reason":            nil,
		"is_byok":                  false,
		"gpu":                      record.GPU,
		"locality":                 record.Locality,
		"status_code":              record.StatusCode,
	}})
}

// handleKey reports the calling key's label and recent spend.
func (r *router) handleKey(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	rctx := ctx.Request().Context()
	today, _, _ := r.s.repo.GetWorkspaceUsage(rctx, cc.AuthInfo.Workspace.ExternalId, 1)
	week, _, _ := r.s.repo.GetWorkspaceUsage(rctx, cc.AuthInfo.Workspace.ExternalId, 7)
	month, perEndpoint, _ := r.s.repo.GetWorkspaceUsage(rctx, cc.AuthInfo.Workspace.ExternalId, 30)

	label := cc.AuthInfo.Token.ExternalId
	if cc.AuthInfo.Workspace.Name != "" {
		label = cc.AuthInfo.Workspace.Name + "/" + label
	}
	endpoints := map[string]any{}
	for id, usage := range perEndpoint {
		endpoints[id] = map[string]any{
			"requests":          usage.Requests,
			"prompt_tokens":     usage.PromptTokens,
			"completion_tokens": usage.CompletionTokens,
			"images":            usage.Images,
			"cost":              costUSD(usage.CostMicroUSD),
		}
	}
	return ctx.JSON(http.StatusOK, map[string]any{"data": map[string]any{
		"label":               label,
		"limit":               nil,
		"usage":               costUSD(month.CostMicroUSD),
		"usage_daily":         costUSD(today.CostMicroUSD),
		"usage_weekly":        costUSD(week.CostMicroUSD),
		"usage_monthly":       costUSD(month.CostMicroUSD),
		"is_free_tier":        false,
		"is_provisioning_key": cc.AuthInfo.Token.TokenType == types.TokenTypeClusterAdmin,
		"rate_limit": map[string]any{
			"requests": r.s.config.Routing.PerWorkspaceConcurrency,
			"interval": "concurrent",
		},
		"endpoints": endpoints,
	}})
}
