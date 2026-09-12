package managedendpoint

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"mime"
	"mime/multipart"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/common/llmroute"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

// The /v1 route is the one admission layer: publication lookup, caller
// authorization and credit check, then replica selection and proxying.
// OpenAI-compatible paths are thin protocol adapters over it.

const (
	headerReplicaPin    = "X-Beam-Endpoint-Replica"
	headerRequestID     = "X-Request-ID"
	headerEndpointID    = "X-Beam-Endpoint-ID"
	headerReplicaServed = "X-Beam-Replica"
)

// protocol is one OpenAI-style path: where a model server serves it and
// whether it carries LLM usage (affinity routing; usage in the final chunk).
type protocol struct {
	upstream string
	llm      bool
}

var protocols = map[types.EndpointRoute]protocol{
	types.EndpointRouteChatCompletions:  {"/v1/chat/completions", true},
	types.EndpointRouteCompletions:      {"/v1/completions", true},
	types.EndpointRouteEmbeddings:       {"/v1/embeddings", false},
	types.EndpointRouteImageGenerations: {"/v1/images/generations", false},
	types.EndpointRouteImageEdits:       {"/v1/images/edits", false},
	types.EndpointRouteAudioSpeech:      {"/v1/audio/speech", false},
	types.EndpointRouteInvoke:           {"/invoke", false},
}

// kindRoutes are the protocols each engine kind serves besides invoke.
var kindRoutes = map[types.EndpointKind][]types.EndpointRoute{
	types.EndpointKindLLM:       {types.EndpointRouteChatCompletions, types.EndpointRouteCompletions},
	types.EndpointKindEmbedding: {types.EndpointRouteEmbeddings},
	types.EndpointKindImage:     {types.EndpointRouteImageGenerations, types.EndpointRouteImageEdits},
}

// serves reports whether an app's engine kind serves a route.
func serves(app *types.ManagedEndpoint, route types.EndpointRoute) bool {
	return route == types.EndpointRouteInvoke || slices.Contains(kindRoutes[app.Spec.Kind], route)
}

type router struct {
	s        *Service
	prefix   string
	selector llmroute.Selector

	states    sync.Map // endpoint id -> *llmroute.State (shared affinity and pressure)
	inflight  sync.Map // replica id -> *atomic.Int64
	admission sync.Map // endpoint id / endpoint|workspace -> *atomic.Int64 (per gateway; see admit)
}

func newRouter(s *Service) *router {
	return &router{s: s, prefix: s.config.RoutePrefix}
}

func (r *router) mount(group *echo.Group, authMiddleware echo.MiddlewareFunc) {
	g := group.Group(r.prefix, openAIErrors, authMiddleware)
	// The catalog is public: anonymous callers see public models, a token
	// adds the models its workspace may call.
	g.GET("/models", r.handleListModels)
	g.GET("/models/openrouter", r.handleListOpenRouterModels)
	g.GET("/models/:author/:slug", r.handleGetModel)
	g.GET("/models/:slug", r.handleGetModel)
	g.GET("/generation", auth.WithAuth(r.handleGeneration))
	for name := range protocols {
		if name.ModelScoped() {
			g.POST("/models/:author/:slug/"+string(name), auth.WithAuth(r.handleRoute))
			g.POST("/models/:slug/"+string(name), auth.WithAuth(r.handleRoute))
		} else {
			g.POST("/"+string(name), auth.WithAuth(r.handleRoute))
		}
	}
}

func (r *router) state(endpointID string) *llmroute.State {
	st, _ := r.states.LoadOrStore(endpointID, llmroute.NewState(r.s.rdb, "managed_endpoint:route:"+endpointID))
	return st.(*llmroute.State)
}

func counter(m *sync.Map, key string) *atomic.Int64 {
	v, _ := m.LoadOrStore(key, &atomic.Int64{})
	return v.(*atomic.Int64)
}

func (r *router) handleRoute(ctx echo.Context) error {
	cc, ok := ctx.(*auth.HttpAuthContext)
	if !ok || !workspaceCaller(cc.AuthInfo) {
		return errUnauthorized.write(ctx)
	}
	if !r.s.Enabled() {
		return errEndpointsDisabled.write(ctx)
	}
	route, pathModel, ok := routeFromPath(r.prefix, ctx.Request().URL.Path)
	if !ok {
		return errUnknownRoute.write(ctx)
	}
	rq := &routeRequest{
		ctx: ctx, auth: cc.AuthInfo, route: route, proto: protocols[route],
		requestID: "gen-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:20], startedAt: time.Now(),
	}
	ctx.Response().Header().Set(headerRequestID, rq.requestID)
	if clusterAdmin(cc.AuthInfo) {
		rq.pinReplica = strings.TrimSpace(ctx.Request().Header.Get(headerReplicaPin))
	}
	if rerr := r.readRequest(rq, pathModel); rerr != nil {
		return rerr.write(ctx)
	}
	app, rerr := r.resolveEndpoint(ctx.Request().Context(), rq)
	if rerr != nil {
		return rerr.write(ctx)
	}
	rq.app = app
	rq.prepareBody(app)
	if rerr := r.admit(ctx.Request().Context(), rq, app); rerr != nil {
		return rerr.write(ctx)
	}
	defer r.release(rq, app)
	rq.charge = &types.Charge{
		ID: rq.requestID, WorkspaceID: rq.auth.Workspace.ExternalId, TokenID: rq.auth.Token.ExternalId,
		AppID: app.Spec.ID, Version: app.Version, Route: route, Pricing: app.Pricing, Stream: rq.stream, AcceptedAt: rq.startedAt.UTC(),
	}
	fleet, err := r.s.repo.GetFleet(ctx.Request().Context())
	if err != nil {
		return errRegistry.write(ctx)
	}
	if rq.serverless = fleet.Serverless(app.Spec.ID) && rq.pinReplica == ""; rq.serverless {
		release, err := r.holdDemand(rq)
		if err != nil {
			if errors.Is(err, errDemandLimit) {
				return capacityError("endpoint is at capacity, retry shortly").write(ctx)
			}
			return errRegistry.write(ctx)
		}
		defer release()
	}
	return r.serve(rq, app)
}

// resolveEndpoint picks the first requested model the caller may use that
// serves the route, preferring one with ready replicas.
func (r *router) resolveEndpoint(ctx context.Context, rq *routeRequest) (*types.ManagedEndpoint, *routeError) {
	var first *types.ManagedEndpoint
	var denied *routeError
	for _, model := range rq.models {
		app, err := r.s.repo.GetEndpoint(ctx, model)
		if err != nil {
			return nil, errRegistry
		}
		switch {
		case app == nil || !app.Callable():
			continue
		case !serves(app, rq.route):
			denied = notFound("route_not_supported", fmt.Sprintf("model %s does not serve %s", model, rq.route))
			continue
		case !r.allowed(ctx, app, rq.auth):
			denied = forbidden("model_not_allowed", fmt.Sprintf("model %s is not available to this workspace", model))
			continue
		}
		if first == nil {
			first = app
		}
		replicas, err := r.servingReplicas(ctx, app, rq.pinReplica, nil)
		if err != nil {
			return nil, errRegistry
		}
		if len(replicas) > 0 {
			for _, replica := range replicas {
				// Zero means unbounded to routing but cannot budget admission.
				capacity := max(replica.Capacity.MaxConcurrency, 0)
				rq.readyCapacity += min(capacity, math.MaxInt64-rq.readyCapacity)
			}
			return app, nil
		}
	}
	switch {
	case first != nil:
		return first, nil
	case denied != nil:
		return nil, denied
	}
	return nil, modelNotFound(rq.models[0])
}

// allowed reports whether a caller may discover and call an app; an
// anonymous caller (nil) sees only public apps.
func (r *router) allowed(ctx context.Context, app *types.ManagedEndpoint, authInfo *auth.AuthInfo) bool {
	if !workspaceCaller(authInfo) {
		return app.Public
	}
	if clusterAdmin(authInfo) {
		return true
	}
	if admin, err := r.s.AdminWorkspace(ctx); err == nil && admin.Id == authInfo.Workspace.Id {
		return true
	}
	return app.Allows(authInfo.Workspace.ExternalId, authInfo.Workspace.Name)
}

// caller is the authenticated identity of a request, nil when anonymous.
func caller(ctx echo.Context) *auth.AuthInfo {
	if cc, ok := ctx.(*auth.HttpAuthContext); ok {
		return cc.AuthInfo
	}
	return nil
}

// admissionKeys are the configured concurrency counters a request holds.
func (r *router) admissionKeys(rq *routeRequest, app *types.ManagedEndpoint) (keys []string, caps []uint32) {
	if c := r.s.config.Routing.PerEndpointConcurrency; c > 0 {
		keys, caps = append(keys, app.Spec.ID), append(caps, c)
	}
	if c := r.s.config.Routing.PerWorkspaceConcurrency; c > 0 {
		keys, caps = append(keys, app.Spec.ID+"|"+rq.auth.Workspace.ExternalId), append(caps, c)
	}
	return keys, caps
}

// admit applies the credit gate and this gateway's concurrency caps; the
// cluster-wide bound is the replicas' MaxConcurrency, enforced in reserve.
func (r *router) admit(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint) *routeError {
	if !app.Pricing.Free() && r.s.scheduler != nil && !clusterAdmin(rq.auth) {
		if gate := r.s.scheduler.CreditGate(); gate != nil {
			if err := gate.Check(ctx, rq.auth.Workspace.ExternalId); err != nil {
				var insufficient *types.InsufficientCreditsError
				if errors.As(err, &insufficient) {
					return errInsufficientCredits
				}
				return errBillingUnavailable
			}
		}
	}
	keys, caps := r.admissionKeys(rq, app)
	for i, key := range keys {
		if counter(&r.admission, key).Add(1) > int64(caps[i]) {
			for _, held := range keys[:i+1] {
				counter(&r.admission, held).Add(-1)
			}
			if i == 0 && key == app.Spec.ID {
				return capacityError("endpoint is at capacity, retry shortly")
			}
			return capacityError("too many concurrent requests for this workspace")
		}
	}
	return nil
}

func (r *router) release(rq *routeRequest, app *types.ManagedEndpoint) {
	keys, _ := r.admissionKeys(rq, app)
	for _, key := range keys {
		counter(&r.admission, key).Add(-1)
	}
}

// A rejected request may be gone before the controller ticks. Coalesce these
// requests into one short-lived signal, separate from active generation leases.
func (r *router) wake(ctx context.Context, rq *routeRequest) *routeError {
	if rq.serverless {
		if _, err := r.s.demand(ctx, rq.app.Spec.ID, demandWake, "", 0); err != nil {
			return errRegistry
		}
	}
	return nil
}

// holdDemand registers an admitted request with the controller for the rest
// of its life. Losing the lease cancels the request rather than serving work
// the controller cannot observe.
func (r *router) holdDemand(rq *routeRequest) (release func(), err error) {
	id, reqCtx := rq.app.Spec.ID, rq.ctx.Request().Context()
	if _, err := r.s.demand(reqCtx, id, leaseAcquire, rq.requestID, rq.readyCapacity); err != nil {
		return nil, err
	}
	ticker := time.NewTicker(leaseRenewInterval)
	ctx, stop := r.s.keepAlive(reqCtx, ticker.C, func(ctx context.Context) error {
		_, err := r.s.demand(ctx, id, leaseRenew, rq.requestID, 0)
		return err
	}, errDemandLeaseLost)
	rq.ctx.SetRequest(rq.ctx.Request().WithContext(ctx))
	return func() {
		ticker.Stop()
		stop()
		_, _ = r.s.demand(context.Background(), id, leaseRelease, rq.requestID, 0)
	}, nil
}

// routeRequest is one request through admission, replica selection and proxying.
type routeRequest struct {
	ctx       echo.Context
	auth      *auth.AuthInfo
	requestID string
	startedAt time.Time

	// What the caller asked for
	route      types.EndpointRoute
	proto      protocol
	models     []string // requested, in preference order
	body       []byte
	payload    map[string]any // decoded JSON body; nil for multipart or empty bodies
	stream     bool
	info       *llmroute.RequestInfo
	pinReplica string

	// What admission decided
	app           *types.ManagedEndpoint
	charge        *types.Charge
	serverless    bool
	readyCapacity int64 // finite serving slots from the selected app's replicas
	queueWait     time.Duration
}

// routeFromPath names the protocol and, for model-scoped paths, the model.
func routeFromPath(prefix, path string) (route types.EndpointRoute, model string, ok bool) {
	rest := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSuffix(path, "/"), prefix), "/")
	if id, ok := strings.CutPrefix(rest, "models/"); ok {
		i := strings.LastIndex(id, "/")
		if i <= 0 {
			return "", "", false
		}
		route = types.EndpointRoute(id[i+1:])
		return route, id[:i], route.ModelScoped()
	}
	route = types.EndpointRoute(rest)
	_, ok = protocols[route]
	return route, "", ok && !route.ModelScoped()
}

// readRequest reads the body once and collects the requested models, in
// preference order: the path, then `model`, then `models`.
func (r *router) readRequest(rq *routeRequest, pathModel string) *routeError {
	req := rq.ctx.Request()
	body, err := io.ReadAll(io.LimitReader(req.Body, maxBody+1))
	if err != nil {
		return errBodyUnreadable
	}
	if len(body) > maxBody {
		return errBodyTooLarge
	}
	rq.body = body
	if pathModel != "" {
		rq.models = []string{pathModel}
	}
	contentType, params, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
	switch {
	case strings.HasPrefix(contentType, "multipart/"):
		if model := multipartModel(params["boundary"], body); model != "" {
			rq.models = append(rq.models, model)
		}
	case len(bytes.TrimSpace(body)) > 0 && (pathModel == "" || strings.Contains(contentType, "json")):
		if rq.payload, err = decodeRequestJSON(body); err != nil {
			if pathModel != "" {
				break // an /invoke body is the app's own schema
			}
			return errNotJSONObject
		}
		if model, _ := rq.payload["model"].(string); model != "" {
			rq.models = append(rq.models, model)
		}
		if list, ok := rq.payload["models"].([]any); ok {
			for _, m := range list {
				if s, ok := m.(string); ok && s != "" {
					rq.models = append(rq.models, s)
				}
			}
		}
		if rq.proto.llm {
			if err := normalizeReasoning(rq.payload); err != nil {
				return badRequest("invalid_reasoning", err.Error())
			}
			rq.stream, _ = rq.payload["stream"].(bool)
		}
	}
	if len(rq.models) == 0 {
		return errMissingModel
	}
	return nil
}

// prepareBody makes the selected app the model the engine sees and asks LLM
// streams for usage (every chunk on vLLM, so a preempted stream still shows
// the tokens it produced). Model-scoped bodies pass through untouched.
func (rq *routeRequest) prepareBody(app *types.ManagedEndpoint) {
	if rq.payload == nil || rq.route.ModelScoped() {
		return
	}
	rq.payload["model"] = app.Spec.ID
	delete(rq.payload, "models")
	if rq.stream && rq.proto.llm {
		options, _ := rq.payload["stream_options"].(map[string]any)
		if options == nil {
			options = map[string]any{}
		}
		options["include_usage"] = true
		if app.Spec.Engine == types.EngineVLLM {
			options["continuous_usage_stats"] = true
		}
		rq.payload["stream_options"] = options
	}
	if body, err := json.Marshal(rq.payload); err == nil {
		rq.body = body
	}
}

// decodeRequestJSON keeps integers beyond 2^53 and tool schemas exact.
func decodeRequestJSON(body []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("request body must contain one JSON object")
	}
	return payload, nil
}

func multipartModel(boundary string, body []byte) string {
	if boundary == "" {
		return ""
	}
	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	for {
		part, err := reader.NextPart()
		if err != nil {
			return ""
		}
		if part.FormName() == "model" {
			value, _ := io.ReadAll(io.LimitReader(part, 1024))
			return strings.TrimSpace(string(value))
		}
	}
}

// normalizeReasoning maps OpenRouter's effort/enable controls onto the
// engine's OpenAI schema; unsupported controls fail visibly rather than
// silently generating (and charging for) unwanted reasoning.
func normalizeReasoning(payload map[string]any) error {
	raw, exists := payload["reasoning"]
	if !exists {
		return nil
	}
	reasoning, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("reasoning must be an object")
	}
	effort := ""
	if raw, exists := reasoning["effort"]; exists {
		effort, ok = raw.(string)
		if !ok || !slices.Contains([]string{"none", "minimal", "low", "medium", "high", "xhigh", "max"}, effort) {
			return fmt.Errorf("reasoning.effort is invalid")
		}
	}
	for key, value := range reasoning {
		switch key {
		case "enabled":
			enabled, ok := value.(bool)
			if !ok {
				return fmt.Errorf("reasoning.enabled must be a boolean")
			}
			if !enabled && effort != "" && effort != "none" || enabled && effort == "none" {
				return fmt.Errorf("reasoning.enabled conflicts with reasoning.effort")
			}
			if !enabled {
				effort = "none"
			} else if effort == "" {
				effort = "medium"
			}
		case "effort":
		case "exclude":
			if value != false {
				return fmt.Errorf("reasoning.exclude=true is not supported")
			}
		default:
			return fmt.Errorf("reasoning.%s is not supported; use reasoning.effort or reasoning.enabled", key)
		}
	}
	if effort != "" {
		if existing, ok := payload["reasoning_effort"]; ok && existing != effort {
			return fmt.Errorf("reasoning conflicts with reasoning_effort")
		}
		payload["reasoning_effort"] = effort
	}
	delete(payload, "reasoning")
	return nil
}

// Caller-facing shapes. A model is listed as OpenAI's /v1/models entry with
// OpenRouter's catalog fields on it, so either client can discover what is
// served; a charge shows the caller its own settlement and nothing about the
// platform workspace, stub or container that served it.

type modelView struct {
	ID                  string            `json:"id"`
	CanonicalSlug       string            `json:"canonical_slug"`
	Object              string            `json:"object"`
	Name                string            `json:"name"`
	Description         string            `json:"description"`
	Created             int64             `json:"created"`
	OwnedBy             string            `json:"owned_by"`
	ContextLength       uint32            `json:"context_length"`
	Kind                string            `json:"kind"`
	Architecture        modelArchitecture `json:"architecture"`
	Pricing             modelPricing      `json:"pricing"`
	TopProvider         modelProvider     `json:"top_provider"`
	PerRequestLimits    *struct{}         `json:"per_request_limits"`
	SupportedParameters []string          `json:"supported_parameters"`
	// Beam extensions: live state for the dashboard and the route paths served.
	IsReady   bool              `json:"is_ready"`
	Endpoints map[string]string `json:"endpoints"`
}

type modelArchitecture struct {
	Modality         string   `json:"modality"`
	InputModalities  []string `json:"input_modalities"`
	OutputModalities []string `json:"output_modalities"`
	Tokenizer        string   `json:"tokenizer"`
	InstructType     *string  `json:"instruct_type"`
}

// modelPricing is per-unit prices as OpenRouter renders them: "0" when unset.
type modelPricing struct {
	Prompt         string `json:"prompt"`
	Completion     string `json:"completion"`
	Request        string `json:"request"`
	InputCacheRead string `json:"input_cache_read,omitempty"`
}

type modelProvider struct {
	ContextLength       uint32  `json:"context_length"`
	MaxCompletionTokens *uint32 `json:"max_completion_tokens"`
	IsModerated         bool    `json:"is_moderated"`
}

// openRouterOutputs maps engine kinds to OpenRouter output modalities; the rest are text.
var openRouterOutputs = map[types.EndpointKind]string{types.EndpointKindEmbedding: "embeddings", types.EndpointKindImage: "image"}

func (r *router) model(app *types.ManagedEndpoint, ready bool) modelView {
	output := cmp.Or(openRouterOutputs[app.Spec.Kind], "text")
	v := modelView{
		ID: app.Spec.ID, CanonicalSlug: app.Spec.ID, Object: "model", Name: app.Catalog.Name, Description: app.Catalog.Description,
		Created: app.CreatedAt.Unix(), OwnedBy: providerName, ContextLength: app.Catalog.ContextLength, Kind: string(app.Spec.Kind),
		Architecture: modelArchitecture{Modality: "text->" + output, InputModalities: []string{"text"}, OutputModalities: []string{output}, Tokenizer: "Other"},
		Pricing:      modelPricing{cmp.Or(app.Pricing.PromptTokens, "0"), cmp.Or(app.Pricing.CompletionTokens, "0"), cmp.Or(app.Pricing.Request, "0"), app.Pricing.CachedPromptTokens},
		TopProvider:  modelProvider{ContextLength: app.Catalog.ContextLength},
		IsReady:      ready, SupportedParameters: []string{}, Endpoints: map[string]string{},
	}
	if m := app.OpenRouter; m != nil {
		if m.MaxOutputTokens > 0 {
			v.TopProvider.MaxCompletionTokens = &m.MaxOutputTokens
		}
		v.SupportedParameters = slices.Sorted(maps.Keys(m.SupportedParameters))
	}
	for name := range protocols {
		switch {
		case !serves(app, name):
		case name.ModelScoped():
			v.Endpoints[string(name)] = r.prefix + "/models/" + app.Spec.ID + "/" + string(name)
		default:
			v.Endpoints[strings.ReplaceAll(string(name), "/", "_")] = r.prefix + "/" + string(name)
		}
	}
	return v
}

// visible lists the callable apps the caller may use, with whether each has a serving replica.
func (r *router) visible(ctx context.Context, a *auth.AuthInfo) ([]*types.ManagedEndpoint, map[string]bool, error) {
	all, err := r.s.repo.ListEndpoints(ctx)
	if err != nil {
		return nil, nil, err
	}
	apps := slices.DeleteFunc(all, func(app *types.ManagedEndpoint) bool { return !app.Callable() || !r.allowed(ctx, app, a) })
	replicas, _ := r.s.repo.ListAllReplicas(ctx)
	ready := map[string]bool{}
	for _, replica := range replicas {
		ready[replica.EndpointID] = ready[replica.EndpointID] || replica.Serving()
	}
	return apps, ready, nil
}

func (r *router) handleListModels(ctx echo.Context) error {
	apps, ready, err := r.visible(ctx.Request().Context(), caller(ctx))
	if err != nil {
		return errRegistry.write(ctx)
	}
	data := make([]modelView, 0, len(apps))
	for _, app := range apps {
		data = append(data, r.model(app, ready[app.Spec.ID]))
	}
	return ctx.JSON(http.StatusOK, map[string]any{"object": "list", "data": data})
}

// handleGetModel is OpenAI's retrieve-model call for one published model.
func (r *router) handleGetModel(ctx echo.Context) error {
	id := strings.TrimPrefix(ctx.Param("author")+"/"+ctx.Param("slug"), "/")
	apps, ready, err := r.visible(ctx.Request().Context(), caller(ctx))
	if err != nil {
		return errRegistry.write(ctx)
	}
	for _, app := range apps {
		if app.Spec.ID == id {
			return ctx.JSON(http.StatusOK, r.model(app, ready[app.Spec.ID]))
		}
	}
	return modelNotFound(id).write(ctx)
}

// handleListOpenRouterModels publishes the provider catalog: OpenRouter's v2
// schema is closed, so it stays separate from the OpenAI list.
func (r *router) handleListOpenRouterModels(ctx echo.Context) error {
	apps, _, err := r.visible(ctx.Request().Context(), caller(ctx))
	if err != nil {
		return errRegistry.write(ctx)
	}
	data := make([]map[string]any, 0, len(apps))
	for _, app := range apps {
		if app.OpenRouter == nil {
			continue
		}
		document := openRouterModel(app)
		if app.OpenRouter.ValidateFor(app.Spec.Kind, app.Catalog, app.Pricing) != nil || types.ValidateOpenRouterDocument(document) != nil {
			return errInvalidCatalog.write(ctx)
		}
		data = append(data, document)
	}
	return ctx.JSON(http.StatusOK, map[string]any{"object": "list", "data": data})
}

func openRouterModel(app *types.ManagedEndpoint) map[string]any {
	m := app.OpenRouter
	input := map[string]any{"type": "text"}
	if app.Catalog.ContextLength > 0 {
		input["supported_inputs"] = map[string]any{"max_context_length": map[string]any{"value": app.Catalog.ContextLength, "unit": "token"}}
	}
	output := m.Output(cmp.Or(openRouterOutputs[app.Spec.Kind], "text"))
	price := func(kind, unit, cost string) map[string]any {
		return map[string]any{"type": kind, "unit": unit, "cost_usd": cost}
	}
	// Omitted charges are absent SKUs; an explicit zero is a real free SKU.
	var inputPrices, outputPrices []map[string]any
	if p := app.Pricing; p.PerToken() {
		inputPrices = append(inputPrices, price("prompt", "token", p.PromptTokens))
		if p.CachedPromptTokens != "" {
			cached := price("cached_prompt", "token", p.CachedPromptTokens)
			cached["implicit"] = true // Beam charges observed engine cache hits
			inputPrices = append(inputPrices, cached)
		}
		if p.CompletionTokens != "" {
			outputPrices = append(outputPrices, price("completion", "token", p.CompletionTokens))
		}
	}
	if len(inputPrices) > 0 {
		input["pricing"] = inputPrices
	}
	if len(outputPrices) > 0 {
		output["pricing"] = outputPrices
	}
	document := map[string]any{
		"schema_version": "2.4", "id": app.Spec.ID, "name": app.Catalog.Name, "created": app.CreatedAt.Unix(), "description": app.Catalog.Description,
		"input_modalities": []any{input}, "output_modalities": []any{output},
	}
	if app.Pricing.PerRequest() {
		document["pricing"] = []map[string]any{price("request", "request", app.Pricing.Request)}
	}
	if m.HuggingFaceID != "" {
		document["hugging_face_id"] = m.HuggingFaceID
	}
	if m.Quantization != "" {
		document["quantization"] = m.Quantization
	}
	if len(m.Datacenters) > 0 {
		document["datacenters"] = m.Datacenters
	}
	return document
}

type chargeView struct {
	Status    types.ChargeStatus `json:"status"`
	TotalCost float64            `json:"total_cost"`
	Pricing   types.Pricing      `json:"pricing"`
	Usage     types.Usage        `json:"usage"`
	SettledAt *time.Time         `json:"settled_at"`
}

// generationView is OpenRouter's generation record.
type generationView struct {
	ID                     string     `json:"id"`
	Model                  string     `json:"model"`
	ProviderName           string     `json:"provider_name"`
	CreatedAt              time.Time  `json:"created_at"`
	Streamed               bool       `json:"streamed"`
	GenerationTime         int64      `json:"generation_time"`
	Latency                int64      `json:"latency"`
	TokensPrompt           int64      `json:"tokens_prompt"`
	TokensCompletion       int64      `json:"tokens_completion"`
	NativeTokensPrompt     int64      `json:"native_tokens_prompt"`
	NativeTokensCompletion int64      `json:"native_tokens_completion"`
	NativeTokensCached     int64      `json:"native_tokens_cached"`
	TotalCost              float64    `json:"total_cost"`
	Usage                  float64    `json:"usage"`
	CacheDiscount          *float64   `json:"cache_discount"`
	FinishReason           *string    `json:"finish_reason"`
	IsBYOK                 bool       `json:"is_byok"`
	GPU                    string     `json:"gpu"`
	StatusCode             int        `json:"status_code"`
	Charge                 chargeView `json:"charge"`
}

// costUSD renders micro-dollars as the float OpenRouter puts in usage.cost.
func costUSD(microUSD int64) float64 { return float64(microUSD) / 1_000_000 }

// mayRead: a charge is visible to its caller and to cluster admins.
func mayRead(a *auth.AuthInfo, c *types.Charge) bool {
	return c.WorkspaceID == a.Workspace.ExternalId || clusterAdmin(a)
}

// handleGeneration returns the metered record of one request by id in
// OpenRouter's generation shape.
func (r *router) handleGeneration(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	id := strings.TrimSpace(ctx.QueryParam("id"))
	if id == "" {
		return errMissingID.write(ctx)
	}
	c, err := r.s.repo.GetCharge(ctx.Request().Context(), id)
	if err != nil || c == nil || !mayRead(cc.AuthInfo, c) {
		return errGenerationNotFound.write(ctx)
	}
	var settledAt *time.Time
	if !c.SettledAt.IsZero() {
		t := c.SettledAt.UTC()
		settledAt = &t
	}
	return ctx.JSON(http.StatusOK, map[string]any{"data": generationView{
		ID: c.ID, Model: c.AppID, ProviderName: providerName, CreatedAt: c.AcceptedAt.UTC(),
		Streamed: c.Stream, GenerationTime: c.DurationMs, Latency: c.TTFTMs,
		TokensPrompt: c.Work.PromptTokens, TokensCompletion: c.Work.CompletionTokens,
		NativeTokensPrompt: c.Work.PromptTokens, NativeTokensCompletion: c.Work.CompletionTokens, NativeTokensCached: c.Work.CachedTokens,
		TotalCost: costUSD(c.Cost.MicroUSD), Usage: costUSD(c.Cost.MicroUSD),
		GPU: c.GPU, StatusCode: c.StatusCode,
		Charge: chargeView{Status: c.Status, TotalCost: costUSD(c.Cost.MicroUSD), Pricing: c.Pricing, Usage: c.Usage(), SettledAt: settledAt},
	}})
}

// Errors on /v1 use the OpenAI envelope: {"error": {"message", "type", "code",
// "param"}}. The type follows the status; the code names the exact cause.
type routeError struct {
	Status  int
	Code    string
	Message string
}

func (e *routeError) Error() string { return e.Message }

func (e *routeError) write(ctx echo.Context) error {
	h := ctx.Response().Header()
	h.Set("Content-Type", "application/json")
	h.Del("Content-Encoding")
	if e.Status == http.StatusTooManyRequests && h.Get("Retry-After") == "" {
		h.Set("Retry-After", "1")
	}
	body := map[string]any{"message": e.Message, "type": errorType(e.Status), "code": e.Code, "param": nil}
	return ctx.JSON(e.Status, map[string]any{"error": body})
}

const (
	errorTypeInvalidRequest = "invalid_request_error"
	errorTypeServer         = "server_error"
)

var errorTypes = map[int]string{
	http.StatusUnauthorized:    "authentication_error",
	http.StatusForbidden:       "authentication_error",
	http.StatusPaymentRequired: "insufficient_quota",
	http.StatusTooManyRequests: "rate_limit_error",
	http.StatusNotFound:        "not_found_error",
}

func errorType(status int) string {
	if kind, ok := errorTypes[status]; ok {
		return kind
	}
	if status >= 500 {
		return errorTypeServer
	}
	return errorTypeInvalidRequest
}

// Fixed causes. Dynamic messages use the constructors below.
var (
	errUnauthorized          = &routeError{http.StatusUnauthorized, "unauthorized", "a workspace token is required"}
	errEndpointsDisabled     = &routeError{http.StatusNotFound, "not_found", "managed endpoints are not enabled"}
	errUnknownRoute          = &routeError{http.StatusNotFound, "not_found", "unknown route"}
	errRegistry              = &routeError{http.StatusServiceUnavailable, "registry_unavailable", "endpoint registry unavailable"}
	errBodyUnreadable        = &routeError{http.StatusBadRequest, "invalid_body", "failed to read request body"}
	errBodyTooLarge          = &routeError{http.StatusRequestEntityTooLarge, "body_too_large", "request body exceeds 64MB"}
	errNotJSONObject         = &routeError{http.StatusBadRequest, "invalid_json", "request body must be a JSON object"}
	errMissingModel          = &routeError{http.StatusBadRequest, "missing_model", "the model field is required"}
	errInsufficientCredits   = &routeError{http.StatusPaymentRequired, "insufficient_credits", "insufficient credits: add credits to continue using inference endpoints"}
	errBillingUnavailable    = &routeError{http.StatusServiceUnavailable, "billing_unavailable", "billing check unavailable"}
	errUpstreamUnavailable   = &routeError{http.StatusBadGateway, "upstream_unavailable", "upstream replicas failed"}
	errUpstreamEnded         = &routeError{http.StatusBadGateway, "upstream_failed", "upstream response ended early"}
	errUpstreamTooLarge      = &routeError{http.StatusBadGateway, "upstream_too_large", "upstream response exceeds 64MB"}
	errClientClosed          = &routeError{statusClientClosed, "client_closed", "client closed request"}
	errGatewayDraining       = &routeError{http.StatusServiceUnavailable, "gateway_draining", "gateway is restarting, retry shortly"}
	errAccountingUnavailable = &routeError{http.StatusServiceUnavailable, "accounting_unavailable", "Unable to record request usage"}
	errMissingUsage          = &routeError{http.StatusBadGateway, "missing_usage", "upstream response carried no usage; request not billed"}
	errMissingID             = &routeError{http.StatusBadRequest, "missing_id", "id query parameter is required"}
	errGenerationNotFound    = &routeError{http.StatusNotFound, "generation_not_found", "generation not found"}
	errInvalidCatalog        = &routeError{http.StatusServiceUnavailable, "invalid_catalog", "provider catalog configuration is invalid"}
)

// statusClientClosed is nginx's convention for a client that went away first.
const statusClientClosed = 499

func badRequest(code, message string) *routeError {
	return &routeError{http.StatusBadRequest, code, message}
}

func notFound(code, message string) *routeError {
	return &routeError{http.StatusNotFound, code, message}
}

func forbidden(code, message string) *routeError {
	return &routeError{http.StatusForbidden, code, message}
}

func capacityError(message string) *routeError {
	return &routeError{http.StatusTooManyRequests, "rate_limit_exceeded", message}
}

func modelNotFound(model string) *routeError {
	return notFound("model_not_found", fmt.Sprintf("model %s not found", model))
}

// openAIErrors rewrites echo's own errors (auth middleware, bad routes) into
// the envelope, unless a response has already started.
func openAIErrors(next echo.HandlerFunc) echo.HandlerFunc {
	return func(ctx echo.Context) error {
		err := next(ctx)
		if err == nil || ctx.Response().Committed {
			return err
		}
		var httpErr *echo.HTTPError
		if !errors.As(err, &httpErr) {
			return err
		}
		return fromHTTPError(httpErr).write(ctx)
	}
}

func fromHTTPError(httpErr *echo.HTTPError) *routeError {
	code := "invalid_request"
	switch {
	case httpErr.Code == http.StatusUnauthorized || httpErr.Code == http.StatusForbidden:
		code = "invalid_api_key"
	case httpErr.Code >= 500:
		code = errorTypeServer
	}
	return &routeError{httpErr.Code, code, http.StatusText(httpErr.Code)}
}
