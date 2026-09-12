package managedendpoint

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/common/llmroute"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

// request is one hosted request through admission and dispatch.
type routeRequest struct {
	ctx           echo.Context
	auth          *auth.AuthInfo
	route         types.EndpointRoute
	proto         protocol
	requestID     string
	models        []string // requested, in preference order
	body          []byte
	payload       map[string]any // decoded JSON body; nil for multipart or empty bodies
	stream        bool
	info          *llmroute.RequestInfo
	pinReplica    string
	startedAt     time.Time
	queueWait     time.Duration
	serverless    bool
	readyCapacity int64 // finite serving slots from the selected app's replicas
	app           *types.ManagedEndpoint
	charge        *types.Charge
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
				break // /invoke and /tasks bodies are the app's own schema
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

// deploymentURL is the gateway path that executes an ordinary deployment.
// ASGI apps receive the OpenAI route as their path; every other kind takes
// the body at its invoke URL. Task queues are enqueued through their own put.
// deploymentURL is the gateway path of the execution service for an app. An
// ASGI app takes a single path segment, so an OpenAI route reaches it as
// `images-generations`, `audio-speech`; invoke and queued tasks hit its root.
func deploymentURL(app *types.ManagedEndpoint, route types.EndpointRoute) string {
	kind := app.StubType.Kind()
	path := "/" + kind + "/id/" + app.StubID
	if kind == types.StubTypeASGI && !route.ModelScoped() {
		path += "/" + strings.ReplaceAll(string(route), "/", "-")
	}
	return path
}

// serveDeployment relays one synchronous request through the deployment's
// execution service as the platform workspace. The caller's identity stays
// on the charge, never on the executed request.
func (r *router) serveDeployment(rq *routeRequest, app *types.ManagedEndpoint) error {
	token, err := r.s.adminTokenKey(rq.ctx.Request().Context())
	if err != nil {
		r.finish(rq, app, nil, errRegistry.Status, types.Work{}, false, 0, err.Error())
		return errRegistry.write(rq.ctx)
	}
	_, err = r.proxy(rq.ctx.Request().Context(), rq, app, nil, loopback{r.s.gateway}, deploymentURL(app, rq.route), "Bearer "+token)
	if err != nil && !rq.ctx.Response().Committed {
		r.finish(rq, app, nil, errUpstreamUnavailable.Status, types.Work{}, false, 0, err.Error())
		return errUpstreamUnavailable.write(rq.ctx)
	}
	return nil
}

// enqueue submits queued work and opens its charge under the task id, so
// retries and duplicate completions settle one charge once. Nothing is
// billed until the task completes successfully.
func (r *router) enqueue(rq *routeRequest, app *types.ManagedEndpoint) error {
	ctx := rq.ctx.Request().Context()
	token, err := r.s.adminTokenKey(ctx)
	if err != nil {
		return errRegistry.write(rq.ctx)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://gateway"+deploymentURL(app, rq.route), bytes.NewReader(rq.body))
	req.Header.Set("Content-Type", rq.ctx.Request().Header.Get("Content-Type"))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := loopback{r.s.gateway}.RoundTrip(req)
	if err != nil {
		return errUpstreamUnavailable.write(rq.ctx)
	}
	defer resp.Body.Close()
	var out struct {
		TaskID string `json:"task_id"`
		Error  string `json:"error"`
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	_ = json.Unmarshal(body, &out)
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return capacityError(out.Error).write(rq.ctx)
	case resp.StatusCode >= 300 || out.TaskID == "":
		return (&routeError{http.StatusBadGateway, "enqueue_failed", strings.TrimSpace(out.Error + " task was not accepted")}).write(rq.ctx)
	}
	rq.charge.ID = out.TaskID
	if err := r.s.billing.open(ctx, rq.charge); err != nil {
		log.Error().Err(err).Str("task_id", out.TaskID).Msg("managed endpoints: task accepted but charge not journaled")
		return errAccountingUnavailable.write(rq.ctx)
	}
	return rq.ctx.JSON(http.StatusAccepted, r.taskView(rq.charge, map[string]any{"status": string(types.TaskStatusPending)}))
}

// handleTask reads or cancels one queued task. The caller is authorized
// against the charge, not the platform workspace that executes the task.
func (r *router) handleTask(ctx echo.Context) error {
	cc, ok := ctx.(*auth.HttpAuthContext)
	if !ok || !workspaceCaller(cc.AuthInfo) {
		return errUnauthorized.write(ctx)
	}
	if !r.s.Enabled() {
		return errEndpointsDisabled.write(ctx)
	}
	rctx := ctx.Request().Context()
	charge, err := r.s.repo.GetCharge(rctx, ctx.Param("id"))
	if err != nil || charge == nil || !charge.Route.Async() || !mayRead(cc.AuthInfo, charge) {
		return errGenerationNotFound.write(ctx)
	}
	admin, err := r.s.AdminWorkspace(rctx)
	if err != nil {
		return errRegistry.write(ctx)
	}
	token, err := r.s.adminTokenKey(rctx)
	if err != nil {
		return errRegistry.write(ctx)
	}
	method, path, body := http.MethodGet, "/api/v1/task/"+admin.ExternalId+"/"+charge.ID, ""
	if ctx.Request().Method == http.MethodDelete {
		method, path, body = http.MethodDelete, "/api/v1/task/"+admin.ExternalId, `{"task_ids":["`+charge.ID+`"]}`
	}
	req, _ := http.NewRequestWithContext(rctx, method, "http://gateway"+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := loopback{r.s.gateway}.RoundTrip(req)
	if err != nil {
		return errUpstreamUnavailable.write(ctx)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if resp.StatusCode >= 300 {
		return (&routeError{resp.StatusCode, "task_error", http.StatusText(resp.StatusCode)}).write(ctx)
	}
	var task map[string]any
	_ = json.Unmarshal(raw, &task)
	if method == http.MethodDelete {
		task = map[string]any{"status": string(types.TaskStatusCancelled)}
	}
	// Settle on read: a completed task is billed the first time anyone sees it done.
	if charge, err = r.s.billing.settleTask(rctx, charge); err != nil {
		log.Warn().Err(err).Str("task_id", charge.ID).Msg("managed endpoints: settle task on read")
	}
	return ctx.JSON(http.StatusOK, r.taskView(charge, task))
}

// taskView is the caller-facing task: its own fields plus the charge, with
// nothing about the platform workspace, stub or container that ran it.
func (r *router) taskView(c *types.Charge, task map[string]any) map[string]any {
	view := map[string]any{"id": c.ID, "object": "task", "model": c.AppID, "created_at": c.AcceptedAt.UTC().Format(time.RFC3339Nano)}
	for _, key := range []string{"status", "started_at", "ended_at", "outputs", "result", "failure_reason"} {
		if value, ok := task[key]; ok && value != nil {
			view[key] = value
		}
	}
	view["charge"] = chargeView(c)
	return view
}

// loopback dispatches through the gateway's own router in-process, so hosted
// requests to ordinary deployments go through the exact handler a customer
// would hit, with the admin workspace's identity.
type loopback struct{ handler http.Handler }

type loopbackWriter struct {
	header  http.Header
	status  int
	body    *io.PipeWriter
	started chan struct{}
	once    sync.Once
	flusher func()
}

func (w *loopbackWriter) Header() http.Header { return w.header }

func (w *loopbackWriter) WriteHeader(status int) {
	w.once.Do(func() { w.status = status; close(w.started) })
}

func (w *loopbackWriter) Write(p []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.body.Write(p)
}

func (w *loopbackWriter) Flush() {}

func (t loopback) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.handler == nil {
		return nil, errors.New("gateway router unavailable")
	}
	pr, pw := io.Pipe()
	w := &loopbackWriter{header: http.Header{}, body: pw, started: make(chan struct{})}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				pw.CloseWithError(fmt.Errorf("handler panicked: %v", r))
			}
		}()
		t.handler.ServeHTTP(w, req)
		w.WriteHeader(http.StatusOK)
		pw.Close()
	}()
	select {
	case <-w.started:
	case <-req.Context().Done():
		pr.Close()
		return nil, req.Context().Err()
	}
	return &http.Response{StatusCode: w.status, Header: w.header, Body: pr, Request: req, ContentLength: -1}, nil
}
