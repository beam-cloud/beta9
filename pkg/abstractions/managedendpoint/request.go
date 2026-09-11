package managedendpoint

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

// Request parsing: which route and model a request names, and the body the
// engine receives. The body is decoded once and re-encoded once.

func routeFromPath(prefix, path string) (types.EndpointRoute, string, bool) {
	rest := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSuffix(path, "/"), prefix), "/")
	if id, ok := strings.CutPrefix(rest, "models/"); ok {
		id, ok = strings.CutSuffix(id, "/invoke")
		return types.EndpointRouteInvoke, id, ok && id != ""
	}
	route := types.EndpointRoute(rest)
	_, ok := adapters[route]
	return route, "", ok && route != types.EndpointRouteInvoke
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
	case len(bytes.TrimSpace(body)) > 0:
		if rq.payload, err = decodeRequestJSON(body); err != nil {
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
		if rq.adapter.LLM {
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

// prepareBody makes the selected endpoint the model the engine sees and asks
// LLM streams for usage: a final chunk, or every chunk on vLLM so a preempted
// stream still shows the tokens it produced. /invoke bodies are the app's own
// schema and pass through untouched.
func (rq *routeRequest) prepareBody(endpoint *types.ManagedEndpoint) {
	if rq.payload == nil || rq.route == types.EndpointRouteInvoke {
		return
	}
	rq.payload["model"] = endpoint.Spec.ID
	delete(rq.payload, "models")
	if rq.stream && rq.adapter.LLM {
		options, _ := rq.payload["stream_options"].(map[string]any)
		if options == nil {
			options = map[string]any{}
		}
		options["include_usage"] = true
		if endpoint.Spec.Engine == "vllm" {
			options["continuous_usage_stats"] = true
		}
		rq.payload["stream_options"] = options
	}
	if body, err := json.Marshal(rq.payload); err == nil {
		rq.body = body
	}
}

// Preserve tool schemas and provider parameters exactly when adding routing
// fields. float64 would silently round JSON integers larger than 2^53.
func decodeRequestJSON(body []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
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

// normalizeReasoning maps OpenRouter's documented effort/enable controls to
// the engine's OpenAI schema. Unsupported budgets/exclusion must fail visibly
// instead of silently generating (and charging for) unwanted reasoning.
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
			if !enabled {
				if effort != "" && effort != "none" {
					return fmt.Errorf("reasoning.enabled conflicts with reasoning.effort")
				}
				effort = "none"
			} else {
				if effort == "none" {
					return fmt.Errorf("reasoning.enabled conflicts with reasoning.effort")
				}
				if effort == "" {
					effort = "medium"
				}
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
