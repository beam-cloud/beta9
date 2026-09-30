package apiv1

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/beam-cloud/beta9/pkg/auth"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// The curated tools cover the build/ship/operate loop by name. Everything else
// the gateway serves is reachable through `api`, which runs a request through
// the gateway's own router in-process: same routes, same auth, nothing to keep
// in sync. `invoke` does the same against a deployment's URL.

type identityHeadersKey struct{}

var identityHeaders = []string{"Authorization", auth.CallerHeader, auth.AgentSessionHeader}

// withIdentityHeaders carries the MCP caller's identity so in-process requests
// authenticate and attribute exactly like the call that triggered them.
func withIdentityHeaders(ctx context.Context, h http.Header) context.Context {
	kept := http.Header{}
	for _, key := range identityHeaders {
		if v := h.Get(key); v != "" {
			kept.Set(key, v)
		}
	}
	return context.WithValue(ctx, identityHeadersKey{}, kept)
}

const maxInProcessBody = 4 << 20

// boundedResponse prevents an unbounded upstream response from consuming gateway
// memory. A response exceeding the limit is explicitly unsuccessful, never parsed
// as a different type of successful body.
type boundedResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
	size   int64
	cancel context.CancelFunc
}

func (w *boundedResponse) Header() http.Header {
	return w.header
}

func (w *boundedResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *boundedResponse) Flush() {
	w.WriteHeader(http.StatusOK)
}

func (w *boundedResponse) Write(p []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	w.size += int64(len(p))
	remaining := maxInProcessBody - w.body.Len()
	if len(p) > remaining {
		_, _ = w.body.Write(p[:remaining])
		w.cancel()
		return remaining, fmt.Errorf("MCP response exceeds %d bytes", maxInProcessBody)
	}
	return w.body.Write(p)
}

// serve runs through the existing router, preserving its authorization and
// response semantics. Cancellation bounds both slow and oversized responses.
func (g *MCPGroup) serve(ctx context.Context, method, url string, body []byte, headers map[string]string) (any, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, fail("INVALID_ARGS", "%s", err)
	}
	if identity, _ := ctx.Value(identityHeadersKey{}).(http.Header); identity != nil {
		for key, values := range identity {
			req.Header[key] = append([]string(nil), values...)
		}
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	req.Host = req.URL.Host
	req.RemoteAddr = "127.0.0.1:0"

	rec := &boundedResponse{header: http.Header{}, cancel: cancel}
	g.router.ServeHTTP(rec, req)
	return rec.result(ctx), nil
}

func (rec *boundedResponse) result(ctx context.Context) map[string]any {
	if rec.status == 0 {
		rec.status = http.StatusOK
	}

	out := map[string]any{
		"status":         rec.status,
		"headers":        rec.header,
		"content_type":   rec.header.Get("Content-Type"),
		"bytes_received": rec.size,
		"truncated":      rec.size > maxInProcessBody,
		"is_error":       rec.status >= 400,
	}
	raw := rec.body.Bytes()
	if rec.size > maxInProcessBody {
		out["is_error"] = true
		out["code"] = "RESPONSE_TOO_LARGE"
		out["error"] = "Response exceeded the inline limit; use the local HTTP artifact tool to save the response to a file."
		out["limit_bytes"] = maxInProcessBody
	} else if ctx.Err() != nil {
		out["is_error"] = true
		out["code"] = "CANCELLED"
		out["error"] = ctx.Err().Error()
	} else {
		var parsed any
		mediaType, _, _ := mime.ParseMediaType(rec.header.Get("Content-Type"))
		jsonType := mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")
		textType := strings.HasPrefix(mediaType, "text/") || jsonType || mediaType == ""

		if jsonType && json.Unmarshal(raw, &parsed) == nil {
			out["body"] = parsed
			out["body_type"] = "json"
		} else if utf8.Valid(raw) && textType {
			out["body"] = string(raw)
			out["body_type"] = "text"
		} else {
			out["body_base64"] = base64.StdEncoding.EncodeToString(raw)
			out["body_type"] = "binary"
		}
	}
	return out
}

func requestBody(args toolArgs) ([]byte, error) {
	if value, ok := args["body_base64"]; ok {
		if _, exists := args["body"]; exists {
			return nil, fail("INVALID_ARGS", "choose body or body_base64")
		}
		encoded, ok := value.(string)
		if !ok {
			return nil, fail("INVALID_ARGS", "body_base64 must be a string")
		}
		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fail("INVALID_ARGS", "invalid base64 body")
		}
		return raw, nil
	}
	return jsonBody(args), nil
}

func (g *MCPGroup) api(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	method := strings.ToUpper(args.str("method"))
	if method == "" {
		method = http.MethodGet
	}
	readOnly := method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions
	if !readOnly && !args.boolean("confirm") {
		return nil, fail("NEEDS_CONFIRMATION", "api %s changes state. Call again with confirm=true.", method)
	}
	path := strings.ReplaceAll(args.str("path"), "{ws}", a.Workspace.ExternalId)
	if !strings.HasPrefix(path, "/") {
		return nil, fail("INVALID_ARGS", "path must start with /")
	}
	body, err := requestBody(args)
	if err != nil {
		return nil, err
	}
	response, err := g.serve(ctx, method, g.config.GatewayService.HTTP.GetExternalURL()+path, body, args.stringMap("headers"))
	if err == nil && strings.HasPrefix(path, "/api/v1/gateway/") {
		envelope := response.(map[string]any)
		if payload, ok := envelope["body"].(map[string]any); ok && payload["ok"] == false {
			envelope["is_error"] = true
			envelope["code"] = "BACKEND_REJECTED"
		}
	}
	return response, err
}

func jsonBody(args toolArgs) []byte {
	raw, ok := args["body"]
	if !ok || raw == nil {
		return nil
	}
	body, _ := json.Marshal(raw)
	return body
}

func (g *MCPGroup) apiRoutes(_ context.Context, _ *auth.AuthInfo, args toolArgs) (any, error) {
	routes := g.router.Routes()
	out := make([]string, 0, len(routes))
	seen := map[string]bool{}
	for _, r := range routes {
		if !strings.HasPrefix(r.Path, HttpServerBaseRoute+"/") || strings.HasPrefix(r.Path, HttpServerBaseRoute+"/mcp") || strings.HasSuffix(r.Path, "*") || r.Name == "echo_route_not_found" || strings.Contains(r.Name, "Cluster") {
			continue
		}
		line := r.Method + " " + strings.ReplaceAll(r.Path, ":workspaceId", "{ws}")
		if !seen[line] {
			seen[line] = true
			out = append(out, line)
		}
	}
	sort.Strings(out)
	return map[string]any{
		"routes":             out,
		"gateway_operations": gatewayOperations(args.str("path")),
		"notes":              "{ws} is your workspace id. " + HttpServerBaseRoute + "/gateway/* proxies the gRPC services (pods, images, volumes, disks); gateway_operations contains live protobuf-derived input/output schemas; filter by path for detail. Substitute path parameters and put remaining GET arguments in the query string. JSON bodies use the shown field names; bytes use base64. Server streams return the gateway streaming representation.",
	}, nil
}

func (g *MCPGroup) invoke(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	method := strings.ToUpper(args.str("method"))
	if method == "" {
		method = http.MethodPost
	}
	cfg, err := d.Stub.UnmarshalConfig()
	if err != nil {
		return nil, err
	}
	if cfg.TCP {
		return nil, fail("UNSUPPORTED_PROTOCOL", "TCP deployments require a database or TCP client, not HTTP invoke")
	}
	if !cfg.Authorized {
		ctx = withIdentityHeaders(ctx, nil)
	}
	url, err := g.exactDeploymentURL(d, int(args.num("port", 0)))
	if err != nil {
		return nil, err
	}
	path := args.str("path")
	if strings.Contains(path, "://") || strings.HasPrefix(path, "//") {
		return nil, fail("INVALID_ARGS", "path must be relative to the deployment")
	}
	url = strings.TrimSuffix(url, "/") + "/" + strings.TrimPrefix(path, "/")
	body, err := requestBody(args)
	if err != nil {
		return nil, err
	}
	return g.serve(ctx, method, url, body, args.stringMap("headers"))
}

// gatewayOperations derives customer-facing RPC schemas from the same descriptors
// used by the gateway. No second catalog of sandbox/file/storage fields to drift.
func gatewayOperations(filter string) []map[string]any {
	out := []map[string]any{}
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		services := file.Services()
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			switch string(service.Name()) {
			case "PodService", "DiskService", "VolumeService", "ImageService", "GatewayService":
			default:
				continue
			}
			methods := service.Methods()
			for j := 0; j < methods.Len(); j++ {
				method := methods.Get(j)
				if service.Name() == "GatewayService" {
					switch string(method.Name()) {
					case "GetOrCreateStub", "DeployStub", "GetURL", "GetUrl",
						"ListContainers", "StopContainer", "ListDeployments", "StartDeployment",
						"StopDeployment", "ScaleDeployment", "ListTasks", "StopTasks", "ListPools":
					default:
						continue
					}
				}
				if operation := gatewayOperation(method, filter); operation != nil {
					out = append(out, operation)
				}
			}
		}
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i]["path"].(string) < out[j]["path"].(string) })
	return out
}

func gatewayOperation(method protoreflect.MethodDescriptor, filter string) map[string]any {
	if !proto.HasExtension(method.Options(), annotations.E_Http) {
		return nil
	}

	rule, ok := proto.GetExtension(method.Options(), annotations.E_Http).(*annotations.HttpRule)
	if !ok {
		return nil
	}

	verb, path := "", ""
	switch p := rule.Pattern.(type) {
	case *annotations.HttpRule_Get:
		verb, path = "GET", p.Get
	case *annotations.HttpRule_Post:
		verb, path = "POST", p.Post
	case *annotations.HttpRule_Put:
		verb, path = "PUT", p.Put
	case *annotations.HttpRule_Delete:
		verb, path = "DELETE", p.Delete
	case *annotations.HttpRule_Patch:
		verb, path = "PATCH", p.Patch
	}
	if path == "" {
		return nil
	}

	path = "/api/v1/gateway" + path
	if filter != "" && !strings.Contains(path, filter) && !strings.Contains(string(method.Name()), filter) {
		return nil
	}

	operation := map[string]any{
		"method":           verb,
		"path":             path,
		"rpc":              string(method.FullName()),
		"server_streaming": method.IsStreamingServer(),
		"client_streaming": method.IsStreamingClient(),
	}
	if filter != "" {
		operation["input_schema"] = protoMessageSchema(method.Input(), 0)
		operation["output_schema"] = protoMessageSchema(method.Output(), 0)
	}

	return operation
}

func protoMessageSchema(message protoreflect.MessageDescriptor, depth int) map[string]any {
	properties := map[string]any{}
	if depth > 5 {
		return map[string]any{"type": "object", "description": string(message.FullName())}
	}
	for i := 0; i < message.Fields().Len(); i++ {
		field := message.Fields().Get(i)
		entry := protoFieldSchema(field, depth)
		if field.IsMap() {
			entry = map[string]any{"type": "object", "additionalProperties": protoFieldSchema(field.MapValue(), depth)}
		} else if field.IsList() {
			entry = map[string]any{"type": "array", "items": entry}
		}
		properties[field.JSONName()] = entry
	}
	return map[string]any{"type": "object", "properties": properties, "description": string(message.FullName())}
}

func protoFieldSchema(field protoreflect.FieldDescriptor, depth int) map[string]any {
	switch field.Kind() {
	case protoreflect.MessageKind:
		return protoMessageSchema(field.Message(), depth+1)
	case protoreflect.BoolKind:
		return map[string]any{"type": "boolean"}
	case protoreflect.StringKind:
		return map[string]any{"type": "string"}
	case protoreflect.BytesKind:
		return map[string]any{"type": "string", "contentEncoding": "base64"}
	case protoreflect.EnumKind:
		names := []string{}
		for i := 0; i < field.Enum().Values().Len(); i++ {
			names = append(names, string(field.Enum().Values().Get(i).Name()))
		}
		return map[string]any{"type": "string", "enum": names}
	case protoreflect.Int64Kind, protoreflect.Uint64Kind, protoreflect.Sint64Kind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		return map[string]any{"type": "string", "pattern": "^-?[0-9]+$", "description": "64-bit integer encoded as a decimal string"}
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return map[string]any{"type": "number"}
	default:
		return map[string]any{"type": "integer"}
	}
}
