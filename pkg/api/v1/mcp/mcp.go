package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"strings"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
)

// Default protocol for clients that omit a version.
const mcpProtocolVersion = "2025-11-25"

// Gateway provides the service operations used by MCP tools.
type Gateway interface {
	apiv1.DatabaseManager
	StopDeployment(ctx context.Context, in *pb.StopDeploymentRequest) (*pb.StopDeploymentResponse, error)
	StartDeployment(ctx context.Context, in *pb.StartDeploymentRequest) (*pb.StartDeploymentResponse, error)
	DeleteDeployment(ctx context.Context, in *pb.DeleteDeploymentRequest) (*pb.DeleteDeploymentResponse, error)
	ScaleDeployment(ctx context.Context, in *pb.ScaleDeploymentRequest) (*pb.ScaleDeploymentResponse, error)
	DeployStub(ctx context.Context, in *pb.DeployStubRequest) (*pb.DeployStubResponse, error)
	ListTasks(ctx context.Context, in *pb.ListTasksRequest) (*pb.ListTasksResponse, error)
	StopTasks(ctx context.Context, in *pb.StopTasksRequest) (*pb.StopTasksResponse, error)
	ActiveDeploymentByName(ctx context.Context, workspace *types.Workspace, name string) (*types.DeploymentWithRelated, error)
	SetDeploymentEnv(ctx context.Context, authInfo *auth.AuthInfo, appName string, set map[string]string, unset []string) (*pb.DeployStubResponse, error)
	RedeployWithConfig(ctx context.Context, authInfo *auth.AuthInfo, appName string, mutate func(*types.StubConfigV1) error) (*pb.DeployStubResponse, error)
	RedeployStub(ctx context.Context, authInfo *auth.AuthInfo, deployment *types.DeploymentWithRelated, mutate func(*types.StubConfigV1) error) (*pb.DeployStubResponse, error)
	SecretValue(ctx context.Context, workspace *types.Workspace, name string) (string, error)
	DeploymentURL(d *types.DeploymentWithRelated) (string, error)
	WorkspaceCredit(ctx context.Context, workspace *types.Workspace) *types.CreditStatus
}

type Group struct {
	router        *echo.Echo
	gws           Gateway
	backendRepo   repository.BackendRepository
	workspaceRepo repository.WorkspaceRepository
	eventRepo     repository.EventRepository
	config        types.AppConfig
	tools         []mcpTool
	byName        map[string]*mcpTool
}

func NewGroup(g *echo.Group, router *echo.Echo, gws Gateway, backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository, eventRepo repository.EventRepository, config types.AppConfig) *Group {
	group := &Group{
		router:        router,
		gws:           gws,
		backendRepo:   backendRepo,
		workspaceRepo: workspaceRepo,
		eventRepo:     eventRepo,
		config:        config,
	}
	group.tools = group.catalog()
	group.prepareTools()
	g.POST("", group.authorize(group.Post))
	g.GET("", group.authorize(group.Get))
	g.DELETE("", group.authorize(func(ctx echo.Context) error { return ctx.NoContent(http.StatusOK) }))
	router.GET("/.well-known/oauth-protected-resource", group.oauthMetadata)
	router.GET("/.well-known/oauth-protected-resource/api/v1/mcp", group.oauthMetadata)
	return group
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// Get: no server-initiated stream; clients fall back to plain request/response.
func (g *Group) Get(ctx echo.Context) error {
	return ctx.NoContent(http.StatusMethodNotAllowed)
}

func (g *Group) dispatch(ctx context.Context, authInfo *auth.AuthInfo, req rpcRequest) rpcResponse {
	resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}
	switch req.Method {
	case "server/discover":
		resp.Result = map[string]any{
			"capabilities":      map[string]any{"tools": map[string]any{}},
			"serverInfo":        map[string]any{"name": "beam", "version": "1"},
			"instructions":      g.instructions(),
			"supportedVersions": []string{mcpStatelessVersion},
		}
	case "initialize":
		var init struct {
			Version string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &init)
		version := mcpProtocolVersion
		if init.Version == "2025-03-26" || init.Version == "2025-06-18" {
			version = init.Version
		}
		resp.Result = map[string]any{
			"protocolVersion": version,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": "beta9", "version": "1"},
			"instructions":    g.instructions(),
		}
	case "ping":
		resp.Result = map[string]any{}
	case "tools/list":
		out := make([]map[string]any, 0, len(g.tools))
		for _, t := range g.tools {
			if !g.allowed(authInfo, &t) || !g.selected(ctx, &t) {
				continue
			}
			out = append(out, t.describe())
		}
		resp.Result = map[string]any{"tools": out}
	case "tools/call":
		var params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			resp.Error = &rpcError{Code: -32602, Message: "invalid params"}
			return resp
		}
		tool, ok := g.byName[params.Name]
		if !ok {
			resp.Error = &rpcError{Code: -32602, Message: fmt.Sprintf("unknown tool %q", params.Name)}
			return resp
		}
		resp.Result = g.call(ctx, authInfo, tool, params.Arguments)
	default:
		resp.Error = &rpcError{Code: -32601, Message: "method not found"}
	}
	return resp
}

type toolArgs map[string]any

func (a toolArgs) str(key string) string {
	v, _ := a[key].(string)
	return strings.TrimSpace(v)
}

// rawString preserves significant whitespace in credentials and request bodies.
func (a toolArgs) rawString(key string) string {
	v, _ := a[key].(string)
	return v
}

func (a toolArgs) num(key string, fallback float64) float64 {
	if v, ok := a[key].(float64); ok {
		return v
	}
	return fallback
}

func (a toolArgs) boolean(key string) bool {
	v, _ := a[key].(bool)
	return v
}

func (a toolArgs) strings(key string) []string {
	items, _ := a[key].([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func (a toolArgs) stringMap(key string) map[string]string {
	obj, _ := a[key].(map[string]any)
	out := make(map[string]string, len(obj))
	for k, v := range obj {
		out[k] = fmt.Sprint(v)
	}
	return out
}

// toolError carries a machine-readable code alongside the message.
type toolError struct {
	Code    string
	Message string
}

func (e *toolError) Error() string { return e.Message }

func fail(code, format string, args ...any) error {
	return &toolError{Code: code, Message: fmt.Sprintf(format, args...)}
}

type toolFn func(ctx context.Context, authInfo *auth.AuthInfo, args toolArgs) (any, error)

type mcpTool struct {
	Name        string
	Description string
	Schema      map[string]any
	Destructive bool
	Confirm     string // non-empty: the call needs confirm=true and this is the warning
	Run         toolFn
	ReadOnly    bool
	Idempotent  bool
	OpenWorld   bool
	Category    string
	Scope       string
	Unfiltered  bool // listed whatever ?features=, ?tools= or ?readonly= a connection sets
	validate    func(any) error
}

func (t mcpTool) describe() map[string]any {
	destructive := t.Destructive || t.Confirm != ""
	return map[string]any{
		"name":        t.Name,
		"description": t.Description,
		"inputSchema": t.Schema,
		"_meta":       map[string]any{"beam/category": t.Category, "beam/scope": t.Scope},
		"annotations": map[string]any{
			"destructiveHint": destructive,
			"readOnlyHint":    t.ReadOnly,
			"idempotentHint":  t.Idempotent,
			"openWorldHint":   t.OpenWorld,
		},
	}
}

func (g *Group) call(ctx context.Context, authInfo *auth.AuthInfo, tool *mcpTool, args map[string]any) map[string]any {
	if args == nil {
		args = map[string]any{}
	}
	validate := tool.validate
	if validate == nil {
		var err error
		validate, err = compileToolSchema(tool.Schema)
		if err != nil {
			return toolResult(map[string]any{"code": "INTERNAL_ERROR", "error": "Invalid tool contract"}, true)
		}
	}
	if err := validate(args); err != nil {
		return toolResult(map[string]any{"code": "INVALID_ARGS", "error": err.Error()}, true)
	}
	if !g.allowed(authInfo, tool) {
		return toolResult(map[string]any{
			"code":           "FORBIDDEN",
			"error":          "This credential cannot call " + tool.Name,
			"required_scope": tool.Scope,
		}, true)
	}
	if !g.selected(ctx, tool) {
		return toolResult(map[string]any{"code": "TOOL_UNAVAILABLE", "error": "Tool is excluded by this connection's filter"}, true)
	}

	// Validate shared bounded fields before any backend call or integer conversion.
	bounds := map[string][2]float64{
		"limit":          {1, 1000},
		"offset":         {0, 1000000},
		"tail":           {1, 10000},
		"window_minutes": {1, 10080},
		"since_minutes":  {0, 10080},
		"wait_seconds":   {0, 55},
		"containers":     {0, 1000},
		"port":           {0, 65535},
		"cpu":            {1, 1000000},
		"memory":         {1, 10000000},
	}
	for key, bound := range bounds {
		raw, exists := args[key]
		if !exists || tool.Category == "vm" {
			continue
		}

		number, ok := raw.(float64)
		valid := ok && math.Trunc(number) == number && number >= bound[0] && number <= bound[1]
		if !valid {
			return toolResult(map[string]any{
				"code":  "INVALID_ARGS",
				"error": fmt.Sprintf("%s must be an integer between %g and %g", key, bound[0], bound[1]),
			}, true)
		}
	}

	if tool.Confirm != "" && !toolArgs(args).boolean("confirm") {
		return toolResult(map[string]any{"error": tool.Confirm + " Call again with confirm=true.", "code": "NEEDS_CONFIRMATION"}, true)
	}

	out, err := tool.Run(auth.ContextWithAuthInfo(ctx, authInfo), authInfo, toolArgs(args))
	if err != nil {
		result := map[string]any{"error": err.Error(), "code": "ERROR"}
		var te *toolError
		var insufficient *types.InsufficientCreditsError
		if errors.As(err, &te) {
			result["code"] = te.Code
		} else if errors.As(err, &insufficient) {
			result["code"] = "INSUFFICIENT_CREDITS"
			// A client holding several saved profiles must be able to tell which
			// workspace was refused; another profile's balance says nothing here.
			// Job results keep only the message, so it carries both identifiers.
			if authInfo != nil && authInfo.Workspace != nil {
				result["error"] = fmt.Sprintf("%s (workspace %s, id %s)", err.Error(), authInfo.Workspace.Name, authInfo.Workspace.ExternalId)
				result["workspace_id"] = authInfo.Workspace.ExternalId
				result["workspace_name"] = authInfo.Workspace.Name
			}
		}
		return toolResult(result, true)
	}

	if payload, ok := out.(mcpPayload); ok {
		return map[string]any(payload)
	}
	failed := false
	if response, ok := out.(map[string]any); ok {
		failed, _ = response["is_error"].(bool)
	}
	return toolResult(out, failed)
}

// toolResult wraps a tool's value; structuredContent must be an object, so lists become {"items": [...]}.
func toolResult(value any, isError bool) map[string]any {
	if value == nil {
		value = map[string]any{}
	}
	if reflect.ValueOf(value).Kind() == reflect.Slice {
		value = map[string]any{"items": value}
	}
	text, _ := json.Marshal(value)
	return map[string]any{
		"content":           []map[string]any{{"type": "text", "text": string(text)}},
		"structuredContent": value,
		"isError":           isError,
	}
}

type props map[string]any

func schema(p props, required ...string) map[string]any {
	s := map[string]any{"type": "object", "properties": p, "additionalProperties": false}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

func str(description string) map[string]any {
	if description == "" {
		return map[string]any{"type": "string"}
	}
	return map[string]any{"type": "string", "description": description}
}

func integer(def int) map[string]any { return map[string]any{"type": "integer", "default": def} }

func boolean() map[string]any { return map[string]any{"type": "boolean"} }

func strList(description string) map[string]any {
	return map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": description}
}

var databaseKind = map[string]any{"type": "string", "enum": []string{"postgres", "redis", "mysql", "mongo"}}

var logStreams = []string{types.EventLogStreamStdout, types.EventLogStreamStderr, types.EventLogStreamSystem}

var logStream = map[string]any{"type": "string", "enum": logStreams, "description": "Keep only this stream; default all"}

// servesRequests: only the endpoint abstraction records request stats.
func servesRequests(t types.StubType) bool {
	kind := t.Kind()
	return kind == types.StubTypeEndpoint || kind == types.StubTypeASGI
}
