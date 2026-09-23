package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
)

// The MCP server lives in the gateway: agents connect over HTTP with a workspace
// token and get the same operations the dashboard and CLI use. Stateless
// JSON-RPC (Streamable HTTP without a server stream); every call is one POST.
const mcpProtocolVersion = "2025-03-26"

// MCPGateway is what the tools need from the assembled gateway service.
type MCPGateway interface {
	DatabaseManager
	ListDeployments(ctx context.Context, in *pb.ListDeploymentsRequest) (*pb.ListDeploymentsResponse, error)
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
	SecretValue(ctx context.Context, workspace *types.Workspace, name string) (string, error)
	DeploymentURL(d *types.DeploymentWithRelated) (string, error)
}

type MCPGroup struct {
	router        *echo.Echo // the gateway itself, for in-process `api` and `invoke` calls
	gws           MCPGateway
	backendRepo   repository.BackendRepository
	workspaceRepo repository.WorkspaceRepository
	eventRepo     repository.EventRepository
	config        types.AppConfig
	tools         []mcpTool
	byName        map[string]*mcpTool
}

func NewMCPGroup(g *echo.Group, router *echo.Echo, gws MCPGateway, backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository, eventRepo repository.EventRepository, config types.AppConfig) *MCPGroup {
	group := &MCPGroup{router: router, gws: gws, backendRepo: backendRepo, workspaceRepo: workspaceRepo, eventRepo: eventRepo, config: config}
	group.tools = group.catalog()
	group.byName = make(map[string]*mcpTool, len(group.tools))
	for i := range group.tools {
		group.byName[group.tools[i].Name] = &group.tools[i]
	}
	g.POST("", auth.WithAuth(group.Post))
	g.GET("", auth.WithAuth(group.Get))
	g.DELETE("", auth.WithAuth(func(ctx echo.Context) error { return ctx.NoContent(http.StatusOK) }))
	return group
}

// --- protocol ------------------------------------------------------------------

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// Get: no server-initiated stream; clients fall back to plain request/response.
func (g *MCPGroup) Get(ctx echo.Context) error {
	return ctx.NoContent(http.StatusMethodNotAllowed)
}

func (g *MCPGroup) Post(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	var raw json.RawMessage
	if err := json.NewDecoder(ctx.Request().Body).Decode(&raw); err != nil {
		return parseError(ctx)
	}
	batch := len(raw) > 0 && raw[0] == '['
	var requests []rpcRequest
	if batch {
		if err := json.Unmarshal(raw, &requests); err != nil {
			return parseError(ctx)
		}
	} else {
		requests = make([]rpcRequest, 1)
		if err := json.Unmarshal(raw, &requests[0]); err != nil {
			return parseError(ctx)
		}
	}

	reqCtx := withIdentityHeaders(ctx.Request().Context(), ctx.Request().Header)
	responses := make([]rpcResponse, 0, len(requests))
	for _, req := range requests {
		if len(req.ID) == 0 || string(req.ID) == "null" {
			continue // notification
		}
		responses = append(responses, g.dispatch(reqCtx, cc.AuthInfo, req))
	}
	switch {
	case len(responses) == 0:
		return ctx.NoContent(http.StatusAccepted)
	case batch:
		return ctx.JSON(http.StatusOK, responses)
	default:
		return ctx.JSON(http.StatusOK, responses[0])
	}
}

func parseError(ctx echo.Context) error {
	return ctx.JSON(http.StatusBadRequest, rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{-32700, "parse error"}})
}

func (g *MCPGroup) dispatch(ctx context.Context, authInfo *auth.AuthInfo, req rpcRequest) rpcResponse {
	resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}
	switch req.Method {
	case "initialize":
		resp.Result = map[string]any{
			"protocolVersion": mcpProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": "beta9", "version": "1"},
			"instructions": "Serverless GPU/CPU apps, one-off containers and managed databases in one workspace. " +
				"Start with whoami and list_apps. Ship code with the CLI in the project directory (`deploy`); " +
				"everything else is here: read config with get_app, change it with update_config, wire services with connect_services or set_env " +
				"(${{db.NAME.DATABASE_URL}}, ${{secret.NAME}}, ${{app.NAME.URL}}), provision databases, manage secrets, " +
				"group apps into stacks, call apps with invoke, read logs and request stats. Anything without a tool: api_routes then api. " +
				"Tools that say so need confirm=true.",
		}
	case "ping":
		resp.Result = map[string]any{}
	case "tools/list":
		out := make([]map[string]any, 0, len(g.tools))
		for _, t := range g.tools {
			out = append(out, t.describe())
		}
		resp.Result = map[string]any{"tools": out}
	case "tools/call":
		var params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			resp.Error = &rpcError{-32602, "invalid params"}
			return resp
		}
		tool, ok := g.byName[params.Name]
		if !ok {
			resp.Error = &rpcError{-32602, fmt.Sprintf("unknown tool %q", params.Name)}
			return resp
		}
		resp.Result = g.call(ctx, authInfo, tool, params.Arguments)
	default:
		resp.Error = &rpcError{-32601, "method not found"}
	}
	return resp
}

// --- tools --------------------------------------------------------------------------

type toolArgs map[string]any

func (a toolArgs) str(key string) string {
	v, _ := a[key].(string)
	return strings.TrimSpace(v)
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
}

func (t mcpTool) describe() map[string]any {
	destructive := t.Destructive || t.Confirm != ""
	return map[string]any{
		"name":        t.Name,
		"description": t.Description,
		"inputSchema": t.Schema,
		"annotations": map[string]any{"destructiveHint": destructive, "readOnlyHint": !destructive},
	}
}

func (g *MCPGroup) call(ctx context.Context, authInfo *auth.AuthInfo, tool *mcpTool, args map[string]any) map[string]any {
	if args == nil {
		args = map[string]any{}
	}
	if required, _ := tool.Schema["required"].([]string); len(required) > 0 {
		for _, key := range required {
			if _, ok := args[key]; !ok {
				return toolResult(map[string]any{"error": fmt.Sprintf("%s is required", key), "code": "INVALID_ARGS"}, true)
			}
		}
	}
	if tool.Confirm != "" && !toolArgs(args).boolean("confirm") {
		return toolResult(map[string]any{"error": tool.Confirm + " Call again with confirm=true.", "code": "NEEDS_CONFIRMATION"}, true)
	}
	out, err := tool.Run(auth.ContextWithAuthInfo(ctx, authInfo), authInfo, toolArgs(args))
	if err != nil {
		code := "ERROR"
		var te *toolError
		if errors.As(err, &te) {
			code = te.Code
		}
		return toolResult(map[string]any{"error": err.Error(), "code": code}, true)
	}
	return toolResult(out, false)
}

// toolResult wraps a tool's value; structuredContent must be an object, so lists become {"items": [...]}.
func toolResult(value any, isError bool) map[string]any {
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

// --- schema helpers ---------------------------------------------------------------------

type props map[string]any

func schema(p props, required ...string) map[string]any {
	s := map[string]any{"type": "object", "properties": p}
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
