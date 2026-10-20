package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func mcpIdentity() *auth.AuthInfo {
	storageID := uint(1)
	return &auth.AuthInfo{Workspace: &types.Workspace{Id: 1, ExternalId: "ws", Storage: &types.WorkspaceStorage{Id: &storageID}}, Token: &types.Token{Id: 1, Active: true, Key: "private-test-key", TokenType: types.TokenTypeWorkspace}}
}

func TestMCPCompleteContracts(t *testing.T) {
	g := newTestGroup()
	g.prepareTools()
	for _, tool := range g.tools {
		require.NotEmpty(t, tool.Scope, tool.Name)
		require.NotEmpty(t, tool.Category, tool.Name)
		if tool.ReadOnly {
			require.False(t, tool.Destructive, tool.Name)
		}
		if tool.Confirm != "" {
			require.False(t, tool.ReadOnly, tool.Name)
		}
	}
	for _, name := range []string{"create_vm", "vm_exec", "vm_write_file", "vm_desktop_screenshot", "vm_prompt", "vm_docker", "create_vm_template"} {
		require.Contains(t, g.byName, name)
	}
	for _, name := range []string{"vm_exec", "vm_prompt", "vm_docker", "vm_desktop_launch"} {
		require.True(t, g.byName[name].OpenWorld, name)
		require.False(t, g.byName[name].Idempotent, name)
	}
	for _, args := range []map[string]any{{"name": 42.0}, {"name": "vm", "unknown": true}, {"name": "vm", "auto_resume": "true"}, {"name": "vm", "idle_action": "explode"}} {
		result := g.call(context.Background(), mcpIdentity(), g.byName["update_vm"], args)
		require.Equal(t, "INVALID_ARGS", result["structuredContent"].(map[string]any)["code"])
	}
}

func TestMCPVMResourceContract(t *testing.T) {
	g := newTestGroup()
	g.prepareTools()
	properties := vmSpecSchema()["properties"].(props)
	require.Equal(t, 4000, properties["cpu"].(map[string]any)["default"])
	require.Equal(t, 8192, properties["memory"].(map[string]any)["default"])
	require.Equal(t, 0, properties["idle_timeout"].(map[string]any)["default"])
	for _, spec := range []map[string]any{
		{"image_id": "base", "desktop": true, "cpu": 1000, "memory": 1024},
		{"image_id": "base", "cpu": 100, "memory": 256, "idle_timeout": 365 * 24 * 60 * 60},
	} {
		require.NoError(t, g.byName["create_vm"].validate(map[string]any{"spec": spec}))
	}
	for _, spec := range []map[string]any{
		{"image_id": "base", "desktop": true, "memory": 1023},
		{"image_id": "base", "cpu": 99},
		{"image_id": "base", "memory": 255},
		{"image_id": "base", "idle_timeout": -1},
	} {
		require.Error(t, g.byName["create_vm"].validate(map[string]any{"spec": spec}))
	}
}

type mcpSecretRecorder struct {
	repository.BackendRepository
	writes int
}

type mcpWebhookRecorder struct {
	repository.WorkspaceRepository
	workspace string
	webhooks  []types.WorkspaceWebhook
}

func (r *mcpWebhookRecorder) SetWebhook(_ context.Context, workspace string, webhook types.WorkspaceWebhook) error {
	r.workspace = workspace
	r.webhooks = append(r.webhooks, webhook)
	return nil
}

func TestMCPWebhookCreation(t *testing.T) {
	for _, tc := range []struct {
		name, url string
		canceled  bool
		allowed   bool
	}{
		{name: "normalized URL", url: "  https://8.8.8.8/hook  ", allowed: true},
		{name: "canceled", url: "https://8.8.8.8/hook", canceled: true},
		{name: "invalid port", url: "https://8.8.8.8:65536/hook"},
		{name: "local destination", url: "http://127.0.0.1/hook"},
		{name: "reserved destination", url: "http://0.0.0.1/hook"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newTestGroup()
			recorder := &mcpWebhookRecorder{}
			g.workspaceRepo = recorder
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			out, err := g.createWebhook(ctx, mcpIdentity(), toolArgs{"url": tc.url})
			if tc.allowed {
				require.NoError(t, err)
				require.Len(t, recorder.webhooks, 1)
				require.Equal(t, "ws", recorder.workspace)
				require.Equal(t, strings.TrimSpace(tc.url), recorder.webhooks[0].URL)
				require.Equal(t, recorder.webhooks[0].URL, out.(types.WorkspaceWebhook).URL)
			} else {
				require.Error(t, err)
				require.Empty(t, recorder.webhooks)
			}
		})
	}
}

func (r *mcpSecretRecorder) CreateSecret(context.Context, *types.Workspace, uint, string, string, bool, []string) (*types.Secret, error) {
	r.writes++
	return &types.Secret{}, nil
}

func TestMCPRestrictedCredentialCannotWriteSecrets(t *testing.T) {
	g := newTestGroup()
	recorder := &mcpSecretRecorder{}
	g.backendRepo = recorder
	identity := mcpIdentity()
	identity.Token.TokenType = types.TokenTypeWorkspaceRestricted
	result := g.call(context.Background(), identity, g.byName["create_secret"], map[string]any{"name": "SAFE", "value": "fake"})
	if result["isError"] != true {
		t.Fatal("restricted secret write was accepted")
	}
	require.Equal(t, 0, recorder.writes)
	identity.Token.TokenType = types.TokenTypeWorkspace
	result = g.call(context.Background(), identity, g.byName["create_secret"], map[string]any{"name": "SAFE", "value": "fake"})
	require.Equal(t, false, result["isError"])
	require.Equal(t, 1, recorder.writes)
}

func TestMCPOAuthScopeAndFilterCannotBeBypassed(t *testing.T) {
	g := newTestGroup()
	a := mcpIdentity()
	a.OAuth = &auth.OAuthGrant{Scopes: []string{"beam:read"}}
	for _, name := range []string{"create_vm", "vm_exec", "vm_access_token", "vm_access_url", "api", "create_secret"} {
		require.False(t, g.allowed(a, g.byName[name]), name)
	}
	require.True(t, g.allowed(a, g.byName["get_vm"]))
	ctx := context.WithValue(context.Background(), mcpSelectionKey{}, mcpSelection{Features: []string{"vm"}, ReadOnly: true})
	result := g.dispatch(ctx, a, rpcRequest{JSONRPC: "2.0", ID: json.RawMessage("1"), Method: "tools/list"})
	for _, item := range result.Result.(map[string]any)["tools"].([]map[string]any) {
		tool := g.byName[item["name"].(string)]
		require.Equal(t, "vm", tool.Category)
		require.True(t, tool.ReadOnly)
	}
	a.OAuth.Scopes = append(a.OAuth.Scopes, "beam:write", "beam:exec", "beam:credentials")
	require.False(t, g.allowed(a, g.byName["api"]), "OAuth never grants unscoped API traversal")
}

func TestMCPProtocolRejectsInvalidEnvelopes(t *testing.T) {
	g := newTestGroup()
	for _, body := range []string{`{"jsonrpc":"1.0","id":1,"method":"ping"}`, `[]`, `{"jsonrpc":"2.0","id":true,"method":"ping"}`, `{"jsonrpc":"2.0","id":1,"method":"ping"} {"trailing":true}`} {
		r := httptest.NewRecorder()
		c := &auth.HttpAuthContext{Context: g.router.NewContext(httptest.NewRequest(http.MethodPost, "/api/v1/mcp", strings.NewReader(body)), r), AuthInfo: mcpIdentity()}
		require.NoError(t, g.Post(c))
		require.Equal(t, http.StatusBadRequest, r.Code, body)
	}
	body := `{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}}}`
	r := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/mcp", strings.NewReader(body))
	request.Header.Set("MCP-Protocol-Version", "2026-07-28")
	request.Header.Set("Mcp-Method", "server/discover")
	c := &auth.HttpAuthContext{Context: g.router.NewContext(request, r), AuthInfo: mcpIdentity()}
	require.NoError(t, g.Post(c))
	require.Equal(t, http.StatusOK, r.Code)
	require.Contains(t, r.Body.String(), `"resultType":"complete"`)
}

type mcpWorkspaceAuth struct {
	repository.WorkspaceRepository
	identity *auth.AuthInfo
}

func (r *mcpWorkspaceAuth) AuthorizeToken(key string) (*types.Token, *types.Workspace, error) {
	if key != r.identity.Token.Key {
		return nil, nil, errors.New("bad token")
	}
	return r.identity.Token, r.identity.Workspace, nil
}

type mcpBackendAuth struct{ repository.BackendRepository }

func (r *mcpBackendAuth) AuthorizeToken(context.Context, string) (*types.Token, *types.Workspace, error) {
	return nil, nil, errors.New("bad token")
}

func TestMCPOAuthDiscoveryAndIntrospection(t *testing.T) {
	active := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer introspection-test-secret", r.Header.Get("Authorization"))
		require.NoError(t, r.ParseForm())
		require.Equal(t, "https://app.test/api/v1/mcp", r.Form.Get("resource"))
		_ = json.NewEncoder(w).Encode(map[string]any{"active": active, "iss": "https://api.test/v2/oauth", "resource": "https://app.test/api/v1/mcp", "client_id": "test", "sub": "1", "workspace_id": "ws", "workspace_token": "private-test-key", "scope": "beam:read"})
	}))
	defer server.Close()
	g := newTestGroup()
	g.backendRepo = &mcpBackendAuth{}
	g.workspaceRepo = &mcpWorkspaceAuth{identity: mcpIdentity()}
	g.config.GatewayService.MCPOAuth = types.MCPOAuthConfig{Issuer: "https://api.test/v2/oauth", Resource: "https://app.test/api/v1/mcp", IntrospectionURL: server.URL, IntrospectionSecret: "introspection-test-secret"}
	g.router.POST("/api/v1/mcp", g.authorize(g.Post))
	g.router.GET("/.well-known/oauth-protected-resource/api/v1/mcp", g.oauthMetadata)
	request := func(token string) *httptest.ResponseRecorder {
		r := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		g.router.ServeHTTP(r, req)
		return r
	}
	missing := request("")
	require.Equal(t, 401, missing.Code)
	require.Contains(t, missing.Header().Get("WWW-Authenticate"), "resource_metadata")
	accepted := request("bmo_test")
	require.Equal(t, 200, accepted.Code)
	require.Contains(t, accepted.Body.String(), "get_vm")
	require.NotContains(t, accepted.Body.String(), `"name":"vm_exec"`)
	require.NotContains(t, accepted.Body.String(), "private-test-key")
	active = false
	rejected := request("bmo_test")
	require.Equal(t, 401, rejected.Code)
	require.Contains(t, rejected.Header().Get("WWW-Authenticate"), `error="invalid_token"`)
}

func TestMCPVMExecPinsWorkspaceAndArgv(t *testing.T) {
	g := newTestGroup()
	g.router.GET("/api/v1/vm/ws/:name", func(c echo.Context) error {
		return c.JSON(200, map[string]any{"id": "vm-id", "container_id": "owned-container", "desired_state": "running"})
	})
	g.router.POST("/api/v1/vm/ws/:name/touch", func(c echo.Context) error { return c.JSON(200, map[string]any{}) })
	g.router.POST("/api/v1/gateway/pods/:container/exec", func(c echo.Context) error {
		require.Equal(t, "owned-container", c.Param("container"))
		var body map[string]any
		require.NoError(t, c.Bind(&body))
		require.Equal(t, `'printf' '%s' 'a'\''b $(no expansion)'`, body["command"])
		return c.JSON(200, map[string]any{"ok": true, "pid": 42})
	})
	args := toolArgs{"name": "vm", "command": []any{"printf", "%s", "a'b $(no expansion)"}}
	value, err := g.vmExec(context.Background(), mcpIdentity(), args)
	require.NoError(t, err)
	require.Equal(t, "owned-container", value.(map[string]any)["container_id"])
	args["container_id"] = "previous-container"
	_, err = g.vmPod(context.Background(), mcpIdentity(), args, http.MethodGet, "/status", nil, nil)
	require.ErrorContains(t, err, "previous container")
}

func TestMCPVMWaitPreservesNonzeroExitAndOutput(t *testing.T) {
	g := newTestGroup()
	touches := 0
	g.router.GET("/api/v1/vm/ws/:name", func(c echo.Context) error {
		return c.JSON(200, map[string]any{"id": "vm", "container_id": "container", "desired_state": "running"})
	})
	g.router.POST("/api/v1/vm/ws/:name/touch", func(c echo.Context) error {
		touches++
		if touches == 1 {
			return c.JSON(409, map[string]any{"message": "VM operation already in progress"})
		}
		return c.JSON(200, map[string]any{})
	})
	g.router.POST("/api/v1/gateway/pods/:container/exec", func(c echo.Context) error { return c.JSON(200, map[string]any{"ok": true, "pid": 42, "exitCode": -1}) })
	g.router.GET("/api/v1/gateway/pods/:container/status", func(c echo.Context) error {
		return c.JSON(200, map[string]any{"ok": true, "status": "exited", "exitCode": 7})
	})
	g.router.GET("/api/v1/gateway/pods/:container/stdout", func(c echo.Context) error { return c.JSON(200, map[string]any{"ok": true, "stdout": "partial output"}) })
	g.router.GET("/api/v1/gateway/pods/:container/stderr", func(c echo.Context) error { return c.JSON(200, map[string]any{"ok": true, "stderr": "command failed"}) })
	result := g.call(context.Background(), mcpIdentity(), g.byName["vm_exec"], map[string]any{"name": "vm", "command": []any{"false"}, "wait": true})
	require.Equal(t, true, result["isError"])
	data := result["structuredContent"].(map[string]any)
	require.Equal(t, float64(7), data["exit_code"])
	require.Equal(t, "partial output", data["stdout"])
	require.Equal(t, "command failed", data["stderr"])
	_, err := mcpAPIValue(map[string]any{"status": 200, "body": map[string]any{"ok": false, "errorMsg": "guest unavailable"}}, nil)
	require.ErrorContains(t, err, "guest unavailable")
}
