package apiv1

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func newTestMCPGroup() *MCPGroup {
	g := &MCPGroup{router: echo.New()}
	g.config.GatewayService.HTTP.ExternalHost = "gateway.test"
	g.tools = g.catalog()
	g.byName = make(map[string]*mcpTool, len(g.tools))
	for i := range g.tools {
		g.byName[g.tools[i].Name] = &g.tools[i]
	}
	return g
}

func TestMCPCatalogSchemas(t *testing.T) {
	g := newTestMCPGroup()
	seen := map[string]bool{}
	for _, tool := range g.tools {
		require.False(t, seen[tool.Name], tool.Name)
		seen[tool.Name] = true
		props := tool.Schema["properties"].(props)
		required, _ := tool.Schema["required"].([]string)
		for _, key := range required {
			require.Contains(t, props, key, tool.Name)
		}
		if tool.Confirm != "" {
			require.Contains(t, props, "confirm", tool.Name)
		}
	}
}

func TestMCPCallGates(t *testing.T) {
	g := newTestMCPGroup()
	ctx := context.Background()

	res := g.call(ctx, nil, g.byName["get_app"], nil)
	require.Equal(t, "INVALID_ARGS", res["structuredContent"].(map[string]any)["code"])

	res = g.call(ctx, nil, g.byName["logs"], map[string]any{"name": "x", "stream": "console"})
	require.Equal(t, "INVALID_ARGS", res["structuredContent"].(map[string]any)["code"])
	require.Contains(t, res["structuredContent"].(map[string]any)["error"], "stdout, stderr, system")

	for stubType, counted := range map[string]bool{
		types.StubTypeEndpointDeployment:  true,
		types.StubTypeASGIDeployment:      true,
		types.StubTypeEndpointServe:       true,
		types.StubTypePodDeployment:       false,
		types.StubTypeFunctionDeployment:  false,
		types.StubTypeTaskQueueDeployment: false,
	} {
		require.Equal(t, counted, servesRequests(types.StubType(stubType)), stubType)
	}

	res = g.call(ctx, nil, g.byName["delete_app"], map[string]any{"name": "x"})
	require.Equal(t, "NEEDS_CONFIRMATION", res["structuredContent"].(map[string]any)["code"])

	resp := g.dispatch(ctx, nil, rpcRequest{JSONRPC: "2.0", Method: "tools/call", Params: json.RawMessage(`{"name":"nope"}`)})
	require.Equal(t, -32602, resp.Error.Code)
}

type creditGateway struct {
	MCPGateway
	status *types.CreditStatus
}

func (c creditGateway) WorkspaceCredit(context.Context, *types.Workspace) *types.CreditStatus {
	return c.status
}

func TestMCPSurfacesCredit(t *testing.T) {
	ctx := context.Background()
	a := &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "ws-1", Name: "36dc7a"}}
	denied := &types.CreditStatus{Code: "insufficient_credits", Message: "add credits at https://example/credits"}

	g := newTestMCPGroup()
	g.gws = creditGateway{status: denied}
	out, err := g.whoami(ctx, a, nil)
	require.NoError(t, err)
	require.Equal(t, denied, out.(map[string]any)["credit"])

	g.gws = creditGateway{}
	out, _ = g.whoami(ctx, a, nil)
	require.NotContains(t, out.(map[string]any), "credit")

	refused := mcpTool{Name: "refused", Schema: schema(props{}), Run: func(context.Context, *auth.AuthInfo, toolArgs) (any, error) {
		return nil, &types.InsufficientCreditsError{WorkspaceId: "ws-1", Reason: denied.Message}
	}}
	result := g.call(ctx, a, &refused, nil)["structuredContent"].(map[string]any)
	require.Equal(t, "INSUFFICIENT_CREDITS", result["code"])
	require.Equal(t, "ws-1", result["workspace_id"])
	require.Equal(t, "36dc7a", result["workspace_name"])
	require.Contains(t, result["error"], "(workspace 36dc7a)")
}

// api runs through the gateway router with the caller's identity; anything the
// router serves is reachable without a dedicated tool.
func TestMCPApiInProcess(t *testing.T) {
	g := newTestMCPGroup()
	g.router.GET(HttpServerBaseRoute+"/thing/:workspaceId", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"ws": c.Param("workspaceId"), "auth": c.Request().Header.Get("Authorization")})
	})
	g.router.POST(HttpServerBaseRoute+"/thing/:workspaceId", func(c echo.Context) error { return c.NoContent(http.StatusCreated) })

	headers := http.Header{}
	headers.Set("Authorization", "Bearer tok")
	ctx := withIdentityHeaders(context.Background(), headers)
	a := &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "ws1"}}

	out, err := g.api(ctx, a, toolArgs{"path": "/api/v1/thing/{ws}"})
	require.NoError(t, err)
	res := out.(map[string]any)
	require.Equal(t, http.StatusOK, res["status"])
	require.Equal(t, map[string]any{"ws": "ws1", "auth": "Bearer tok"}, res["body"])

	_, err = g.api(ctx, a, toolArgs{"method": "POST", "path": "/api/v1/thing/{ws}"})
	require.ErrorContains(t, err, "confirm=true")

	out, err = g.api(ctx, a, toolArgs{"method": "POST", "path": "/api/v1/thing/{ws}", "confirm": true})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, out.(map[string]any)["status"])

	out, err = g.apiRoutes(ctx, a, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"GET /api/v1/thing/{ws}", "POST /api/v1/thing/{ws}"}, out.(map[string]any)["routes"])
}
