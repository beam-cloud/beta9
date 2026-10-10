package apiv1

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
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

// A pod serves its ports, so the task settings its stub config carries anyway
// stay out of its view.
func TestMCPAppConfigView(t *testing.T) {
	cfg := &types.StubConfigV1{Workers: 1, ConcurrentRequests: 1, KeepWarmSeconds: 600, Ports: []uint32{3001}}

	pod := appConfigView(types.StubType(types.StubTypePodDeployment), cfg)
	for _, key := range podUnusedConfig {
		require.NotContains(t, pod, key)
	}
	require.Contains(t, pod, "keep_warm_seconds")
	require.Contains(t, pod, "autoscaler")
	require.Contains(t, pod, "ports")

	endpoint := appConfigView(types.StubType(types.StubTypeEndpointDeployment), cfg)
	require.Contains(t, endpoint, "workers")
	require.Contains(t, endpoint, "task_policy")
}

// A container is up once its own server answers /, whatever it answers short of
// a server error or the gateway's 429; a named path, or a runner's /health,
// must answer 2xx.
func TestMCPReadinessCheck(t *testing.T) {
	answer := func(status int) map[string]any { return map[string]any{"status": status, "is_error": status >= 400} }

	path, serving := readinessCheck(types.StubTypePod, "")
	require.Equal(t, "/", path)
	for status, ready := range map[int]bool{200: true, 302: true, 401: true, 404: true, 429: false, 502: false, 503: false} {
		require.Equal(t, ready, serving(answer(status)), status)
	}

	path, serving = readinessCheck(types.StubTypePod, "/healthz")
	require.Equal(t, "/healthz", path)
	require.True(t, serving(answer(204)))
	require.False(t, serving(answer(404)))
	require.True(t, serving(map[string]any{"status": 200, "is_error": true, "code": "RESPONSE_TOO_LARGE"}))

	path, serving = readinessCheck("endpoint", "")
	require.Equal(t, "/health", path)
	require.False(t, serving(answer(404)))
}

// A request the deadline cut off before the app answered has no status: the
// default 200 would read as a crash-looping app serving.
func TestMCPCancelledResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	unanswered := (&boundedResponse{header: http.Header{}}).result(ctx)
	require.NotContains(t, unanswered, "status")
	require.Equal(t, "CANCELLED", unanswered["code"])
	require.Contains(t, unanswered["error"], "no answer before the deadline")

	started := &boundedResponse{header: http.Header{}}
	started.WriteHeader(http.StatusAccepted)
	require.Equal(t, http.StatusAccepted, started.result(ctx)["status"])
	require.Equal(t, "CANCELLED", started.result(ctx)["code"])
}

// A readiness answer can be a whole page; what an agent reads of it is bounded.
func TestMCPClipAnswer(t *testing.T) {
	page := strings.Repeat("x", healthAnswerMax+10)
	clipped := clipAnswer(map[string]any{"status": 200, "headers": http.Header{"Set-Cookie": {"s"}}, "body": page})
	require.Equal(t, map[string]any{"status": 200, "body": page[:healthAnswerMax] + "..."}, clipped)

	small := map[string]any{"status": "ok"}
	require.Equal(t, small, clipAnswer(map[string]any{"body": small})["body"])
	require.NotContains(t, clipAnswer(map[string]any{"body_base64": "AAAA"}), "body_base64")
}

// A multi-port container's latest URL has a port placeholder; each port gets
// its own URL an agent can open or share.
func TestMCPPortURLs(t *testing.T) {
	d := &types.DeploymentWithRelated{Stub: types.Stub{Config: `{"ports": [3000, 9001]}`}}
	require.Equal(t, map[string]string{
		"3000": "https://dep-latest-3000.app.test",
		"9001": "https://dep-latest-9001.app.test",
	}, portURLs(d, "https://dep-latest-"+common.PortPlaceholder+".app.test"))
	require.Nil(t, portURLs(d, "https://dep-latest-3000.app.test"))
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
	require.Contains(t, result["error"], "(workspace 36dc7a, id ws-1)")
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

	out, err = g.apiRoutes(ctx, a, toolArgs{"path": "/api/v1/other"})
	require.NoError(t, err)
	require.Empty(t, out.(map[string]any)["routes"])
}
