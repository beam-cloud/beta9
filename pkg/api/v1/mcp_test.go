package apiv1

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestMCPGroup() *MCPGroup {
	g := &MCPGroup{}
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

	res = g.call(ctx, nil, g.byName["delete_app"], map[string]any{"name": "x"})
	require.Equal(t, "NEEDS_CONFIRMATION", res["structuredContent"].(map[string]any)["code"])

	resp := g.dispatch(ctx, nil, rpcRequest{JSONRPC: "2.0", Method: "tools/call", Params: json.RawMessage(`{"name":"nope"}`)})
	require.Equal(t, -32602, resp.Error.Code)
}
