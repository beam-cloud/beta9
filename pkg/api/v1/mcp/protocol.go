package mcp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/labstack/echo/v4"
)

const mcpStatelessVersion = "2026-07-28"
const mcpMetaPrefix = "io.modelcontextprotocol/"

func rpcFailure(c echo.Context, id json.RawMessage, code int, message string, data any) error {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	return c.JSON(http.StatusBadRequest, rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message, Data: data}})
}

func validRPC(req rpcRequest) bool {
	if req.JSONRPC != "2.0" || req.Method == "" {
		return false
	}
	if len(req.ID) == 0 {
		return true
	}
	var value any
	if json.Unmarshal(req.ID, &value) != nil {
		return false
	}
	switch value.(type) {
	case string, float64:
		return true
	default:
		return false
	}
}

func (g *Group) Post(c echo.Context) error {
	cc, ok := c.(*auth.HttpAuthContext)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}
	decoder := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, 2<<20))
	var raw json.RawMessage
	if decoder.Decode(&raw) != nil {
		return rpcFailure(c, nil, -32700, "parse error", nil)
	}
	if decoder.Decode(new(any)) != io.EOF {
		return rpcFailure(c, nil, -32700, "expected one JSON document", nil)
	}
	var req rpcRequest
	// MCP requires individual JSON-RPC messages.
	if json.Unmarshal(raw, &req) != nil || !validRPC(req) {
		return rpcFailure(c, nil, -32600, "invalid request", nil)
	}
	var params struct {
		Meta map[string]any `json:"_meta"`
		Name string         `json:"name"`
		URI  string         `json:"uri"`
	}
	if len(req.Params) > 0 && (strings.TrimSpace(string(req.Params)) == "null" || json.Unmarshal(req.Params, &params) != nil) {
		return rpcFailure(c, req.ID, -32602, "params must be an object", nil)
	}
	version, _ := params.Meta[mcpMetaPrefix+"protocolVersion"].(string)
	modern := version != "" || c.Request().Header.Get("MCP-Protocol-Version") == mcpStatelessVersion
	if modern {
		if version != mcpStatelessVersion {
			return rpcFailure(c, req.ID, -32022, "unsupported protocol version", map[string]any{"supported": []string{mcpStatelessVersion}, "requested": version})
		}
		for _, key := range []string{"clientInfo", "clientCapabilities"} {
			if value, ok := params.Meta[mcpMetaPrefix+key].(map[string]any); !ok || value == nil {
				return rpcFailure(c, req.ID, -32602, "missing or invalid protocol metadata: "+key, nil)
			}
		}
		if c.Request().Header.Get("MCP-Protocol-Version") != version || c.Request().Header.Get("Mcp-Method") != req.Method {
			return rpcFailure(c, req.ID, -32020, "protocol headers must match the request body", nil)
		}
		if req.Method == "tools/call" && c.Request().Header.Get("Mcp-Name") != params.Name {
			return rpcFailure(c, req.ID, -32020, "Mcp-Name must match the tool name", nil)
		}
		if req.Method == "initialize" || req.Method == "ping" {
			return rpcFailure(c, req.ID, -32601, "method not found", nil)
		}
	}
	if len(req.ID) == 0 {
		return c.NoContent(http.StatusAccepted)
	}
	selection := mcpSelection{
		Features: splitMCPFilter(c.QueryParam("features")),
		Tools:    splitMCPFilter(c.QueryParam("tools")),
		ReadOnly: c.QueryParam("readonly") == "true" || c.QueryParam("readonly") == "1",
	}
	ctx := withIdentityHeaders(c.Request().Context(), c.Request().Header)
	ctx = context.WithValue(ctx, mcpSelectionKey{}, selection)
	response := g.dispatch(ctx, cc.AuthInfo, req)
	if modern && response.Error == nil {
		result, _ := response.Result.(map[string]any)
		if result != nil {
			result["resultType"] = "complete"
			result["_meta"] = map[string]any{mcpMetaPrefix + "serverInfo": map[string]any{"name": "beam", "version": "1"}}
			if req.Method == "tools/list" || req.Method == "server/discover" {
				result["ttlMs"] = 0
				result["cacheScope"] = "private"
			}
		}
	}
	if response.Error != nil && modern {
		return c.JSON(http.StatusBadRequest, response)
	}
	return c.JSON(http.StatusOK, response)
}

func splitMCPFilter(value string) []string {
	var items []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}
