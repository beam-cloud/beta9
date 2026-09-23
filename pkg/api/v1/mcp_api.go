package apiv1

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
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

const maxInProcessBody = 256 << 10

// serve runs one request through the gateway router and shapes the response.
func (g *MCPGroup) serve(ctx context.Context, method, url string, body []byte, headers map[string]string) (any, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(string(body)))
	if err != nil {
		return nil, fail("INVALID_ARGS", "%s", err)
	}
	if identity, _ := ctx.Value(identityHeadersKey{}).(http.Header); identity != nil {
		for key, values := range identity {
			req.Header[key] = values
		}
	}
	if len(body) > 0 && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	req.Host = req.URL.Host
	req.RemoteAddr = "127.0.0.1:0"

	rec := httptest.NewRecorder()
	g.router.ServeHTTP(rec, req)

	raw, _ := io.ReadAll(io.LimitReader(rec.Body, maxInProcessBody))
	out := map[string]any{"status": rec.Code, "content_type": rec.Header().Get("Content-Type")}
	var parsed any
	if json.Unmarshal(raw, &parsed) == nil {
		out["body"] = parsed
	} else {
		out["body"] = string(raw)
	}
	if rec.Body.Len() > maxInProcessBody {
		out["truncated"] = true
	}
	return out, nil
}

func (g *MCPGroup) api(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	method := strings.ToUpper(args.str("method"))
	if method == "" {
		method = http.MethodGet
	}
	if method != http.MethodGet && !args.boolean("confirm") {
		return nil, fail("NEEDS_CONFIRMATION", "api %s changes state. Call again with confirm=true.", method)
	}
	path := strings.ReplaceAll(args.str("path"), "{ws}", a.Workspace.ExternalId)
	if !strings.HasPrefix(path, "/") {
		return nil, fail("INVALID_ARGS", "path must start with /")
	}
	var body []byte
	if raw, ok := args["body"]; ok && raw != nil {
		body, _ = json.Marshal(raw)
	}
	return g.serve(ctx, method, g.config.GatewayService.HTTP.GetExternalURL()+path, body, args.stringMap("headers"))
}

func (g *MCPGroup) apiRoutes(_ context.Context, _ *auth.AuthInfo, _ toolArgs) (any, error) {
	routes := g.router.Routes()
	out := make([]string, 0, len(routes))
	seen := map[string]bool{}
	for _, r := range routes {
		if !strings.HasPrefix(r.Path, HttpServerBaseRoute+"/") || strings.HasPrefix(r.Path, HttpServerBaseRoute+"/mcp") || strings.HasSuffix(r.Path, "*") {
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
		"routes": out,
		"notes":  "{ws} is your workspace id. " + HttpServerBaseRoute + "/gateway/* proxies the gRPC services (pods, images, volumes, disks); their OpenAPI specs live in docs/openapi of the beta9 repo.",
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
	var body []byte
	if raw, ok := args["body"]; ok && raw != nil {
		body, _ = json.Marshal(raw)
	}
	url := strings.TrimSuffix(g.deploymentURL(d), "/") + "/" + strings.TrimPrefix(args.str("path"), "/")
	return g.serve(ctx, method, url, body, args.stringMap("headers"))
}
