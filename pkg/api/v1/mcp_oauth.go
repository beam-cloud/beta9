package apiv1

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/labstack/echo/v4"
)

// Introspection is bounded and never follows redirects to another credential recipient.
var mcpOAuthClient = &http.Client{
	Timeout:       10 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

func (g *MCPGroup) oauthResource() string {
	if value := g.config.GatewayService.MCPOAuth.Resource; value != "" {
		return value
	}
	return strings.TrimRight(g.config.GatewayService.HTTP.GetExternalURL(), "/") + "/api/v1/mcp"
}

func (g *MCPGroup) oauthEnabled() bool {
	o := g.config.GatewayService.MCPOAuth
	return o.Issuer != "" && o.IntrospectionURL != "" && o.IntrospectionSecret != ""
}

func (g *MCPGroup) oauthMetadata(c echo.Context) error {
	if !g.oauthEnabled() {
		return echo.NewHTTPError(http.StatusNotFound, "OAuth is not configured")
	}
	c.Response().Header().Set("Cache-Control", "public, max-age=300")
	return c.JSON(http.StatusOK, map[string]any{
		"resource": g.oauthResource(), "authorization_servers": []string{g.config.GatewayService.MCPOAuth.Issuer},
		"scopes_supported":         []string{"beam:read", "beam:write", "beam:exec", "beam:credentials"},
		"bearer_methods_supported": []string{"header"},
	})
}

func (g *MCPGroup) oauthChallenge(c echo.Context, rejected bool) error {
	challenge := "Bearer"
	if rejected {
		challenge += ` error="invalid_token"`
	}
	if g.oauthEnabled() {
		resource, _ := url.Parse(g.oauthResource())
		resource.Path = "/.well-known/oauth-protected-resource" + resource.Path
		if rejected {
			challenge += ","
		}
		challenge += fmt.Sprintf(` resource_metadata="%s"`, resource.String())
	}
	c.Response().Header().Set("WWW-Authenticate", challenge)
	c.Response().Header().Set("Cache-Control", "no-store")
	return echo.NewHTTPError(http.StatusUnauthorized, "Connect with OAuth or a valid workspace token")
}

func (g *MCPGroup) authorize(next echo.HandlerFunc) echo.HandlerFunc {
	standard := auth.AuthMiddleware(g.backendRepo, g.workspaceRepo)
	return func(c echo.Context) error {
		header := c.Request().Header.Get("Authorization")
		if !strings.HasPrefix(header, "Bearer ") || strings.TrimPrefix(header, "Bearer ") == "" {
			return g.oauthChallenge(c, header != "")
		}
		token := strings.TrimPrefix(header, "Bearer ")
		var grant *auth.OAuthGrant
		if strings.HasPrefix(token, "bmo_") {
			if !g.oauthEnabled() {
				return g.oauthChallenge(c, true)
			}
			o := g.config.GatewayService.MCPOAuth
			body := url.Values{"token": {token}, "resource": {g.oauthResource()}}
			req, err := http.NewRequestWithContext(c.Request().Context(), http.MethodPost, o.IntrospectionURL, strings.NewReader(body.Encode()))
			if err != nil {
				return echo.NewHTTPError(http.StatusServiceUnavailable, "OAuth validation is unavailable")
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Authorization", "Bearer "+o.IntrospectionSecret)
			response, err := mcpOAuthClient.Do(req)
			if err != nil {
				return echo.NewHTTPError(http.StatusServiceUnavailable, "OAuth validation is unavailable; retry")
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				return echo.NewHTTPError(http.StatusServiceUnavailable, "OAuth validation is unavailable; retry")
			}
			var result struct {
				Active         bool   `json:"active"`
				Issuer         string `json:"iss"`
				Resource       string `json:"resource"`
				WorkspaceID    string `json:"workspace_id"`
				WorkspaceToken string `json:"workspace_token"`
				ClientID       string `json:"client_id"`
				Subject        string `json:"sub"`
				Scope          string `json:"scope"`
			}
			if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&result); err != nil {
				return echo.NewHTTPError(http.StatusServiceUnavailable, "Invalid OAuth validation response")
			}
			if !result.Active || result.Issuer != o.Issuer || result.Resource != g.oauthResource() || result.WorkspaceID == "" || result.WorkspaceToken == "" || result.ClientID == "" || result.Subject == "" {
				return g.oauthChallenge(c, true)
			}
			grant = &auth.OAuthGrant{ClientID: result.ClientID, Subject: result.Subject, Resource: result.Resource, Scopes: strings.Fields(result.Scope)}
			// Resolve the private credential using the same workspace-token policy.
			// OAuth credentials are deliberately accepted only on the MCP endpoint.
			cloned := c.Request().Clone(c.Request().Context())
			cloned.Header = c.Request().Header.Clone()
			cloned.Header.Set("Authorization", "Bearer "+result.WorkspaceToken)
			c.SetRequest(cloned)
			err = standard(func(cc echo.Context) error {
				info, ok := cc.(*auth.HttpAuthContext)
				if !ok || info.AuthInfo.Workspace.ExternalId != result.WorkspaceID {
					return g.oauthChallenge(cc, true)
				}
				info.AuthInfo.OAuth = grant
				return next(cc)
			})(c)
			if e, ok := err.(*echo.HTTPError); ok && e.Code == http.StatusUnauthorized {
				return g.oauthChallenge(c, true)
			}
			return err
		}
		err := standard(auth.WithAuth(next))(c)
		if e, ok := err.(*echo.HTTPError); ok && e.Code == http.StatusUnauthorized {
			return g.oauthChallenge(c, true)
		}
		return err
	}
}
