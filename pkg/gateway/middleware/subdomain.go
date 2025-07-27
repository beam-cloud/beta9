package gateway

import (
	"fmt"
	"net"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/labstack/echo/v4"
)

const (
	handlerKeyTtl time.Duration = 5 * time.Minute
)

// Subdomain is middleware that routes requests based on the subdomain format.
//
// It extracts either a deployment's subdomain or a stub ID from the request's host.
// Based on the extracted value, it fetches the corresponding deployment or stub,
// builds the appropriate handler path, and invokes the corresponding handler.
//
// Supported Subdomain Formats:
// - {subdomain}.app.example.com                  // Routes to the "latest" version of a deployment
// - {subdomain}-latest.app.example.com           // Routes to the "latest" version of a deployment
// - {subdomain}-v{version}.app.example.com       // Routes to a specific version of a deployment
// - {stubId}.app.example.com                     // Routes to a specified stub, typically used for serves
func Subdomain(externalURL string, backendRepo common.SubdomainBackendRepo, redisClient *common.RedisClient) echo.MiddlewareFunc {
	baseDomain := ParseHostFromURL(externalURL)

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ctx echo.Context) error {
			subdomain := ParseSubdomain(ctx.Request().Host, baseDomain)
			if subdomain == "" {
				return next(ctx)
			}

			handlerKey := fmt.Sprintf("middleware:subdomain:%s:handler", subdomain)
			handlerPath := redisClient.Get(ctx.Request().Context(), handlerKey).Val()

			if handlerPath == "" {
				fields, err := common.ParseSubdomainFields(subdomain)
				if err != nil {
					return next(ctx)
				}

				stub, err := common.GetStubForSubdomain(ctx.Request().Context(), backendRepo, fields)
				if err != nil {
					return next(ctx)
				}

				handlerPath = common.BuildHandlerPath(stub, fields)
				if fields.Version > 0 || fields.StubId != "" {
					redisClient.Set(ctx.Request().Context(), handlerKey, handlerPath, handlerKeyTtl)
				}
			}

			originalPath := ctx.Request().URL.Path
			hasTrailingSlash := strings.HasSuffix(originalPath, "/") && originalPath != "/"

			handlerPathFull := path.Join("/", handlerPath, originalPath)
			if hasTrailingSlash && !strings.HasSuffix(handlerPathFull, "/") {
				handlerPathFull += "/"
			}

			ctx.Echo().Router().Find(ctx.Request().Method, handlerPathFull, ctx)
			if handler := ctx.Handler(); handler != nil {
				ctx.Request().URL.Path = handlerPathFull
				return handler(ctx)
			}

			return next(ctx)
		}
	}
}

func ParseSubdomain(host, baseDomain string) string {
	// Remove port if present
	h, _, err := net.SplitHostPort(host)
	if err != nil {
		h = host // If error, assume no port
	}

	// Normalize to lower case
	normalizedHost := strings.ToLower(h)
	normalizedBaseDomain := strings.ToLower(baseDomain)

	// If host equals baseDomain, return empty string
	if normalizedHost == normalizedBaseDomain {
		return ""
	}

	// Check if host ends with "." + baseDomain
	if strings.HasSuffix(normalizedHost, "."+normalizedBaseDomain) {
		return strings.TrimSuffix(normalizedHost, "."+normalizedBaseDomain)
	}

	return ""
}

func ParseHostFromURL(s string) string {
	if !strings.HasPrefix(s, "http://") && !strings.HasPrefix(s, "https://") {
		// Add a scheme to the URL so that it can be parsed correctly.
		s = "http://" + s
	}

	parsedURL, err := url.Parse(s)
	if err != nil {
		return ""
	}

	return parsedURL.Hostname()
}
