package middleware

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
)

const (
	handlerKeyTtl time.Duration = 5 * time.Minute

	// Unknown custom domains are cached briefly so that traffic for hosts we
	// don't own cannot hit the database on every request.
	negativeHandlerValue                = "-"
	inactiveHandlerValue                = "!"
	negativeHandlerKeyTtl time.Duration = 30 * time.Second

	// Version 0 follows the latest deployment; routing for it is invalidated on
	// every deploy/start/stop/delete, and this short TTL bounds any missed event.
	latestHandlerKeyTtl time.Duration = time.Minute
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
	return SubdomainWithReservedHosts(externalURL, nil, backendRepo, redisClient)
}

func encodeCachedHandler(handlerPath, workspaceExternalID string) string {
	return workspaceExternalID + "|" + handlerPath
}

func decodeCachedHandler(raw string) (handlerPath, workspaceExternalID string, ok bool) {
	if raw == "" || raw == negativeHandlerValue || raw == inactiveHandlerValue {
		return "", "", false
	}
	// Legacy cache entries stored only the path (starting with "/"); treat them
	// as a cache miss so they are refreshed with workspace ownership metadata.
	if strings.HasPrefix(raw, "/") {
		return "", "", false
	}
	wsID, p, found := strings.Cut(raw, "|")
	if !found || p == "" {
		return "", "", false
	}
	return p, wsID, true
}

// SubdomainWithReservedHosts behaves like Subdomain while excluding reserved
// platform hosts (gRPC, Pod TCP, internal hostnames) from custom domain lookup.
func SubdomainWithReservedHosts(externalURL string, reservedHosts []string, backendRepo common.SubdomainBackendRepo, redisClient *common.RedisClient) echo.MiddlewareFunc {
	baseDomain := ParseHostFromURL(externalURL)

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ctx echo.Context) error {
			var handlerPath string
			var expectedWorkspaceID string
			var isCustomDomain bool
			host := ctx.Request().Host

			if subdomain := ParseSubdomain(host, baseDomain); subdomain != "" {
				handlerKey := fmt.Sprintf("middleware:subdomain:%s:handler", subdomain)
				if redisClient != nil {
					if raw, err := redisClient.Get(ctx.Request().Context(), handlerKey).Result(); err == nil {
						if p, wsID, ok := decodeCachedHandler(raw); ok {
							handlerPath = p
							expectedWorkspaceID = wsID
						}
					}
				}

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
					expectedWorkspaceID = fields.WorkspaceExternalId
					if redisClient != nil && (fields.Version > 0 || fields.StubId != "") {
						redisClient.Set(ctx.Request().Context(), handlerKey, encodeCachedHandler(handlerPath, expectedWorkspaceID), handlerKeyTtl)
					}
				}
			} else if IsCandidateCustomDomain(host, baseDomain, reservedHosts...) {
				isCustomDomain = true
				normalizedHost := common.NormalizeDomain(host)
				handlerKey := common.CustomDomainHandlerCacheKey(normalizedHost)
				redisHealthy := false
				if redisClient != nil {
					val, err := redisClient.Get(ctx.Request().Context(), handlerKey).Result()
					if err == nil {
						redisHealthy = true
						switch val {
						case negativeHandlerValue:
							return next(ctx)
						case inactiveHandlerValue:
							return echo.NewHTTPError(http.StatusNotFound)
						default:
							if p, wsID, ok := decodeCachedHandler(val); ok {
								handlerPath = p
								expectedWorkspaceID = wsID
							}
						}
					} else if errors.Is(err, redis.Nil) {
						redisHealthy = true
					}
				}

				if handlerPath == "" {
					var (
						stub   *types.Stub
						fields *common.SubdomainFields
						err    error
					)
					if redisHealthy {
						stub, fields, err = common.LookupStubForCustomDomain(ctx.Request().Context(), backendRepo, normalizedHost)
					} else {
						stub, fields, err = common.GetStubForCustomDomain(ctx.Request().Context(), backendRepo, normalizedHost)
					}
					if err != nil || stub == nil {
						if errors.Is(err, types.ErrCustomDomainDeploymentInactive) {
							if redisClient != nil {
								redisClient.Set(ctx.Request().Context(), handlerKey, inactiveHandlerValue, negativeHandlerKeyTtl)
							}
							return echo.NewHTTPError(http.StatusNotFound)
						}
						if redisClient != nil && errors.Is(err, types.ErrCustomDomainNotFound) {
							redisClient.Set(ctx.Request().Context(), handlerKey, negativeHandlerValue, negativeHandlerKeyTtl)
						}
						return next(ctx)
					}

					handlerPath = common.BuildHandlerPath(stub, fields)
					if handlerPath == "" {
						return echo.NewHTTPError(http.StatusNotFound)
					}
					expectedWorkspaceID = fields.WorkspaceExternalId
					if redisClient != nil {
						ttl := handlerKeyTtl
						if fields.Version == 0 {
							ttl = latestHandlerKeyTtl
						}
						redisClient.Set(ctx.Request().Context(), handlerKey, encodeCachedHandler(handlerPath, expectedWorkspaceID), ttl)
					}
				}
			} else {
				return next(ctx)
			}

			originalPath := ctx.Request().URL.Path
			hasTrailingSlash := strings.HasSuffix(originalPath, "/") && originalPath != "/"
			// Clean the incoming path first so dot-segments cannot traverse out of handlerPath.
			cleanedOriginal := path.Clean("/" + originalPath)

			handlerPathFull := path.Join("/", handlerPath, cleanedOriginal)
			if cleanedOriginal != "/" && hasTrailingSlash && !strings.HasSuffix(handlerPathFull, "/") {
				handlerPathFull += "/"
			}

			prevHandler := ctx.Handler()
			origReq := ctx.Request()
			if expectedWorkspaceID != "" {
				ctx.SetRequest(origReq.WithContext(common.WithExpectedWorkspaceExternalID(origReq.Context(), expectedWorkspaceID)))
			}
			ctx.SetHandler(nil)
			ctx.Echo().Router().Find(ctx.Request().Method, handlerPathFull, ctx)
			if handler := ctx.Handler(); handler != nil {
				ctx.Request().URL.Path = handlerPathFull
				return handler(ctx)
			}
			if isCustomDomain {
				return echo.NewHTTPError(http.StatusNotFound)
			}
			ctx.SetRequest(origReq)
			ctx.SetHandler(prevHandler)
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

func IsCandidateCustomDomain(host, baseDomain string, reserved ...string) bool {
	normalizedHost := common.NormalizeDomain(host)
	normalizedBase := common.NormalizeDomain(baseDomain)
	if normalizedHost == "" || normalizedHost == normalizedBase {
		return false
	}
	if normalizedHost == "localhost" || strings.HasSuffix(normalizedHost, ".localhost") {
		return false
	}
	if strings.HasSuffix(normalizedHost, ".cluster.local") {
		return false
	}
	if net.ParseIP(normalizedHost) != nil {
		return false
	}
	if !strings.Contains(normalizedHost, ".") {
		return false
	}
	if normalizedBase != "" && strings.HasSuffix(normalizedHost, "."+normalizedBase) {
		return false
	}
	for _, entry := range reserved {
		r := common.NormalizeDomain(entry)
		if r != "" && (normalizedHost == r || strings.HasSuffix(normalizedHost, "."+r)) {
			return false
		}
	}
	return true
}
