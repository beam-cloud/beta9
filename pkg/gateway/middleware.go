package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

const (
	handlerKeyTtl time.Duration = 5 * time.Minute
)

var (
	subdomainRegex *regexp.Regexp = regexp.MustCompile(
		`^` +
			`(?:` +
			`(?P<StubGroup>[a-zA-Z0-9-]+-[a-zA-Z0-9]{7})(?:-(?P<Version>v[0-9]+|latest))?` + // Matches StubGroup-Version. Version is optional.
			`|` +
			`(?P<StubID>[a-f0-9-]{36})` + // Matches Stub ID
			`)$`,
	)

	ErrSubdomainDoesNotMatchRegex = errors.New("subdomain does not match regex")
)

type SubdomainBackendRepo interface {
	GetStubByExternalId(ctx context.Context, externalId string, queryFilters ...types.QueryFilter) (*types.StubWithRelated, error)
	GetDeploymentByStubGroup(ctx context.Context, version uint, stubGroup string) (*types.DeploymentWithRelated, error)
}

// subdomainMiddleware is a middleware that parses the subdomain from the request and routes it to the correct handler.
//
// The expected subdomain format is one of the following:
// - <stubGroup>-<version>.<baseDomain>
// - <stubId>.<baseDomain>
//
// The middleware extracts the stub group and optionally the version, or the stub id from the subdomain. It then fetches
// the corresponding stub or deployment, builds a path, and then routes the request to the corresponding handler. If the
// subdomain does not match the expected format, or if the stub cannot be found, the middleware will pass the request.
//
// Example subdomains that can be parsed:
// - myapp-7a7db8c.app.example.com         // No version, defaults to "latest"
// - myapp-7a7db8c-latest.app.example.com  // "latest" version
// - myapp-7a7db8c-v1.app.example.com      // "v1" as the version
// - 8f32e485-2b2e-4238-9878-490eb9b0a9d3.app.example.com // Stub ID as the version
func subdomainMiddleware(externalURL string, backendRepo SubdomainBackendRepo, redisClient *common.RedisClient) echo.MiddlewareFunc {
	baseDomain := parseHostFromURL(externalURL)

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ctx echo.Context) error {
			subdomain := parseSubdomain(ctx.Request().Host, baseDomain)
			if subdomain == "" {
				return next(ctx)
			}

			handlerKey := fmt.Sprintf("middleware:subdomain:%s:handler", subdomain)
			handlerPath := redisClient.Get(ctx.Request().Context(), handlerKey).Val()

			if handlerPath == "" {
				fmt.Println("Looking up handler path in database")
				fields, err := parseSubdomainFields(subdomain)
				if err != nil {
					return next(ctx)
				}

				stub, err := getStubForSubdomain(ctx.Request().Context(), backendRepo, fields)
				if err != nil {
					return next(ctx)
				}

				handlerPath = buildHandlerPath(stub, fields)
				redisClient.Set(ctx.Request().Context(), handlerKey, handlerPath, handlerKeyTtl)
			}

			handlerPathFull := path.Join("/", handlerPath, ctx.Request().URL.Path)
			ctx.Echo().Router().Find(ctx.Request().Method, handlerPathFull, ctx)

			if handler := ctx.Handler(); handler != nil {
				ctx.Request().URL.Path = handlerPathFull
				return handler(ctx)
			}

			return next(ctx)
		}
	}
}

type SubdomainFields struct {
	Name      string
	Version   string
	StubId    string
	StubGroup string
}

func parseSubdomain(host, baseDomain string) string {
	domain := "." + baseDomain
	if strings.HasSuffix(host, domain) {
		return strings.TrimSuffix(host, domain)
	}
	return ""
}

func parseSubdomainFields(subdomain string) (*SubdomainFields, error) {
	match := subdomainRegex.FindStringSubmatch(subdomain)
	if len(match) == 0 {
		return nil, ErrSubdomainDoesNotMatchRegex
	}

	fields := make(map[string]string)
	for i, name := range subdomainRegex.SubexpNames() {
		if i != 0 && name != "" {
			fields[name] = match[i]
		}
	}

	return &SubdomainFields{
		StubId:    fields["StubID"],
		StubGroup: fields["StubGroup"],
		Version:   fields["Version"],
	}, nil
}

func getStubForSubdomain(ctx context.Context, repo SubdomainBackendRepo, fields *SubdomainFields) (*types.Stub, error) {
	if fields.StubId != "" {
		stubRelated, err := repo.GetStubByExternalId(ctx, fields.StubId)
		if err != nil {
			return nil, err
		}
		if stubRelated == nil {
			return nil, errors.New("stub not found")
		}

		return &stubRelated.Stub, nil
	}

	version := parseVersion(fields.Version)
	deployment, err := repo.GetDeploymentByStubGroup(ctx, version, fields.StubGroup)
	if err != nil {
		return nil, err
	}

	fields.Name = deployment.Name

	return &deployment.Stub, nil
}

// parseVersion parses the version string and returns the version number.
// If the version is unparsable, this will return 0.
func parseVersion(version string) uint {
	if strings.HasPrefix(version, "v") {
		if i, err := strconv.Atoi(strings.TrimPrefix(version, "v")); err == nil {
			return uint(i)
		}
	}
	return 0
}

// buildHandlerPath builds the path for the handler based on the stub and subdomain fields.
// The extraPaths are appended to the end of the path.
// It supports /public, /id, and /name/version paths.
func buildHandlerPath(stub *types.Stub, fields *SubdomainFields, extraPaths ...string) string {
	pathSegments := []string{"/" + stub.Type.Kind()}

	if stubConfig, err := stub.UnmarshalConfig(); err == nil && !stubConfig.Authorized {
		pathSegments = append(pathSegments, "public", stub.ExternalId)
	} else if fields.StubId != "" {
		pathSegments = append(pathSegments, "id", fields.StubId)
	} else {
		pathSegments = append(pathSegments, fields.Name, fields.Version)
	}

	pathSegments = append(pathSegments, extraPaths...)
	return path.Join(pathSegments...)
}

func parseHostFromURL(s string) string {
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
