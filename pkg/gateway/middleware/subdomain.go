package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
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
			`(?P<Subdomain>[a-zA-Z0-9-]+-[a-zA-Z0-9]{7})(?:-(?P<Version>v[0-9]+|latest))?` + // Matches Subdomain-Version. Version is optional.
			`|` +
			`(?P<StubID>[a-f0-9-]{36})` + // Matches Stub ID
			`)$`,
	)

	ErrSubdomainDoesNotMatchRegex = errors.New("subdomain does not match regex")
)

type SubdomainBackendRepo interface {
	GetStubByExternalId(ctx context.Context, externalId string, queryFilters ...types.QueryFilter) (*types.StubWithRelated, error)
	GetDeploymentBySubdomain(ctx context.Context, subdomain string, version uint) (*types.DeploymentWithRelated, error)
}

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
func Subdomain(externalURL string, backendRepo SubdomainBackendRepo, redisClient *common.RedisClient) echo.MiddlewareFunc {
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
				fields, err := parseSubdomainFields(subdomain)
				if err != nil {
					return next(ctx)
				}

				stub, err := getStubForSubdomain(ctx.Request().Context(), backendRepo, fields)
				if err != nil {
					return next(ctx)
				}

				handlerPath = buildHandlerPath(stub, fields)
				if fields.Version > 0 || fields.StubId != "" {
					redisClient.Set(ctx.Request().Context(), handlerKey, handlerPath, handlerKeyTtl)
				}
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
	Version   uint
	StubId    string
	Subdomain string
}

func parseSubdomain(host, baseDomain string) string {
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
		Subdomain: fields["Subdomain"],
		Version:   parseVersion(fields["Version"]),
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

	deployment, err := repo.GetDeploymentBySubdomain(ctx, fields.Subdomain, fields.Version)
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
		pathSegments = append(pathSegments, fields.Name)

		if fields.Version > 0 {
			pathSegments = append(pathSegments, fmt.Sprintf("v%d", fields.Version))
		} else {
			pathSegments = append(pathSegments, "latest")
		}
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
