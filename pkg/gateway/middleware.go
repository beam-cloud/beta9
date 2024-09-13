package gateway

import (
	"context"
	"errors"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

var (
	subdomainRegex *regexp.Regexp = regexp.MustCompile(
		`^` +
			`(?P<name>[a-zA-Z0-9-]+)-` +
			`(?P<hash>[a-zA-Z0-9]{7})` +
			`(?:-(?P<version>v[0-9]+|latest|[a-f0-9-]{36}))?` + // Version is optional. Will default to latest version.
			`$`,
	)
)

type SubdomainBackendRepo interface {
	GetStubByExternalId(ctx context.Context, externalId string, queryFilters ...types.QueryFilter) (*types.StubWithRelated, error)
	GetDeploymentByStubGroup(ctx context.Context, name string, version uint, stubGroup string) (*types.DeploymentWithRelated, error)
}

// subdomainMiddleware is a middleware that parses the subdomain from the request and routes it to the correct handler.
//
// The expected subdomain format is one of the following:
// - "<name>-<hash>-<version>.<baseDomain>"
// - "<name>-<hash>.<baseDomain>"
//
// The middleware extracts the subdomain parts and locates the appropriate stub based on the parsed subdomain,
// then routes the request to the corresponding handler. If the subdomain does not match the expected format,
// or if the stub cannot be found, the middleware will pass the request.
//
// Example subdomains that can be parsed:
// - myapp-7a7db8c.app.example.com         // No version, defaults to "latest"
// - myapp-7a7db8c-latest.app.example.com  // "latest" version
// - myapp-7a7db8c-v1.app.example.com      // "v1" as the version
// - myapp-7a7db8c-8f32e485-2b2e-4238-9878-490eb9b0a9d3.app.example.com // Stub ID as the version
func subdomainMiddleware(externalURL string, backendRepo SubdomainBackendRepo) echo.MiddlewareFunc {
	baseDomain := parseHostFromURL(externalURL)

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ctx echo.Context) error {
			subdomain := parseSubdomain(ctx.Request().Host, baseDomain)
			if subdomain == "" {
				return next(ctx)
			}

			fields, err := parseSubdomainFields(subdomainRegex, subdomain)
			if err != nil {
				return next(ctx)
			}

			stub, err := getStubForSubdomain(ctx.Request().Context(), backendRepo, fields)
			if err != nil {
				return next(ctx)
			}

			handlerPath := buildHandlerPath(stub, fields, ctx.Request().URL.Path)
			ctx.Echo().Router().Find(ctx.Request().Method, handlerPath, ctx)

			if handler := ctx.Handler(); handler != nil {
				ctx.Request().URL.Path = handlerPath
				return handler(ctx)
			}

			return next(ctx)
		}
	}
}

type SubdomainFields struct {
	Name      string
	Version   string
	StubGroup string
}

func parseSubdomain(host, baseDomain string) string {
	domain := "." + baseDomain
	if strings.HasSuffix(host, domain) {
		return strings.TrimSuffix(host, domain)
	}
	return ""
}

func parseSubdomainFields(re *regexp.Regexp, subdomain string) (*SubdomainFields, error) {
	match := re.FindStringSubmatch(subdomain)
	if len(match) == 0 {
		return nil, errors.New("subdomain does not match regex")
	}

	fields := make(map[string]string)
	for i, name := range re.SubexpNames() {
		if i != 0 && name != "" {
			fields[name] = match[i]
		}
	}

	if fields["name"] == "" || fields["hash"] == "" {
		return nil, errors.New("subdomain does not contain required fields")
	}

	return &SubdomainFields{
		Name:      fields["name"],
		StubGroup: fields["name"] + "-" + fields["hash"],
		Version:   fields["version"],
	}, nil
}

func getStubForSubdomain(ctx context.Context, repo SubdomainBackendRepo, fields *SubdomainFields) (*types.Stub, error) {
	if isValidExternalID(fields.Version) {
		stubRelated, err := repo.GetStubByExternalId(ctx, fields.Version)
		if err != nil {
			return nil, err
		}

		return &stubRelated.Stub, nil
	}

	version := parseVersion(fields.Version)
	deployment, err := repo.GetDeploymentByStubGroup(ctx, fields.Name, version, fields.StubGroup)
	if err != nil {
		return nil, err
	}

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
	} else if isValidExternalID(fields.Version) {
		pathSegments = append(pathSegments, "id", fields.Version)
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

func isValidExternalID(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil
}
