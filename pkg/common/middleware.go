package common

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

var (
	subdomainRegex = regexp.MustCompile(
		`^` +
			`(?:` +
			// Deployment form: something-abcdefg optional -vN or -latest
			`(?P<Subdomain>[a-zA-Z0-9-]+-[a-zA-Z0-9]{7})(?:-(?P<Version>v[0-9]+|latest))?` +
			// OR StubID form: 36-char (UUID)
			`|` +
			`(?P<StubID>[a-f0-9-]{36})` +
			`)` +
			// Optional "-8080"-style port suffix
			`(?:-(?P<Port>[0-9]+))?` +
			`$`,
	)

	ErrSubdomainDoesNotMatchRegex = errors.New("subdomain does not match regex")
)

type SubdomainFields struct {
	Name      string
	Version   uint
	StubId    string
	Subdomain string
	Port      uint32
}

type SubdomainBackendRepo interface {
	GetStubByExternalId(ctx context.Context, externalId string, queryFilters ...types.QueryFilter) (*types.StubWithRelated, error)
	GetDeploymentBySubdomain(ctx context.Context, subdomain string, version uint) (*types.DeploymentWithRelated, error)
}

func GetStubForSubdomain(ctx context.Context, repo SubdomainBackendRepo, fields *SubdomainFields) (*types.Stub, error) {
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

func ParseSubdomainFields(subdomain string) (*SubdomainFields, error) {
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

	// Parse the numeric port if provided
	var portVal uint32
	if portStr := fields["Port"]; portStr != "" {
		if p, err := strconv.ParseUint(portStr, 10, 32); err == nil {
			portVal = uint32(p)
		}
	}

	return &SubdomainFields{
		StubId:    fields["StubID"],
		Subdomain: fields["Subdomain"],
		Version:   ParseVersion(fields["Version"]),
		Port:      portVal,
	}, nil
}

// BuildHandlerPath builds the path for the handler based on the stub and subdomain fields.
// The extraPaths are appended to the end of the path.
// It supports /public, /id, and /name/version paths.
func BuildHandlerPath(stub *types.Stub, fields *SubdomainFields, extraPaths ...string) string {
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

	// If a port is specified, add it as a separate path segment
	if fields != nil && fields.Port > 0 {
		pathSegments = append(pathSegments, fmt.Sprintf("%d", fields.Port))
	}

	pathSegments = append(pathSegments, extraPaths...)
	return path.Join(pathSegments...)
}

// parseVersion parses the version string and returns the version number.
// If the version is unparsable, this will return 0.
func ParseVersion(version string) uint {
	if strings.HasPrefix(version, "v") {
		if i, err := strconv.Atoi(strings.TrimPrefix(version, "v")); err == nil {
			return uint(i)
		}
	}
	return 0
}
