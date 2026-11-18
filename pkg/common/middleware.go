package common

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

var (
	subdomainRegex = regexp.MustCompile(
		`^` +
			`(?:` +
			// Container form: <stub kind>-<stub UUID>-<suffix> optional -port
			`(?P<ContainerID>(?:` + strings.Join(types.StubScopedContainerPrefixes(), "|") + `)-[a-f0-9-]{36}-[a-zA-Z0-9]{8})` +
			`|` +
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
	domainLabelRegex              = regexp.MustCompile(`^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})$`)
)

type SubdomainFields struct {
	Name                string
	Version             uint
	StubId              string
	ContainerId         string
	Subdomain           string
	Port                uint32
	WorkspaceExternalId string
}

type expectedWorkspaceExternalIDContextKey struct{}

// WithExpectedWorkspaceExternalID attaches the workspace external ID that owns
// the routed host (custom domain or deployment subdomain) to ctx.
func WithExpectedWorkspaceExternalID(ctx context.Context, workspaceExternalID string) context.Context {
	if ctx == nil || workspaceExternalID == "" {
		return ctx
	}
	return context.WithValue(ctx, expectedWorkspaceExternalIDContextKey{}, workspaceExternalID)
}

// ExpectedWorkspaceExternalID returns the owning workspace external ID attached
// by host routing middleware, if any.
func ExpectedWorkspaceExternalID(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	val, ok := ctx.Value(expectedWorkspaceExternalIDContextKey{}).(string)
	if !ok || val == "" {
		return "", false
	}
	return val, true
}

type SubdomainBackendRepo interface {
	GetStubByExternalId(ctx context.Context, externalId string, queryFilters ...types.QueryFilter) (*types.StubWithRelated, error)
	GetDeploymentBySubdomain(ctx context.Context, subdomain string, version uint) (*types.DeploymentWithRelated, error)
	GetDeploymentByCustomDomain(ctx context.Context, domain string) (*types.DeploymentWithRelated, *types.CustomDomain, error)
}

// NormalizeDomain is kept for callers in this package tree; see types.NormalizeDomain.
func NormalizeDomain(host string) string {
	return types.NormalizeDomain(host)
}

// ReservedDomains returns the platform-owned hostnames that custom domains must
// never be able to claim (gateway, gRPC and pod TCP external hosts, plus any
// operator-configured hosts such as load balancer hostnames).
func ReservedDomains(cfg types.AppConfig) []string {
	candidates := []string{
		cfg.GatewayService.Host,
		cfg.GatewayService.HTTP.ExternalHost,
		cfg.GatewayService.GRPC.ExternalHost,
		cfg.Abstractions.Pod.TCP.ExternalHost,
	}
	candidates = append(candidates, cfg.GatewayService.HTTP.ReservedHosts...)
	if parsed, err := url.Parse(cfg.GatewayService.HTTP.GetExternalURL()); err == nil {
		candidates = append(candidates, parsed.Hostname())
	}

	seen := map[string]struct{}{}
	reserved := make([]string, 0, len(candidates))
	for _, c := range candidates {
		n := NormalizeDomain(c)
		if n == "" {
			continue
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		reserved = append(reserved, n)
	}
	return reserved
}

// CustomDomainNames extracts the domain names from a list of custom domains.
func CustomDomainNames(domains []types.CustomDomain) []string {
	names := make([]string, 0, len(domains))
	for _, d := range domains {
		names = append(names, d.Domain)
	}
	return names
}

type customDomainLister interface {
	ListCustomDomains(ctx context.Context, workspaceId uint, deploymentName string) ([]types.CustomDomain, error)
}

// InvalidateDeploymentCustomDomains drops cached routing for every custom domain bound to a deployment.
func InvalidateDeploymentCustomDomains(ctx context.Context, repo customDomainLister, redisClient *RedisClient, workspaceId uint, deploymentName string) {
	if repo == nil || workspaceId == 0 || deploymentName == "" {
		return
	}
	domains, err := repo.ListCustomDomains(ctx, workspaceId, deploymentName)
	if err != nil || len(domains) == 0 {
		return
	}
	InvalidateCustomDomainCache(ctx, redisClient, CustomDomainNames(domains)...)
}

// customDomainMisses is an in-process negative cache fallback when Redis is
// unavailable so unknown hosts cannot hit the database on every request.
type missEntry struct {
	err     error
	expires time.Time
}

type missCache struct {
	mu      sync.Mutex
	entries map[string]missEntry
	ttl     time.Duration
	max     int
}

var customDomainMisses = &missCache{entries: map[string]missEntry{}, ttl: 30 * time.Second, max: 10000}

func (m *missCache) get(key string) (error, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.entries[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expires) {
		delete(m.entries, key)
		return nil, false
	}
	return entry.err, true
}

func (m *missCache) add(key string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) >= m.max {
		now := time.Now()
		for k, entry := range m.entries {
			if now.After(entry.expires) {
				delete(m.entries, k)
			}
		}
		if len(m.entries) >= m.max {
			m.entries = map[string]missEntry{}
		}
	}
	m.entries[key] = missEntry{err: err, expires: time.Now().Add(m.ttl)}
}

func (m *missCache) remove(keys ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.entries, k)
	}
}

func ValidateCustomDomainName(domain string, reservedDomains ...string) error {
	trimmed := strings.TrimSpace(domain)
	if trimmed == "" || strings.ContainsAny(trimmed, ":/\\@?# \t\n\r") {
		return types.ErrInvalidCustomDomain
	}

	normalized := NormalizeDomain(trimmed)
	if normalized == "" || len(normalized) > 253 {
		return types.ErrInvalidCustomDomain
	}
	if normalized == "localhost" || strings.HasSuffix(normalized, ".localhost") ||
		normalized == "cluster.local" || strings.HasSuffix(normalized, ".cluster.local") {
		return types.ErrInvalidCustomDomain
	}
	if net.ParseIP(normalized) != nil {
		return types.ErrInvalidCustomDomain
	}
	if !domainLabelRegex.MatchString(normalized) {
		return types.ErrInvalidCustomDomain
	}
	for _, reserved := range reservedDomains {
		base := NormalizeDomain(reserved)
		if base != "" && (normalized == base || strings.HasSuffix(normalized, "."+base)) {
			return types.ErrInvalidCustomDomain
		}
	}
	return nil
}

func CustomDomainHandlerCacheKey(domain string) string {
	return fmt.Sprintf("middleware:custom_domain:%s:handler", NormalizeDomain(domain))
}

func CustomDomainCheckCacheKey(domain string) string {
	return fmt.Sprintf("api:custom_domain:%s:check", NormalizeDomain(domain))
}

func InvalidateCustomDomainCache(ctx context.Context, redisClient *RedisClient, domains ...string) {
	if len(domains) == 0 {
		return
	}
	keys := make([]string, 0, len(domains)*2)
	normalizedDomains := make([]string, 0, len(domains))
	for _, domain := range domains {
		normalized := NormalizeDomain(domain)
		if normalized == "" {
			continue
		}
		normalizedDomains = append(normalizedDomains, normalized)
		keys = append(keys, CustomDomainHandlerCacheKey(normalized), CustomDomainCheckCacheKey(normalized))
	}
	if len(keys) == 0 {
		return
	}
	customDomainMisses.remove(normalizedDomains...)
	if redisClient == nil {
		return
	}
	_ = redisClient.Del(ctx, keys...).Err()
}

// LookupStubForCustomDomain resolves a custom domain directly from repo
// without consulting the process-local miss cache (for callers backed by Redis).
func LookupStubForCustomDomain(ctx context.Context, repo SubdomainBackendRepo, domain string) (*types.Stub, *SubdomainFields, error) {
	if repo == nil {
		return nil, nil, types.ErrCustomDomainNotFound
	}
	normalized := NormalizeDomain(domain)
	if normalized == "" {
		return nil, nil, types.ErrCustomDomainNotFound
	}
	deployment, customDomain, err := repo.GetDeploymentByCustomDomain(ctx, normalized)
	if err != nil {
		return nil, nil, err
	}
	if deployment == nil || customDomain == nil {
		if customDomain != nil {
			return nil, nil, types.ErrCustomDomainDeploymentInactive
		}
		return nil, nil, types.ErrCustomDomainNotFound
	}

	if !types.SupportsCustomDomain(deployment.Stub.Type) {
		return nil, nil, types.ErrCustomDomainDeploymentInactive
	}

	var port uint32
	if deployment.Stub.Type.Kind() == types.StubTypePod {
		port = customDomain.Port
		var stubConfig types.StubConfigV1
		if err := json.Unmarshal([]byte(deployment.Stub.Config), &stubConfig); err != nil {
			return nil, nil, types.ErrCustomDomainDeploymentInactive
		}
		if stubConfig.TCP || len(stubConfig.Ports) == 0 {
			return nil, nil, types.ErrCustomDomainDeploymentInactive
		}
		if port == 0 {
			port = stubConfig.Ports[0]
		} else if !slices.Contains(stubConfig.Ports, port) {
			return nil, nil, types.ErrCustomDomainDeploymentInactive
		}
		if port == 0 {
			return nil, nil, types.ErrCustomDomainDeploymentInactive
		}
	}

	fields := &SubdomainFields{
		Name:                deployment.Name,
		Version:             customDomain.Version,
		Subdomain:           deployment.Subdomain,
		Port:                port,
		WorkspaceExternalId: deployment.Workspace.ExternalId,
	}
	return &deployment.Stub, fields, nil
}

// GetStubForCustomDomain resolves a custom domain with an in-process negative
// cache fallback for callers without a healthy shared Redis cache.
func GetStubForCustomDomain(ctx context.Context, repo SubdomainBackendRepo, domain string) (*types.Stub, *SubdomainFields, error) {
	if repo == nil {
		return nil, nil, types.ErrCustomDomainNotFound
	}
	normalized := NormalizeDomain(domain)
	if cachedErr, ok := customDomainMisses.get(normalized); ok {
		return nil, nil, cachedErr
	}
	stub, fields, err := LookupStubForCustomDomain(ctx, repo, normalized)
	if err != nil {
		if errors.Is(err, types.ErrCustomDomainNotFound) {
			customDomainMisses.add(normalized, types.ErrCustomDomainNotFound)
		} else if errors.Is(err, types.ErrCustomDomainDeploymentInactive) {
			customDomainMisses.add(normalized, types.ErrCustomDomainDeploymentInactive)
		}
		return nil, nil, err
	}
	customDomainMisses.remove(normalized)
	return stub, fields, nil
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

		fields.WorkspaceExternalId = stubRelated.Workspace.ExternalId
		return &stubRelated.Stub, nil
	}

	deployment, err := repo.GetDeploymentBySubdomain(ctx, fields.Subdomain, fields.Version)
	if err != nil {
		return nil, err
	}

	fields.Name = deployment.Name
	fields.WorkspaceExternalId = deployment.Workspace.ExternalId
	return &deployment.Stub, nil
}

func ParseSubdomain(subdomain string, baseDomain string) (*SubdomainFields, error) {
	if strings.HasSuffix(subdomain, "."+baseDomain) {
		subdomain = strings.TrimSuffix(subdomain, "."+baseDomain)
	} else if strings.HasSuffix(subdomain, baseDomain) {
		subdomain = strings.TrimSuffix(subdomain, baseDomain)
	}

	fields, err := ParseSubdomainFields(subdomain)
	if err != nil {
		return nil, err
	}

	return fields, nil
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

	stubID := fields["StubID"]
	containerID := fields["ContainerID"]
	if containerID != "" {
		if extracted, ok := ExtractStubIdFromStubScopedContainerId(containerID); ok {
			stubID = extracted
		}
	}

	return &SubdomainFields{
		StubId:      stubID,
		ContainerId: containerID,
		Subdomain:   fields["Subdomain"],
		Version:     ParseVersion(fields["Version"]),
		Port:        portVal,
	}, nil
}

// BuildHandlerPath builds the path for the handler based on the stub and subdomain fields.
// The extraPaths are appended to the end of the path.
// It supports /public, /id, and /name/version paths.
func BuildHandlerPath(stub *types.Stub, fields *SubdomainFields, extraPaths ...string) string {
	pathSegments := []string{"/" + stub.Type.RouteKind()}

	if fields != nil && fields.ContainerId != "" {
		pathSegments = append(pathSegments, "container", fields.StubId, fields.ContainerId)
	} else if stubConfig, err := stub.UnmarshalConfig(); err == nil && !stubConfig.Authorized {
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
