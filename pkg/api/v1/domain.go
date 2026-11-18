package apiv1

import (
	"cmp"
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/types/serializer"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog/log"
)

type domainDNSResolver interface {
	LookupTXT(ctx context.Context, name string) ([]string, error)
	LookupCNAME(ctx context.Context, host string) (string, error)
}

var domainStubTypeAliases = map[string]string{
	"endpoint":            types.StubTypeEndpointDeployment,
	"endpoint/deployment": types.StubTypeEndpointDeployment,
	"http":                types.StubTypeEndpointDeployment,
	"asgi":                types.StubTypeASGIDeployment,
	"asgi/deployment":     types.StubTypeASGIDeployment,
	"pod":                 types.StubTypePodDeployment,
	"pod/deployment":      types.StubTypePodDeployment,
}

var supportedCustomDomainStubTypes = []string{
	types.StubTypeEndpointDeployment,
	types.StubTypeASGIDeployment,
	types.StubTypePodDeployment,
}

var unsupportedCustomDomainStubTypes = []string{
	types.StubTypeFunctionDeployment,
	types.StubTypeTaskQueueDeployment,
	types.StubTypeScheduledJobDeployment,
	types.StubTypeBotDeployment,
	types.StubTypeManagedEndpointDeployment,
}

const (
	domainCheckAllowedValue     = "1"
	domainCheckDeniedValue      = "0"
	domainCheckPositiveCacheTTL = 5 * time.Minute
	domainCheckNegativeCacheTTL = 30 * time.Second
)

type DomainGroup struct {
	routerGroup *echo.Group
	backendRepo repository.BackendRepository
	redisClient *common.RedisClient
	config      types.AppConfig
	resolver    domainDNSResolver
}

func extractDomainCheckToken(ctx echo.Context) string {
	if headerToken := strings.TrimSpace(ctx.Request().Header.Get("X-Domain-Check-Token")); headerToken != "" {
		return headerToken
	}
	if authHeader := ctx.Request().Header.Get("Authorization"); strings.HasPrefix(authHeader, "Bearer ") {
		if bearerToken := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer ")); bearerToken != "" {
			return bearerToken
		}
	}
	return strings.TrimSpace(ctx.QueryParam("token"))
}

func domainCheckTokenMatches(ctx echo.Context, expected string) bool {
	expected = strings.TrimSpace(expected)
	if expected == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(extractDomainCheckToken(ctx)), []byte(expected)) == 1
}

func rateLimitClientKey(ctx echo.Context) string {
	// Ignore spoofable X-Forwarded-For / X-Real-IP headers unless Echo has an
	// explicit IPExtractor configured for trusted proxies.
	if e := ctx.Echo(); e != nil && e.IPExtractor != nil {
		if ip := strings.TrimSpace(e.IPExtractor(ctx.Request())); ip != "" {
			return ip
		}
	}
	remoteAddr := strings.TrimSpace(ctx.Request().RemoteAddr)
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil && host != "" {
		return host
	}
	if remoteAddr != "" {
		return remoteAddr
	}
	return "unknown"
}

const domainCheckClusterWindow = time.Minute

// requests per client per window, across all replicas (var so tests can lower it)
var domainCheckClusterLimit int64 = 1200

// clusterRateLimit caps /check per client across gateway replicas using Redis.
// The in-memory limiter is per replica; this one is shared. It fails open when
// Redis is unavailable so a Redis outage cannot block certificate issuance.
func (g *DomainGroup) clusterRateLimit(next echo.HandlerFunc) echo.HandlerFunc {
	return func(ctx echo.Context) error {
		if g.redisClient == nil || domainCheckTokenMatches(ctx, g.config.GatewayService.HTTP.DomainCheckToken) {
			return next(ctx)
		}
		key := fmt.Sprintf("api:custom_domain:check:rl:%s", rateLimitClientKey(ctx))
		reqCtx := ctx.Request().Context()
		count, err := g.redisClient.Incr(reqCtx, key).Result()
		if err != nil {
			return next(ctx)
		}
		if count == 1 {
			_ = g.redisClient.Expire(reqCtx, key, domainCheckClusterWindow).Err()
		}
		if count > domainCheckClusterLimit {
			return ctx.NoContent(http.StatusTooManyRequests)
		}
		return next(ctx)
	}
}

func NewDomainGroup(
	g *echo.Group,
	authMiddleware echo.MiddlewareFunc,
	backendRepo repository.BackendRepository,
	redisClient *common.RedisClient,
	config types.AppConfig,
) *DomainGroup {
	if strings.TrimSpace(config.GatewayService.HTTP.DomainCheckToken) == "" {
		if config.GatewayService.HTTP.DomainCheckAllowUnauthenticated {
			log.Warn().Msg("gateway.http.domainCheckToken is empty and domainCheckAllowUnauthenticated is set; /api/v1/domain/check is unauthenticated")
		} else {
			log.Warn().Msg("gateway.http.domainCheckToken is empty; /api/v1/domain/check is disabled and on-demand TLS will be refused")
		}
	}

	group := &DomainGroup{
		routerGroup: g,
		backendRepo: backendRepo,
		redisClient: redisClient,
		config:      config,
		resolver:    net.DefaultResolver,
	}

	g.GET("/check", group.Check, middleware.RateLimiterWithConfig(middleware.RateLimiterConfig{
		Skipper: func(ctx echo.Context) bool {
			return domainCheckTokenMatches(ctx, group.config.GatewayService.HTTP.DomainCheckToken)
		},
		IdentifierExtractor: func(ctx echo.Context) (string, error) {
			return rateLimitClientKey(ctx), nil
		},
		Store: middleware.NewRateLimiterMemoryStoreWithConfig(
			middleware.RateLimiterMemoryStoreConfig{Rate: 20, Burst: 40, ExpiresIn: time.Minute},
		),
	}), group.clusterRateLimit)
	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.List), authMiddleware)
	g.POST("/:workspaceId", auth.WithStrictWorkspaceAuth(group.Create), authMiddleware)
	g.GET("/:workspaceId/:domainId", auth.WithWorkspaceAuth(group.Retrieve), authMiddleware)
	g.POST("/:workspaceId/:domainId/verify", auth.WithStrictWorkspaceAuth(group.Verify), authMiddleware)
	g.DELETE("/:workspaceId/:domainId", auth.WithStrictWorkspaceAuth(group.Delete), authMiddleware)

	return group
}

type CreateCustomDomainRequest struct {
	Domain         string `json:"domain"`
	DeploymentName string `json:"deployment_name"`
	DeploymentID   string `json:"deployment_id"`
	StubType       string `json:"stub_type"`
	Version        uint   `json:"version"`
	Port           uint32 `json:"port"`
}

func (g *DomainGroup) baseDomain() string {
	if host := strings.TrimSpace(g.config.GatewayService.HTTP.ExternalHost); host != "" {
		return common.NormalizeDomain(host)
	}
	if parsed, err := url.Parse(g.config.GatewayService.HTTP.GetExternalURL()); err == nil {
		return common.NormalizeDomain(parsed.Hostname())
	}
	return ""
}

func (g *DomainGroup) enrichCustomDomain(cd *types.CustomDomain) {
	if cd == nil {
		return
	}
	baseDomain := g.baseDomain()
	cd.TXTRecordName = "_beta9-challenge." + cd.Domain
	// Only a deployment-specific target is accepted; the bare base domain is
	// shared by everyone, so pointing at it proves nothing.
	if cd.Subdomain != "" && baseDomain != "" {
		cd.CNAMETarget = fmt.Sprintf("%s.%s", cd.Subdomain, baseDomain)
	}
	cd.URL = common.BuildCustomDomainURL(g.config.GatewayService.HTTP.GetExternalURL(), cd.Domain)
}

func normalizeDomainStubType(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if mapped, ok := domainStubTypeAliases[strings.ToLower(trimmed)]; ok {
		return mapped
	}
	return trimmed
}

func (g *DomainGroup) lookupDeploymentByExactName(ctx context.Context, workspaceId uint, name, stubType string, version uint) (*types.DeploymentWithRelated, error) {
	var (
		dep *types.DeploymentWithRelated
		err error
	)
	if version > 0 {
		dep, err = g.backendRepo.GetDeploymentByNameAndVersion(ctx, workspaceId, name, version, stubType)
	} else {
		dep, err = g.backendRepo.GetLatestDeploymentByName(ctx, workspaceId, name, stubType, true)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return dep, nil
}

func (g *DomainGroup) resolveDeploymentTarget(ctx context.Context, workspaceId uint, req *CreateCustomDomainRequest) (*types.DeploymentWithRelated, error) {
	stubType := normalizeDomainStubType(req.StubType)

	if deploymentID := strings.TrimSpace(req.DeploymentID); deploymentID != "" {
		dep, err := g.backendRepo.GetDeploymentByExternalId(ctx, workspaceId, deploymentID)
		if err != nil || dep == nil {
			return nil, errors.New("Deployment not found")
		}
		depStubType := cmp.Or(dep.StubType, string(dep.Stub.Type))
		reqName := strings.TrimSpace(req.DeploymentName)
		if reqName != "" && reqName != dep.Name {
			return nil, fmt.Errorf("deployment_name %q does not match deployment_id %q", reqName, deploymentID)
		}
		if stubType != "" && stubType != depStubType {
			return nil, fmt.Errorf("stub_type %q does not match deployment_id %q", stubType, deploymentID)
		}
		if req.Version > 0 && req.Version != dep.Version {
			return nil, fmt.Errorf("version %d does not match deployment_id %q", req.Version, deploymentID)
		}
		req.DeploymentName = dep.Name
		req.StubType = depStubType
		req.Version = dep.Version
		return dep, nil
	}

	deploymentName := strings.TrimSpace(req.DeploymentName)
	if deploymentName == "" {
		return nil, errors.New("deployment_name or deployment_id is required")
	}
	req.DeploymentName = deploymentName

	if stubType != "" {
		if !types.SupportsCustomDomain(types.StubType(stubType)) {
			return nil, errors.New("custom domains are only supported for endpoint, asgi, and HTTP pod deployments")
		}
		dep, err := g.lookupDeploymentByExactName(ctx, workspaceId, deploymentName, stubType, req.Version)
		if err != nil || dep == nil {
			return nil, errors.New("Deployment not found")
		}
		req.StubType = stubType
		return dep, nil
	}

	var matches []*types.DeploymentWithRelated
	for _, candidateType := range supportedCustomDomainStubTypes {
		dep, err := g.lookupDeploymentByExactName(ctx, workspaceId, deploymentName, candidateType, req.Version)
		if err != nil {
			return nil, errors.New("Deployment not found")
		}
		if dep != nil {
			matches = append(matches, dep)
		}
	}

	if len(matches) > 1 {
		return nil, fmt.Errorf("multiple deployment types exist for %q; specify stub_type", deploymentName)
	}
	if len(matches) == 1 {
		dep := matches[0]
		req.StubType = cmp.Or(dep.StubType, string(dep.Stub.Type))
		return dep, nil
	}

	for _, candidateType := range unsupportedCustomDomainStubTypes {
		if dep, err := g.lookupDeploymentByExactName(ctx, workspaceId, deploymentName, candidateType, req.Version); err == nil && dep != nil {
			return nil, errors.New("custom domains are only supported for endpoint, asgi, and HTTP pod deployments")
		}
	}
	return nil, errors.New("Deployment not found")
}

// Check is an On-Demand TLS verification endpoint for reverse proxies (e.g. Caddy).
func (g *DomainGroup) Check(ctx echo.Context) error {
	if token := strings.TrimSpace(g.config.GatewayService.HTTP.DomainCheckToken); token != "" {
		if !domainCheckTokenMatches(ctx, token) {
			return HTTPNotFound()
		}
	} else if !g.config.GatewayService.HTTP.DomainCheckAllowUnauthenticated {
		return HTTPNotFound()
	}

	domain := common.NormalizeDomain(ctx.QueryParam("domain"))
	if domain == "" {
		return HTTPBadRequest("domain query parameter is required")
	}

	reqCtx := ctx.Request().Context()
	cacheKey := common.CustomDomainCheckCacheKey(domain)
	// Read the binding revision before the cache and database lookups so a
	// slow request cannot cache an answer for a binding that has since changed.
	rev := common.CustomDomainRevision(reqCtx, g.redisClient, domain)
	if g.redisClient != nil {
		if val, err := g.redisClient.Get(reqCtx, cacheKey).Result(); err == nil {
			switch val {
			case domainCheckAllowedValue:
				return ctx.JSON(http.StatusOK, map[string]any{"allowed": true})
			case domainCheckDeniedValue:
				return HTTPNotFound()
			}
		}
	}

	cd, err := g.backendRepo.GetActiveCustomDomainByDomain(reqCtx, domain)
	if err != nil {
		return HTTPInternalServerError("Failed to lookup custom domain")
	}
	// Only verified domains are allowed; pending or failed domains must never obtain certificates.
	if cd == nil || cd.Status != types.CustomDomainStatusActive {
		common.SetCustomDomainCacheIfCurrent(reqCtx, g.redisClient, domain, rev, cacheKey, domainCheckDeniedValue, domainCheckNegativeCacheTTL)
		return HTTPNotFound()
	}

	common.SetCustomDomainCacheIfCurrent(reqCtx, g.redisClient, domain, rev, cacheKey, domainCheckAllowedValue, domainCheckPositiveCacheTTL)

	return ctx.JSON(http.StatusOK, map[string]any{"allowed": true})
}

func (g *DomainGroup) List(ctx echo.Context) error {
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), ctx.Param("workspaceId"))
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	domains, err := g.backendRepo.ListCustomDomains(ctx.Request().Context(), workspace.Id, ctx.QueryParam("deployment_name"))
	if err != nil {
		return HTTPInternalServerError("Failed to list custom domains")
	}

	for i := range domains {
		g.enrichCustomDomain(&domains[i])
	}

	serialized, err := serializer.Serialize(domains)
	if err != nil {
		return HTTPInternalServerError("Failed to serialize response")
	}

	return ctx.JSON(http.StatusOK, serialized)
}

func (g *DomainGroup) Create(ctx echo.Context) error {
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), ctx.Param("workspaceId"))
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	var req CreateCustomDomainRequest
	if err := ctx.Bind(&req); err != nil {
		return HTTPBadRequest("Invalid payload")
	}

	if err := common.ValidateCustomDomainName(req.Domain, common.ReservedDomains(g.config)...); err != nil {
		return HTTPBadRequest(err.Error())
	}
	if req.Port > 65535 {
		return HTTPBadRequest("port must be between 0 and 65535")
	}
	if req.Version > math.MaxInt32 {
		return HTTPBadRequest("invalid deployment version")
	}

	dep, err := g.resolveDeploymentTarget(ctx.Request().Context(), workspace.Id, &req)
	if err != nil {
		return HTTPBadRequest(err.Error())
	}

	effectiveType := types.StubType(cmp.Or(req.StubType, dep.StubType, string(dep.Stub.Type)))
	if !types.SupportsCustomDomain(effectiveType) {
		return HTTPBadRequest("custom domains are only supported for endpoint, asgi, and HTTP pod deployments")
	}
	var stubConfig types.StubConfigV1
	if strings.TrimSpace(dep.Stub.Config) != "" {
		if err := json.Unmarshal([]byte(dep.Stub.Config), &stubConfig); err != nil {
			return HTTPInternalServerError("Failed to parse deployment config")
		}
	}
	if effectiveType.Kind() == types.StubTypePod {
		if stubConfig.TCP {
			return HTTPBadRequest("custom domains are not supported for raw TCP pods")
		}
		if len(stubConfig.Ports) == 0 {
			return HTTPBadRequest("custom domains on pods require at least one exposed port")
		}
		if req.Port > 0 && !slices.Contains(stubConfig.Ports, req.Port) {
			return HTTPBadRequest(fmt.Sprintf("port %d is not exposed by pod deployment", req.Port))
		}
	} else if req.Port > 0 {
		return HTTPBadRequest("port can only be specified for pod deployments")
	}

	token, err := repository.GenerateCustomDomainVerificationToken()
	if err != nil {
		return HTTPInternalServerError("Failed to generate verification token")
	}

	created, err := g.backendRepo.CreateCustomDomain(ctx.Request().Context(), &types.CustomDomain{
		WorkspaceId:       workspace.Id,
		Domain:            common.NormalizeDomain(req.Domain),
		DeploymentName:    req.DeploymentName,
		StubType:          req.StubType,
		Version:           req.Version,
		Port:              req.Port,
		Status:            types.CustomDomainStatusPending,
		Source:            types.CustomDomainSourceAPI,
		VerificationToken: token,
		Subdomain:         dep.Subdomain,
	})
	if err != nil {
		if errors.Is(err, types.ErrCustomDomainAlreadyExists) {
			return HTTPConflict(err.Error())
		}
		if errors.Is(err, types.ErrInvalidCustomDomain) {
			return HTTPBadRequest(err.Error())
		}
		return HTTPInternalServerError("Failed to create custom domain")
	}

	if created.Subdomain == "" {
		created.Subdomain = dep.Subdomain
	}
	common.InvalidateCustomDomainCache(ctx.Request().Context(), g.redisClient, created.Domain)
	g.enrichCustomDomain(created)

	serialized, err := serializer.Serialize(created)
	if err != nil {
		return HTTPInternalServerError("Failed to serialize response")
	}

	return ctx.JSON(http.StatusCreated, serialized)
}

func (g *DomainGroup) Retrieve(ctx echo.Context) error {
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), ctx.Param("workspaceId"))
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	cd, err := g.backendRepo.GetCustomDomainByIdentifier(ctx.Request().Context(), workspace.Id, ctx.Param("domainId"))
	if err != nil {
		return HTTPInternalServerError("Failed to get custom domain")
	}
	if cd == nil {
		return HTTPNotFound()
	}

	g.enrichCustomDomain(cd)
	serialized, err := serializer.Serialize(cd)
	if err != nil {
		return HTTPInternalServerError("Failed to serialize response")
	}

	return ctx.JSON(http.StatusOK, serialized)
}

func (g *DomainGroup) Verify(ctx echo.Context) error {
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), ctx.Param("workspaceId"))
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	cd, err := g.backendRepo.GetCustomDomainByIdentifier(ctx.Request().Context(), workspace.Id, ctx.Param("domainId"))
	if err != nil {
		return HTTPInternalServerError("Failed to get custom domain")
	}
	if cd == nil {
		return HTTPNotFound()
	}

	g.enrichCustomDomain(cd)

	verified, reason := verifyCustomDomainDNS(ctx.Request().Context(), g.resolver, cd)
	if !verified {
		return ctx.JSON(http.StatusBadRequest, map[string]any{
			"message":            reason,
			"verified":           false,
			"domain":             cd.Domain,
			"txt_record_name":    cd.TXTRecordName,
			"verification_token": cd.VerificationToken,
			"cname_target":       cd.CNAMETarget,
		})
	}

	updated, err := g.backendRepo.ActivateCustomDomain(
		ctx.Request().Context(),
		workspace.Id,
		cd.ExternalId,
	)
	if err != nil {
		if errors.Is(err, types.ErrCustomDomainNotFound) {
			return HTTPNotFound()
		}
		return HTTPInternalServerError("Failed to verify custom domain")
	}

	common.InvalidateCustomDomainCache(ctx.Request().Context(), g.redisClient, updated.Domain)
	g.enrichCustomDomain(updated)

	serialized, err := serializer.Serialize(updated)
	if err != nil {
		return HTTPInternalServerError("Failed to serialize response")
	}

	return ctx.JSON(http.StatusOK, serialized)
}

func (g *DomainGroup) Delete(ctx echo.Context) error {
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), ctx.Param("workspaceId"))
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	deleted, err := g.backendRepo.DeleteCustomDomain(ctx.Request().Context(), workspace.Id, ctx.Param("domainId"))
	if err != nil {
		if errors.Is(err, types.ErrCustomDomainNotFound) {
			return HTTPNotFound()
		}
		return HTTPInternalServerError("Failed to delete custom domain")
	}

	common.InvalidateCustomDomainCache(ctx.Request().Context(), g.redisClient, deleted.Domain)
	return ctx.NoContent(http.StatusNoContent)
}

func verifyCustomDomainDNS(ctx context.Context, resolver domainDNSResolver, cd *types.CustomDomain) (bool, string) {
	if resolver == nil {
		resolver = net.DefaultResolver
	}

	txtName := "_beta9-challenge." + cd.Domain
	target := common.NormalizeDomain(cd.CNAMETarget)

	// Ownership is proven only by the claim's own TXT token or by a CNAME to
	// this deployment's own target. Matching IP addresses is deliberately not
	// accepted: deployment targets share gateway addresses, so any domain that
	// already points at the gateway would pass for every workspace.
	//
	// Each proof runs concurrently with its own deadline. A resolver that is
	// slow or silent for one record type (TXT queries for names that do not
	// exist are commonly dropped) must not starve the other checks.
	checks := []func(ctx context.Context) bool{
		func(ctx context.Context) bool {
			txts, err := resolver.LookupTXT(ctx, txtName)
			if err != nil {
				return false
			}
			for _, txt := range txts {
				if strings.TrimSpace(txt) == cd.VerificationToken {
					return true
				}
			}
			return false
		},
	}
	if target != "" {
		checks = append(checks,
			func(ctx context.Context) bool {
				cname, err := resolver.LookupCNAME(ctx, cd.Domain)
				if err != nil {
					return false
				}
				// Must point at this deployment's own target, not any host under the base domain.
				normalized := common.NormalizeDomain(cname)
				return normalized != "" && normalized == target
			},
		)
	}

	results := make(chan bool, len(checks))
	for _, check := range checks {
		go func(check func(ctx context.Context) bool) {
			dnsCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			results <- check(dnsCtx)
		}(check)
	}
	for range checks {
		if <-results {
			return true, ""
		}
	}

	if cd.CNAMETarget == "" {
		return false, fmt.Sprintf(
			"DNS verification failed for %s: add TXT record %s=%s",
			cd.Domain, txtName, cd.VerificationToken,
		)
	}
	return false, fmt.Sprintf(
		"DNS verification failed for %s: add TXT record %s=%s or CNAME to %s",
		cd.Domain,
		txtName,
		cd.VerificationToken,
		cd.CNAMETarget,
	)
}
