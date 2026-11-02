package gateway

import (
	"context"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	repositoryservices "github.com/beam-cloud/beta9/pkg/gateway/services/repository"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

const registryProxyCredentialTTL = 10 * time.Minute

var registryDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

// registryProxy serves the build registry to workers on private and provider
// pools, which hold no build-registry credentials. A worker may read manifests
// of its containers' images by tag, and manifests and blobs by digest, which
// only those manifests reveal. It may upload blobs and push the snapshot tags
// ("<stub-id>-<unix-time>") of its containers' stubs.
type registryProxy struct {
	config        types.AppConfig
	backendRepo   repository.BackendRepository
	workspaceRepo repository.WorkspaceRepository
	containerRepo repository.ContainerRepository
	upstream      *httputil.ReverseProxy

	mu          sync.Mutex
	credentials string
	expiresAt   time.Time
}

func newRegistryProxy(config types.AppConfig, backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository, containerRepo repository.ContainerRepository) *registryProxy {
	target := &url.URL{Scheme: "https", Host: config.ImageService.EffectiveBuildRegistry()}
	if config.ImageService.BuildRegistryInsecure {
		target.Scheme = "http"
	}
	return &registryProxy{
		config:        config,
		backendRepo:   backendRepo,
		workspaceRepo: workspaceRepo,
		containerRepo: containerRepo,
		upstream: &httputil.ReverseProxy{
			Rewrite: func(r *httputil.ProxyRequest) {
				r.SetURL(target)
				// The gateway strips trailing slashes; registries require this one.
				if strings.HasSuffix(r.Out.URL.Path, "/blobs/uploads") {
					r.Out.URL.Path += "/"
				}
			},
			// Upload sessions continue through the proxy; blob redirects to
			// object storage go to the worker unchanged.
			ModifyResponse: func(resp *http.Response) error {
				if location, err := url.Parse(resp.Header.Get("Location")); err == nil && location.Host == target.Host {
					resp.Header.Set("Location", location.RequestURI())
				}
				return nil
			},
		},
	}
}

func (p *registryProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token := p.authorize(r)
	if token == nil {
		w.Header().Set("WWW-Authenticate", `Basic realm="beta9"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.URL.Path == "/v2" || r.URL.Path == "/v2/" {
		return
	}
	if !p.permitted(r, token.WorkerID) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	r.Header.Del("Authorization")
	if user, password, ok := strings.Cut(p.registryCredentials(r.Context()), ":"); ok {
		r.SetBasicAuth(user, password)
	}
	p.upstream.ServeHTTP(w, r)
}

// authorize returns the request's token when it belongs to a worker on a
// private or provider pool.
func (p *registryProxy) authorize(r *http.Request) *types.Token {
	_, tokenKey, ok := r.BasicAuth()
	if !ok || tokenKey == "" {
		return nil
	}
	token, workspace, err := p.workspaceRepo.AuthorizeToken(tokenKey)
	if err != nil {
		if token, workspace, err = p.backendRepo.AuthorizeToken(r.Context(), tokenKey); err != nil {
			return nil
		}
		_ = p.workspaceRepo.SetAuthorizationToken(token, workspace)
	}
	if !token.Active || token.DisabledByClusterAdmin || token.TokenType != types.TokenTypeWorkerPrivate || token.WorkerID == "" {
		return nil
	}
	return token
}

func (p *registryProxy) permitted(r *http.Request, workerID string) bool {
	repo := p.config.ImageService.BuildRepositoryName
	rest, ok := strings.CutPrefix(r.URL.Path, "/v2/"+repo+"/")
	if !ok || r.URL.RawPath != "" || path.Clean(r.URL.Path) != r.URL.Path {
		return false
	}
	kind, ref, _ := strings.Cut(rest, "/")
	read := r.Method == http.MethodGet || r.Method == http.MethodHead

	switch {
	case kind == "blobs" && read:
		return registryDigest.MatchString(ref)
	case kind == "blobs" && (ref == "uploads" || strings.HasPrefix(ref, "uploads/")):
		from := r.URL.Query().Get("from")
		return (from == "" || from == repo) && (r.Method == http.MethodPost || r.Method == http.MethodPatch || r.Method == http.MethodPut)
	case kind == "manifests" && read:
		return registryDigest.MatchString(ref) || p.workerImage(r.Context(), workerID, ref)
	case kind == "manifests" && r.Method == http.MethodPut:
		return p.workerSnapshotTag(workerID, ref)
	}
	return false
}

func (p *registryProxy) workerImage(ctx context.Context, workerID, imageID string) bool {
	containers, err := p.containerRepo.GetActiveContainersByWorkerId(workerID)
	if err != nil {
		return false
	}
	seen := map[string]bool{}
	for _, container := range containers {
		if seen[container.StubId] {
			continue
		}
		seen[container.StubId] = true
		stub, err := p.backendRepo.GetStubByExternalId(ctx, container.StubId)
		if err != nil || stub == nil {
			continue
		}
		if config, err := stub.UnmarshalConfig(); err == nil && config != nil && config.Runtime.ImageId == imageID {
			return true
		}
	}
	return false
}

func (p *registryProxy) workerSnapshotTag(workerID, tag string) bool {
	i := strings.LastIndexByte(tag, '-')
	if i <= 0 {
		return false
	}
	if _, err := strconv.ParseUint(tag[i+1:], 10, 64); err != nil {
		return false
	}
	containers, err := p.containerRepo.GetActiveContainersByWorkerId(workerID)
	if err != nil {
		return false
	}
	for _, container := range containers {
		if container.StubId == tag[:i] {
			return true
		}
	}
	return false
}

func (p *registryProxy) registryCredentials(ctx context.Context) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.credentials == "" || time.Now().After(p.expiresAt) {
		p.credentials = repositoryservices.BuildRegistryCredentials(ctx, p.config.ImageService)
		p.expiresAt = time.Now().Add(registryProxyCredentialTTL)
	}
	return p.credentials
}
