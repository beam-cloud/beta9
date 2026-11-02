package gateway

import (
	"context"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	repositoryservices "github.com/beam-cloud/beta9/pkg/gateway/services/repository"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

const registryProxyCredentialTTL = 10 * time.Minute

var registryDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

// registryProxy serves the build registry to workers on private and provider
// pools, which hold no build-registry credentials. A worker may read manifests
// of its containers' images by tag, and manifests and blobs by digest, which
// only those manifests reveal. It may upload blobs and push the filesystem
// snapshot tags of its containers' stubs.
type registryProxy struct {
	config        types.AppConfig
	backendRepo   repository.BackendRepository
	tokens        auth.Tokens
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
		tokens:        auth.NewTokens(backendRepo, workspaceRepo),
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
	worker := p.authorize(r)
	if worker == nil {
		w.Header().Set("WWW-Authenticate", `Basic realm="beta9"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.URL.Path == "/v2" || r.URL.Path == "/v2/" {
		return
	}
	if !p.permitted(r, worker) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	r.Header.Del("Authorization")
	if user, password, ok := strings.Cut(p.registryCredentials(r.Context()), ":"); ok {
		r.SetBasicAuth(user, password)
	}
	p.upstream.ServeHTTP(w, r)
}

// authorize returns the worker making the request when it is on a private or
// provider pool.
func (p *registryProxy) authorize(r *http.Request) *auth.Worker {
	_, tokenKey, ok := r.BasicAuth()
	if !ok || tokenKey == "" {
		return nil
	}
	token, workspace, err := p.tokens.Authenticate(r.Context(), tokenKey)
	if err != nil {
		return nil
	}
	worker, err := auth.WorkerFromAuthInfo(&auth.AuthInfo{Token: token, Workspace: workspace})
	if err != nil || worker.Trusted {
		return nil
	}
	return worker
}

func (p *registryProxy) permitted(r *http.Request, worker *auth.Worker) bool {
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
		return registryDigest.MatchString(ref) || p.workerImage(r.Context(), worker, ref)
	case kind == "manifests" && r.Method == http.MethodPut:
		stubID, ok := types.SnapshotImageStub(ref)
		return ok && worker.RunsStub(p.containerRepo, stubID)
	}
	return false
}

func (p *registryProxy) workerImage(ctx context.Context, worker *auth.Worker, imageID string) bool {
	seen := map[string]bool{}
	for _, container := range worker.Containers(p.containerRepo, "") {
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

func (p *registryProxy) registryCredentials(ctx context.Context) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.credentials == "" || time.Now().After(p.expiresAt) {
		p.credentials = repositoryservices.BuildRegistryCredentials(ctx, p.config.ImageService)
		p.expiresAt = time.Now().Add(registryProxyCredentialTTL)
	}
	return p.credentials
}
