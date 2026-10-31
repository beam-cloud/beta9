package repository_services

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type registryScope struct {
	token  *types.Token
	grant  *repository.WorkerImageGrant
	key    string
	global bool
}
type registryProxy struct {
	service       *WorkerRepositoryService
	rdb           *common.RedisClient
	upstream      *url.URL
	client        *http.Client
	authMu        sync.Mutex
	auth          string
	authUntil     time.Time
	manifestMu    sync.Mutex
	manifests     map[string]registryManifest
	manifestBytes int
}

type registryManifest struct {
	data  []byte
	until time.Time
}

func NewWorkerRegistryProxy(config types.AppConfig, backend repository.BackendRepository, containers repository.ContainerRepository, rdb *common.RedisClient) (http.Handler, error) {
	scheme := "https"
	if config.ImageService.BuildRegistryInsecure {
		scheme = "http"
	}
	upstream, err := url.Parse(scheme + "://" + strings.TrimPrefix(strings.TrimPrefix(config.ImageService.EffectiveBuildRegistry(), "https://"), "http://"))
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	service := &WorkerRepositoryService{appConfig: config, backendRepo: backend, containerRepo: containers, cacheCoordinatorToken: configuredCacheCoordinatorToken(config.Cache.Coordinator.Token), imageGrants: repository.NewWorkerImageGrantRepository(rdb, config.Database.Postgres.EncryptionKey)}
	return &registryProxy{service: service, rdb: rdb, upstream: upstream, client: &http.Client{Transport: transport, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (p *registryProxy) scope(r *http.Request) (*registryScope, error) {
	containerID, key, ok := r.BasicAuth()
	if !ok {
		return nil, fmt.Errorf("registry authentication is required")
	}
	if containerID == "cache" && p.service.cacheCoordinatorToken != "" && subtle.ConstantTimeCompare([]byte(key), []byte(p.service.cacheCoordinatorToken)) == 1 {
		return &registryScope{global: true}, nil
	}
	token, _, err := p.service.backendRepo.AuthorizeToken(r.Context(), key)
	if err != nil || token == nil || !token.ValidAt(time.Now()) || !types.IsWorkerTokenType(token.TokenType) || token.WorkerID == "" {
		return nil, fmt.Errorf("invalid registry credential")
	}
	state, err := containerStateForWorker(p.service.containerRepo, containerID, token.WorkerID)
	if err != nil {
		return nil, err
	}
	grant, err := p.service.imageGrants.Get(r.Context(), token.WorkerID, containerID)
	if err != nil || grant == nil || grant.WorkspaceID != state.WorkspaceId || grant.StubID != state.StubId {
		return nil, fmt.Errorf("image assignment is unavailable")
	}
	sum := sha256.Sum256([]byte(token.WorkerID + "\x00" + containerID))
	return &registryScope{grant: grant, key: hex.EncodeToString(sum[:]), token: token}, nil
}

func (p *registryProxy) credentials(ctx context.Context) (string, error) {
	p.authMu.Lock()
	defer p.authMu.Unlock()
	if p.auth != "" && time.Now().Before(p.authUntil) {
		return p.auth, nil
	}
	value := p.service.buildRegistryCredentials(ctx, p.upstream.Host)
	if value == "" && p.service.appConfig.ImageService.BuildRegistryCredentials.Type != "" {
		return "", fmt.Errorf("registry access is unavailable")
	}
	p.auth = value
	p.authUntil = time.Now().Add(5 * time.Minute)
	return value, nil
}

func validRegistryDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != 71 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}
func snapshotTag(grant *repository.WorkerImageGrant, tag string) bool {
	if !grant.Export || grant.StubID == "" || !strings.HasPrefix(tag, grant.StubID+"-") {
		return false
	}
	number, err := strconv.ParseInt(strings.TrimPrefix(tag, grant.StubID+"-"), 10, 64)
	return err == nil && number > 0
}
func (p *registryProxy) tagAllowed(scope *registryScope, tag string, write bool) bool {
	if scope.global {
		return !write
	}
	if snapshotTag(scope.grant, tag) {
		return true
	}
	if write {
		return scope.grant.Build && tag == scope.grant.OutputImageID
	}
	return imageGrantReads(scope.grant, tag, p.service.appConfig.ImageService.EffectiveBuildRegistry(), p.service.appConfig.ImageService.BuildRepositoryName)
}
func (p *registryProxy) ownDigest(ctx context.Context, scope *registryScope, digest string) bool {
	if scope.global {
		return true
	}
	value, err := p.rdb.SIsMember(ctx, "registry-content:"+scope.key, digest).Result()
	return err == nil && value
}
func (p *registryProxy) rememberDigest(ctx context.Context, scope *registryScope, digest string) error {
	if scope.global || !validRegistryDigest(digest) {
		return fmt.Errorf("invalid registry digest")
	}
	key := "registry-content:" + scope.key
	if err := p.rdb.SAdd(ctx, key, digest).Err(); err != nil {
		return err
	}
	return p.rdb.Expire(ctx, key, 24*time.Hour).Err()
}

func (p *registryProxy) fetchManifest(ctx context.Context, reference string) ([]byte, error) {
	p.manifestMu.Lock()
	cached, ok := p.manifests[reference]
	p.manifestMu.Unlock()
	if ok && time.Now().Before(cached.until) {
		return cached.data, nil
	}
	endpoint := *p.upstream
	endpoint.Path = "/v2/" + strings.Trim(p.service.appConfig.ImageService.BuildRepositoryName, "/") + "/manifests/" + reference
	request, err := http.NewRequestWithContext(ctx, "GET", endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	credentials, err := p.credentials(ctx)
	if err != nil {
		return nil, err
	}
	if username, password, ok := strings.Cut(credentials, ":"); ok {
		request.SetBasicAuth(username, password)
	}
	request.Header.Set("Accept", "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json")
	response, err := p.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("image manifest is unavailable")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20+1))
	if err != nil || len(data) > 4<<20 {
		return nil, fmt.Errorf("image manifest is too large")
	}
	p.manifestMu.Lock()
	if p.manifests == nil || p.manifestBytes+len(data) > 16<<20 || len(p.manifests) >= 128 {
		p.manifests = make(map[string]registryManifest)
		p.manifestBytes = 0
	}
	if previous, exists := p.manifests[reference]; exists {
		p.manifestBytes -= len(previous.data)
	}
	p.manifests[reference] = registryManifest{data: data, until: time.Now().Add(30 * time.Second)}
	p.manifestBytes += len(data)
	p.manifestMu.Unlock()
	return data, nil
}
func manifestDigests(data []byte) ([]string, error) {
	type descriptor struct {
		Digest string `json:"digest"`
	}
	var manifest struct {
		SchemaVersion int          `json:"schemaVersion"`
		Config        descriptor   `json:"config"`
		Layers        []descriptor `json:"layers"`
		Manifests     []descriptor `json:"manifests"`
		Subject       descriptor   `json:"subject"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil || manifest.SchemaVersion != 2 {
		return nil, fmt.Errorf("invalid manifest")
	}
	var out []string
	for _, item := range append(append(manifest.Layers, manifest.Manifests...), manifest.Config, manifest.Subject) {
		if item.Digest != "" {
			if !validRegistryDigest(item.Digest) {
				return nil, fmt.Errorf("invalid manifest digest")
			}
			out = append(out, item.Digest)
		}
	}
	return out, nil
}
func (p *registryProxy) digestAllowed(ctx context.Context, scope *registryScope, digest string) bool {
	if !validRegistryDigest(digest) {
		return false
	}
	if p.ownDigest(ctx, scope, digest) {
		return true
	}
	tags := []string{scope.grant.ImageID, scope.grant.OutputImageID}
	prefix := p.upstream.Host + "/" + strings.Trim(p.service.appConfig.ImageService.BuildRepositoryName, "/") + ":"
	if strings.HasPrefix(scope.grant.SourceImage, prefix) {
		tags = append(tags, strings.TrimPrefix(scope.grant.SourceImage, prefix))
	}
	if digestPrefix := strings.TrimSuffix(prefix, ":") + "@"; strings.HasPrefix(scope.grant.SourceImage, digestPrefix) {
		tags = append(tags, strings.TrimPrefix(scope.grant.SourceImage, digestPrefix))
	}
	seen := map[string]bool{}
	var walk func(string, int) bool
	walk = func(ref string, depth int) bool {
		if depth > 4 || seen[ref] || len(seen) > 1024 {
			return false
		}
		seen[ref] = true
		body, err := p.fetchManifest(ctx, ref)
		if err != nil {
			return false
		}
		sum := sha256.Sum256(body)
		if "sha256:"+hex.EncodeToString(sum[:]) == digest {
			return true
		}
		digests, err := manifestDigests(body)
		if err != nil {
			return false
		}
		for _, entry := range digests {
			if entry == digest {
				return true
			}
		}
		// Only indexes contain child manifests. Layer/config requests are never
		// recursively fetched as manifests.
		var index struct {
			Manifests []struct {
				Digest string `json:"digest"`
			} `json:"manifests"`
		}
		_ = json.Unmarshal(body, &index)
		for _, entry := range index.Manifests {
			if walk(entry.Digest, depth+1) {
				return true
			}
		}
		return false
	}
	for _, tag := range tags {
		if tag != "" && walk(tag, 0) {
			return true
		}
	}
	return false
}

func (p *registryProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	deny := func(status int) { http.Error(w, http.StatusText(status), status) }
	scope, err := p.scope(r)
	if err != nil {
		w.Header().Set("WWW-Authenticate", `Basic realm="beta9"`)
		deny(http.StatusUnauthorized)
		return
	}
	if scope.token != nil {
		live, cancel := auth.LiveCredentialContext(r.Context(), p.service.backendRepo, scope.token)
		defer cancel()
		r = r.WithContext(live)
	}
	if r.URL.Path == "/v2" || r.URL.Path == "/v2/" {
		w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
		w.WriteHeader(http.StatusOK)
		return
	}
	repo := strings.Trim(p.service.appConfig.ImageService.BuildRepositoryName, "/")
	prefix := "/v2/" + repo + "/"
	if repo == "" || path.Clean(r.URL.Path) != strings.TrimSuffix(r.URL.Path, "/") || strings.Contains(r.URL.Path, "\\") || !strings.HasPrefix(r.URL.Path, prefix) {
		deny(http.StatusForbidden)
		return
	}
	resource := strings.TrimPrefix(r.URL.Path, prefix)
	read := r.Method == http.MethodGet || r.Method == http.MethodHead
	remember := ""
	var newSession bool
	switch {
	case strings.HasPrefix(resource, "manifests/"):
		reference := strings.TrimPrefix(resource, "manifests/")
		if read {
			if !p.tagAllowed(scope, reference, false) && !p.digestAllowed(r.Context(), scope, reference) {
				deny(http.StatusForbidden)
				return
			}
		} else {
			if r.Method != http.MethodPut || scope.global || (!scope.grant.Build && !scope.grant.Export) || (!p.tagAllowed(scope, reference, true) && !validRegistryDigest(reference)) {
				deny(http.StatusForbidden)
				return
			}
			data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<20))
			if err != nil {
				deny(http.StatusRequestEntityTooLarge)
				return
			}
			digests, err := manifestDigests(data)
			if err != nil {
				deny(http.StatusBadRequest)
				return
			}
			for _, digest := range digests {
				if !p.digestAllowed(r.Context(), scope, digest) {
					deny(http.StatusForbidden)
					return
				}
			}
			sum := sha256.Sum256(data)
			remember = "sha256:" + hex.EncodeToString(sum[:])
			if validRegistryDigest(reference) && reference != remember {
				deny(http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))
			r.ContentLength = int64(len(data))
		}
	case strings.HasPrefix(resource, "blobs/uploads"):
		if scope.global || (!scope.grant.Build && !scope.grant.Export) {
			deny(http.StatusForbidden)
			return
		}
		suffix := strings.TrimPrefix(resource, "blobs/uploads")
		if r.Method == http.MethodPost && (suffix == "" || suffix == "/") {
			query := r.URL.Query()
			query.Del("mount")
			query.Del("from")
			r.URL.RawQuery = query.Encode()
			newSession = true
		} else {
			session := strings.TrimPrefix(suffix, "/")
			if session == "" || strings.Contains(session, "/") || (r.Method != http.MethodPatch && r.Method != http.MethodPut && r.Method != http.MethodDelete) {
				deny(http.StatusForbidden)
				return
			}
			owner, err := p.rdb.Get(r.Context(), "registry-upload:"+session).Result()
			if err != nil || owner != scope.key {
				deny(http.StatusForbidden)
				return
			}
			if r.Method == http.MethodPut {
				remember = r.URL.Query().Get("digest")
				if !validRegistryDigest(remember) {
					deny(http.StatusBadRequest)
					return
				}
			}
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16<<30)
	case strings.HasPrefix(resource, "blobs/"):
		digest := strings.TrimPrefix(resource, "blobs/")
		if !read || !p.digestAllowed(r.Context(), scope, digest) {
			if r.Method == http.MethodHead {
				deny(http.StatusNotFound)
			} else {
				deny(http.StatusForbidden)
			}
			return
		}
	default:
		deny(http.StatusForbidden)
		return
	}
	credentials, err := p.credentials(r.Context())
	if err != nil {
		deny(http.StatusBadGateway)
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(p.upstream)
	proxy.Transport = p.client.Transport
	director := proxy.Director
	proxy.Director = func(request *http.Request) {
		director(request)
		request.Host = p.upstream.Host
		request.Header.Del("Authorization")
		request.Header.Del("Proxy-Authorization")
		request.Header.Del("Cookie")
		if username, password, ok := strings.Cut(credentials, ":"); ok {
			request.SetBasicAuth(username, password)
		}
	}
	proxy.ModifyResponse = func(response *http.Response) error {
		location := response.Header.Get("Location")
		if location != "" {
			endpoint, err := url.Parse(location)
			if err != nil {
				return err
			}
			if endpoint.Host == p.upstream.Host || endpoint.Host == "" {
				if !strings.HasPrefix(endpoint.Path, prefix) {
					return fmt.Errorf("invalid registry location")
				}
				response.Header.Set("Location", endpoint.RequestURI())
				if newSession && response.StatusCode == http.StatusAccepted {
					session := strings.TrimPrefix(endpoint.Path, prefix+"blobs/uploads/")
					if session == "" || strings.Contains(session, "/") {
						return fmt.Errorf("invalid upload session")
					}
					if err := p.rdb.Set(r.Context(), "registry-upload:"+session, scope.key, time.Hour).Err(); err != nil {
						return err
					}
				}
			}
		}
		if remember != "" && response.StatusCode >= 200 && response.StatusCode < 300 {
			return p.rememberDigest(r.Context(), scope, remember)
		}
		response.Header.Del("WWW-Authenticate")
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "Registry unavailable", http.StatusBadGateway)
	}
	proxy.ServeHTTP(w, r)
}
