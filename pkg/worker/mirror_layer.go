package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/mirror"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	mirrorLayerMountPattern = "/tmp/%s/mirrors"
	// How long a generation stays mounted after its last VM, so that a
	// restart or a fork finds it warm.
	mirrorLayerIdleTimeout = 10 * time.Minute
	// How often a mounted generation is checked against the store.
	mirrorLayerRevocationInterval = time.Minute
	mirrorLayerMountTimeout       = 5 * time.Minute
)

// mirrorBucket is the part of a workspace bucket a mirror layer reads.
type mirrorBucket interface {
	Exists(ctx context.Context, key string) (bool, error)
	DownloadWithReader(ctx context.Context, key string) (io.ReadCloser, error)
}

type mirrorMount interface {
	Unmount() error
}

// mirrorLayers serves the mirror generations this worker's VMs are pinned
// to. A generation never changes, so every VM pinned to one shares a mount.
type mirrorLayers struct {
	ctx   context.Context
	root  string
	cache mirror.ObjectCache
	open  func(context.Context, *types.ContainerRequest) (mirrorBucket, error)
	mount func(string, *mirror.FS) (mirrorMount, error)
	idle  time.Duration

	mu     sync.Mutex
	layers map[string]*mirrorLayer
}

type mirrorLayer struct {
	key         string
	path        string
	manifestKey string
	ready       chan struct{}
	err         error
	bucket      mirrorBucket
	source      *mirror.CachedSource
	fsys        *mirror.FS
	server      mirrorMount

	// Guarded by mirrorLayers.mu.
	refs      int
	idleSince time.Time
	retired   bool
}

func newMirrorLayers(ctx context.Context, workerID string, cacheClient *cache.Client) *mirrorLayers {
	m := &mirrorLayers{
		ctx:    ctx,
		root:   fmt.Sprintf(mirrorLayerMountPattern, workerID),
		idle:   mirrorLayerIdleTimeout,
		layers: map[string]*mirrorLayer{},
		open: func(ctx context.Context, request *types.ContainerRequest) (mirrorBucket, error) {
			return clients.NewWorkspaceStorageClient(ctx, request.Workspace.Name, request.Workspace.Storage)
		},
		mount: func(path string, fsys *mirror.FS) (mirrorMount, error) {
			return mirror.Mount(path, fsys)
		},
	}
	if cacheClient != nil {
		m.cache = mirrorNodeCache{cacheClient}
	}
	return m
}

// mirrorNodeCache keeps mirror objects in the node's content cache under
// their SHA-256, where every worker on the node finds them.
type mirrorNodeCache struct{ client *cache.Client }

func (c mirrorNodeCache) ReadObject(ctx context.Context, digest mirror.Digest, offset int64, dest []byte) bool {
	n, err := c.client.ReadLocalContentInto(ctx, digest.String(), offset, dest)
	return err == nil && n == int64(len(dest))
}

func (c mirrorNodeCache) StoreObject(ctx context.Context, digest mirror.Digest, data []byte) error {
	return c.client.StoreLocalContent(ctx, digest.String(), data)
}

// acquire mounts the pinned generation, or joins its mount, and returns the
// directory to use as an overlay layer. The caller releases it once nothing
// is mounted over it.
func (m *mirrorLayers) acquire(ctx context.Context, request *types.ContainerRequest, pin *types.VMMirror) (string, func(), error) {
	// The pin becomes a bucket key and a path; take nothing in it on trust.
	if _, err := uuid.Parse(pin.ID); err != nil {
		return "", nil, errors.New("invalid mirror id")
	}
	digest, err := mirror.ParseDigest(pin.ManifestDigest)
	if err != nil {
		return "", nil, errors.New("invalid mirror manifest digest")
	}
	if pin.ManifestSize <= 0 || pin.ManifestSize > mirror.MaxManifestSize {
		return "", nil, errors.New("invalid mirror manifest size")
	}
	key := fmt.Sprintf("%d/%s/%s", request.Workspace.Id, pin.ID, digest)

	m.mu.Lock()
	layer := m.layers[key]
	if layer == nil {
		name := sha256.Sum256([]byte(key))
		layer = &mirrorLayer{
			key:         key,
			path:        filepath.Join(m.root, hex.EncodeToString(name[:16])),
			manifestKey: types.MirrorObjectKey(pin.ID, digest.String()),
			ready:       make(chan struct{}),
		}
		m.layers[key] = layer
		// The mount is shared, so it must not fail because the first VM to
		// ask for it was cancelled.
		go m.start(layer, request, pin.ID, mirror.Object{Digest: digest, Size: uint64(pin.ManifestSize)})
	}
	layer.refs++
	m.mu.Unlock()

	select {
	case <-layer.ready:
	case <-ctx.Done():
		m.release(layer)
		return "", nil, ctx.Err()
	}
	if layer.err != nil {
		m.release(layer)
		return "", nil, layer.err
	}
	var once sync.Once
	return layer.path, func() { once.Do(func() { m.release(layer) }) }, nil
}

func (m *mirrorLayers) start(layer *mirrorLayer, request *types.ContainerRequest, mirrorID string, manifest mirror.Object) {
	ctx, cancel := context.WithTimeout(m.ctx, mirrorLayerMountTimeout)
	defer cancel()
	err := m.serve(ctx, layer, request, mirrorID, manifest)
	m.mu.Lock()
	layer.err = err
	if err != nil && m.layers[layer.key] == layer {
		// Let the next VM try again.
		delete(m.layers, layer.key)
	}
	m.mu.Unlock()
	close(layer.ready)
}

func (m *mirrorLayers) serve(ctx context.Context, layer *mirrorLayer, request *types.ContainerRequest, mirrorID string, manifest mirror.Object) error {
	bucket, err := m.open(ctx, request)
	if err != nil {
		return fmt.Errorf("open mirror store: %w", err)
	}
	// The node cache may still hold a purged mirror's objects; the store
	// decides whether the generation exists.
	exists, err := bucket.Exists(ctx, layer.manifestKey)
	if err != nil {
		return fmt.Errorf("check mirror generation: %w", err)
	}
	if !exists {
		return errors.New("mirror generation is no longer in the store")
	}
	source := &mirror.CachedSource{
		Cache:    m.cache,
		Attempts: 3,
		Download: func(ctx context.Context, digest mirror.Digest) (io.ReadCloser, error) {
			return bucket.DownloadWithReader(ctx, types.MirrorObjectKey(mirrorID, digest.String()))
		},
	}
	stored := make([]byte, manifest.Size)
	if err := source.ReadObject(ctx, manifest, 0, stored); err != nil {
		return fmt.Errorf("read mirror manifest: %w", err)
	}
	parsed, err := mirror.Parse(stored)
	if err != nil {
		return err
	}
	// The cache is trusted for file contents as for every other reader of
	// it, but the manifest decides what the whole layer contains.
	if parsed.Digest() != manifest.Digest {
		return mirror.ErrObjectMismatch
	}
	fsys, err := mirror.NewFS(parsed, source, mirror.FSOptions{})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(layer.path, 0o755); err != nil {
		return err
	}
	server, err := m.mount(layer.path, fsys)
	if err != nil {
		return fmt.Errorf("mount mirror layer: %w", err)
	}
	layer.bucket, layer.source, layer.fsys, layer.server = bucket, source, fsys, server
	log.Info().Str("mirror_id", mirrorID).Str("manifest", manifest.Digest.String()).Uint32("entries", parsed.Entries()).Str("path", layer.path).Msg("mounted mirror layer")
	return nil
}

func (m *mirrorLayers) release(layer *mirrorLayer) {
	m.mu.Lock()
	layer.refs--
	unmount := layer.refs == 0 && layer.retired
	if layer.refs == 0 {
		layer.idleSince = time.Now()
	}
	m.mu.Unlock()
	if unmount {
		m.unmount(layer)
	}
}

func (m *mirrorLayers) unmount(layer *mirrorLayer) {
	if layer.server == nil {
		return
	}
	if err := layer.server.Unmount(); err != nil {
		log.Warn().Err(err).Str("path", layer.path).Msg("could not unmount mirror layer")
		return
	}
	_ = os.Remove(layer.path)
}

// retire takes a layer out of use: nothing new joins it, and it is
// unmounted when its last VM lets go.
func (m *mirrorLayers) retire(layer *mirrorLayer) (unmount bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if layer.retired {
		return false
	}
	layer.retired = true
	if m.layers[layer.key] == layer {
		delete(m.layers, layer.key)
	}
	return layer.refs == 0
}

func (m *mirrorLayers) mounted() []*mirrorLayer {
	m.mu.Lock()
	defer m.mu.Unlock()
	layers := make([]*mirrorLayer, 0, len(m.layers))
	for _, layer := range m.layers {
		select {
		case <-layer.ready:
			if layer.err == nil {
				layers = append(layers, layer)
			}
		default:
		}
	}
	return layers
}

// pruneIdle unmounts the generations no VM has used since cutoff.
func (m *mirrorLayers) pruneIdle(cutoff time.Time) int {
	pruned := 0
	for _, layer := range m.mounted() {
		m.mu.Lock()
		idle := layer.refs == 0 && layer.idleSince.Before(cutoff)
		m.mu.Unlock()
		if idle && m.retire(layer) {
			m.unmount(layer)
			pruned++
		}
	}
	return pruned
}

// revokePurged stops serving the generations whose manifest has left the
// store, which is what a purge does. Only a definite answer from the store
// counts: an unreachable store revokes nothing.
func (m *mirrorLayers) revokePurged(ctx context.Context) int {
	revoked := 0
	for _, layer := range m.mounted() {
		exists, err := layer.bucket.Exists(ctx, layer.manifestKey)
		if err != nil || exists {
			continue
		}
		layer.fsys.Revoke()
		layer.source.Forget()
		revoked++
		log.Warn().Str("path", layer.path).Msg("mirror was purged; its layer no longer returns file contents")
		if m.retire(layer) {
			m.unmount(layer)
		}
	}
	return revoked
}

func (m *mirrorLayers) run() {
	prune := time.NewTicker(time.Minute)
	defer prune.Stop()
	revoke := time.NewTicker(mirrorLayerRevocationInterval)
	defer revoke.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-prune.C:
			m.pruneIdle(time.Now().Add(-m.idle))
		case <-revoke.C:
			ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
			m.revokePurged(ctx)
			cancel()
		}
	}
}

// attachMirrorLayer puts the mirror generation a VM is pinned to between
// its image and everything writable. The returned release must run after
// the overlay is unmounted.
func (s *Worker) attachMirrorLayer(ctx context.Context, request *types.ContainerRequest, overlay *common.ContainerOverlay) (func(), error) {
	config := requestStubConfig(request)
	if config == nil || config.Mirror == nil {
		return func() {}, nil
	}
	if s.mirrorLayers == nil {
		return nil, errors.New("this worker cannot mount mirror layers")
	}
	if err := s.ensureDurableDiskSnapshotStorage(ctx, request); err != nil {
		return nil, fmt.Errorf("mirror store credentials: %w", err)
	}
	path, release, err := s.mirrorLayers.acquire(ctx, request, config.Mirror)
	if err != nil {
		return nil, err
	}
	if err := overlay.SetReadOnlyLayers(path); err != nil {
		release()
		return nil, err
	}
	return release, nil
}
