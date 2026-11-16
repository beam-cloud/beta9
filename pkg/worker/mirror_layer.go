package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/vm/mirror"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/google/uuid"
	"github.com/moby/sys/user"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	mirrorLayerMountPattern = "/tmp/%s/mirrors"
	// How long a generation stays mounted after its last VM, so that a
	// restart or a fork finds it warm.
	mirrorLayerIdleTimeout = 10 * time.Minute
	// How often a mounted generation is checked against the store.
	mirrorLayerRevocationInterval = time.Minute
	mirrorLayerMountTimeout       = 5 * time.Minute
	// How much of a generation a mount downloads ahead of reads, and how
	// many objects at once.
	mirrorPrefetchBytes       = 4 << 30
	mirrorPrefetchConcurrency = 4
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
// to. A generation never changes, so the VMs pinned to one share a mount
// when their images have the same homes.
//
// Everything the worker does for mirrors is in this file. The rest of the
// worker reaches it in three places: NewWorker makes the mirrorLayers, spawn
// calls attachMirror before it sets up a container's overlay, and
// ContainerArchive asks mirrorArchiveRefusal.
type mirrorLayers struct {
	ctx   context.Context
	root  string
	cache mirror.ContentStore
	open  func(context.Context, *types.ContainerRequest) (mirrorBucket, error)
	mount func(string, *mirror.FS) (mirrorMount, error)
	idle  time.Duration
	// prefetchBytes bounds what a mount reads into the node cache ahead of
	// the guest; zero turns that off.
	prefetchBytes int64

	mu     sync.Mutex
	layers map[string]*mirrorLayer
}

type mirrorLayer struct {
	key          string
	path         string
	manifestKey  string
	ready        chan struct{}
	err          error
	bucket       mirrorBucket
	source       *mirror.CachedSource
	fsys         *mirror.FS
	server       mirrorMount
	stopPrefetch context.CancelFunc

	// Guarded by mirrorLayers.mu.
	refs      int
	idleSince time.Time
	retired   bool
	closing   chan struct{}
}

func newMirrorLayers(ctx context.Context, workerID string, cacheClient *cache.Client) *mirrorLayers {
	m := &mirrorLayers{
		ctx:           ctx,
		root:          fmt.Sprintf(mirrorLayerMountPattern, workerID),
		idle:          mirrorLayerIdleTimeout,
		prefetchBytes: mirrorPrefetchBytes,
		layers:        map[string]*mirrorLayer{},
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
	go m.run()
	return m
}

// mirrorNodeCache is the node's content cache, where every worker on the
// node finds what another has stored.
type mirrorNodeCache struct{ client *cache.Client }

func (c mirrorNodeCache) ReadContent(ctx context.Context, key string, offset int64, dest []byte) bool {
	n, err := c.client.ReadLocalContentInto(ctx, key, offset, dest)
	return err == nil && n == int64(len(dest))
}

func (c mirrorNodeCache) StoreContent(ctx context.Context, key string, content []byte) error {
	return c.client.StoreLocalContent(ctx, key, content)
}

// acquire mounts the pinned generation, or joins its mount, and returns the
// directory to use as an overlay layer. The caller releases it once nothing
// is mounted over it.
func (m *mirrorLayers) acquire(ctx context.Context, request *types.ContainerRequest, pin *types.VMMirror, placements []mirror.Placement) (string, func(), error) {
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
	if len(placements) == 0 {
		placements = []mirror.Placement{mirror.RootHome}
	}
	key := fmt.Sprintf("%d/%s/%s", request.Workspace.Id, pin.ID, digest)
	for _, placement := range placements {
		// Ownership is the mount's, not the overlay's.
		key += fmt.Sprintf(" %q %d:%d %o", placement.Path, placement.UID, placement.GID, placement.Mode)
	}

	m.mu.Lock()
	layer := m.layers[key]
	for layer != nil {
		if layer.retired {
			m.mu.Unlock()
			return "", nil, errors.New("mirror generation is no longer in the store")
		}
		closing := layer.closing
		if closing == nil {
			break
		}
		m.mu.Unlock()
		select {
		case <-closing:
		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
		m.mu.Lock()
		layer = m.layers[key]
	}
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
		go m.start(layer, request, pin.ID, mirror.Object{Digest: digest, Size: uint64(pin.ManifestSize)}, placements)
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

func (m *mirrorLayers) start(layer *mirrorLayer, request *types.ContainerRequest, mirrorID string, manifest mirror.Object, placements []mirror.Placement) {
	ctx, cancel := context.WithTimeout(m.ctx, mirrorLayerMountTimeout)
	defer cancel()
	err := m.serve(ctx, layer, request, mirrorID, manifest, placements)
	m.mu.Lock()
	layer.err = err
	if err != nil && m.layers[layer.key] == layer {
		// Let the next VM try again.
		delete(m.layers, layer.key)
	}
	m.mu.Unlock()
	close(layer.ready)
}

func (m *mirrorLayers) serve(ctx context.Context, layer *mirrorLayer, request *types.ContainerRequest, mirrorID string, manifest mirror.Object, placements []mirror.Placement) error {
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
		Attempts: 3,
		Download: func(ctx context.Context, digest mirror.Digest) (io.ReadCloser, error) {
			return bucket.DownloadWithReader(ctx, types.MirrorObjectKey(mirrorID, digest.String()))
		},
	}
	if m.cache != nil {
		// The node cache is shared with other workspaces, and a manifest
		// can name any digest: only what this workspace's mounts of this
		// mirror have put there is visible here.
		source.Cache = mirror.ScopedCache{Store: m.cache, Scope: fmt.Appendf(nil, "mirror %d %s", request.Workspace.Id, mirrorID)}
	}
	parsed, err := source.Manifest(ctx, manifest)
	if err != nil {
		return fmt.Errorf("read mirror manifest: %w", err)
	}
	fsys, err := mirror.NewFS(parsed, source, mirror.FSOptions{Placements: placements})
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
	if m.cache != nil && m.prefetchBytes > 0 {
		prefetchCtx, stop := context.WithCancel(m.ctx)
		layer.stopPrefetch = stop
		go m.prefetch(prefetchCtx, layer, parsed)
	}
	return nil
}

// prefetch reads a mounted generation's objects into the node cache ahead
// of the guest, in prefetchOrder. It runs beside the mount and never fails
// it: it stops at the byte cap, at the first object it cannot fetch, and
// when the layer is unmounted or revoked.
func (m *mirrorLayers) prefetch(ctx context.Context, layer *mirrorLayer, manifest *mirror.Manifest) {
	started := time.Now()
	var count, total atomic.Int64
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(mirrorPrefetchConcurrency)
	for _, object := range prefetchOrder(manifest) {
		if groupCtx.Err() != nil || total.Load() >= m.prefetchBytes {
			break
		}
		group.Go(func() error {
			if total.Load() >= m.prefetchBytes {
				return nil
			}
			downloaded, err := layer.source.Prefetch(groupCtx, object)
			if downloaded {
				count.Add(1)
				total.Add(int64(object.Size))
			}
			return err
		})
	}
	err := group.Wait()
	if err == nil {
		err = ctx.Err()
	}
	event := log.Info()
	if err != nil && !errors.Is(err, context.Canceled) {
		event = log.Warn()
	}
	event.Err(err).Str("path", layer.path).Int64("objects", count.Load()).Int64("bytes", total.Load()).
		Bool("capped", total.Load() >= m.prefetchBytes).Dur("duration", time.Since(started)).Msg("prefetched mirror layer")
}

// prefetchOrder lists a generation's objects with those holding small files
// first, since a session opens many small files early, then the chunks of
// large ones. Each object follows the first file that uses it, in the
// manifest's breadth-first order, so a home's own files come before deep
// trees. Packing is the writer's choice; files of one extent within the
// reference pack limit are taken as packed.
func prefetchOrder(manifest *mirror.Manifest) []mirror.Object {
	packLimit := uint64(mirror.DefaultWriterOptions().PackLimit)
	seen := make([]bool, manifest.Objects())
	var packs, chunks []mirror.Object
	for index := range manifest.Entries() {
		entry := manifest.Entry(index)
		if entry.Kind != mirror.KindFile {
			continue
		}
		for extent := entry.First; extent < entry.First+entry.Count; extent++ {
			object := manifest.Extent(extent).Object
			if seen[object] {
				continue
			}
			seen[object] = true
			if entry.Count == 1 && entry.Size <= packLimit {
				packs = append(packs, manifest.Object(object))
			} else {
				chunks = append(chunks, manifest.Object(object))
			}
		}
	}
	return append(packs, chunks...)
}

func (m *mirrorLayers) release(layer *mirrorLayer) {
	m.mu.Lock()
	layer.refs--
	unmount := layer.refs == 0 && layer.retired && layer.closing == nil
	if layer.refs == 0 {
		layer.idleSince = time.Now()
	}
	if unmount {
		layer.closing = make(chan struct{})
	}
	m.mu.Unlock()
	if unmount {
		m.unmount(layer)
	}
}

func (m *mirrorLayers) unmount(layer *mirrorLayer) {
	if layer.stopPrefetch != nil {
		layer.stopPrefetch()
	}
	err := layer.server.Unmount()
	if err != nil {
		log.Warn().Err(err).Str("path", layer.path).Msg("could not unmount mirror layer")
	} else {
		_ = os.Remove(layer.path)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		delete(m.layers, layer.key)
	}
	close(layer.closing)
	layer.closing = nil
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
		idle := m.layers[layer.key] == layer && layer.closing == nil && layer.refs == 0 && layer.idleSince.Before(cutoff)
		if idle {
			// Reserve the path until unmount finishes. Acquire cannot add
			// a reference once the idle check has succeeded.
			layer.closing = make(chan struct{})
		}
		m.mu.Unlock()
		if idle {
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
		m.mu.Lock()
		if m.layers[layer.key] != layer || layer.retired {
			m.mu.Unlock()
			continue
		}
		layer.retired = true
		unmount := layer.refs == 0 && layer.closing == nil
		if unmount {
			layer.closing = make(chan struct{})
		}
		m.mu.Unlock()
		if layer.stopPrefetch != nil {
			layer.stopPrefetch()
		}
		layer.fsys.Revoke()
		layer.source.Forget()
		revoked++
		log.Warn().Str("path", layer.path).Msg("mirror was purged; its layer no longer returns file contents")
		if unmount {
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

// imageDesktopUserFile is where a desktop image names the account its session
// runs as.
const imageDesktopUserFile = "opt/beam-vm/desktop-user"

// mirrorPlacements are the homes a mirror appears as in a VM of the image at
// rootfs. A pin with a home places the tree there alone, owned by the desktop
// user, or by root in an image without one. Otherwise the tree is root's
// home, and the desktop user's when the image has one. That account is the
// image's own, so its IDs and its home are read from the image, and each of
// those homes keeps the mode the image gave it.
func mirrorPlacements(rootfs, home string) ([]mirror.Placement, error) {
	root, err := os.OpenRoot(rootfs)
	if err != nil {
		return nil, err
	}
	defer root.Close()

	desktop, err := imageDesktopUser(root)
	if err != nil {
		return nil, err
	}
	if home != "" {
		placement := mirror.Placement{Path: home, Mode: 0o750}
		if desktop != nil {
			placement.UID, placement.GID = uint32(desktop.Uid), uint32(desktop.Gid)
		}
		return []mirror.Placement{placement}, nil
	}
	placements := []mirror.Placement{mirror.RootHome}
	// A desktop that runs as root has its home already.
	if desktop != nil && path.Clean(desktop.Home) != mirror.RootHome.Path {
		placements = append(placements, mirror.Placement{Path: desktop.Home, UID: uint32(desktop.Uid), GID: uint32(desktop.Gid), Mode: 0o750})
	}
	for i := range placements {
		home, err := securejoin.SecureJoinVFS(".", placements[i].Path, root)
		if err != nil {
			return nil, fmt.Errorf("resolve image %s: %w", placements[i].Path, err)
		}
		if info, err := root.Stat(home); err == nil && info.IsDir() {
			placements[i].Mode = uint32(info.Mode().Perm())
		}
	}
	return placements, nil
}

// imageDesktopUser is the account an image's desktop session runs as, or nil
// for an image without one.
func imageDesktopUser(root *os.Root) (*user.User, error) {
	named, err := openImageFile(root, imageDesktopUserFile)
	if err != nil || named == nil {
		return nil, err
	}
	defer named.Close()
	content, err := io.ReadAll(io.LimitReader(named, 256))
	if err != nil {
		return nil, fmt.Errorf("read image %s: %w", imageDesktopUserFile, err)
	}
	name := strings.TrimSpace(string(content))
	passwd, err := openImageFile(root, "etc/passwd")
	if err != nil || passwd == nil || name == "" {
		return nil, err
	}
	defer passwd.Close()
	accounts, err := user.ParsePasswdFilter(passwd, func(account user.User) bool { return account.Name == name })
	if err != nil {
		return nil, fmt.Errorf("read image etc/passwd: %w", err)
	}
	if len(accounts) == 0 {
		return nil, nil
	}
	if uint64(accounts[0].Uid) > math.MaxUint32 || uint64(accounts[0].Gid) > math.MaxUint32 {
		return nil, fmt.Errorf("image account %q has an ID out of range", name)
	}
	return &accounts[0], nil
}

// openImageFile opens a regular file of an image by its path inside the
// image, following the image's symlinks no further than its root. It returns
// nil for a file the image does not have.
func openImageFile(root *os.Root, name string) (*os.File, error) {
	path, err := securejoin.SecureJoinVFS(".", name, root)
	if err != nil {
		return nil, fmt.Errorf("resolve image %s: %w", name, err)
	}
	file, err := root.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open image %s: %w", name, err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		file.Close()
		return nil, fmt.Errorf("image %s is not a regular file", name)
	}
	return file, nil
}

// attachMirror puts the mirror generation a VM is pinned to between its
// image and everything writable, and tells the guest where the tree is when
// the pin names a home. A container without a mirror is left as it is. The
// returned release must run after the overlay is unmounted.
func (s *Worker) attachMirror(ctx context.Context, request *types.ContainerRequest, spec *specs.Spec, overlay *common.ContainerOverlay) (release func(), err error) {
	config := requestStubConfig(request)
	if config == nil || config.Mirror == nil {
		return func() {}, nil
	}
	defer func() {
		if err != nil {
			log.Error().Str("container_id", request.ContainerId).Err(err).Msg("failed to attach mirror layer")
		}
	}()
	if s.mirrorLayers == nil {
		return nil, errors.New("this worker cannot mount mirror layers")
	}
	// The home came from a laptop, by way of the gateway.
	home := config.Mirror.Home
	if home != "" && !mirror.ValidHome(home) {
		return nil, errors.New("invalid mirror home")
	}
	if err := s.ensureDurableDiskSnapshotStorage(ctx, request); err != nil {
		return nil, fmt.Errorf("mirror store credentials: %w", err)
	}
	placements, err := mirrorPlacements(overlay.RootPath(), home)
	if err != nil {
		return nil, fmt.Errorf("find the image's home directories: %w", err)
	}
	path, release, err := s.mirrorLayers.acquire(ctx, request, config.Mirror, placements)
	if err != nil {
		return nil, err
	}
	if err := overlay.SetReadOnlyLayers(path); err != nil {
		release()
		return nil, err
	}
	if home != "" {
		// Per launch, not in the stub: a guest started without the layer
		// must not look for the tree.
		spec.Process.Env = append(spec.Process.Env, "BEAM_VM_MIRROR_HOME="+home)
	}
	return release, nil
}

// mirrorArchiveRefusal is why a container cannot be archived as an image, or
// empty when it can. The merged root of a VM with a mirror includes the
// mirror layer, and its writable layer holds every mirrored file the VM
// changed: either archive would copy mirrored files into the image.
func mirrorArchiveRefusal(request *types.ContainerRequest) string {
	if config := requestStubConfig(request); config == nil || config.Mirror == nil {
		return ""
	}
	return "A VM with a mirror cannot be archived as an image: its disk holds the mirrored files it changed"
}
