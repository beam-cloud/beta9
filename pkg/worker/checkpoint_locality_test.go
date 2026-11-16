package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const checkpointTestLocality = "locality-a"

// checkpointLocality is the cache hosts of one locality. They share the
// stream of required content and the index of recent stubs, as its hosts do.
type checkpointLocality struct {
	t        *testing.T
	ctx      context.Context
	cfg      cache.Config
	metadata *localityRecentMetadataStore
	events   *fakeEventRepo
	hosts    []cache.Host
}

func newCheckpointLocality(t *testing.T) *checkpointLocality {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := testCacheManagerConfig(t.TempDir()).Cache
	cfg.Client.NTopHosts = 2
	cfg.Server.DiskCacheEvictWatermarkPct = 1
	return &checkpointLocality{
		t:   t,
		ctx: ctx,
		cfg: cfg,
		metadata: &localityRecentMetadataStore{
			MockCacheMetadataStore: cache.NewMockCacheMetadataStore(),
			stubs:                  map[string][]cache.RecentStub{},
		},
		events: &fakeEventRepo{},
	}
}

// host adds a cache host. Its requests go to answer, when there is one, in
// place of the host's own server.
func (l *checkpointLocality) host(id string, answer func(*cache.Server) pb.CacheServer) *cache.Server {
	l.t.Helper()
	cfg := l.cfg
	cfg.Server.DiskCacheDir = l.t.TempDir()
	server, err := cache.NewServerWithOptions(l.ctx, cfg, checkpointTestLocality, cache.WithServerMetadataStore(cache.NewMockCacheMetadataStore()), cache.WithServerHostID(id))
	require.NoError(l.t, err)
	l.t.Cleanup(func() { require.NoError(l.t, server.Close()) })

	var addr string
	if answer == nil {
		addr, err = server.Serve("127.0.0.1:0", "")
		require.NoError(l.t, err)
	} else {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(l.t, err)
		service := grpc.NewServer()
		pb.RegisterCacheServer(service, answer(server))
		go service.Serve(listener)
		l.t.Cleanup(service.Stop)
		addr = listener.Addr().String()
	}
	host := server.Host()
	require.NotNil(l.t, host)
	host.Addr, host.PrivateAddr = addr, addr
	l.hosts = append(l.hosts, *host)
	return server
}

// worker is the worker that serves one of the locality's hosts. It knows the
// hosts the locality has when it is made.
func (l *checkpointLocality) worker(server *cache.Server) *Worker {
	l.t.Helper()
	cfg := l.cfg
	cfg.Server.DiskCacheDir = l.t.TempDir()
	hosts := append([]cache.Host(nil), l.hosts...)
	client, err := cache.NewClientWithHostDirectory(l.ctx, cfg, l.metadata, testHostDirectoryFunc(func(context.Context, string) ([]*cache.Host, error) {
		// Each answer is the client's own, as a directory's is.
		listed := make([]*cache.Host, len(hosts))
		for i, host := range hosts {
			listed[i] = &host
		}
		return listed, nil
	}), checkpointTestLocality)
	require.NoError(l.t, err)
	l.t.Cleanup(func() { require.NoError(l.t, client.Cleanup()) })
	client.AttachLocalServer(server)
	require.Eventually(l.t, func() bool {
		return len(client.RankedReadHosts("probe")) == len(hosts)
	}, 3*time.Second, 20*time.Millisecond)

	reporter := newTestReporter(l.events)
	reporter.metadata = l.metadata
	reporter.locality = checkpointTestLocality
	reporter.recentStubTTL = time.Hour
	return &Worker{cacheManager: &WorkerCacheManager{
		ctx:                l.ctx,
		config:             types.AppConfig{Cache: l.cfg},
		locality:           checkpointTestLocality,
		accelerator:        "CPU",
		metadataStore:      l.metadata,
		eventRepo:          l.events,
		client:             client,
		server:             server,
		checkpointRoot:     filepath.Join(l.t.TempDir(), "checkpoints"),
		reporter:           reporter,
		reconcileFailures:  make(map[string]time.Time),
		reconcileSuccesses: make(map[string]time.Time),
	}}
}

// cacheEvents is what the locality's hosts have reported of their copies.
func (l *checkpointLocality) cacheEvents() []types.EventPlatformCacheSchema {
	l.events.mu.Lock()
	defer l.events.mu.Unlock()
	return append([]types.EventPlatformCacheSchema(nil), l.events.cacheEvents...)
}

type testCheckpoint struct {
	id      string
	archive string
	hash    string
	size    int64
}

// newTestCheckpoint archives a checkpoint that no other has the content of.
func newTestCheckpoint(t *testing.T, id string) testCheckpoint {
	t.Helper()
	checkpointPath := filepath.Join(t.TempDir(), id)
	createMaterializedCheckpointForTest(t, checkpointPath)
	require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, "memory"), []byte("RAM of "+id), 0644))
	archive := filepath.Join(t.TempDir(), id+checkpointArchiveExtension)
	require.NoError(t, createTar(checkpointPath, archive))
	hash, size, err := fileSHA256(archive)
	require.NoError(t, err)
	return testCheckpoint{id: id, archive: archive, hash: hash, size: size}
}

func (c testCheckpoint) originKey() string {
	return "checkpoints/" + c.id + checkpointArchiveExtension
}

func (c testCheckpoint) metadata() *checkpointCacheMetadata {
	return &checkpointCacheMetadata{
		hash:        c.hash,
		sizeBytes:   c.size,
		originKey:   c.originKey(),
		locality:    checkpointTestLocality,
		accelerator: "CPU",
	}
}

// storeOn puts the checkpoint's archive in a host's cache, as the worker that
// made the checkpoint does.
func (c testCheckpoint) storeOn(t *testing.T, worker *Worker) {
	t.Helper()
	worker.cacheCheckpointArchiveAsync(c.id, c.archive, c.originKey(), c.hash, nil)
	require.Eventually(t, func() bool {
		_, err := os.Stat(c.archive)
		return os.IsNotExist(err)
	}, 5*time.Second, 10*time.Millisecond, "the archive is removed once the cache has been offered it")
}

// reportFrom publishes the checkpoint as what the request's stub requires.
func (c testCheckpoint) reportFrom(worker *Worker, request *types.ContainerRequest) {
	worker.reportCheckpointRequiredContent(request, c.id, c.metadata())
	worker.cacheManager.reporter.flush()
}

func (c testCheckpoint) materializedOn(worker *Worker) bool {
	manager := worker.cacheManager
	return manager.server.HasCompleteContent(c.hash, c.size) && checkpointMaterialized(filepath.Join(manager.checkpointRoot, c.id))
}

// checkpointBucket is as much of an object store as an archive is read from.
// It counts the reads of the checkpoint's object.
func checkpointBucket(t *testing.T, worker *Worker, checkpoint testCheckpoint) *atomic.Int32 {
	t.Helper()
	object, err := os.ReadFile(checkpoint.archive)
	require.NoError(t, err)
	reads := &atomic.Int32{}
	bucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bucket/"+checkpoint.originKey() {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "<Error><Code>NoSuchKey</Code><Message>no such key</Message></Error>")
			return
		}
		if r.Method == http.MethodGet {
			reads.Add(1)
		}
		http.ServeContent(w, r, "", time.Unix(1700000000, 0), bytes.NewReader(object))
	}))
	t.Cleanup(bucket.Close)
	worker.cacheManager.originCredsCache = make(map[string]*originCredentials)
	worker.cacheManager.workerRepo = &fakeImageCredentialWorkerRepo{resp: &pb.GetCacheOriginCredentialsResponse{
		Ok: true,
		WorkspaceStorage: &pb.CacheWorkspaceStorageCredentials{
			EndpointUrl:    bucket.URL,
			Region:         "us-east-1",
			BucketName:     "bucket",
			AccessKey:      "test",
			SecretKey:      "test",
			ForcePathStyle: true,
		},
	}}
	return reads
}

func setCheckpointArchiveWait(t *testing.T, wait time.Duration) {
	previous := checkpointArchiveWait
	checkpointArchiveWait = wait
	t.Cleanup(func() { checkpointArchiveWait = previous })
}

func persistentVMRequest() *types.ContainerRequest {
	return &types.ContainerRequest{
		WorkspaceId: "workspace",
		StubId:      "vm",
		UseVM:       true,
		Env:         []string{"BEAM_VM_SYSTEMD=1"},
	}
}

// A checkpoint is on every host of its locality, and each of them asks all
// the others for it. The host that makes one keeps the archive, whichever
// host its hash selects.
func TestPausedCheckpointStaysOnItsHostForPeersToCopy(t *testing.T) {
	locality := newCheckpointLocality(t)
	pausingServer := locality.host("pausing-host", nil)
	peerServer := locality.host("peer-host", nil)
	pausing, peer := locality.worker(pausingServer), locality.worker(peerServer)

	var checkpoint testCheckpoint
	for i := 0; checkpoint.id == "" && i < 64; i++ {
		candidate := newTestCheckpoint(t, fmt.Sprintf("checkpoint-%d", i))
		if pausing.cacheManager.client.RankedReadHosts(candidate.hash)[0].HostId == peerServer.HostID() {
			checkpoint = candidate
		}
	}
	require.NotEmpty(t, checkpoint.id, "expected a checkpoint whose hash selects the peer")

	checkpoint.storeOn(t, pausing)
	require.True(t, pausingServer.HasCompleteContent(checkpoint.hash, checkpoint.size))
	require.False(t, peerServer.HasCompleteContent(checkpoint.hash, checkpoint.size))

	checkpoint.reportFrom(pausing, persistentVMRequest())
	peer.cacheManager.reconcileOnce(false)

	require.True(t, checkpoint.materializedOn(peer))
	events := locality.cacheEvents()
	require.Len(t, events, 1)
	require.Equal(t, peerServer.HostID(), events[0].LogicalHost)
	require.Equal(t, types.CacheAuditStatusMaterialized, events[0].Status)
}

// A checkpoint is reported while its archive is on its way into the cache of
// the host that reports it. The others wait for it there.
func TestCheckpointOnNoHostYetIsAwaitedBeforeTheOrigin(t *testing.T) {
	locality := newCheckpointLocality(t)
	pausing := locality.worker(locality.host("pausing-host", nil))
	peer := locality.worker(locality.host("peer-host", nil))
	checkpoint := newTestCheckpoint(t, "checkpoint")
	originReads := checkpointBucket(t, peer, checkpoint)
	checkpoint.reportFrom(pausing, persistentVMRequest())

	for range 3 {
		peer.cacheManager.reconcileOnce(false)
	}
	require.False(t, checkpoint.materializedOn(peer))
	require.Zero(t, originReads.Load())
	require.Empty(t, locality.cacheEvents(), "a checkpoint that is waited for is not an attempt at one")

	checkpoint.storeOn(t, pausing)
	peer.cacheManager.reconcileOnce(false)

	require.True(t, checkpoint.materializedOn(peer))
	require.Zero(t, originReads.Load())
}

func TestCheckpointNoHostStoresIsDownloadedFromTheOrigin(t *testing.T) {
	setCheckpointArchiveWait(t, 50*time.Millisecond)
	locality := newCheckpointLocality(t)
	pausing := locality.worker(locality.host("pausing-host", nil))
	peer := locality.worker(locality.host("peer-host", nil))
	checkpoint := newTestCheckpoint(t, "checkpoint")
	originReads := checkpointBucket(t, peer, checkpoint)
	checkpoint.reportFrom(pausing, persistentVMRequest())

	peer.cacheManager.reconcileOnce(false)
	require.False(t, checkpoint.materializedOn(peer))
	require.Zero(t, originReads.Load())

	require.Eventually(t, func() bool {
		peer.cacheManager.reconcileOnce(false)
		return checkpoint.materializedOn(peer)
	}, 5*time.Second, 20*time.Millisecond)
	require.Positive(t, originReads.Load())
	require.Empty(t, peer.cacheManager.checkpointAwaited)
}

// cutShortCacheHost answers for a cache host that has the content it is asked
// for and goes away while it sends it.
type cutShortCacheHost struct {
	*cache.Server
	streams atomic.Int32
}

func (h *cutShortCacheHost) GetContentStream(request *pb.CacheGetContentRequest, stream pb.Cache_GetContentStreamServer) error {
	h.streams.Add(1)
	if err := h.Server.GetContentStream(request, &cutShortContentStream{Cache_GetContentStreamServer: stream}); err != nil {
		return err
	}
	return status.Error(codes.Unavailable, "cache host is going away")
}

type cutShortContentStream struct {
	pb.Cache_GetContentStreamServer
	sent bool
}

func (s *cutShortContentStream) Send(response *pb.CacheGetContentResponse) error {
	if s.sent {
		return status.Error(codes.Unavailable, "cache host is going away")
	}
	s.sent = true
	return s.Cache_GetContentStreamServer.Send(&pb.CacheGetContentResponse{Ok: true, Content: response.Content[:len(response.Content)/2]})
}

// The origin has a checkpoint whether or not a host that had it still does.
func TestCheckpointCopyCutShortIsDownloadedFromTheOrigin(t *testing.T) {
	locality := newCheckpointLocality(t)
	holding := &cutShortCacheHost{}
	locality.host("holding-host", func(server *cache.Server) pb.CacheServer {
		holding.Server = server
		return holding
	})
	destination := locality.worker(locality.host("destination-host", nil))
	checkpoint := newTestCheckpoint(t, "checkpoint")
	originReads := checkpointBucket(t, destination, checkpoint)
	archive, err := os.Open(checkpoint.archive)
	require.NoError(t, err)
	_, _, err = holding.StoreReader(locality.ctx, archive, checkpoint.hash)
	require.NoError(t, err)
	require.NoError(t, archive.Close())
	checkpoint.reportFrom(destination, persistentVMRequest())

	destination.cacheManager.reconcileOnce(false)

	require.Positive(t, holding.streams.Load(), "the host that has the archive was not asked for it")
	require.Positive(t, originReads.Load())
	require.True(t, checkpoint.materializedOn(destination))
	events := locality.cacheEvents()
	require.Len(t, events, 1)
	require.Equal(t, types.CacheAuditStatusMaterialized, events[0].Status)
}

// With no origin to fall back to, a copy that is cut short is a failure of
// the host it came from, and is backed off like one.
func TestCheckpointCopyCutShortWithoutAnOriginIsAReplicaFailure(t *testing.T) {
	locality := newCheckpointLocality(t)
	holding := &cutShortCacheHost{}
	locality.host("holding-host", func(server *cache.Server) pb.CacheServer {
		holding.Server = server
		return holding
	})
	destination := locality.worker(locality.host("destination-host", nil))
	checkpoint := newTestCheckpoint(t, "checkpoint")
	archive, err := os.Open(checkpoint.archive)
	require.NoError(t, err)
	_, _, err = holding.StoreReader(locality.ctx, archive, checkpoint.hash)
	require.NoError(t, err)
	require.NoError(t, archive.Close())

	manager := destination.cacheManager
	status := manager.materializeCheckpoint(locality.ctx, manager.server, cache.RecentStub{WorkspaceID: "workspace", StubID: "vm"}, types.CacheRequiredContentItem{
		Kind:         types.CacheContentKindCheckpoint,
		Hash:         checkpoint.hash,
		SizeBytes:    checkpoint.size,
		CheckpointID: checkpoint.id,
	}, checkpoint.hash)

	require.Equal(t, types.CacheAuditStatusReplicaFailure, status)
	require.True(t, reconcileStatusIsFailure(status))
}

// A VM resumes from a checkpoint once. The hosts that have not copied it by
// then have no use for it.
func TestRestoredVMCheckpointIsNoLongerCopiedToPeers(t *testing.T) {
	locality := newCheckpointLocality(t)
	pausing := locality.worker(locality.host("pausing-host", nil))
	peer := locality.worker(locality.host("peer-host", nil))
	vm := persistentVMRequest()

	first := newTestCheckpoint(t, "first")
	first.storeOn(t, pausing)
	first.reportFrom(pausing, vm)
	pausing.retireRestoredCheckpoint(vm, first.id)
	pausing.cacheManager.reporter.flush()

	peer.cacheManager.reconcileOnce(false)
	require.False(t, first.materializedOn(peer))
	require.False(t, peer.cacheManager.server.HasCompleteContent(first.hash, first.size))
	require.NotContains(t, *peer.cacheManager.warmCheckpoints.Load(), first.id)
	require.Empty(t, locality.cacheEvents())

	// The VM pauses again. The record of its restore, published late by
	// another worker, withdraws nothing of the checkpoint it has now.
	second := newTestCheckpoint(t, "second")
	second.storeOn(t, pausing)
	second.reportFrom(pausing, vm)
	peer.retireRestoredCheckpoint(vm, first.id)
	peer.cacheManager.reporter.flush()

	peer.cacheManager.reconcileOnce(false)
	require.True(t, second.materializedOn(peer))
	require.Contains(t, *peer.cacheManager.warmCheckpoints.Load(), second.id)

	// A checkpoint that is restored again and again stays.
	function := &types.ContainerRequest{WorkspaceId: "workspace", StubId: "function"}
	pausing.retireRestoredCheckpoint(function, "function-checkpoint")
	require.Empty(t, pausing.cacheManager.reporter.pending)
}

func TestAttemptRestoreCheckpointRetiresPersistentVMCheckpoint(t *testing.T) {
	for _, test := range []struct {
		name    string
		request *types.ContainerRequest
		retired bool
	}{
		{"vm", persistentVMRequest(), true},
		{"function", &types.ContainerRequest{WorkspaceId: "workspace", StubId: "function"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			containerID := "container-restore-retire-" + test.name
			t.Cleanup(func() { _ = os.RemoveAll(filepath.Join("/tmp", containerID)) })
			events := &fakeEventRepo{}
			reporter := newTestReporter(events)
			checkpointRoot := t.TempDir()
			createMaterializedCheckpointForTest(t, filepath.Join(checkpointRoot, "checkpoint"))
			worker := &Worker{
				criuManager:        &startedCRIUManager{},
				backendRepoClient:  &fakeBackendRepoClient{},
				containerInstances: common.NewSafeMap[*ContainerInstance](),
				cacheManager:       &WorkerCacheManager{checkpointRoot: checkpointRoot, reporter: reporter},
			}
			worker.containerInstances.Set(containerID, &ContainerInstance{Id: containerID, Runtime: &mockRuntime{name: "runc"}})
			request := test.request
			request.ContainerId = containerID
			request.ConfigPath = filepath.Join(t.TempDir(), "config.json")
			request.Checkpoint = &types.Checkpoint{CheckpointId: "checkpoint", Status: string(types.CheckpointStatusAvailable)}

			_, restored, _, err := worker.attemptRestoreCheckpoint(
				context.Background(),
				request,
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				common.NewOutputWriter(func(string) {}),
				make(chan int, 1),
				make(chan int, 1),
			)
			require.NoError(t, err)
			require.True(t, restored)
			reporter.flush()

			if !test.retired {
				require.Empty(t, events.pushed)
				return
			}
			require.Len(t, events.pushed, 1)
			require.Equal(t, request.StubId, events.pushed[0].StubID)
			require.Equal(t, []types.CacheRequiredContentItem{{
				Kind:         types.CacheContentKindCheckpoint,
				CheckpointID: "checkpoint",
				Retired:      true,
			}}, events.pushed[0].Items)
		})
	}
}
