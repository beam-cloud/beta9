package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type adapterRelayTest struct {
	pb.UnimplementedWorkerRepositoryServiceServer
	peer        pb.CacheClient
	target      *pb.CachePeerTarget
	mu          sync.Mutex
	reads       []*pb.CachePeerReadRequest
	storeHeader *pb.CachePeerStoreRequest
	maxChunk    int
	stores      atomic.Int32
	synthetic   atomic.Bool
}

func (s *adapterRelayTest) valid(target *pb.CachePeerTarget) error {
	if !proto.Equal(target, s.target) {
		return status.Error(codes.PermissionDenied, "wrong immutable target")
	}
	return nil
}
func (s *adapterRelayTest) InspectCachePeer(ctx context.Context, in *pb.CachePeerInspectRequest) (*pb.CachePeerInspectResponse, error) {
	if err := s.valid(in.Target); err != nil {
		return nil, err
	}
	if in.GetState() != nil {
		response, err := s.peer.GetState(ctx, in.GetState())
		return &pb.CachePeerInspectResponse{Response: &pb.CachePeerInspectResponse_State{State: response}}, err
	}
	response, err := s.peer.HasContent(ctx, in.GetHas())
	return &pb.CachePeerInspectResponse{Response: &pb.CachePeerInspectResponse_Has{Has: response}}, err
}
func (s *adapterRelayTest) ReadCachePeer(in *pb.CachePeerReadRequest, stream pb.WorkerRepositoryService_ReadCachePeerServer) error {
	if err := s.valid(in.Target); err != nil {
		return err
	}
	s.mu.Lock()
	s.reads = append(s.reads, proto.Clone(in).(*pb.CachePeerReadRequest))
	s.mu.Unlock()
	if s.synthetic.Load() {
		if in.Length <= 0 || in.Length > peerCacheRangeBytes {
			return status.Error(codes.InvalidArgument, "unbounded synthetic range")
		}
		block := bytes.Repeat([]byte{0x5a}, peerCacheChunkBytes)
		for remaining := in.Length; remaining > 0; {
			n := min(remaining, int64(len(block)))
			if err := stream.Send(&pb.CacheGetContentResponse{Ok: true, Content: block[:n]}); err != nil {
				return err
			}
			remaining -= n
		}
		return nil
	}
	read, err := s.peer.GetContentStream(stream.Context(), &pb.CacheGetContentRequest{Hash: in.Hash, Offset: in.Offset, Length: in.Length})
	if err != nil {
		return err
	}
	for {
		frame, err := read.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		for len(frame.Content) > 0 {
			n := min(len(frame.Content), peerCacheChunkBytes)
			if err := stream.Send(&pb.CacheGetContentResponse{Ok: frame.Ok, Content: frame.Content[:n]}); err != nil {
				return err
			}
			frame.Content = frame.Content[n:]
		}
	}
}
func (s *adapterRelayTest) StoreCachePeer(stream pb.WorkerRepositoryService_StoreCachePeerServer) error {
	s.stores.Add(1)
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if err = s.valid(first.Target); err != nil {
		return err
	}
	s.mu.Lock()
	s.storeHeader = proto.Clone(first).(*pb.CachePeerStoreRequest)
	s.mu.Unlock()
	store, err := s.peer.StoreContent(stream.Context())
	if err != nil {
		return err
	}
	var size int64
	for frame := first; ; {
		if err = s.valid(frame.Target); err != nil {
			return err
		}
		if frame.Content != nil {
			s.mu.Lock()
			s.maxChunk = max(s.maxChunk, len(frame.Content.Content))
			s.mu.Unlock()
			size += int64(len(frame.Content.Content))
			if err = store.Send(frame.Content); err != nil {
				return err
			}
		}
		if frame.Finish {
			if size != first.ExpectedSize {
				return status.Error(codes.DataLoss, "incorrect declared size")
			}
			if _, err = stream.Recv(); err != io.EOF {
				return status.Error(codes.DataLoss, "missing final half-close")
			}
			response, err := store.CloseAndRecv()
			if err != nil {
				return err
			}
			if response.Hash != first.ExpectedHash {
				return status.Error(codes.DataLoss, "incorrect declared hash")
			}
			return stream.SendAndClose(response)
		}
		frame, err = stream.Recv()
		if err != nil {
			return status.Error(codes.DataLoss, "unfinished store")
		}
	}
}

func newAdapterRelayTest(t *testing.T) (*gatewayCachePeerClient, *adapterRelayTest, *cache.Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	server, err := cache.NewServerWithOptions(ctx, cache.Config{Global: cache.GlobalConfig{GRPCMessageSizeBytes: 8 << 20}, Server: cache.ServerConfig{DiskCacheDir: t.TempDir(), DiskCacheMaxUsagePct: 100, PageSizeBytes: 64 << 10, ObjectTtlS: 300}}, "own/pool", cache.WithServerMetadataStore(cache.NewMockCacheMetadataStore()), cache.WithServerHostID("peer"))
	require.NoError(t, err)
	t.Cleanup(func() { server.Close() })
	start := func(register func(*grpc.Server)) *grpc.ClientConn {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		service := grpc.NewServer()
		register(service)
		go service.Serve(listener)
		conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close(); service.Stop(); listener.Close() })
		return conn
	}
	peerConn := start(func(s *grpc.Server) { pb.RegisterCacheServer(s, server) })
	target := &pb.CachePeerTarget{WorkerId: "worker-local", LogicalHostId: "peer", RegistrationId: "lease", PoolName: "pool", Locality: "own/pool"}
	relay := &adapterRelayTest{peer: pb.NewCacheClient(peerConn), target: target}
	conn := start(func(s *grpc.Server) { pb.RegisterWorkerRepositoryServiceServer(s, relay) })
	return &gatewayCachePeerClient{client: pb.NewWorkerRepositoryServiceClient(conn), target: target}, relay, server
}

func TestCachePeerAdapterRoundTripWithRealCache(t *testing.T) {
	client, relay, server := newAdapterRelayTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	body := bytes.Repeat([]byte("typed-relay-pages"), 200000)
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	stream, err := client.StoreContent(ctx)
	require.NoError(t, err)
	spool := stream.(*cachePeerStoreClient)
	path := spool.file.Name()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	require.NoError(t, stream.Send(&pb.CacheStoreContentRequest{Content: body[:1700000]}))
	require.NoError(t, stream.Send(&pb.CacheStoreContentRequest{Content: body[1700000:]}))
	require.Zero(t, relay.stores.Load(), "no remote object is published before local sizing completes")
	stored, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, hash, stored.Hash)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
	require.True(t, server.HasCompleteContent(hash, int64(len(body))))
	state, err := client.GetState(ctx, &pb.CacheGetStateRequest{})
	require.NoError(t, err)
	require.Equal(t, cache.Version, state.Version)
	has, err := client.HasContent(ctx, &pb.CacheHasContentRequest{Hash: hash, ExpectedSize: int64(len(body))})
	require.NoError(t, err)
	require.True(t, has.Exists)
	for range 2 {
		response, err := client.GetContent(ctx, &pb.CacheGetContentRequest{Hash: hash, Offset: 65531, Length: 131077})
		require.NoError(t, err)
		require.Equal(t, body[65531:65531+131077], response.Content)
	}
	read, err := client.GetContentStream(ctx, &pb.CacheGetContentRequest{Hash: hash})
	require.NoError(t, err)
	var received []byte
	for {
		frame, err := read.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.LessOrEqual(t, len(frame.Content), peerCacheChunkBytes)
		received = append(received, frame.Content...)
	}
	require.Equal(t, body, received)
	relay.mu.Lock()
	readCount := len(relay.reads)
	header := relay.storeHeader
	maxChunk := relay.maxChunk
	relay.mu.Unlock()
	empty, err := client.GetContent(ctx, &pb.CacheGetContentRequest{Hash: hash, Length: 0})
	require.NoError(t, err)
	require.True(t, empty.Ok)
	require.Empty(t, empty.Content)
	relay.mu.Lock()
	require.Len(t, relay.reads, readCount)
	relay.mu.Unlock()
	require.Equal(t, int64(len(body)), header.ExpectedSize)
	require.Equal(t, hash, header.ExpectedHash)
	require.LessOrEqual(t, maxChunk, peerCacheChunkBytes)
	_, err = client.StoreContentFromSource(ctx, &pb.CacheStoreContentFromSourceRequest{Source: &pb.CacheSource{AccessKey: "must-stay-local", SecretKey: "must-stay-local"}})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestCachePeerAdapterSpoolCleanupOnCancellationAndInvalidMetadata(t *testing.T) {
	client, relay, _ := newAdapterRelayTest(t)
	for _, kind := range []string{"cancel", "metadata", "size"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := client.StoreContent(ctx)
			require.NoError(t, err)
			spool := stream.(*cachePeerStoreClient)
			path := spool.file.Name()
			switch kind {
			case "cancel":
				require.NoError(t, stream.Send(&pb.CacheStoreContentRequest{Content: []byte("partial")}))
				cancel()
			case "metadata":
				require.NoError(t, stream.Send(&pb.CacheStoreContentRequest{Content: []byte("partial")}))
				require.Error(t, stream.Send(&pb.CacheStoreContentRequest{CachePath: "changed"}))
			case "size":
				require.NoError(t, stream.Send(&pb.CacheStoreContentRequest{Content: []byte("short"), Metadata: &pb.CacheFSMetadata{Size: 100}}))
				_, err = stream.CloseAndRecv()
				require.Equal(t, codes.DataLoss, status.Code(err))
			}
			require.Eventually(t, func() bool { _, err := os.Stat(path); return os.IsNotExist(err) }, time.Second, 10*time.Millisecond)
		})
	}
	require.Zero(t, relay.stores.Load())
}

func TestCachePeerAdapterSplitsLargeReadsWithoutWholeObjectBuffering(t *testing.T) {
	client, relay, _ := newAdapterRelayTest(t)
	relay.synthetic.Store(true)
	length := int64(peerCacheRangeBytes + 257)
	stream, err := client.GetContentStream(context.Background(), &pb.CacheGetContentRequest{Hash: strings.Repeat("a", 64), Offset: 3, Length: length})
	require.NoError(t, err)
	var total int64
	digest := sha256.New()
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		digest.Write(frame.Content)
		total += int64(len(frame.Content))
	}
	require.Equal(t, length, total)
	want := sha256.New()
	_, err = io.CopyN(want, repeatCacheByte{}, length)
	require.NoError(t, err)
	require.Equal(t, want.Sum(nil), digest.Sum(nil))
	relay.mu.Lock()
	defer relay.mu.Unlock()
	require.Len(t, relay.reads, 2)
	require.Equal(t, int64(peerCacheRangeBytes), relay.reads[0].Length)
	require.Equal(t, int64(peerCacheRangeBytes+3), relay.reads[1].Offset)
	require.Equal(t, int64(257), relay.reads[1].Length)
}

type repeatCacheByte struct{}

func (repeatCacheByte) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0x5a
	}
	return len(p), nil
}

func TestCachePeerFactoryRequiresAgentIdentity(t *testing.T) {
	directory := &gatewayCacheHostDirectory{workerID: "worker", poolName: "pool"}
	host := &cache.Host{HostId: "peer", CacheRouteID: "route", RegistrationID: "lease", PoolName: "pool", Locality: "own/pool"}
	_, err := directory.NewCachePeerClient(context.Background(), host)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.False(t, directory.LocalOriginFillRequired())
	directory.agentWorker = true
	require.True(t, directory.LocalOriginFillRequired())
	_, err = directory.NewCachePeerClient(context.Background(), host)
	require.Equal(t, codes.Unavailable, status.Code(err))
}
