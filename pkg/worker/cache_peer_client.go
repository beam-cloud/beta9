package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	peerCacheChunkBytes  = 1 << 20
	peerCacheRangeBytes  = 64 << 20
	peerCacheObjectBytes = 64 << 30
	peerCacheRPCTimeout  = 5 * time.Minute
)

var _ cache.PeerClientFactory = (*gatewayCacheHostDirectory)(nil)
var _ pb.CacheClient = (*gatewayCachePeerClient)(nil)

func (d *gatewayCacheHostDirectory) LocalOriginFillRequired() bool { return d != nil && d.agentWorker }

func (d *gatewayCacheHostDirectory) NewCachePeerClient(_ context.Context, host *cache.Host) (pb.CacheClient, error) {
	if d == nil || !d.agentWorker || d.workerID == "" || host == nil || host.CacheRouteID == "" ||
		host.HostId == "" || host.RegistrationID == "" || host.PoolName != d.poolName || host.Locality == "" {
		return nil, status.Error(codes.FailedPrecondition, "private cache peer identity is unavailable")
	}
	client := d.peerClient
	if client == nil {
		client = d.client
	}
	if client == nil {
		return nil, status.Error(codes.Unavailable, "cache relay connection is unavailable")
	}
	return &gatewayCachePeerClient{client: client, target: &pb.CachePeerTarget{
		WorkerId: d.workerID, LogicalHostId: host.HostId, RegistrationId: host.RegistrationID,
		PoolName: d.poolName, Locality: host.Locality,
	}}, nil
}

type gatewayCachePeerClient struct {
	client pb.WorkerRepositoryServiceClient
	target *pb.CachePeerTarget
}

func (c *gatewayCachePeerClient) GetState(ctx context.Context, in *pb.CacheGetStateRequest, opts ...grpc.CallOption) (*pb.CacheGetStateResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, peerCacheRPCTimeout)
	defer cancel()
	response, err := c.client.InspectCachePeer(ctx, &pb.CachePeerInspectRequest{Target: c.target, Request: &pb.CachePeerInspectRequest_State{State: in}}, opts...)
	if err != nil {
		return nil, err
	}
	if response.GetState() == nil {
		return nil, status.Error(codes.DataLoss, "cache relay returned no state")
	}
	return response.GetState(), nil
}

func (c *gatewayCachePeerClient) HasContent(ctx context.Context, in *pb.CacheHasContentRequest, opts ...grpc.CallOption) (*pb.CacheHasContentResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, peerCacheRPCTimeout)
	defer cancel()
	response, err := c.client.InspectCachePeer(ctx, &pb.CachePeerInspectRequest{Target: c.target, Request: &pb.CachePeerInspectRequest_Has{Has: in}}, opts...)
	if err != nil {
		return nil, err
	}
	if response.GetHas() == nil {
		return nil, status.Error(codes.DataLoss, "cache relay returned no content status")
	}
	return response.GetHas(), nil
}

func (*gatewayCachePeerClient) StoreContentFromSource(context.Context, *pb.CacheStoreContentFromSourceRequest, ...grpc.CallOption) (*pb.CacheStoreContentFromSourceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "cache origins must be filled on the requesting worker")
}
func (*gatewayCachePeerClient) StoreContentFromSourceWithLock(context.Context, *pb.CacheStoreContentFromSourceRequest, ...grpc.CallOption) (*pb.CacheStoreContentFromSourceWithLockResponse, error) {
	return nil, status.Error(codes.Unimplemented, "cache origins must be filled on the requesting worker")
}

func (c *gatewayCachePeerClient) GetContent(ctx context.Context, in *pb.CacheGetContentRequest, opts ...grpc.CallOption) (*pb.CacheGetContentResponse, error) {
	if in == nil || in.Length > peerCacheRangeBytes {
		return nil, status.Error(codes.ResourceExhausted, "unary cache relay read exceeds its bound")
	}
	if in.Offset < 0 || in.Length < 0 || in.Offset > math.MaxInt64-in.Length || len(in.Hash) != sha256.Size*2 || strings.ToLower(in.Hash) != in.Hash {
		return nil, status.Error(codes.InvalidArgument, "invalid cache relay range")
	}
	if _, err := hex.DecodeString(in.Hash); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid cache content identity")
	}
	// Cache's unary operation treats zero length as an empty range. Its
	// streaming operation separately uses zero to mean the whole object.
	if in.Length == 0 {
		return &pb.CacheGetContentResponse{Ok: true}, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.GetContentStream(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	response := &pb.CacheGetContentResponse{Ok: true}
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return response, nil
		}
		if err != nil {
			return nil, err
		}
		if !frame.Ok {
			return &pb.CacheGetContentResponse{Ok: false}, nil
		}
		if len(frame.Content) > peerCacheRangeBytes-len(response.Content) {
			return nil, status.Error(codes.ResourceExhausted, "unary cache relay read exceeds its bound")
		}
		response.Content = append(response.Content, frame.Content...)
	}
}

func (c *gatewayCachePeerClient) GetContentStream(ctx context.Context, in *pb.CacheGetContentRequest, opts ...grpc.CallOption) (pb.Cache_GetContentStreamClient, error) {
	if in == nil || in.Offset < 0 || in.Length < 0 || in.Length > peerCacheObjectBytes || in.Offset > math.MaxInt64-in.Length || (in.Length == 0 && in.Offset > math.MaxInt64-peerCacheObjectBytes) {
		return nil, status.Error(codes.InvalidArgument, "invalid cache relay range")
	}
	ctx, cancel := context.WithTimeout(ctx, peerCacheRPCTimeout)
	request := &pb.CachePeerReadRequest{Target: c.target, Hash: in.Hash, Offset: in.Offset, Length: min(in.Length, int64(peerCacheRangeBytes))}
	stream, err := c.client.ReadCachePeer(ctx, request, opts...)
	if err != nil {
		cancel()
		return nil, err
	}
	return &cachePeerReadClient{ClientStream: stream, client: c.client, target: c.target, ctx: ctx, cancel: cancel,
		stream: stream, request: request, opts: opts, remaining: in.Length, whole: in.Length == 0}, nil
}

type cachePeerReadClient struct {
	grpc.ClientStream
	client                        pb.WorkerRepositoryServiceClient
	target                        *pb.CachePeerTarget
	ctx                           context.Context
	cancel                        context.CancelFunc
	stream                        pb.WorkerRepositoryService_ReadCachePeerClient
	request                       *pb.CachePeerReadRequest
	opts                          []grpc.CallOption
	remaining, segmentRead, total int64
	whole, done                   bool
}

func (r *cachePeerReadClient) Recv() (*pb.CacheGetContentResponse, error) {
	if r.done {
		return nil, io.EOF
	}
	for {
		frame, err := r.stream.Recv()
		if errors.Is(err, io.EOF) {
			if !r.whole && r.segmentRead != r.request.Length {
				r.done = true
				r.cancel()
				return nil, status.Error(codes.DataLoss, "short cache relay read")
			}
			if r.whole || r.remaining == r.segmentRead {
				r.done = true
				r.cancel()
				return nil, io.EOF
			}
			r.remaining -= r.segmentRead
			r.request = &pb.CachePeerReadRequest{Target: r.target, Hash: r.request.Hash, Offset: r.request.Offset + r.segmentRead, Length: min(r.remaining, int64(peerCacheRangeBytes))}
			r.segmentRead = 0
			r.stream, err = r.client.ReadCachePeer(r.ctx, r.request, r.opts...)
			if err != nil {
				r.done = true
				r.cancel()
				return nil, err
			}
			r.ClientStream = r.stream
			continue
		}
		if err != nil {
			r.done = true
			r.cancel()
			return nil, err
		}
		if frame == nil || len(frame.Content) > peerCacheChunkBytes || int64(len(frame.Content)) > peerCacheObjectBytes-r.total || (!r.whole && int64(len(frame.Content)) > r.request.Length-r.segmentRead) {
			r.done = true
			r.cancel()
			return nil, status.Error(codes.DataLoss, "invalid cache relay read frame")
		}
		r.segmentRead += int64(len(frame.Content))
		r.total += int64(len(frame.Content))
		if !frame.Ok {
			r.done = true
			r.cancel()
		}
		return frame, nil
	}
}

func (r *cachePeerReadClient) RecvMsg(value any) error {
	frame, err := r.Recv()
	if err != nil {
		return err
	}
	out, ok := value.(*pb.CacheGetContentResponse)
	if !ok {
		return status.Error(codes.InvalidArgument, "unexpected cache read response")
	}
	proto.Reset(out)
	proto.Merge(out, frame)
	return nil
}

func (c *gatewayCachePeerClient) StoreContent(ctx context.Context, opts ...grpc.CallOption) (pb.Cache_StoreContentClient, error) {
	ctx, cancel := context.WithTimeout(ctx, peerCacheRPCTimeout)
	file, err := os.CreateTemp("", "beam-cache-peer-*")
	if err != nil {
		cancel()
		return nil, err
	}
	stream := &cachePeerStoreClient{ctx: ctx, cancel: cancel, client: c, opts: opts, file: file, hash: sha256.New()}
	context.AfterFunc(ctx, stream.cleanup)
	return stream, nil
}

// Cache's legacy upload API does not declare a size. Spool only on the worker
// to bound and hash it before any remote publication; the gateway never buffers
// a whole object and cancellation always removes this private temporary file.
type cachePeerStoreClient struct {
	ctx           context.Context
	cancel        context.CancelFunc
	client        *gatewayCachePeerClient
	opts          []grpc.CallOption
	mu            sync.Mutex
	file          *os.File
	hash          hash.Hash
	size          int64
	first, closed bool
	cachePath     string
	fileMetadata  *pb.CacheFSMetadata
	once          sync.Once
	response      *pb.CacheStoreContentResponse
	resultErr     error
}

func (s *cachePeerStoreClient) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		name := s.file.Name()
		s.file.Close()
		os.Remove(name)
		s.file = nil
	}
}

func (s *cachePeerStoreClient) Send(frame *pb.CacheStoreContentRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if s.closed || s.file == nil {
		return io.ErrClosedPipe
	}
	if frame == nil {
		s.cancel()
		return status.Error(codes.InvalidArgument, "cache store frame is required")
	}
	if !s.first {
		s.first = true
		s.cachePath = frame.CachePath
		if frame.Metadata != nil {
			s.fileMetadata = proto.Clone(frame.Metadata).(*pb.CacheFSMetadata)
		}
	} else if frame.CachePath != "" || frame.Metadata != nil {
		s.cancel()
		return status.Error(codes.InvalidArgument, "cache store metadata changed")
	}
	if int64(len(frame.Content)) > peerCacheObjectBytes-s.size {
		s.cancel()
		return status.Error(codes.ResourceExhausted, "cache relay store exceeds its bound")
	}
	if _, err := s.file.Write(frame.Content); err != nil {
		s.cancel()
		return err
	}
	s.hash.Write(frame.Content)
	s.size += int64(len(frame.Content))
	return nil
}

func (s *cachePeerStoreClient) CloseAndRecv() (*pb.CacheStoreContentResponse, error) {
	s.once.Do(func() { s.response, s.resultErr = s.finish() })
	return s.response, s.resultErr
}

func (s *cachePeerStoreClient) finish() (*pb.CacheStoreContentResponse, error) {
	defer s.cancel()
	defer s.cleanup()
	s.mu.Lock()
	s.closed = true
	if err := s.ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if s.file == nil {
		s.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	file, size, path := s.file, s.size, s.cachePath
	expected := hex.EncodeToString(s.hash.Sum(nil))
	fileMetadata := s.fileMetadata
	if fileMetadata != nil && (fileMetadata.Size != uint64(size) || (fileMetadata.Hash != "" && fileMetadata.Hash != expected)) {
		s.mu.Unlock()
		return nil, status.Error(codes.DataLoss, "cache upload metadata does not match its bytes")
	}
	_, err := file.Seek(0, io.SeekStart)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	stream, err := s.client.client.StoreCachePeer(s.ctx, s.opts...)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&pb.CachePeerStoreRequest{Target: s.client.target, ExpectedSize: size, ExpectedHash: expected, Content: &pb.CacheStoreContentRequest{CachePath: path, Metadata: fileMetadata}}); err != nil {
		return nil, err
	}
	buffer := make([]byte, peerCacheChunkBytes)
	for {
		n, readErr := file.Read(buffer)
		if n > 0 {
			if err := stream.Send(&pb.CachePeerStoreRequest{Target: s.client.target, Content: &pb.CacheStoreContentRequest{Content: append([]byte(nil), buffer[:n]...)}}); err != nil {
				return nil, err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	if err := stream.Send(&pb.CachePeerStoreRequest{Target: s.client.target, Finish: true}); err != nil {
		return nil, err
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	if response == nil || !response.Ok || response.Hash != expected {
		return nil, status.Error(codes.DataLoss, "cache relay store returned an inconsistent hash")
	}
	return response, nil
}

func (s *cachePeerStoreClient) Header() (metadata.MD, error) {
	return nil, status.Error(codes.Unimplemented, "cache upload headers are unavailable before completion")
}
func (*cachePeerStoreClient) Trailer() metadata.MD       { return nil }
func (s *cachePeerStoreClient) Context() context.Context { return s.ctx }
func (s *cachePeerStoreClient) CloseSend() error         { _, err := s.CloseAndRecv(); return err }
func (s *cachePeerStoreClient) SendMsg(value any) error {
	frame, ok := value.(*pb.CacheStoreContentRequest)
	if !ok {
		return status.Error(codes.InvalidArgument, "unexpected cache upload frame")
	}
	return s.Send(frame)
}
func (s *cachePeerStoreClient) RecvMsg(value any) error {
	response, err := s.CloseAndRecv()
	if err != nil {
		return err
	}
	out, ok := value.(*pb.CacheStoreContentResponse)
	if !ok {
		return status.Error(codes.InvalidArgument, "unexpected cache upload response")
	}
	proto.Reset(out)
	proto.Merge(out, response)
	return nil
}
