package common

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestContainerClientWithDialerDoesNotBlockSharedCacheFill(t *testing.T) {
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	started := time.Now()
	client, err := NewContainerClientWithDialer(context.Background(), "route://worker", "token", dialer)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.Less(t, time.Since(started), 100*time.Millisecond)
}

type statusServer struct {
	pb.UnimplementedContainerServiceServer
}

func (statusServer) ContainerSandboxStatus(context.Context, *pb.ContainerSandboxStatusRequest) (*pb.ContainerSandboxStatusResponse, error) {
	return &pb.ContainerSandboxStatusResponse{Ok: true}, nil
}

func serveStatus(t *testing.T, addr string) *grpc.Server {
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	srv := grpc.NewServer()
	pb.RegisterContainerServiceServer(srv, statusServer{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return srv
}

// deadRouteProxy forwards connections to target. After cut, connections made
// before it stop forwarding and are closed on the client's next write, which
// is all a client learns when a worker dies behind the tailnet.
type deadRouteProxy struct {
	target string
	cut    atomic.Bool
}

func (p *deadRouteProxy) serve(client net.Conn) {
	backend, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	dead := &p.cut
	if dead.Load() {
		dead = new(atomic.Bool)
	}
	go func() {
		buf := make([]byte, 32<<10)
		for {
			n, err := backend.Read(buf)
			if err != nil {
				return
			}
			if !dead.Load() {
				_, _ = client.Write(buf[:n])
			}
		}
	}()
	buf := make([]byte, 32<<10)
	for {
		n, err := client.Read(buf)
		if err != nil || dead.Load() {
			_ = client.Close()
			_ = backend.Close()
			return
		}
		_, _ = backend.Write(buf[:n])
	}
}

// The cached channel to a worker survives the worker restarting under it.
func TestContainerClientWithDialerRetriesAcrossServerRestart(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	target := backend.Addr().String()
	require.NoError(t, backend.Close())
	srv := serveStatus(t, target)

	proxy := &deadRouteProxy{target: target}
	front, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = front.Close() })
	go func() {
		for {
			conn, err := front.Accept()
			if err != nil {
				return
			}
			go proxy.serve(conn)
		}
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", front.Addr().String())
	}
	client, err := NewContainerClientWithDialer(context.Background(), "route://worker", "token", dialer)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.SandboxStatusContext(context.Background(), "c", 1)
	require.NoError(t, err)

	proxy.cut.Store(true)
	srv.Stop()
	serveStatus(t, target)

	_, err = client.SandboxStatusContext(context.Background(), "c", 1)
	require.NoError(t, err)
}

func TestContainerClientUsesTLS(t *testing.T) {
	if !containerClientUsesTLS("127.0.0.1:443") {
		t.Fatal("port 443 should use TLS")
	}
	if !containerClientUsesTLS("[::1]:443") {
		t.Fatal("IPv6 port 443 should use TLS")
	}
	if containerClientUsesTLS("127.0.0.1:51443") {
		t.Fatal("port 51443 should not use TLS")
	}
}

type attachmentClientStream struct {
	pb.ContainerService_ContainerStreamLogsClient
	attach <-chan struct{}
}

func (s *attachmentClientStream) Header() (metadata.MD, error) {
	<-s.attach
	return nil, nil
}

func (s *attachmentClientStream) Recv() (*pb.ContainerLogEntry, error) {
	return nil, io.EOF
}

type attachmentContainerClient struct {
	pb.ContainerServiceClient
	stream pb.ContainerService_ContainerStreamLogsClient
}

func (c *attachmentContainerClient) ContainerStreamLogs(context.Context, *pb.ContainerStreamLogsRequest, ...grpc.CallOption) (pb.ContainerService_ContainerStreamLogsClient, error) {
	return c.stream, nil
}

func TestStreamLogsReadyWaitsForWorkerAttachment(t *testing.T) {
	attach := make(chan struct{})
	ready := make(chan struct{})
	client := &ContainerClient{client: &attachmentContainerClient{
		stream: &attachmentClientStream{attach: attach},
	}}
	done := make(chan error, 1)
	go func() {
		done <- client.StreamLogsWithReady(context.Background(), "container-id", make(chan OutputMsg), func() { close(ready) })
	}()

	select {
	case <-ready:
		t.Fatal("reported ready before worker attachment")
	case <-time.After(20 * time.Millisecond):
	}

	close(attach)
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("did not report worker attachment")
	}
	require.NoError(t, <-done)
}
