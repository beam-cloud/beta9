package common

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type readyContainerServer struct {
	pb.UnimplementedContainerServiceServer
}

func (readyContainerServer) ContainerSandboxStatus(
	context.Context,
	*pb.ContainerSandboxStatusRequest,
) (*pb.ContainerSandboxStatusResponse, error) {
	return &pb.ContainerSandboxStatusResponse{
		Ok:     true,
		Status: "running",
	}, nil
}

func TestContainerClientWithDialerDoesNotBlockSharedCacheFill(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	server := grpc.NewServer()
	pb.RegisterContainerServiceServer(server, readyContainerServer{})
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	releaseDial := make(chan struct{})
	dialStarted := make(chan struct{}, 1)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		select {
		case dialStarted <- struct{}{}:
		default:
		}
		select {
		case <-releaseDial:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		var netDialer net.Dialer
		return netDialer.DialContext(ctx, "tcp", listener.Addr().String())
	}

	type createResult struct {
		client *ContainerClient
		err    error
	}
	created := make(chan createResult, 1)
	go func() {
		client, err := NewContainerClientWithDialer(context.Background(), "route://worker", "token", dialer)
		created <- createResult{client: client, err: err}
	}()

	var client *ContainerClient
	select {
	case result := <-created:
		require.NoError(t, result.err)
		client = result.client
	case <-time.After(100 * time.Millisecond):
		t.Fatal("client creation blocked on the initial backend dial")
	}
	t.Cleanup(func() { _ = client.Close() })

	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("gRPC did not start the backend dial")
	}

	statusDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, err := client.SandboxStatusContext(ctx, "sandbox-1", 0)
		if err == nil && (resp == nil || !resp.Ok) {
			err = io.ErrUnexpectedEOF
		}
		statusDone <- err
	}()

	select {
	case err := <-statusDone:
		t.Fatalf("readiness returned before the backend route was released: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	releasedAt := time.Now()
	close(releaseDial)
	require.NoError(t, <-statusDone)
	require.Less(t, time.Since(releasedAt), 500*time.Millisecond)
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
