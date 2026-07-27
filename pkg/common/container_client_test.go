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
