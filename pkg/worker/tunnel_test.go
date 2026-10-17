package worker

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type guestTunnelRuntime struct {
	runtime.Runtime
	dial func(context.Context, string, uint32) (net.Conn, error)
}

func (r guestTunnelRuntime) DialGuestPort(ctx context.Context, id string, port uint32) (net.Conn, error) {
	return r.dial(ctx, id, port)
}

func TestTunnelDialsGuestOnlyForBoundPorts(t *testing.T) {
	host, guest := net.Pipe()
	defer guest.Close()
	instance := &ContainerInstance{Runtime: guestTunnelRuntime{dial: func(_ context.Context, id string, port uint32) (net.Conn, error) {
		require.Equal(t, "vm", id)
		require.Equal(t, uint32(3773), port)
		return host, nil
	}}}
	defer instance.tunnels.close()
	server := &ContainerRuntimeServer{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	server.containerInstances.Set("vm", instance)
	request := &pb.ContainerTunnelRequest{ContainerId: "vm", SessionId: uuid.NewString(), Port: 3773, Create: true}
	_, err := server.ContainerTunnel(context.Background(), request)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	instance.setContainerPortAddress(3773, "192.0.2.1:3773")
	_, err = server.ContainerTunnel(context.Background(), request)
	require.NoError(t, err)
	require.Same(t, host, instance.tunnels.sessions[request.SessionId].conn)
}

func TestTunnelUsesRequestJournalAcrossAttachments(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	accepted := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		close(accepted)
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		input, _ := io.ReadAll(conn)
		time.Sleep(30 * time.Millisecond)
		conn.Write(append([]byte("echo:"), input...))
	}()
	instance := &ContainerInstance{
		ContainerAddressMap: map[int32]string{2222: listener.Addr().String()},
		Runtime: guestTunnelRuntime{dial: func(context.Context, string, uint32) (net.Conn, error) {
			return nil, errors.New("older guest agent without tunnel support")
		}},
	}
	defer instance.tunnels.close()
	server := &ContainerRuntimeServer{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	server.containerInstances.Set("vm", instance)
	request := &pb.ContainerTunnelRequest{ContainerId: "vm", SessionId: uuid.NewString(), Port: 2222, Create: true}
	call := func(ctx context.Context, id, ack string) (*pb.ContainerTunnelResponse, error) {
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(common.RequestIDHeader, id, common.RequestAckHeader, ack))
		response, err := server.replaySandboxRequest(ctx, request, &grpc.UnaryServerInfo{FullMethod: "/container.ContainerService/ContainerTunnel"}, func(ctx context.Context, in interface{}) (interface{}, error) {
			return server.ContainerTunnel(ctx, in.(*pb.ContainerTunnelRequest))
		})
		if err != nil {
			return nil, err
		}
		return response.(*pb.ContainerTunnelResponse), nil
	}
	for i := 0; i < 2; i++ {
		_, err = server.ContainerTunnel(context.Background(), request)
		require.NoError(t, err)
	}
	<-accepted
	request.Create = false
	request.Input = []byte("stdin")
	writeID := uuid.NewString()
	for i := 0; i < 2; i++ {
		_, err = call(context.Background(), writeID, "")
		require.NoError(t, err)
	}
	request.Input = nil
	request.InputEof = true
	eofID := uuid.NewString()
	for i := 0; i < 2; i++ {
		_, err = call(context.Background(), eofID, writeID)
		require.NoError(t, err)
	}
	request.InputEof = false
	request.Read = true
	readID := uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err = call(ctx, readID, eofID)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	for i := 0; i < 2; i++ {
		response, err := call(context.Background(), readID, eofID)
		require.NoError(t, err)
		require.Equal(t, "echo:stdin", string(response.Output))
	}
	response, err := call(context.Background(), uuid.NewString(), readID)
	require.NoError(t, err)
	require.True(t, response.OutputEof)
	request.Read = false
	request.Close = true
	_, err = call(context.Background(), uuid.NewString(), "")
	require.NoError(t, err)
	_, err = call(context.Background(), uuid.NewString(), "")
	require.Equal(t, codes.NotFound, status.Code(err))
}
