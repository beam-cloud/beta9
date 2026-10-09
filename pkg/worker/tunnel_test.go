package worker

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTunnelKeepsSocketAndReplaysBytesAcrossAttachments(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	connections := make(chan struct{}, 2)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		connections <- struct{}{}
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		input, _ := io.ReadAll(conn)
		conn.Write(append([]byte("echo:"), input...))
	}()
	instance := &ContainerInstance{ContainerAddressMap: map[int32]string{2222: listener.Addr().String()}}
	defer instance.tunnels.close()
	server := &ContainerRuntimeServer{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	server.containerInstances.Set("vm", instance)
	request := &pb.ContainerTunnelRequest{ContainerId: "vm", SessionId: uuid.NewString(), Port: 2222, Create: true}
	_, err = server.ContainerTunnel(context.Background(), request)
	require.NoError(t, err)
	<-connections
	request.Input = []byte("stdin")
	for i := 0; i < 2; i++ {
		response, err := server.ContainerTunnel(context.Background(), request)
		require.NoError(t, err)
		require.Equal(t, uint64(5), response.InputOffset)
	}
	request.Input = nil
	request.InputOffset = 5
	request.InputEof = true
	for i := 0; i < 2; i++ {
		_, err = server.ContainerTunnel(context.Background(), request)
		require.NoError(t, err)
	}
	request.InputEof = false
	request.Read = true
	for i := 0; i < 2; i++ {
		response, err := server.ContainerTunnel(context.Background(), request)
		require.NoError(t, err)
		require.Equal(t, "echo:stdin", string(response.Output))
	}
	request.OutputOffset = 10
	response, err := server.ContainerTunnel(context.Background(), request)
	require.NoError(t, err)
	require.True(t, response.OutputEof)
	request.Close = true
	_, err = server.ContainerTunnel(context.Background(), request)
	require.NoError(t, err)
	_, err = server.ContainerTunnel(context.Background(), request)
	require.Equal(t, codes.NotFound, status.Code(err))
}
