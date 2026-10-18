package worker

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
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
			return nil, microvm.ErrPortUnavailable
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

func TestSlowTunnelDialDoesNotBlockOtherSessions(t *testing.T) {
	var sessions tunnelSessions
	defer sessions.close()
	slow, peer := net.Pipe()
	defer peer.Close()
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	done := make(chan error, 1)
	go func() {
		_, err := sessions.get(context.Background(), &pb.ContainerTunnelRequest{SessionId: "slow", Port: 3773, Create: true}, func(context.Context) (net.Conn, error) { close(started); <-release; return slow, nil })
		done <- err
	}()
	<-started
	fast, other := net.Pipe()
	defer other.Close()
	ready := make(chan error, 1)
	go func() {
		_, err := sessions.get(context.Background(), &pb.ContainerTunnelRequest{SessionId: "fast", Port: 8080, Create: true}, func(context.Context) (net.Conn, error) { return fast, nil })
		ready <- err
	}()
	select {
	case err := <-ready:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("a slow dial blocked another tunnel")
	}
	unblock()
	require.NoError(t, <-done)
}

func TestConcurrentTunnelAttachmentsShareDial(t *testing.T) {
	var sessions tunnelSessions
	defer sessions.close()
	conn, peer := net.Pipe()
	defer peer.Close()
	start := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var calls atomic.Int32
	request := &pb.ContainerTunnelRequest{SessionId: "shared", Port: 3773, Create: true}
	dial := func(context.Context) (net.Conn, error) { calls.Add(1); close(start); <-release; return conn, nil }
	results := make(chan *tunnelSession, 2)
	errs := make(chan error, 2)
	attach := func() {
		session, err := sessions.get(context.Background(), request, dial)
		results <- session
		errs <- err
	}
	go attach()
	<-start
	go attach()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := sessions.get(ctx, request, dial)
	require.ErrorIs(t, err, context.Canceled)
	_, err = sessions.get(context.Background(), &pb.ContainerTunnelRequest{SessionId: "shared", Port: 8080, Create: true}, dial)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	unblock()
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	require.Same(t, <-results, <-results)
	require.Equal(t, int32(1), calls.Load())
}

func TestTunnelCloseCancelsPendingDialsAndReservesCapacity(t *testing.T) {
	var sessions tunnelSessions
	defer sessions.close()
	started := make(chan struct{}, 8)
	done := make(chan error, 8)
	for range 8 {
		go func() {
			_, err := sessions.get(context.Background(), &pb.ContainerTunnelRequest{SessionId: uuid.NewString(), Port: 3773, Create: true}, func(ctx context.Context) (net.Conn, error) {
				started <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			})
			done <- err
		}()
	}
	for range 8 {
		<-started
	}
	_, err := sessions.get(context.Background(), &pb.ContainerTunnelRequest{SessionId: "extra", Port: 8080, Create: true}, func(context.Context) (net.Conn, error) {
		t.Error("pending dials did not reserve capacity")
		return nil, errors.New("unexpected dial")
	})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	sessions.close()
	for range 8 {
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("container close did not cancel its pending dials")
		}
	}
}

func TestGuestTunnelTransportFailureDoesNotDialNIC(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	instance := &ContainerInstance{Runtime: guestTunnelRuntime{dial: func(context.Context, string, uint32) (net.Conn, error) { return nil, context.DeadlineExceeded }}}
	defer instance.tunnels.close()
	instance.setContainerPortAddress(3773, listener.Addr().String())
	server := &ContainerRuntimeServer{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	server.containerInstances.Set("vm", instance)
	_, err = server.ContainerTunnel(context.Background(), &pb.ContainerTunnelRequest{ContainerId: "vm", SessionId: uuid.NewString(), Port: 3773, Create: true})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestTunnelCloseClosesLateDialResult(t *testing.T) {
	var sessions tunnelSessions
	defer sessions.close()
	conn, peer := net.Pipe()
	defer peer.Close()
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := sessions.get(context.Background(), &pb.ContainerTunnelRequest{SessionId: "late", Port: 3773, Create: true}, func(context.Context) (net.Conn, error) { close(started); <-release; return conn, nil })
		done <- err
	}()
	<-started
	sessions.close()
	close(release)
	require.Equal(t, codes.NotFound, status.Code(<-done))
	peer.SetReadDeadline(time.Now().Add(time.Second))
	_, err := peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	require.Empty(t, sessions.sessions)
}
