package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	goproc "github.com/beam-cloud/goproc/pkg"
	goprocpb "github.com/beam-cloud/goproc/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDockerStartupCanceledClassification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{
			name: "wrapped grpc canceled setup status error",
			ctx:  context.Background(),
			err: fmt.Errorf(
				"cgroup setup status failed: %w",
				status.Error(codes.Canceled, "context canceled while waiting for connections to become ready"),
			),
			want: true,
		},
		{
			name: "wrapped grpc deadline setup status error",
			ctx:  context.Background(),
			err: fmt.Errorf(
				"cgroup setup status failed: %w",
				status.Error(codes.DeadlineExceeded, "deadline exceeded while waiting for connections to become ready"),
			),
			want: true,
		},
		{
			name: "shutdown context with ordinary process error",
			ctx:  ctx,
			err:  errors.New("process manager unavailable"),
			want: true,
		},
		{
			name: "direct context cancellation",
			ctx:  context.Background(),
			err:  context.Canceled,
			want: true,
		},
		{
			name: "shutdown transport message",
			ctx:  context.Background(),
			err:  errors.New("rpc error: code = Unavailable desc = transport is closing"),
			want: true,
		},
		{
			name: "real cgroup failure",
			ctx:  context.Background(),
			err:  errors.New("cgroup setup failed with exit code 1: stderr=\"permission denied\""),
			want: false,
		},
		{
			name: "nil error",
			ctx:  context.Background(),
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, dockerStartupCanceled(tt.ctx, tt.err))
		})
	}
}

func TestWorkerDockerStartupCanceledUsesContainerStopState(t *testing.T) {
	worker := &Worker{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	err := errors.New(`cgroup setup status failed: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 192.168.0.193:7111: connect: connection refused"`)

	worker.containerInstances.Set("stopping", &ContainerInstance{StopReason: types.StopContainerReasonUser})
	require.True(t, worker.dockerStartupCanceled(context.Background(), "stopping", err))

	worker.containerInstances.Set("active", &ContainerInstance{})
	require.False(t, worker.dockerStartupCanceled(context.Background(), "active", err))
}

func TestWorkerDockerStartupCanceledTreatsMissingContainerAsTeardown(t *testing.T) {
	worker := &Worker{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	err := errors.New("rpc error: code = Unavailable desc = connection refused")

	require.True(t, worker.dockerStartupCanceled(context.Background(), "already-cleaned-up", err))
}

type readyGoProcServer struct {
	goprocpb.UnimplementedGoProcServer
}

func (readyGoProcServer) Ready(context.Context, *goprocpb.ReadyRequest) (*goprocpb.ReadyResponse, error) {
	return &goprocpb.ReadyResponse{Ok: true}, nil
}

// The process manager comes up only after the runtime has exec'd it, which on a
// cold node can trail image materialization by tens of seconds. The wait must
// ride that out rather than give up on a clock.
func TestWaitForProcessManagerOutlivesSlowStartup(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close()) // port is now refusing connections

	server := grpc.NewServer()
	goprocpb.RegisterGoProcServer(server, readyGoProcServer{})
	defer server.Stop()
	go func() {
		time.Sleep(300 * time.Millisecond)
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return
		}
		_ = server.Serve(listener)
	}()

	instance := &ContainerInstance{
		ContainerAddressMap: map[int32]string{types.WorkerSandboxProcessManagerPort: address},
	}
	// The wait only returns on success or ctx; bound it so a server that never
	// comes up fails the test instead of hanging it.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, ready, stats := (&Worker{}).waitForProcessManager(ctx, "slow", instance)
	require.True(t, ready)
	require.NotNil(t, client)
	require.Greater(t, stats.Failures, 0)
	require.Equal(t, "unavailable", stats.LastClass)
	_ = client.Cleanup()
}

func TestWaitForProcessManagerStopsWithContainer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	instance := &ContainerInstance{ContainerIp: "127.0.0.1"}
	client, ready, stats := (&Worker{}).waitForProcessManager(ctx, "gone", instance)
	require.False(t, ready)
	require.Nil(t, client)
	require.Equal(t, context.Canceled.Error(), stats.LastError)
	require.Greater(t, stats.Attempts, 1)
}

func TestSandboxProcessManagerEndpointFallsBackToContainerIP(t *testing.T) {
	endpoints := sandboxProcessManagerEndpoints(&ContainerInstance{
		ContainerIp: "192.168.0.81",
	})

	require.Len(t, endpoints, 1)
	require.Equal(t, "192.168.0.81", endpoints[0].host)
	require.Equal(t, int(types.WorkerSandboxProcessManagerPort), endpoints[0].port)
}

func TestSandboxProcessManagerEndpointsIncludePublishedAddress(t *testing.T) {
	endpoints := sandboxProcessManagerEndpoints(&ContainerInstance{
		ContainerIp: "192.168.0.81",
		ContainerAddressMap: map[int32]string{
			types.WorkerSandboxProcessManagerPort: "10.42.0.163:35659",
		},
	})

	require.Len(t, endpoints, 2)
	require.Equal(t, "192.168.0.81", endpoints[0].host)
	require.Equal(t, int(types.WorkerSandboxProcessManagerPort), endpoints[0].port)
	require.Equal(t, "10.42.0.163", endpoints[1].host)
	require.Equal(t, 35659, endpoints[1].port)
}

func TestSandboxProcessManagerEndpointsUsePublishedAddressWhenContainerIPMissing(t *testing.T) {
	endpoints := sandboxProcessManagerEndpoints(&ContainerInstance{
		ContainerAddressMap: map[int32]string{
			types.WorkerSandboxProcessManagerPort: "10.42.0.163:35659",
		},
	})

	require.Len(t, endpoints, 1)
	require.Equal(t, "10.42.0.163", endpoints[0].host)
	require.Equal(t, 35659, endpoints[0].port)
}

func TestSandboxProcessManagerEndpointsIgnoreInvalidPublishedAddress(t *testing.T) {
	endpoints := sandboxProcessManagerEndpoints(&ContainerInstance{
		ContainerIp: "192.168.0.81",
		ContainerAddressMap: map[int32]string{
			types.WorkerSandboxProcessManagerPort: "route://not-a-host-port",
		},
	})

	require.Len(t, endpoints, 1)
	require.Equal(t, "192.168.0.81", endpoints[0].host)
	require.Equal(t, int(types.WorkerSandboxProcessManagerPort), endpoints[0].port)
}

// On dual-stack nodes the worker publishes the process manager port on the
// node's IPv6 address. goproc joins host and port with "%s:%d", so the host
// must arrive bracketed or grpc rejects the target with "too many colons" and
// the fallback endpoint is useless exactly when it is needed.
func TestSandboxProcessManagerEndpointsBracketIPv6ForDialing(t *testing.T) {
	endpoints := sandboxProcessManagerEndpoints(&ContainerInstance{
		ContainerIp: "192.168.0.81",
		ContainerAddressMap: map[int32]string{
			types.WorkerSandboxProcessManagerPort: "[2600:1f18:37a4:c02::1a9b]:44209",
		},
	})

	require.Len(t, endpoints, 2)
	require.Equal(t, "192.168.0.81", endpoints[0].dialHost())
	require.Equal(t, "2600:1f18:37a4:c02::1a9b", endpoints[1].host)
	require.Equal(t, "[2600:1f18:37a4:c02::1a9b]", endpoints[1].dialHost())
	require.Equal(t, 44209, endpoints[1].port)

	require.Equal(t, "[fd00::5]", processManagerEndpoint{host: "fd00::5", port: 7111}.dialHost())
	require.Equal(t, "[fd00::5]", processManagerEndpoint{host: "[fd00::5]", port: 7111}.dialHost())
	require.Equal(t, "10.42.0.163", processManagerEndpoint{host: "10.42.0.163", port: 7111}.dialHost())
	require.Equal(t, "sandbox.local", processManagerEndpoint{host: "sandbox.local", port: 7111}.dialHost())
}

// End-to-end version of the above: a real goproc server on an IPv6 address,
// reachable only through the host-mapped fallback endpoint. Before dialHost()
// this failed with "invalid target address ::1:<port> ... too many colons".
func TestNewProcessManagerClientReachesIPv6FallbackEndpoint(t *testing.T) {
	lis, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	srv := grpc.NewServer()
	goprocpb.RegisterGoProcServer(srv, &goproc.GoProcServer{})
	go srv.Serve(lis)
	defer srv.Stop()

	instance := &ContainerInstance{
		ContainerAddressMap: map[int32]string{
			types.WorkerSandboxProcessManagerPort: lis.Addr().String(),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := newProcessManagerClient(ctx, instance)
	require.NoError(t, err)
	defer client.Cleanup()
	require.NoError(t, client.ReadyContext(ctx))
}

func TestDockerSandboxStartupCleanupRemovesStalePidFiles(t *testing.T) {
	require.Equal(t, "rm -f /var/run/docker.pid /var/run/docker/containerd/containerd.pid", dockerSandboxStartupCleanupScript())
}

func TestDockerSandboxShutdownScriptPreservesInnerContainers(t *testing.T) {
	script := dockerSandboxShutdownScript()

	require.Contains(t, script, "docker ps -q")
	require.Contains(t, script, "docker stop -t 2")
	require.Contains(t, script, "docker kill")
	require.NotContains(t, script, "docker rm")
	require.Contains(t, script, "pkill -TERM dockerd")
	require.Contains(t, script, "pkill -KILL containerd")
	require.True(t, strings.HasSuffix(strings.TrimSpace(script), "exit 0"))
}

func TestStopDockerSandboxPreservesTerminalCheckpointState(t *testing.T) {
	instance := &ContainerInstance{
		Request: &types.ContainerRequest{DockerEnabled: true},
	}
	instance.initializeProcessManagerReadiness()
	instance.signalProcessManagerReadiness(true)
	instance.terminalCheckpointCreated.Store(true)

	// A nil process manager would panic if stopDockerSandbox attempted cleanup.
	(&Worker{}).stopDockerSandbox("container-1", instance, false)
}
