package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
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

func TestSandboxProcessManagerEndpointPrefersRegisteredAddress(t *testing.T) {
	endpoint, ok := sandboxProcessManagerEndpoint(&ContainerInstance{
		ContainerIp:        "192.168.0.81",
		ProcessManagerHost: "10.42.0.17",
		ProcessManagerPort: 36273,
	})
	require.True(t, ok)
	require.Equal(t, "10.42.0.17", endpoint.host)
	require.Equal(t, 36273, endpoint.port)
}

func TestSandboxProcessManagerEndpointsIncludesContainerIPFallback(t *testing.T) {
	endpoints := sandboxProcessManagerEndpoints(&ContainerInstance{
		ContainerIp:        "192.168.0.81",
		ProcessManagerHost: "10.42.0.17",
		ProcessManagerPort: 36273,
	})
	require.Len(t, endpoints, 2)
	require.Equal(t, processManagerEndpoint{host: "10.42.0.17", port: 36273}, endpoints[0])
	require.Equal(t, processManagerEndpoint{host: "192.168.0.81", port: int(types.WorkerSandboxProcessManagerPort)}, endpoints[1])
}

func TestSandboxProcessManagerEndpointFallsBackToContainerIP(t *testing.T) {
	endpoint, ok := sandboxProcessManagerEndpoint(&ContainerInstance{
		ContainerIp: "192.168.0.81",
	})
	require.True(t, ok)
	require.Equal(t, "192.168.0.81", endpoint.host)
	require.Equal(t, int(types.WorkerSandboxProcessManagerPort), endpoint.port)
}

func TestDockerSandboxShutdownScriptStopsInnerRuntime(t *testing.T) {
	script := dockerSandboxShutdownScript()

	require.Contains(t, script, "docker ps -q")
	require.Contains(t, script, "docker kill")
	require.Contains(t, script, "docker rm -f")
	require.Contains(t, script, "pkill -TERM dockerd")
	require.Contains(t, script, "pkill -KILL containerd")
	require.True(t, strings.HasSuffix(strings.TrimSpace(script), "exit 0"))
}
