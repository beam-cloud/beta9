package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

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

func TestDockerSandboxShutdownScriptStopsInnerRuntime(t *testing.T) {
	script := dockerSandboxShutdownScript()

	require.Contains(t, script, "docker ps -q")
	require.Contains(t, script, "docker kill")
	require.Contains(t, script, "docker rm -f")
	require.Contains(t, script, "pkill -TERM dockerd")
	require.Contains(t, script, "pkill -KILL containerd")
	require.True(t, strings.HasSuffix(strings.TrimSpace(script), "exit 0"))
}
