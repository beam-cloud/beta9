package common

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCClientRetryInterceptorStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	attempts := 0
	interceptor := GRPCClientRetryInterceptor(3, time.Hour)
	start := time.Now()
	err := interceptor(ctx, "/beam.test/Retry", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		attempts++
		return status.Error(codes.Unavailable, "gateway unavailable")
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)
	require.Less(t, time.Since(start), 100*time.Millisecond)
}
