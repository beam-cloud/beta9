package worker

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProcessManagerSocketWaitsForLateListenerAndHonorsCancellation(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := reserved.Addr().(*net.TCPAddr)
	require.NoError(t, reserved.Close())
	endpoint := processManagerEndpoint{host: "127.0.0.1", port: address.Port}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- waitProcessManagerSocket(ctx, []processManagerEndpoint{endpoint}) }()
	select {
	case err := <-done:
		t.Fatalf("socket preflight returned before listener opened: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	listener, err := net.Listen("tcp", address.String())
	require.NoError(t, err)
	defer listener.Close()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("did not detect listening socket")
	}
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, waitProcessManagerSocket(ctx, []processManagerEndpoint{endpoint}), context.Canceled)
	require.Error(t, waitProcessManagerSocket(context.Background(), nil))
}
