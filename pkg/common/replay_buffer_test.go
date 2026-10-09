package common

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReplayBufferLostReplyAndBackpressure(t *testing.T) {
	buffer := NewReplayBuffer(4)
	require.NoError(t, buffer.Append(context.Background(), []byte("abcd")))
	written := make(chan error, 1)
	go func() { written <- buffer.Append(context.Background(), []byte("ef")) }()
	for i := 0; i < 2; i++ {
		data, eof, err := buffer.Read(context.Background(), 0, 4)
		require.NoError(t, err)
		require.False(t, eof)
		require.Equal(t, "abcd", string(data))
	}
	select {
	case <-written:
		t.Fatal("response delivery freed unacknowledged data")
	case <-time.After(20 * time.Millisecond):
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	data, _, err := buffer.Read(ctx, 4, 4)
	require.NoError(t, err)
	require.Equal(t, "ef", string(data))
	require.NoError(t, <-written)
	_, _, err = buffer.Read(ctx, 0, 4)
	require.Equal(t, codes.OutOfRange, status.Code(err))
	buffer.Close()
	_, eof, err := buffer.Read(ctx, 6, 4)
	require.NoError(t, err)
	require.True(t, eof)
}
