package common

import (
	"context"
	"sync/atomic"
	"testing"

	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRequestJournalSurvivesLostReply(t *testing.T) {
	var journal RequestJournal
	var calls atomic.Int32
	started, finish := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	request := &pb.ContainerSandboxExecRequest{ContainerId: "container", Cmd: "side-effect"}
	run := func(ctx context.Context) (interface{}, error) {
		calls.Add(1)
		close(started)
		<-finish
		require.NoError(t, ctx.Err())
		return &pb.ContainerSandboxExecResponse{Ok: true, Pid: 42, Stdout: "one result"}, nil
	}
	disconnected := make(chan error)
	go func() { _, err := journal.Do(ctx, "id", "exec", request, nil, run); disconnected <- err }()
	<-started
	cancel()
	require.ErrorIs(t, <-disconnected, context.Canceled)
	close(finish)
	response, err := journal.Do(context.Background(), "id", "exec", request, nil, run)
	require.NoError(t, err)
	require.Equal(t, int32(42), response.(*pb.ContainerSandboxExecResponse).Pid)
	require.Equal(t, int32(1), calls.Load())
	request.Cmd = "different-command"
	_, err = journal.Do(context.Background(), "id", "exec", request, nil, run)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestRequestJournalReplaysConsumedOutputUntilAcknowledged(t *testing.T) {
	var journal RequestJournal
	calls := 0
	run := func(context.Context) (interface{}, error) {
		calls++
		return &pb.ContainerSandboxStdoutResponse{Ok: true, Stdout: "consumed output"}, nil
	}
	request := &pb.ContainerSandboxStdoutRequest{Pid: 42}
	first, err := journal.Do(context.Background(), "first", "stdout", request, nil, run)
	require.NoError(t, err)
	replayed, err := journal.Do(context.Background(), "first", "stdout", request, nil, run)
	require.NoError(t, err)
	require.Equal(t, first, replayed)
	require.Equal(t, 1, calls)
	_, err = journal.Do(context.Background(), "next", "stdout", request, []string{"first"}, run)
	require.NoError(t, err)
	require.NotContains(t, journal.entries, "first")
}
