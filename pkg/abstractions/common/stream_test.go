package abstractions

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type testContainerStreamClient struct {
	started chan struct{}
	attach  chan struct{}
	log     common.OutputMsg
}

func (c *testContainerStreamClient) StreamLogsWithReady(_ context.Context, _ string, output chan common.OutputMsg, ready func()) error {
	close(c.started)
	<-c.attach
	output <- c.log
	ready()
	return nil
}

func (c *testContainerStreamClient) SyncWorkspace(context.Context, *pb.SyncContainerWorkspaceRequest) (*pb.SyncContainerWorkspaceResponse, error) {
	return &pb.SyncContainerWorkspaceResponse{}, nil
}

type testStreamContainerRepo struct {
	repository.ContainerRepository
	exitCode int
}

func (r testStreamContainerRepo) GetContainerExitCode(string) (int, error) {
	return r.exitCode, nil
}

func TestContainerStreamWaitsForLogAttachmentBeforeExit(t *testing.T) {
	output := make(chan common.OutputMsg, 1)
	exitEvents := make(chan common.KeyEvent, 1)
	clientResults := make(chan containerClientResult, 1)
	started := make(chan struct{})
	attach := make(chan struct{})
	exited := make(chan int32, 1)

	stream := &ContainerStream{
		sendCallback: func(message common.OutputMsg) error {
			output <- message
			return nil
		},
		exitCallback: func(exitCode int32) error {
			exited <- exitCode
			return nil
		},
		containerRepo: testStreamContainerRepo{exitCode: 7},
	}
	clientResults <- containerClientResult{client: &testContainerStreamClient{
		started: started,
		attach:  attach,
		log:     common.OutputMsg{Msg: "buffered output"},
	}}
	exitEvents <- common.KeyEvent{}

	done := make(chan error, 1)
	go func() {
		done <- stream.handleStreams(context.Background(), "container-1", make(chan common.OutputMsg, 1), exitEvents, clientResults)
	}()

	<-started
	select {
	case <-exited:
		t.Fatal("processed exit before log attachment")
	case <-time.After(20 * time.Millisecond):
	}

	close(attach)
	require.Equal(t, "buffered output", (<-output).Msg)
	require.Equal(t, int32(7), <-exited)
	require.NoError(t, <-done)
}
