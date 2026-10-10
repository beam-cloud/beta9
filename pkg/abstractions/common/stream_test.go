package abstractions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type testContainerStreamClient struct {
	started  chan struct{}
	attach   chan struct{}
	log      common.OutputMsg
	delay    time.Duration
	trailing []common.OutputMsg
}

type pendingContainerStreamRepo struct {
	repository.ContainerRepository
	ready     chan struct{}
	attempted chan struct{}
}

func (r *pendingContainerStreamRepo) GetWorkerAddress(context.Context, string) (string, error) {
	select {
	case <-r.attempted:
	default:
		close(r.attempted)
	}
	select {
	case <-r.ready:
		return "worker.internal", nil
	default:
		return "", errors.New("worker address not assigned")
	}
}

func (r *pendingContainerStreamRepo) GetContainerState(string) (*types.ContainerState, error) {
	return &types.ContainerState{Status: types.ContainerStatusPending}, nil
}

func TestContainerStreamWaitsForPendingContainerWorker(t *testing.T) {
	repo := &pendingContainerStreamRepo{ready: make(chan struct{}), attempted: make(chan struct{})}
	stream := &ContainerStream{containerRepo: repo}
	type addressResult struct {
		host string
		err  error
	}
	result := make(chan addressResult, 1)

	go func() {
		host, err := stream.waitForWorkerAddress(context.Background(), "container-1")
		result <- addressResult{host: host, err: err}
	}()

	<-repo.attempted
	select {
	case <-result:
		t.Fatal("returned before the pending container received a worker")
	case <-time.After(20 * time.Millisecond):
	}

	close(repo.ready)
	got := <-result
	require.NoError(t, got.err)
	require.Equal(t, "worker.internal", got.host)
}

func (c *testContainerStreamClient) StreamLogsWithReady(_ context.Context, _ string, output chan common.OutputMsg, ready func()) error {
	close(c.started)
	<-c.attach
	output <- c.log
	ready()
	time.Sleep(c.delay)
	for _, message := range c.trailing {
		output <- message
	}
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

func TestContainerStreamDrainsBacklogThroughEOF(t *testing.T) {
	attach := make(chan struct{})
	close(attach)
	clients := make(chan containerClientResult, 1)
	clients <- containerClientResult{client: &testContainerStreamClient{
		started: make(chan struct{}), attach: attach, log: common.OutputMsg{Msg: "first"},
		delay: 750 * time.Millisecond, trailing: []common.OutputMsg{{Msg: "last"}},
	}}
	events := make(chan common.KeyEvent, 1)
	events <- common.KeyEvent{}
	var output []string
	exited := false
	stream := &ContainerStream{
		containerRepo: testStreamContainerRepo{},
		sendCallback:  func(message common.OutputMsg) error { output = append(output, message.Msg); return nil },
		exitCallback:  func(int32) error { exited = true; require.Equal(t, []string{"first", "last"}, output); return nil },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, stream.handleStreams(ctx, "container-1", make(chan common.OutputMsg, 1000), events, clients))
	require.True(t, exited)
}
