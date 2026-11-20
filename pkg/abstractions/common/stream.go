package abstractions

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const flushLogsTimeout = 10 * time.Second

// workerAddressPollInterval is how long a stream waits before it asks again
// for the worker that has its container, after an answer that there is none.
var workerAddressPollInterval = time.Second

// A stream that waits for a worker reads what its container waits for after
// pendingReadInterval, and then after twice as long each time, up to
// maxPendingReadInterval. So a container that a worker takes at once costs no
// read, and one that waits for a machine costs one every few seconds. Tests
// shorten these.
var (
	pendingReadInterval    = time.Second
	maxPendingReadInterval = 5 * time.Second
)

// ErrWorkerUnreachable wraps a failure to connect to the worker running a container.
var ErrWorkerUnreachable = errors.New("worker unreachable")

type ContainerStreamOpts struct {
	SendCallback func(o common.OutputMsg) error
	ExitCallback func(exitCode int32) error
	// PendingCallback is optional. It is told what the container waits for
	// until a worker has it, each time that changes (see ContainerPending).
	PendingCallback func(pending ContainerPending) error
	ContainerRepo   repository.ContainerRepository
	Tailscale       *network.Tailscale
	Config          types.AppConfig
	KeyEventManager *common.KeyEventManager
	SyncQueue       chan *pb.SyncContainerWorkspaceRequest
}

func NewContainerStream(opts ContainerStreamOpts) (*ContainerStream, error) {
	return &ContainerStream{
		sendCallback:    opts.SendCallback,
		exitCallback:    opts.ExitCallback,
		pendingCallback: opts.PendingCallback,
		containerRepo:   opts.ContainerRepo,
		tailscale:       opts.Tailscale,
		config:          opts.Config,
		keyEventManager: opts.KeyEventManager,
		syncQueue:       opts.SyncQueue,
	}, nil
}

type ContainerStream struct {
	sendCallback    func(o common.OutputMsg) error
	exitCallback    func(exitCode int32) error
	pendingCallback func(pending ContainerPending) error
	containerRepo   repository.ContainerRepository
	tailscale       *network.Tailscale
	config          types.AppConfig
	keyEventManager *common.KeyEventManager
	syncQueue       chan *pb.SyncContainerWorkspaceRequest
}

// ContainerPending is what a container that has not started waits for, as a
// stream tells its client while there is no output to send: whether a worker
// is starting for it, or there is no capacity for it yet. The scheduler
// stores it on the container's state (types.ContainerPendingPhase).
type ContainerPending struct {
	Phase  types.ContainerPendingPhase
	Reason types.ContainerPendingReason
	// Waited is how long the container has been in this phase, where that
	// is known.
	Waited time.Duration
}

// WaitedSeconds is Waited in whole seconds, as the streams send it.
func (p ContainerPending) WaitedSeconds() uint32 {
	return uint32(max(p.Waited, 0) / time.Second)
}

// containerPending reads what the container waits for from its state.
func containerPending(state *types.ContainerState, now time.Time) ContainerPending {
	pending := ContainerPending{Phase: state.PendingPhase, Reason: state.PendingReason}
	if state.PendingSince > 0 {
		pending.Waited = now.Sub(time.Unix(state.PendingSince, 0))
	}
	return pending
}

type containerStreamClient interface {
	StreamLogsWithReady(context.Context, string, chan common.OutputMsg, func()) error
	SyncWorkspace(context.Context, *pb.SyncContainerWorkspaceRequest) (*pb.SyncContainerWorkspaceResponse, error)
}

type containerClientResult struct {
	client containerStreamClient
	err    error
}

func (l *ContainerStream) Stream(ctx context.Context, authInfo *auth.AuthInfo, containerId string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	outputChan := make(chan common.OutputMsg, 1000)
	keyEventChan := make(chan common.KeyEvent, 1000)
	if err := l.keyEventManager.ListenForKey(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId), keyEventChan); err != nil {
		return err
	}

	clientResult := make(chan containerClientResult, 1)
	go func() {
		hostname, err := l.waitForWorkerAddress(ctx, containerId)
		if err != nil {
			clientResult <- containerClientResult{err: err}
			return
		}

		conn, err := network.ConnectToBackend(ctx, hostname, 30*time.Second, l.tailscale, l.config.Tailscale, l.containerRepo)
		if err != nil {
			clientResult <- containerClientResult{err: fmt.Errorf("%w: %w", ErrWorkerUnreachable, err)}
			return
		}

		client, err := common.NewContainerClient(hostname, conn)
		clientResult <- containerClientResult{client: client, err: err}
	}()

	return l.handleStreams(ctx, containerId, outputChan, keyEventChan, clientResult)
}

// waitForWorkerAddress waits until a worker has the container, and returns
// where to reach it. Until then it tells the client what the container waits
// for, each time that changes.
func (l *ContainerStream) waitForWorkerAddress(ctx context.Context, containerID string) (string, error) {
	// What this starts ends with it.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ticker := time.NewTicker(workerAddressPollInterval)
	defer ticker.Stop()

	// told is what the client was told last.
	var told ContainerPending
	tell := func(pending ContainerPending) error {
		if l.pendingCallback == nil || pending.Phase == "" || (pending.Phase == told.Phase && pending.Reason == told.Reason) {
			return nil
		}
		told = pending
		return l.pendingCallback(pending)
	}

	// A stream with nobody to tell does not read what its container waits
	// for.
	readIn := pendingReadInterval
	var read *time.Timer
	var readNow <-chan time.Time
	if l.pendingCallback != nil {
		read = time.NewTimer(readIn)
		defer read.Stop()
		readNow = read.C
	}

	for {
		// The repository answers when a worker has the container, when the
		// scheduler gives up on it, and after some minutes of neither. What
		// the container waits for is read while it does.
		found := make(chan string, 1)
		go func() {
			hostname, _ := l.containerRepo.GetWorkerAddress(ctx, containerID)
			found <- hostname
		}()

		hostname, answered := "", false
		for !answered {
			select {
			case hostname = <-found:
				answered = true
			case <-readNow:
				// A state that cannot be read says nothing. If the container
				// is gone, the answer about its worker is what ends the wait.
				if state, err := l.containerRepo.GetContainerState(containerID); err == nil && state != nil {
					if err := tell(containerPending(state, time.Now())); err != nil {
						return "", err
					}
				}
				readIn = min(2*readIn, maxPendingReadInterval)
				read.Reset(readIn)
			}
		}

		if hostname != "" {
			// A client that was told the container waits for a worker is
			// told that one has it. From here the container's own output
			// says how it gets on.
			if told.Phase != "" {
				if err := tell(ContainerPending{Phase: types.ContainerPendingStarting}); err != nil {
					return "", err
				}
			}
			return hostname, nil
		}

		state, stateErr := l.containerRepo.GetContainerState(containerID)
		if stateErr != nil || state == nil || state.Status == types.ContainerStatusStopping {
			return "", fmt.Errorf("container stopped before it was assigned to a worker")
		}
		if err := tell(containerPending(state, time.Now())); err != nil {
			return "", err
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

func (l *ContainerStream) handleStreams(
	ctx context.Context,
	containerId string,
	outputChan chan common.OutputMsg,
	keyEventChan chan common.KeyEvent,
	clientResult chan containerClientResult,
) error {
	var lastMessage common.OutputMsg
	var client containerStreamClient
	var exitEvents <-chan common.KeyEvent
	var logStreamReady <-chan struct{}
	var syncQueue <-chan *pb.SyncContainerWorkspaceRequest
	logErrors := make(chan error, 1)
	logsFinished := false

_stream:
	for {
		select {
		case result := <-clientResult:
			clientResult = nil
			if result.err != nil {
				return result.err
			}
			client = result.client
			syncQueue = l.syncQueue
			ready := make(chan struct{})
			logStreamReady = ready
			go func() {
				readyCallback := func() { close(ready) }
				logErrors <- client.StreamLogsWithReady(ctx, containerId, outputChan, readyCallback)
			}()
		case err := <-logErrors:
			logErrors = nil
			if err != nil {
				return err
			}
			logsFinished = true
			// EOF is durable evidence that this log buffer closed. A lost exit
			// notification must not leave an invocation waiting forever.
			for len(outputChan) > 0 {
				if err := l.sendCallback(<-outputChan); err != nil {
					return err
				}
			}
			exitCode, err := l.containerRepo.GetContainerExitCode(containerId)
			if err != nil {
				return status.Error(codes.Unavailable, "container exit status is not yet available")
			}
			return l.exitCallback(int32(exitCode))
		case <-logStreamReady:
			logStreamReady = nil
			exitEvents = keyEventChan
		case req := <-syncQueue:
			_, err := client.SyncWorkspace(ctx, req)
			if err != nil {
				continue
			}
		case o := <-outputChan:
			if err := l.sendCallback(o); err != nil {
				return err
			}

			if o.Done {
				lastMessage = o
				break _stream
			}
		case <-exitEvents:
			exitCode, err := l.containerRepo.GetContainerExitCode(containerId)
			if err != nil {
				exitCode = -1
			}

			// Drain through EOF before delivering the result. A completed
			// invocation may have accumulated logs during a gateway outage.
			flushLogsTimer := time.NewTimer(flushLogsTimeout)
			defer flushLogsTimer.Stop()

			for !logsFinished || len(outputChan) > 0 {
				select {
				case o := <-outputChan:
					if err := l.sendCallback(o); err != nil {
						return err
					}
				case err := <-logErrors:
					logErrors = nil
					if err != nil {
						return err
					}
					logsFinished = true
				case <-flushLogsTimer.C:
					return status.Error(codes.Unavailable, "container logs are still flushing")
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			if err := l.exitCallback(int32(exitCode)); err != nil {
				return err
			}
			return nil

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if !lastMessage.Success {
		return errors.New("failed")
	}

	return nil
}
