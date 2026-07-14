package abstractions

import (
	"context"
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	flushLogsTimeout = 500 * time.Millisecond
)

type ContainerStreamOpts struct {
	SendCallback    func(o common.OutputMsg) error
	ExitCallback    func(exitCode int32) error
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
	containerRepo   repository.ContainerRepository
	tailscale       *network.Tailscale
	config          types.AppConfig
	keyEventManager *common.KeyEventManager
	syncQueue       chan *pb.SyncContainerWorkspaceRequest
}

type containerClientResult struct {
	client *common.ContainerClient
	err    error
}

func (l *ContainerStream) Stream(ctx context.Context, authInfo *auth.AuthInfo, containerId string) error {
	outputChan := make(chan common.OutputMsg, 1000)
	keyEventChan := make(chan common.KeyEvent, 1000)
	if err := l.keyEventManager.ListenForKey(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId), keyEventChan); err != nil {
		return err
	}

	clientResult := make(chan containerClientResult, 1)
	go func() {
		hostname, err := l.containerRepo.GetWorkerAddress(ctx, containerId)
		if err != nil {
			clientResult <- containerClientResult{err: err}
			return
		}

		conn, err := network.ConnectToBackend(ctx, hostname, 30*time.Second, l.tailscale, l.config.Tailscale, l.containerRepo)
		if err != nil {
			clientResult <- containerClientResult{err: err}
			return
		}

		client, err := common.NewContainerClient(hostname, authInfo.Token.Key, conn)
		clientResult <- containerClientResult{client: client, err: err}
	}()

	return l.handleStreams(ctx, containerId, outputChan, keyEventChan, clientResult)
}

func (l *ContainerStream) handleStreams(
	ctx context.Context,
	containerId string,
	outputChan chan common.OutputMsg,
	keyEventChan chan common.KeyEvent,
	clientResult chan containerClientResult,
) error {
	var lastMessage common.OutputMsg
	var client *common.ContainerClient
	var syncQueue <-chan *pb.SyncContainerWorkspaceRequest

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
			go client.StreamLogs(ctx, containerId, outputChan)
		case req := <-syncQueue:
			_, err := client.SyncWorkspace(ctx, req)
			if err != nil {
				continue
			}
		case o := <-outputChan:
			if err := l.sendCallback(o); err != nil {
				lastMessage = o
				break
			}

			if o.Done {
				lastMessage = o
				break _stream
			}
		case <-keyEventChan:
			exitCode, err := l.containerRepo.GetContainerExitCode(containerId)
			if err != nil {
				exitCode = -1
			}

			// After the container exits, flush remaining logs for up to flushLogsTimeout milliseconds
			flushLogsTimer := time.NewTimer(flushLogsTimeout)
			defer flushLogsTimer.Stop()

		_flush:
			for {
				select {
				case o, ok := <-outputChan:
					if !ok {
						break _flush
					}
					if err := l.sendCallback(o); err != nil {
						break _flush
					}
				case <-flushLogsTimer.C:
					break _flush
				}
			}

			if err := l.exitCallback(int32(exitCode)); err != nil {
				return err
			}
			return nil

		case <-ctx.Done():
			// This ensures when the sdk exits, the message printed is
			// that the container timed out.
			if err := l.exitCallback(0); err != nil {
				break _stream
			}
			return nil
		}
	}

	if !lastMessage.Success {
		return errors.New("failed")
	}

	return nil
}
