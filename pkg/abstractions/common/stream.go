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
	"github.com/rs/zerolog/log"
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
	SyncQueue       chan *pb.SyncContainerContentRequest
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
	syncQueue       chan *pb.SyncContainerContentRequest
}

func (l *ContainerStream) Stream(ctx context.Context, authInfo *auth.AuthInfo, containerId string) error {
	hostname, err := l.containerRepo.GetWorkerAddress(ctx, containerId)
	if err != nil {
		return err
	}

	conn, err := network.ConnectToHost(ctx, hostname, time.Second*30, l.tailscale, l.config.Tailscale)
	if err != nil {
		return err
	}

	client, err := common.NewRunCClient(hostname, authInfo.Token.Key, conn)
	if err != nil {
		return err
	}

	outputChan := make(chan common.OutputMsg, 1000)
	keyEventChan := make(chan common.KeyEvent, 1000)

	err = l.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId), keyEventChan)
	if err != nil {
		return err
	}

	go client.StreamLogs(ctx, containerId, outputChan)
	return l.handleStreams(ctx, client, containerId, outputChan, keyEventChan)
}

func (l *ContainerStream) handleStreams(
	ctx context.Context,
	client *common.RunCClient,
	containerId string,
	outputChan chan common.OutputMsg,
	keyEventChan chan common.KeyEvent,
) error {

	var lastMessage common.OutputMsg

_stream:
	for {
		select {
		case req := <-l.syncQueue:
			log.Info().Msgf("Received sync request: %v", req)

			// TODO: do the actual sync?
			status, err := client.Status(containerId)
			if err != nil {
				log.Error().Msgf("Error getting status: %v", err)
				break _stream
			}

			log.Info().Msgf("Status: %v", status)
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
				break _stream
			}

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
