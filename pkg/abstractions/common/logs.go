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
)

type LogStreamOpts struct {
	SendCallback    func(o common.OutputMsg) error
	ExitCallback    func(exitCode int32) error
	ContainerRepo   repository.ContainerRepository
	Tailscale       *network.Tailscale
	Config          types.AppConfig
	KeyEventManager *common.KeyEventManager
}

func NewLogStream(opts LogStreamOpts) (*LogStream, error) {
	return &LogStream{
		sendCallback:    opts.SendCallback,
		exitCallback:    opts.ExitCallback,
		containerRepo:   opts.ContainerRepo,
		tailscale:       opts.Tailscale,
		config:          opts.Config,
		keyEventManager: opts.KeyEventManager,
	}, nil
}

type LogStream struct {
	sendCallback    func(o common.OutputMsg) error
	exitCallback    func(exitCode int32) error
	containerRepo   repository.ContainerRepository
	tailscale       *network.Tailscale
	config          types.AppConfig
	keyEventManager *common.KeyEventManager
}

func (l *LogStream) Stream(ctx context.Context, authInfo *auth.AuthInfo, containerId string) error {
	hostname, err := l.containerRepo.GetWorkerAddress(ctx, containerId)
	if err != nil {
		return err
	}

	ctx = context.WithValue(ctx, "caller", "LogStream.Stream")
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
	return l.handleStreams(ctx, containerId, outputChan, keyEventChan)
}

func (l *LogStream) handleStreams(
	ctx context.Context,
	containerId string,
	outputChan chan common.OutputMsg,
	keyEventChan chan common.KeyEvent,
) error {

	var lastMessage common.OutputMsg

_stream:
	for {
		select {
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
