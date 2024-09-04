package container_tunnel

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v4"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type request struct {
	ctx  echo.Context
	done chan bool
}

type container struct {
	id               string
	address          string
	inFlightRequests int
}

type ConnectionBuffer struct {
	ctx                     context.Context
	httpClient              *http.Client
	tailscale               *network.Tailscale
	tsConfig                types.TailscaleConfig
	stubId                  string
	stubConfig              *types.StubConfigV1
	workspace               *types.Workspace
	rdb                     *common.RedisClient
	containerRepo           repository.ContainerRepository
	buffer                  *abstractions.RingBuffer[request]
	availableContainers     []container
	availableContainersLock sync.RWMutex

	length atomic.Int32
}

func NewConnectionBuffer(
	ctx context.Context,
	rdb *common.RedisClient,
	workspace *types.Workspace,
	stubId string,
	size int,
	containerRepo repository.ContainerRepository,
	stubConfig *types.StubConfigV1,
	tailscale *network.Tailscale,
	tsConfig types.TailscaleConfig,
) *ConnectionBuffer {
	b := &ConnectionBuffer{
		ctx:                 ctx,
		rdb:                 rdb,
		workspace:           workspace,
		stubId:              stubId,
		stubConfig:          stubConfig,
		buffer:              abstractions.NewRingBuffer[request](size),
		availableContainers: []container{},

		availableContainersLock: sync.RWMutex{},
		containerRepo:           containerRepo,
		httpClient:              &http.Client{},
		length:                  atomic.Int32{},

		tailscale: tailscale,
		tsConfig:  tsConfig,
	}

	go b.processRequests()

	return b
}

func (cb *ConnectionBuffer) ForwardRequest(ctx echo.Context, payload *types.TaskPayload, taskMessage *types.TaskMessage) error {
	done := make(chan bool)
	cb.buffer.Push(request{
		ctx:  ctx,
		done: done,
	})

	cb.length.Add(1)
	defer func() {
		cb.length.Add(-1)
	}()

	for {
		select {
		case <-cb.ctx.Done():
			return nil
		case <-done:
			return nil
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (cb *ConnectionBuffer) processRequests() {
	for {
		select {
		case <-cb.ctx.Done():
			return
		default:
			req, ok := cb.buffer.Pop()
			if !ok {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			if req.ctx.Request().Context().Err() != nil {
				// Context has been cancelled
				continue
			}

			// go cb.handleHttpRequest(req)
		}
	}
}

func (cb *ConnectionBuffer) Length() int {
	return int(cb.length.Load())
}
