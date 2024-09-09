package container_app

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type tcpConnection struct {
	src  net.Conn
	dst  net.Conn
	done chan bool
}

type container struct {
	id               string
	address          string
	inFlightRequests int
}

type ConnectionBuffer struct {
	ctx                     context.Context
	tailscale               *network.Tailscale
	tsConfig                types.TailscaleConfig
	stubId                  string
	stubConfig              *types.StubConfigV1
	workspace               *types.Workspace
	rdb                     *common.RedisClient
	containerRepo           repository.ContainerRepository
	buffer                  *abstractions.RingBuffer[tcpConnection]
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
		ctx:                     ctx,
		rdb:                     rdb,
		workspace:               workspace,
		stubId:                  stubId,
		stubConfig:              stubConfig,
		buffer:                  abstractions.NewRingBuffer[tcpConnection](size),
		availableContainers:     []container{},
		availableContainersLock: sync.RWMutex{},
		containerRepo:           containerRepo,
		length:                  atomic.Int32{},
		tailscale:               tailscale,
		tsConfig:                tsConfig,
	}

	go b.processConnections()

	return b
}

func (cb *ConnectionBuffer) ForwardConnection(src, dst net.Conn) error {
	done := make(chan bool)
	cb.buffer.Push(tcpConnection{
		src:  src,
		dst:  dst,
		done: done,
	})

	cb.length.Add(1)
	defer cb.length.Add(-1)

	select {
	case <-cb.ctx.Done():
		return nil
	case <-done:
		return nil
	}
}

func (cb *ConnectionBuffer) processConnections() {
	for {
		select {
		case <-cb.ctx.Done():
			return
		default:
			tcpConn, ok := cb.buffer.Pop()
			if !ok {
				time.Sleep(time.Millisecond * 10)
				continue
			}

			if tcpConn.src == nil || tcpConn.dst == nil {
				continue
			}

			go cb.handleTCPConnection(tcpConn)
		}
	}
}

func (cb *ConnectionBuffer) handleTCPConnection(tcpConn tcpConnection) {
	defer func() {
		tcpConn.src.Close()
		tcpConn.dst.Close()
		close(tcpConn.done)
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	srcReader := newBufferedReader(tcpConn.src)
	dstWriter := newBufferedWriter(tcpConn.dst)

	dstReader := newBufferedReader(tcpConn.dst)
	srcWriter := newBufferedWriter(tcpConn.src)

	go func() {
		defer wg.Done()
		cb.copyData(srcReader, dstWriter)
	}()
	go func() {
		defer wg.Done()
		cb.copyData(dstReader, srcWriter)
	}()

	wg.Wait()
}

func (cb *ConnectionBuffer) copyData(src io.Reader, dst io.Writer) {
	_, err := io.CopyBuffer(dst, src, make([]byte, 32*1024))
	if err != nil {
	}
}

func newBufferedReader(conn net.Conn) io.Reader {
	return io.LimitReader(conn, 64*1024)
}

func newBufferedWriter(conn net.Conn) io.Writer {
	return io.MultiWriter(conn)
}

func (cb *ConnectionBuffer) Length() int {
	return int(cb.length.Load())
}
