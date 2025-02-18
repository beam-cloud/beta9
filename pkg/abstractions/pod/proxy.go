package pod

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	bufferProcessingInterval      time.Duration = time.Millisecond * 100
	containerDiscoveryInterval    time.Duration = time.Millisecond * 500
	containerDialTimeoutDurationS time.Duration = time.Second * 30
	connectionBufferSize          int           = 1024 * 4 // 4KB
	connectionKeepAliveInterval   time.Duration = time.Second * 1
)

type container struct {
	id          string
	address     string
	connections int
}

type connection struct {
	ctx  echo.Context
	done chan struct{}
}

type PodProxyBuffer struct {
	ctx                     context.Context
	rdb                     *common.RedisClient
	workspace               *types.Workspace
	stubId                  string
	size                    int
	containerRepo           repository.ContainerRepository
	keyEventManager         *common.KeyEventManager
	stubConfig              *types.StubConfigV1
	httpClient              *http.Client
	tailscale               *network.Tailscale
	tsConfig                types.TailscaleConfig
	availableContainers     []container
	availableContainersLock sync.RWMutex
	buffer                  *abstractions.RingBuffer[*connection]
}

func NewPodProxyBuffer(ctx context.Context,
	rdb *common.RedisClient,
	workspace *types.Workspace,
	stubId string,
	size int,
	containerRepo repository.ContainerRepository,
	keyEventManager *common.KeyEventManager,
	stubConfig *types.StubConfigV1,
	tailscale *network.Tailscale,
	tsConfig types.TailscaleConfig,
) *PodProxyBuffer {
	pb := &PodProxyBuffer{
		ctx:                     ctx,
		rdb:                     rdb,
		workspace:               workspace,
		stubId:                  stubId,
		size:                    size,
		containerRepo:           containerRepo,
		keyEventManager:         keyEventManager,
		httpClient:              &http.Client{},
		stubConfig:              stubConfig,
		tailscale:               tailscale,
		tsConfig:                tsConfig,
		availableContainers:     []container{},
		availableContainersLock: sync.RWMutex{},
		buffer:                  abstractions.NewRingBuffer[*connection](size),
	}

	go pb.discoverContainers()
	go pb.processBuffer()

	return pb
}

func (pb *PodProxyBuffer) ForwardRequest(ctx echo.Context) error {
	done := make(chan struct{})
	req := &connection{
		ctx:  ctx,
		done: done,
	}
	pb.buffer.Push(req, false)

	for {
		select {
		case <-pb.ctx.Done():
			return nil
		case <-done:
			return nil
		}
	}
}

func (pb *PodProxyBuffer) processBuffer() {
	for {
		select {
		case <-pb.ctx.Done():
			return
		default:
			if len(pb.availableContainers) == 0 {
				time.Sleep(bufferProcessingInterval)
				continue
			}

			conn, ok := pb.buffer.Pop()
			if !ok {
				time.Sleep(bufferProcessingInterval)
				continue
			}

			if conn.ctx.Request().Context().Err() != nil {
				continue
			}

			go pb.handleConnection(conn)
		}
	}
}

func (pb *PodProxyBuffer) handleConnection(conn *connection) {
	pb.availableContainersLock.RLock()

	if len(pb.availableContainers) == 0 {
		pb.buffer.Push(conn, true)
		pb.availableContainersLock.RUnlock()
		return
	}

	container := pb.availableContainers[0]
	pb.availableContainersLock.RUnlock()

	// Capture headers and other metadata
	request := conn.ctx.Request()

	// Hijack the connection
	hijacker, ok := conn.ctx.Response().Writer.(http.Hijacker)
	if !ok {
		conn.ctx.String(http.StatusInternalServerError, "Failed to hijack connection")
		return
	}

	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		conn.ctx.String(http.StatusInternalServerError, "Failed to hijack connection")
		return
	}
	defer clientConn.Close()

	containerConn, err := network.ConnectToHost(request.Context(), container.address, containerDialTimeoutDurationS, pb.tailscale, pb.tsConfig)
	if err != nil {
		log.Error().Msgf("Error dialing pod container %s: %s", container.address, err.Error())
		return
	}
	defer containerConn.Close()

	abstractions.SetConnOptions(containerConn, true, connectionKeepAliveInterval)

	err = pb.incrementConnections(container.id)
	if err != nil {
		pb.buffer.Push(conn, true)
		return
	}
	defer pb.decrementConnections(container.id)

	// Ensure the request URL is correctly formatted for the proxy.
	// We'll set container.address to the Host and put subPath into the Path field.
	request.URL.Scheme = "http"
	request.URL.Host = container.address

	// Get subPath, ensure it starts with a slash, and assign it to the path portion.
	subPath := conn.ctx.Param("subPath")
	if subPath != "" && subPath[0] != '/' {
		subPath = "/" + subPath
	}
	request.URL.Path = subPath

	// Use the http.Request's Write method to send the request
	if err := request.Write(containerConn); err != nil {
		return
	}

	// Start proxying data
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		abstractions.ProxyConn(containerConn, clientConn, conn.done, connectionBufferSize)
	}()

	go func() {
		defer wg.Done()
		abstractions.ProxyConn(clientConn, containerConn, conn.done, connectionBufferSize)
	}()

	wg.Wait()

	select {
	case <-conn.done:
		return
	case <-request.Context().Done():
		return
	case <-pb.ctx.Done():
		return
	}
}

func (pb *PodProxyBuffer) discoverContainers() {
	for {
		select {
		case <-pb.ctx.Done():
			return
		default:
			containerStates, err := pb.containerRepo.GetActiveContainersByStubId(pb.stubId)
			if err != nil {
				continue
			}

			var wg sync.WaitGroup
			availableContainersChan := make(chan container, len(containerStates))

			for _, containerState := range containerStates {
				wg.Add(1)

				go func(cs types.ContainerState) {
					defer wg.Done()
					if cs.Status != types.ContainerStatusRunning {
						return
					}

					containerAddress, err := pb.containerRepo.GetContainerAddress(cs.ContainerId)
					if err != nil {
						return
					}

					currentConnections, err := pb.containerConnections(cs.ContainerId)
					if err != nil {
						return
					}

					connections := currentConnections

					if pb.checkContainerAvailable(containerAddress) {
						availableContainersChan <- container{
							id:          cs.ContainerId,
							address:     containerAddress,
							connections: connections,
						}

						return
					}
				}(containerState)
			}

			wg.Wait()
			close(availableContainersChan)

			// Collect available containers
			availableContainers := make([]container, 0)
			for c := range availableContainersChan {
				availableContainers = append(availableContainers, c)
			}

			// Sort availableContainers by # of connections (ascending)
			sort.Slice(availableContainers, func(i, j int) bool {
				return availableContainers[i].connections < availableContainers[j].connections
			})

			pb.availableContainersLock.Lock()
			pb.availableContainers = availableContainers
			pb.availableContainersLock.Unlock()

			time.Sleep(containerDiscoveryInterval)
		}
	}
}

// checkContainerAvailable checks if a container is available (meaning you can connect to it via a TCP dial)
func (pb *PodProxyBuffer) checkContainerAvailable(containerAddress string) bool {
	conn, err := network.ConnectToHost(pb.ctx, containerAddress, types.RequestTimeoutDurationS, pb.tailscale, pb.tsConfig)
	if err != nil {
		return false
	}

	return conn != nil
}

// containerConnections returns the number of connections currently established with a container
func (pb *PodProxyBuffer) containerConnections(containerId string) (int, error) {
	tokenKey := Keys.podContainerConnections(pb.workspace.Name, pb.stubId, containerId)

	val, err := pb.rdb.Get(pb.ctx, tokenKey).Int()
	if err != nil && err != redis.Nil {
		return 0, err
	} else if err == redis.Nil {
		created, err := pb.rdb.SetNX(pb.ctx, tokenKey, 0, 0).Result()
		if err != nil {
			return 0, err
		}

		if created {
			return 0, nil
		}

		connections, err := pb.rdb.Get(pb.ctx, tokenKey).Int()
		if err != nil {
			return 0, err
		}

		return connections, nil
	}

	return val, nil
}

func (pb *PodProxyBuffer) incrementConnections(containerId string) error {
	key := Keys.podContainerConnections(pb.workspace.Name, pb.stubId, containerId)
	_, err := pb.rdb.Incr(pb.ctx, key).Result()
	if err != nil {
		return err
	}

	err = pb.rdb.Expire(pb.ctx, key, podContainerConnectionTimeout).Err()
	if err != nil {
		return err
	}

	return nil
}

func (pb *PodProxyBuffer) decrementConnections(containerId string) error {
	key := Keys.podContainerConnections(pb.workspace.Name, pb.stubId, containerId)

	connections, err := pb.rdb.Decr(pb.ctx, key).Result()
	if err != nil {
		return err
	}

	if connections < 0 {
		pb.rdb.Incr(pb.ctx, key)
	}

	err = pb.rdb.Expire(pb.ctx, key, podContainerConnectionTimeout).Err()
	if err != nil {
		return err
	}

	return nil
}
