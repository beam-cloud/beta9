package pod

import (
	"context"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"net/http"

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

	log.Info().Msg("Forwarded request")

	for {
		select {
		case <-pb.ctx.Done():
			log.Info().Msg("Pod proxy buffer context done")
			return nil
		case <-done:
			log.Info().Msg("Forwarded request done")
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
				// log.Info().Msg("No connections to process")
				time.Sleep(bufferProcessingInterval)
				continue
			}

			log.Info().Msg("Processing connection")

			if conn.ctx.Request().Context().Err() != nil {
				log.Info().Msg("Connection context error")
				continue
			}

			go pb.handleRequest(conn)
		}
	}
}

func (pb *PodProxyBuffer) handleRequest(req *connection) {
	pb.handleHttpRequest(req, pb.availableContainers[0])
}

func (pb *PodProxyBuffer) handleConnection(conn *connection) {
	log.Info().Msg("Handling connection")
	pb.availableContainersLock.RLock()

	log.Info().Msgf("Available containers: %v", pb.availableContainers)

	if len(pb.availableContainers) == 0 {
		log.Info().Msg("No available containers")
		pb.buffer.Push(conn, true)
		pb.availableContainersLock.RUnlock()
		return
	}

	container := pb.availableContainers[0]
	pb.availableContainersLock.RUnlock()

	// Capture headers and other metadata
	request := conn.ctx.Request()
	headers := request.Header.Clone()

	// Hijack the connection
	hijacker, ok := conn.ctx.Response().Writer.(http.Hijacker)
	if !ok {
		conn.ctx.String(http.StatusInternalServerError, "Failed to hijack connection")
		return
	}

	log.Info().Msg("Hijacking connection")

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

	log.Info().Msg("Incrementing connections")

	err = pb.incrementConnections(container.id)
	if err != nil {
		pb.buffer.Push(conn, true)
		return
	}
	defer pb.decrementConnections(container.id)

	// Manually send the request line and headers to the container
	fmt.Fprintf(containerConn, "%s %s %s\r\n", request.Method, fmt.Sprintf("/%s", conn.ctx.Param("subPath")), request.Proto)
	headers.Write(containerConn)
	fmt.Fprint(containerConn, "\r\n")

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

func (pb *PodProxyBuffer) getHttpClient(address string) (*http.Client, error) {
	// If it isn't an tailnet address, just return the standard http client
	if !pb.tsConfig.Enabled || !strings.Contains(address, pb.tsConfig.HostName) {
		return pb.httpClient, nil
	}

	conn, err := network.ConnectToHost(pb.ctx, address, types.RequestTimeoutDurationS, pb.tailscale, pb.tsConfig)
	if err != nil {
		return nil, err
	}

	// Create a custom transport that uses the established connection
	// Either using tailscale or not
	transport := &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return conn, nil
		},
	}

	client := &http.Client{
		Transport: transport,
	}

	return client, nil
}

func (pb *PodProxyBuffer) handleHttpRequest(req *connection, c container) {
	request := req.ctx.Request()

	var requestBody io.ReadCloser = request.Body

	httpClient, err := pb.getHttpClient(c.address)
	if err != nil {
		req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "Internal server error",
		})
		return
	}
	containerUrl := fmt.Sprintf("http://%s/%s", c.address, req.ctx.Param("subPath"))

	httpReq, err := http.NewRequestWithContext(request.Context(), request.Method, containerUrl, requestBody)
	if err != nil {
		req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "Internal server error",
		})
		return
	}

	// Copy headers to new request
	for key, values := range request.Header {
		for _, val := range values {
			httpReq.Header.Add(key, val)
		}
	} // Send heartbeat via redis for duration of request

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	// Set response headers and status code before writing the body
	for key, values := range resp.Header {
		for _, value := range values {
			req.ctx.Response().Header().Add(key, value)
		}
	}
	req.ctx.Response().WriteHeader(resp.StatusCode)

	// Check if we can stream the response
	streamingSupported := true
	flusher, ok := req.ctx.Response().Writer.(http.Flusher)
	if !ok {
		streamingSupported = false
	}

	// Send response to client in chunks
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			req.ctx.Response().Writer.Write(buf[:n])

			if streamingSupported {
				flusher.Flush()
			}
		}

		if err != nil {
			if err != io.EOF && err != context.Canceled {
				req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
					"error": "Internal server error",
				})
			}

			break
		}
	}
}
