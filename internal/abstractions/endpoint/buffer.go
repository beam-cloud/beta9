package endpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	abstractions "github.com/beam-cloud/beta9/internal/abstractions/common"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
)

type request struct {
	ctx     echo.Context
	payload *types.TaskPayload
	taskId  string
	done    chan bool
}

type container struct {
	id       string
	address  string
	inFlight int
}

type RequestBuffer struct {
	ctx                   context.Context
	httpClient            *http.Client
	httpHealthCheckClient *http.Client

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

func NewRequestBuffer(
	ctx context.Context,
	rdb *common.RedisClient,
	workspace *types.Workspace,
	stubId string,
	size int,
	containerRepo repository.ContainerRepository,
	stubConfig *types.StubConfigV1,
) *RequestBuffer {
	b := &RequestBuffer{
		ctx:                     ctx,
		rdb:                     rdb,
		workspace:               workspace,
		stubId:                  stubId,
		stubConfig:              stubConfig,
		buffer:                  abstractions.NewRingBuffer[request](size),
		availableContainers:     []container{},
		availableContainersLock: sync.RWMutex{},
		containerRepo:           containerRepo,
		httpClient:              &http.Client{},
		httpHealthCheckClient: &http.Client{
			Timeout: 1 * time.Second,
		},
		length: atomic.Int32{},
	}

	go b.discoverContainers()
	go b.processRequests()

	return b
}

func (rb *RequestBuffer) ForwardRequest(ctx echo.Context, payload *types.TaskPayload, taskId string) error {
	done := make(chan bool)
	rb.buffer.Push(request{
		ctx:     ctx,
		done:    done,
		payload: payload,
		taskId:  taskId,
	})

	rb.length.Add(1)
	defer func() {
		rb.length.Add(-1)
	}()

	for {
		select {
		case <-rb.ctx.Done():
		case <-done:
			return nil
		}
	}
}

func (rb *RequestBuffer) processRequests() {
	for {
		select {
		case <-rb.ctx.Done():
			return
		default:
			req, ok := rb.buffer.Pop()

			if !ok {
				continue
			}

			if req.ctx.Request().Context().Err() != nil {
				// Context has been cancelled
				continue
			}

			go rb.handleHttpRequest(req)
		}
	}
}

func (rb *RequestBuffer) Length() int {
	return int(rb.length.Load())
}

func (rb *RequestBuffer) checkAddressIsReady(address string) bool {
	resp, err := rb.httpHealthCheckClient.Get(fmt.Sprintf("http://%s/health", address))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (rb *RequestBuffer) discoverContainers() {
	for {
		select {
		case <-rb.ctx.Done():
			return
		default:
			containerStates, err := rb.containerRepo.GetActiveContainersByStubId(rb.stubId)
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

					containerAddress, err := rb.containerRepo.GetContainerAddress(cs.ContainerId)
					if err != nil {
						return
					}

					inFlight, err := rb.requestsInFlight(cs.ContainerId)
					if err != nil {
						return
					}

					if rb.checkAddressIsReady(containerAddress) {
						availableContainersChan <- container{
							id:       cs.ContainerId,
							address:  containerAddress,
							inFlight: inFlight,
						}
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

			// Sort availableContainers by # of in-flight requests (ascending)
			sort.Slice(availableContainers, func(i, j int) bool {
				return availableContainers[i].inFlight < availableContainers[j].inFlight
			})

			rb.availableContainersLock.Lock()
			rb.availableContainers = availableContainers
			rb.availableContainersLock.Unlock()

			time.Sleep(1 * time.Second)
		}
	}
}

func (rb *RequestBuffer) requestsInFlight(containerId string) (int, error) {
	val, err := rb.rdb.Get(rb.ctx, Keys.endpointRequestsInFlight(rb.workspace.Name, rb.stubId, containerId)).Int()
	if err != nil && err != redis.Nil {
		return 0, err
	} else if err == redis.Nil {
		return 0, nil
	}

	return val, nil
}

func (rb *RequestBuffer) incrementRequestsInFlight(containerId string) error {
	err := rb.rdb.Incr(rb.ctx, Keys.endpointRequestsInFlight(rb.workspace.Name, rb.stubId, containerId)).Err()
	if err != nil {
		return err
	}

	err = rb.rdb.Expire(rb.ctx, Keys.endpointRequestsInFlight(rb.workspace.Name, rb.stubId, containerId), time.Duration(endpointRequestTimeoutS)*time.Second).Err()
	if err != nil {
		return err
	}

	return nil
}

func (rb *RequestBuffer) decrementRequestsInFlight(containerId string) error {
	err := rb.rdb.Decr(rb.ctx, Keys.endpointRequestsInFlight(rb.workspace.Name, rb.stubId, containerId)).Err()
	if err != nil {
		return err
	}

	err = rb.rdb.Expire(rb.ctx, Keys.endpointRequestsInFlight(rb.workspace.Name, rb.stubId, containerId), time.Duration(endpointRequestTimeoutS)*time.Second).Err()
	if err != nil {
		return err
	}

	return nil
}

func (rb *RequestBuffer) handleHttpRequest(req request) {
	rb.availableContainersLock.RLock()
	if len(rb.availableContainers) == 0 {
		rb.availableContainersLock.RUnlock()
		rb.buffer.Push(req)
		return
	}

	// Select an available container to forward the request to (whichever one has the lowest # of inflight requests)
	// Basically least-connections load balancing
	c := rb.availableContainers[0]
	rb.availableContainersLock.RUnlock()

	err := rb.incrementRequestsInFlight(c.id)
	if err != nil {
		return
	}

	request := req.ctx.Request()
	requestBody, err := json.Marshal(req.payload)
	if err != nil {
		return
	}

	containerUrl := fmt.Sprintf("http://%s", c.address)
	httpReq, err := http.NewRequestWithContext(request.Context(), request.Method, containerUrl, bytes.NewReader(requestBody))
	if err != nil {
		req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "Internal server error",
		})
		req.done <- true
		return
	}

	httpReq.Header.Add("X-TASK-ID", req.taskId) // Add task ID to header
	go rb.heartBeat(req, c.id)                  // Send heartbeat via redis for duration of request

	resp, err := rb.httpClient.Do(httpReq)
	if err != nil {
		req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "Internal server error",
		})
		req.done <- true
		return
	}

	defer resp.Body.Close()
	defer rb.afterRequest(req, c.id)

	// Read the response body
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "Internal server error",
		})
		return
	}

	req.ctx.Response().Writer.WriteHeader(resp.StatusCode)
	req.ctx.Response().Writer.Write(bytes)
}

func (rb *RequestBuffer) heartBeat(req request, containerId string) {
	ctx := req.ctx.Request().Context()
	ticker := time.NewTicker(endpointRequestHeartbeatInterval)
	defer ticker.Stop()

	rb.rdb.Set(rb.ctx, Keys.endpointRequestHeartbeat(rb.workspace.Name, rb.stubId, req.taskId), containerId, endpointRequestHeartbeatInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rb.rdb.Set(rb.ctx, Keys.endpointRequestHeartbeat(rb.workspace.Name, rb.stubId, req.taskId), containerId, endpointRequestHeartbeatInterval)
		}
	}
}

func (rb *RequestBuffer) afterRequest(req request, containerId string) {
	defer func() {
		req.done <- true
	}()

	rb.decrementRequestsInFlight(containerId)

	// Set keep warm lock
	if rb.stubConfig.KeepWarmSeconds == 0 {
		return
	}
	rb.rdb.SetEx(
		context.Background(),
		Keys.endpointKeepWarmLock(rb.workspace.Name, rb.stubId, containerId),
		1,
		time.Duration(rb.stubConfig.KeepWarmSeconds)*time.Second,
	)
}
