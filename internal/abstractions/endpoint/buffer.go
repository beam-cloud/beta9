package endpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	abstractions "github.com/beam-cloud/beta9/internal/abstractions/common"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type request struct {
	ctx     echo.Context
	payload *types.TaskPayload
	done    chan bool
}

type container struct {
	id      string
	address string
}

type RequestBuffer struct {
	ctx        context.Context
	httpClient *http.Client

	stubId                  string
	stubConfig              *types.StubConfigV1
	workspace               *types.Workspace
	rdb                     *common.RedisClient
	containerRepo           repository.ContainerRepository
	buffer                  *abstractions.RingBuffer[request]
	availableContainers     []container
	availableContainersLock sync.Mutex

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
		availableContainersLock: sync.Mutex{},
		containerRepo:           containerRepo,
		httpClient:              &http.Client{},
		length:                  atomic.Int32{},
	}

	go b.discoverContainers()
	go b.processRequests()

	return b
}

func (rb *RequestBuffer) ForwardRequest(ctx echo.Context, payload *types.TaskPayload) error {
	done := make(chan bool)
	rb.buffer.Push(request{
		ctx:     ctx,
		done:    done,
		payload: payload,
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
	// Make a request to the health endpoint to check if the container is ready
	resp, err := rb.httpClient.Get(fmt.Sprintf("http://%s/health", address))
	if err != nil {
		return false
	}

	if resp.StatusCode == http.StatusOK {
		return true
	}

	return false
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

			availableContainers := []container{}
			for _, containerState := range containerStates {
				if containerState.Status == types.ContainerStatusRunning {
					containerAddress, err := rb.containerRepo.GetContainerAddress(containerState.ContainerId)
					if err != nil {
						continue
					}

					if rb.checkAddressIsReady(containerAddress) {
						availableContainers = append(availableContainers, container{
							id:      containerState.ContainerId,
							address: containerAddress,
						})
					}
				}
			}

			rb.availableContainersLock.Lock()
			rb.availableContainers = availableContainers
			rb.availableContainersLock.Unlock()

			time.Sleep(1 * time.Second) // TODO: make this configurable
		}
	}
}

func (rb *RequestBuffer) handleHttpRequest(req request) {
	rb.availableContainersLock.Lock()
	if len(rb.availableContainers) == 0 {
		rb.availableContainersLock.Unlock()
		rb.buffer.Push(req)
		return
	}

	// Select a random available container to forward the request to
	randIndex := rand.Intn(len(rb.availableContainers))
	c := rb.availableContainers[randIndex]
	rb.availableContainersLock.Unlock()

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

func (rb *RequestBuffer) afterRequest(req request, containerId string) {
	defer func() { req.done <- true }()

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
