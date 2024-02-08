package endpoint

import (
	"context"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	abCommon "github.com/beam-cloud/beta9/internal/abstractions/common"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type Request struct {
	ctx  echo.Context
	done chan bool
}

type RequestBuffer struct {
	ctx        context.Context
	httpClient *http.Client

	stubId              string
	workspace           *types.Workspace
	rdb                 *common.RedisClient
	buffer              *abCommon.RingBuffer[Request]
	containerRepo       repository.ContainerRepository
	availableContainers []string

	length atomic.Int32
}

func NewRequestBuffer(
	ctx context.Context,
	rdb *common.RedisClient,
	workspace *types.Workspace,
	stubId string,
	size int,
	containers *map[string]*ContainerDetails,
	containerRepo repository.ContainerRepository,
) *RequestBuffer {
	b := &RequestBuffer{
		ctx:           ctx,
		rdb:           rdb,
		workspace:     workspace,
		stubId:        stubId,
		buffer:        abCommon.NewRingBuffer[Request](size),
		containerRepo: containerRepo,
		httpClient:    &http.Client{},
		length:        atomic.Int32{},
	}
	go b.discoverContainers()
	go b.ProcessRequests()

	return b
}

func (rb *RequestBuffer) ForwardRequest(ctx echo.Context) error {
	done := make(chan bool)
	rb.buffer.Push(Request{
		ctx:  ctx,
		done: done,
	})

	rb.length.Add(1)
	defer func() {
		rb.length.Add(-1)
	}()

	timeoutTicker := time.NewTicker(2 * time.Minute) // TODO: make this configurable
	defer timeoutTicker.Stop()

	for {
		select {
		case <-rb.ctx.Done():
			return nil
		case <-done:
			return nil
		case <-timeoutTicker.C:
			ctx.JSON(http.StatusRequestTimeout, map[string]interface{}{
				"error": "Request timed out",
			})
			return nil
		}
	}
}

func (rb *RequestBuffer) ProcessRequests() {
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
	resp, err := rb.httpClient.Get("http://" + address + "/health")
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
			containerNamePrefix := common.RedisKeys.ContainerName(endpointContainerPrefix, rb.stubId, "*")
			containerStates, err := rb.containerRepo.GetActiveContainersByPrefix(containerNamePrefix)
			if err != nil {

			}

			availableContainers := []string{}

			for _, containerState := range containerStates {
				if containerState.Status == types.ContainerStatusRunning {
					containerAddress, err := rb.containerRepo.GetContainerAddress(containerState.ContainerId)
					if err != nil {
						continue
					}

					if rb.checkAddressIsReady(containerAddress) {
						availableContainers = append(availableContainers, containerAddress)
					}
				}
			}

			rb.availableContainers = availableContainers

			time.Sleep(5 * time.Second) // TODO: make this configurable
		}
	}
}

func (rb *RequestBuffer) handleHttpRequest(req Request) {
	if len(rb.availableContainers) == 0 {
		rb.buffer.Push(req)
		return
	}

	// select a random container to forward the request to
	randIndex := rand.Intn(len(rb.availableContainers))
	containerAddress := rb.availableContainers[randIndex]

	request := req.ctx.Request()
	containerUrl := "http://" + containerAddress

	httpReq, err := http.NewRequestWithContext(request.Context(), request.Method, containerUrl, request.Body)
	if err != nil {
		req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "Internal server error", // TODO: improve error message
		})
		return
	}

	resp, err := rb.httpClient.Do(httpReq)
	if err != nil {
		req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "Internal server error", // TODO: improve error message
		})
		return
	}

	defer resp.Body.Close()
	defer func() {
		req.done <- true
	}()

	// Read the response body
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		req.ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "Internal server error", // TODO: improve error message
		})
		return
	}

	req.ctx.Response().Writer.WriteHeader(resp.StatusCode)
	req.ctx.Response().Writer.Write(bytes)
}
