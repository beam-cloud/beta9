package endpoint

import (
	"context"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	abCommon "github.com/beam-cloud/beta9/internal/abstractions/common"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/okteto/okteto/pkg/log"
)

type RequestData struct {
	ctx            context.Context
	Method         string
	Headers        http.Header
	Body           io.ReadCloser
	ResponseWriter *io.PipeWriter
}

type RequestBuffer struct {
	stubId        string
	workspace     *types.Workspace
	rdb           *common.RedisClient
	buffer        *abCommon.RingBuffer[RequestData]
	containers    *map[string]*ContainerDetails
	containerRepo repository.ContainerRepository
	httpClient    *http.Client

	lock   *sync.Mutex
	length int
}

func NewRequestBuffer(
	rdb *common.RedisClient,
	workspace *types.Workspace,
	stubId string,
	size int,
	containers *map[string]*ContainerDetails,
	containerRepo repository.ContainerRepository,
) *RequestBuffer {
	return &RequestBuffer{
		rdb:           rdb,
		workspace:     workspace,
		stubId:        stubId,
		containers:    containers,
		buffer:        abCommon.NewRingBuffer[RequestData](size),
		containerRepo: containerRepo,
		httpClient:    &http.Client{},
		lock:          &sync.Mutex{},
		length:        0,
	}
}

func (rb *RequestBuffer) ForwardRequest(req RequestData) ([]byte, error) {
	reader, writer := io.Pipe()
	req.ResponseWriter = writer
	rb.buffer.Push(req)

	rb.lock.Lock()
	rb.length++
	rb.lock.Unlock()

	defer func() {
		rb.lock.Lock()
		rb.length--
		rb.lock.Unlock()
	}()

	go func() {
		<-req.ctx.Done()
		writer.Write([]byte("Request timed out"))
		writer.Close()
	}()

	var buf []byte
	var chunkSize int = 1024
	for {
		chunk := make([]byte, chunkSize)
		n, err := reader.Read(chunk)
		if err != nil {
			break
		}

		buf = append(buf, chunk[:n]...)
	}

	return buf, nil
}

func (rb *RequestBuffer) ProcessRequests() {
	for {
		req, ok := rb.buffer.Pop()

		if !ok {
			continue
		}

		if req.ctx.Err() != nil {
			// Context has been cancelled
			continue
		}

		go rb.handleRequest(req)
	}
}

func (rb *RequestBuffer) Length() int {
	rb.lock.Lock()
	defer rb.lock.Unlock()

	return rb.length
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

func (rb *RequestBuffer) searchForContainers(stubId string) (*ContainerDetails, error) {
	containerNamePrefix := common.RedisKeys.ContainerName(endpointContainerPrefix, stubId, "*")
	containerStates, err := rb.containerRepo.GetActiveContainersByPrefix(containerNamePrefix)
	if err != nil {
		return &ContainerDetails{}, err
	}

	runningContainers := []types.ContainerState{}

	for _, containerState := range containerStates {
		if containerState.Status == types.ContainerStatusRunning {
			runningContainers = append(runningContainers, containerState)
		}
	}

	if len(runningContainers) == 0 {
		return &ContainerDetails{}, nil
	}

	randIndex := rand.Intn(len(runningContainers))
	containerState := runningContainers[randIndex]

	containerDetails, exists := (*rb.containers)[containerState.ContainerId]
	if !exists {
		containerDetails = &ContainerDetails{
			id:    containerState.ContainerId,
			mutex: &sync.Mutex{},
			ready: false,
		}
		(*rb.containers)[containerState.ContainerId] = containerDetails
	}

	if containerDetails.ready {
		return containerDetails, nil
	}

	containerAddress, err := rb.containerRepo.GetContainerAddress(containerState.ContainerId)
	if err != nil {
		return containerDetails, err
	}

	containerDetails.Address = containerAddress

	if rb.checkAddressIsReady(containerAddress) {
		containerDetails.ready = true
	}

	return containerDetails, nil
}

func (rb *RequestBuffer) handleRequest(req RequestData) {
	containerDetails, err := rb.searchForContainers(rb.stubId)
	if err != nil {
		rb.buffer.Push(req)
		log.Println("Error searching for containers ", err)
		return
	}

	if !containerDetails.ready {
		rb.buffer.Push(req)
		return
	}

	if err := rb.prepareRequest(containerDetails, req); err != nil {
		rb.buffer.Push(req)
		log.Println("Error preparing request ", err)
		return
	}

	defer rb.closeRequest(containerDetails, req)

	containerUrl := "http://" + containerDetails.Address

	// Make http request to container
	httpReq, err := http.NewRequest(req.Method, containerUrl, req.Body)
	if err != nil {
		log.Println("Error making request to container ", err)
		return
	}

	httpReq.Header = req.Headers
	resp, err := rb.httpClient.Do(httpReq)
	if err != nil {
		log.Println("Error making request to container ", err)
		return
	}

	// Forward response to original request
	_, err = io.Copy(req.ResponseWriter, resp.Body)
}

func (rb *RequestBuffer) prepareRequest(cd *ContainerDetails, req RequestData) error {
	if cd.ActiveRequestCount == 0 {
		// Set the initial request infight lock
		err := rb.rdb.SetEx(context.TODO(), Keys.endpointBufferRequestsInflightLock(rb.workspace.Name, rb.stubId, cd.id), 1, 5*time.Second).Err()
		if err != nil {
			return err
		}
	}

	cd.mutex.Lock()
	cd.ActiveRequestCount++
	cd.mutex.Unlock()

	return nil
}

func (rb *RequestBuffer) closeRequest(cd *ContainerDetails, req RequestData) {
	req.ResponseWriter.Close()
	cd.mutex.Lock()
	cd.ActiveRequestCount--
	cd.mutex.Unlock()

	if cd.ActiveRequestCount == 0 {
		// Remove the inflight lock
		if err := rb.rdb.Del(context.TODO(), Keys.endpointBufferRequestsInflightLock(rb.workspace.Name, rb.stubId, cd.id)).Err(); err != nil {
			log.Debug("Error removing inflight lock", err) // TODO: better error handling
			return
		}

		if err := rb.rdb.SetEx(context.TODO(), Keys.endpointKeepWarmLock(rb.workspace.Name, rb.stubId, cd.id), 1, 30*time.Second).Err(); err != nil {
			log.Debug("Error setting keep warm lock", err) // TODO: better error handling
			return
		}
	}
}
