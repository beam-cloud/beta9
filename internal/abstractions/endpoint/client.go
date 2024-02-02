package endpoint

import (
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
)

type RingBufferEndpointClient struct {
	buffer        *RingBuffer
	containerRepo repository.ContainerRepository
}

func NewRingBufferEndpointClient(size int, containerRepo repository.ContainerRepository) *RingBufferEndpointClient {
	return &RingBufferEndpointClient{
		buffer:        NewRingBuffer(size),
		containerRepo: containerRepo,
	}
}

func (c *RingBufferEndpointClient) ForwardRequest(req RequestData) ([]byte, error) {
	reader, writer := io.Pipe()
	req.ResponseWriter = writer
	c.buffer.Push(req)

	go func() {
		time.Sleep(5 * time.Second)
		writer.Write([]byte("Request timed out"))
		writer.Close()
	}()

	var buf []byte
	var chunkSize int = 1024
	for {
		chunk := make([]byte, chunkSize)
		n, err := reader.Read(chunk)
		if err != nil {
			log.Println("Error reading from pipe", err)
			break
		}

		buf = append(buf, chunk[:n]...)
	}

	return buf, nil
}

func (c *RingBufferEndpointClient) HandleRequests() {
	for {
		req, ok := c.buffer.Pop()

		if !ok {
			continue
		}

		go c.handleRequest(req)
	}
}

func (c *RingBufferEndpointClient) searchForContainers(stubId string) (string, error) {
	containerNamePrefix := common.RedisKeys.ContainerName(endpointContainerPrefix, stubId, "*")

	containerStates, err := c.containerRepo.GetActiveContainersByPrefix(containerNamePrefix)
	if err != nil {
		log.Println("Error getting active containers", err) // TODO: remove
		return "", err
	}

	// Find smarter way to load balance between containers

	randIndex := rand.Intn(len(containerStates))
	containerState := containerStates[randIndex]

	containerHost, err := c.containerRepo.GetContainerWorkerHostname(containerState.ContainerId)
	if err != nil {
		log.Println("Error getting container worker hostname", err) // TODO: remove
		return "", err
	}

	return containerHost, nil
}

func (c *RingBufferEndpointClient) handleRequest(req RequestData) {
	defer req.ResponseWriter.Close()

	containerHost, err := c.searchForContainers(req.stubId)
	if err != nil {
		log.Println("Error searching for containers", err) // TODO: remove
		return
	}

	// Make http request to container
	resp, err := http.NewRequest(req.Method, containerHost+req.URL, req.Body)
	if err != nil {
		log.Println("Error making request to container", err) // TODO: remove
		return
	}

	// Forward response to original request
	_, err = io.Copy(req.ResponseWriter, resp.Body)
}
