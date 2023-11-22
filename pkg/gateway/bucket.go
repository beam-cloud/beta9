package gateway

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/beam/pkg/adapters/queue"
	"github.com/beam-cloud/beam/pkg/common"
	"github.com/beam-cloud/beam/pkg/repository"
	"github.com/beam-cloud/beam/pkg/types"
	pb "github.com/beam-cloud/beam/proto"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type RequestBucketBase struct {
	Name               string
	BucketId           string
	AppId              string
	Status             string
	IdentityId         string
	TriggerType        string
	ImageTag           string
	ScaleDownDelay     *float32
	AppConfig          types.BeamAppConfig
	TaskPolicy         *types.TaskPolicy
	TaskPolicyRaw      []byte
	ContainerEventChan chan types.ContainerEvent
	Containers         map[string]bool
	queueClient        queue.TaskQueue
	beamRepo           repository.BeamRepository
	bucketRepo         repository.RequestBucketRepository
	taskRepo           repository.TaskRepository
	workBus            *WorkBus
	bucketLock         *common.RedisLock
	bucketCtx          context.Context
	closeFunc          context.CancelFunc
	unloadBucketChan   chan string
}

type DeploymentRequestBucket struct {
	RequestBucketBase
	Version           uint
	ScaleEventChan    chan int
	AutoScaler        *AutoScaler
	AutoscalingConfig types.AutoScaling
	AutoscalerConfig  types.Autoscaler
	maxPendingTasks   uint
	workers           uint
}

type ServeRequestBucket struct {
	RequestBucketBase
	rdb *common.RedisClient
}

type RequestBucketConfig struct {
	AppId       string
	AppConfig   types.BeamAppConfig
	BucketId    string
	Status      string
	IdentityId  string
	TriggerType string
	ImageTag    string
}

type DeploymentRequestBucketConfig struct {
	RequestBucketConfig
	AutoscalingConfig types.AutoScaling
	AutoscalerConfig  types.Autoscaler
	Version           uint
	ScaleDownDelay    float32
	MaxPendingTasks   uint
	Workers           uint
}

func sendTimeoutResponse(ctx *gin.Context, response gin.H) {
	ctx.JSON(http.StatusPermanentRedirect, response)
}

func (rb *RequestBucketBase) stopContainers(containersToStop int) error {
	rand.Seed(time.Now().UnixNano())

	containerIds, err := rb.bucketRepo.GetStoppableContainers(rb.Status)
	if err != nil {
		return err
	}

	for i := 0; i < containersToStop && len(containerIds) > 0; i++ {
		// Randomly pick a container from the containerIds slice
		idx := rand.Intn(len(containerIds))
		containerId := containerIds[idx]

		// Send the stop container
		stopRequest := &pb.StopContainerRequest{
			ContainerId: containerId,
		}

		_, err := rb.workBus.Client.StopContainer(rb.bucketCtx, stopRequest)
		if err != nil {
			log.Printf("<%s> Unable to stop container: %v", rb.Name, err)
			return err
		}

		// Remove the containerId from the containerIds slice to avoid sending multiple stop requests to the same container
		containerIds = append(containerIds[:idx], containerIds[idx+1:]...)
	}

	return nil
}

func (rb *RequestBucketBase) EnqueueTask(ctx *gin.Context, taskId string) error {
	_, err := rb.queueClient.Push(rb.IdentityId, rb.Name, taskId, ctx)
	if err != nil {
		log.Printf("<%s> Failed to insert task into queue: %v\n", rb.Name, err)
		rb.beamRepo.DeleteTask(taskId)
		ctx.JSON(http.StatusBadRequest, err)
		return err
	}

	return nil
}

func (rb *RequestBucketBase) HandleRequest(ctx *gin.Context, taskId string) error {
	switch rb.TriggerType {
	case common.TriggerTypeASGI:
		return rb.handleASGIRequest(ctx)
	case common.TriggerTypeCronJob, common.TriggerTypeWebhook, common.TriggerTypeRestApi:
		err := rb.EnqueueTask(ctx, taskId)
		if err != nil {
			return err
		}

	}

	if rb.TriggerType == common.TriggerTypeRestApi {
		return rb.handleSyncRequest(ctx, taskId)
	}

	ctx.JSON(http.StatusOK, types.QueueResponse{
		TaskId: taskId,
	})

	return nil
}

func (rb *RequestBucketBase) handleSyncRequest(ctx *gin.Context, taskId string) error {
	timeoutResponse := gin.H{"task_id": taskId, "message": "Task timed out"}
	containerHostname, err := rb.bucketRepo.WaitForTaskCompletion(taskId)
	if err != nil {
		sendTimeoutResponse(ctx, timeoutResponse)
		return err
	}

	path := fmt.Sprintf("http://%s/%s", containerHostname, taskId)
	resp, err := http.Get(path)
	if err != nil {
		sendTimeoutResponse(ctx, timeoutResponse)
		return err
	}

	defer resp.Body.Close()
	ctx.Status(resp.StatusCode)

	// Copy response headers to original request
	for k, vv := range resp.Header {
		for _, v := range vv {
			ctx.Header(k, v)
		}
	}

	io.Copy(ctx.Writer, resp.Body)
	return nil
}

func (rb *RequestBucketBase) handleASGIRequest(ctx *gin.Context) error {
	err := rb.taskRepo.IncrementTasksInFlight(rb.Name, rb.IdentityId)
	if err != nil {
		return err
	}
	defer rb.taskRepo.DecrementTasksInFlight(rb.Name, rb.IdentityId)

	// Pick an available container to send the request
	host, err := rb.bucketRepo.GetAvailableContainerHost()
	if err != nil || host == nil {
		sendTimeoutResponse(ctx, gin.H{"message": "Request timed out"})
		return err
	}

	// If we found a container to send the request, set keepwarm lock after the request completes
	if host != nil && rb.ScaleDownDelay != nil {
		defer func() {
			rb.bucketRepo.SetKeepWarm(host.ContainerId, *rb.ScaleDownDelay)
		}()
	}

	path := strings.ReplaceAll(ctx.Param("route"), fmt.Sprintf("/%s", rb.AppId), "")

	// Add subpath, if one exists
	subPath, exists := ctx.Get("subPath")
	if exists {
		path = fmt.Sprintf("%s%s", path, subPath)
	}

	if ctx.GetBool("subDomain") {
		path = ctx.GetString("subPath")
	}

	upgrader := websocket.Upgrader{}
	if websocket.IsWebSocketUpgrade(ctx.Request) {
		u := url.URL{Scheme: "ws", Host: host.Hostname, Path: path}

		headers := make(http.Header)
		clientConn, _, err := websocket.DefaultDialer.Dial(u.String(), headers)
		if err != nil {
			return err
		}
		defer clientConn.Close()

		serverConn, err := upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
		if err != nil {
			return err
		}
		defer serverConn.Close()

		quit := make(chan bool)
		go func() {
			for {
				messageType, p, err := serverConn.ReadMessage()
				if err != nil {
					quit <- true
					return
				}

				if err := clientConn.WriteMessage(messageType, p); err != nil {
					quit <- true
					return
				}
			}
		}()

		go func() {
			for {
				messageType, p, err := clientConn.ReadMessage()
				if err != nil {
					quit <- true
					return
				}

				if err := serverConn.WriteMessage(messageType, p); err != nil {
					quit <- true
					return
				}
			}
		}()

		<-quit // Wait for either goroutine to signal termination

		return nil
	} else {
		proxy := httputil.NewSingleHostReverseProxy(&url.URL{})
		proxy.Director = func(req *http.Request) {
			req.Header = ctx.Request.Header
			req.Method = ctx.Request.Method
			req.Host = ctx.Request.Host
			req.URL.Scheme = "http"
			req.URL.Host = host.Hostname
			req.URL.Path = path
		}

		defer func() {
			if r := recover(); r != nil {
				log.Printf("<%s> ASGI request aborted\n", rb.Name)
			}
		}()

		proxy.ServeHTTP(ctx.Writer, ctx.Request)
	}

	return nil
}

func (rb *RequestBucketBase) Close() {
	rb.closeFunc()
}

func (rb *RequestBucketBase) GetName() string {
	return rb.Name
}

func (rb *RequestBucketBase) GetContainerEventChan() chan types.ContainerEvent {
	return rb.ContainerEventChan
}
