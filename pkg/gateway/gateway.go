package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	common "github.com/beam-cloud/beam/pkg/common"
	"github.com/beam-cloud/beam/pkg/repository"
	"github.com/beam-cloud/beam/pkg/types"
	pb "github.com/beam-cloud/beam/proto"
	beat "github.com/beam-cloud/beat/pkg"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Activator struct {
	BaseURL            string
	RequestBuckets     map[string]types.RequestBucket
	RequestBucketNames map[string][]string
	beatService        *beat.BeatService
	eventBus           *common.EventBus
	stateStore         *common.StateStore
	redisClient        *common.RedisClient
	QueueClient        *QueueClient
	BeamRepo           repository.BeamRepository
	metricsRepo        repository.MetricsStatsdRepository
	WorkBus            *WorkBus

	unloadBucketChan chan string
	keyEventManager  *common.KeyEventManager
	keyEventChan     chan common.KeyEvent
	ctx              context.Context
	cancelFunc       context.CancelFunc
}

type WorkBus struct {
	Client pb.SchedulerClient
	Conn   *grpc.ClientConn
}

func NewWorkBusConnection(host string) (*WorkBus, error) {
	conn, err := grpc.Dial(fmt.Sprintf(host, common.Secrets().Get("BEAM_NAMESPACE")), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := pb.NewSchedulerClient(conn)
	return &WorkBus{Client: client, Conn: conn}, nil
}

func NewActivator() (*Activator, error) {
	redisClient, err := common.NewRedisClient(common.WithClientName("BeamActivator"))
	if err != nil {
		return nil, err
	}

	stateStore := common.Store(redisClient)

	ctx, cancel := context.WithCancel(context.Background())
	activator := &Activator{
		stateStore:         stateStore,
		redisClient:        redisClient,
		RequestBuckets:     make(map[string]types.RequestBucket),
		RequestBucketNames: make(map[string][]string),
		ctx:                ctx,
		cancelFunc:         cancel,
		keyEventChan:       make(chan common.KeyEvent),
		unloadBucketChan:   make(chan string),
	}

	beamRepo, err := repository.NewBeamPostgresRepository()
	if err != nil {
		return nil, err
	}

	metricsRepo := repository.NewMetricsStatsdRepository()

	workBus, err := NewWorkBusConnection(common.SchedulerHost)
	if err != nil {
		return nil, err
	}

	beatService, err := beat.NewBeatService()
	if err != nil {
		return nil, err
	}

	keyEventManager, err := common.NewKeyEventManager(redisClient)
	if err != nil {
		return nil, err
	}

	eventBus := common.NewEventBus(redisClient, common.EventBusSubscriber{Type: common.EventTypeStopDeployment, Callback: func(event *common.Event) bool {
		appId, ok := event.Args["app_id"].(string)
		if !ok {
			return false
		}

		rawVersion, ok := event.Args["app_version"].(float64)
		if !ok {
			return false
		}
		version := int(rawVersion)

		// Unload the request bucket first, so we can refresh the deployment state
		bucketName := common.Names.RequestBucketName(appId, uint(version))
		activator.unloadRequestBucket(bucketName)

		// Once the bucket is loaded again, it should have an updated deployment status
		// The request bucket will then handle the spin down of any containers associated with it
		_, err := activator.loadDeploymentRequestBucket(bucketName)
		return err == nil
	}})

	activator.QueueClient = NewQueueClient(redisClient)
	activator.BeamRepo = beamRepo
	activator.metricsRepo = metricsRepo
	activator.WorkBus = workBus
	activator.keyEventManager = keyEventManager
	activator.beatService = beatService
	activator.eventBus = eventBus

	go activator.QueueClient.MonitorTasks(activator.ctx, beamRepo)
	go activator.keyEventManager.ListenForPattern(activator.ctx, common.RedisKeys.SchedulerContainerState(types.DeploymentContainerPrefix), activator.keyEventChan)
	go activator.beatService.Run(activator.ctx)
	go activator.handleDeploymentEvents()
	go activator.monitorBuckets()
	go activator.eventBus.ReceiveEvents(activator.ctx)

	return activator, nil
}

func (a *Activator) monitorBuckets() {
	for {
		select {
		case bucketName := <-a.unloadBucketChan:
			a.unloadRequestBucket(bucketName)
		case <-a.ctx.Done():
			return
		}
	}
}

func (a *Activator) handleDeploymentEvents() {
	for {
		select {
		case event := <-a.keyEventChan:
			containerId := fmt.Sprintf("%s%s", types.DeploymentContainerPrefix, event.Key)
			operation := event.Operation

			containerIdParts := strings.Split(containerId, "-")
			bucketName := strings.Join(containerIdParts[1:3], "-")

			bucket, ok := a.RequestBuckets[bucketName]
			if !ok {
				_bucket, err := a.loadDeploymentRequestBucket(bucketName)
				bucket = *_bucket
				if err != nil {
					continue
				}
			}

			containerEventChan := bucket.GetContainerEventChan()

			switch operation {
			case common.KeyOperationSet, common.KeyOperationHSet:
				containerEventChan <- types.ContainerEvent{
					ContainerId: containerId,
					Change:      +1,
				}
			case common.KeyOperationDel, common.KeyOperationExpired:
				containerEventChan <- types.ContainerEvent{
					ContainerId: containerId,
					Change:      -1,
				}
			}
		case <-a.ctx.Done():
			return
		}
	}
}

func (a *Activator) parseAppConfig(config json.RawMessage) (types.BeamAppConfig, error) {
	var appConfig types.BeamAppConfig
	err := json.Unmarshal(config, &appConfig)

	// TODO: this is needed for backwards compatibility with older versions of the SDK
	// Once everyone is migrated over, it can be removed
	if err != nil || appConfig.AppSpecVersion != "v3" {
		var deprecratedAppConfig types.BeamUserAppConfig
		if err := json.Unmarshal(config, &deprecratedAppConfig); err != nil {
			return appConfig, err
		}

		// log.Printf("<%s> Loaded old style config, mapping to new style: %+v\n", bucketName, deprecratedAppConfig)
		appConfig.Name = deprecratedAppConfig.App.Name
		appConfig.SdkVersion = nil
		appConfig.Runtime.Cpu = deprecratedAppConfig.App.Cpu
		appConfig.Runtime.Memory = deprecratedAppConfig.App.Memory
		appConfig.Runtime.Gpu = deprecratedAppConfig.App.Gpu
		appConfig.Runtime.Image.PythonPackages = deprecratedAppConfig.App.PythonPackages
		appConfig.Runtime.Image.PythonVersion = deprecratedAppConfig.App.PythonVersion
		appConfig.Runtime.Image.Commands = deprecratedAppConfig.App.Commands
		appConfig.Mounts = deprecratedAppConfig.Mounts
		appConfig.Triggers = []types.Trigger{
			{
				Outputs:         deprecratedAppConfig.Outputs,
				Handler:         deprecratedAppConfig.Triggers[0].Handler,
				Loader:          deprecratedAppConfig.Triggers[0].Loader,
				TriggerType:     deprecratedAppConfig.Triggers[0].TriggerType,
				When:            deprecratedAppConfig.Triggers[0].When,
				CallbackUrl:     deprecratedAppConfig.Triggers[0].CallbackUrl,
				MaxPendingTasks: &deprecratedAppConfig.Triggers[0].MaxPendingTasks,
				KeepWarmSeconds: &deprecratedAppConfig.Triggers[0].KeepWarmSeconds,
				AutoScaling:     &deprecratedAppConfig.AutoScaling,
			},
		}

	}

	return appConfig, nil
}

// Given a bucket name, load request bucket
func (a *Activator) loadDeploymentRequestBucket(bucketName string) (*types.RequestBucket, error) {
	requestBucket, ok := a.RequestBuckets[bucketName]
	if ok {
		return &requestBucket, nil
	}

	splitBucketName := strings.Split(bucketName, "-")
	if len(splitBucketName) < 2 {
		return nil, errors.New("invalid request bucket name")
	}

	appId := splitBucketName[0]
	version64, err := strconv.ParseUint(splitBucketName[1], 10, 64)
	if err != nil {
		return nil, err
	}

	version := uint(version64)
	appDeployment, app, deploymentPackage, err := a.BeamRepo.GetDeployment(appId, &version)
	if err != nil || appDeployment == nil {
		return nil, err
	}

	identity, err := a.BeamRepo.RetrieveUserByPk(app.IdentityId)
	if err != nil || identity == nil {
		return nil, err
	}

	appConfig, err := a.parseAppConfig(deploymentPackage.Config)
	if err != nil {
		return nil, err
	}

	trigger := appConfig.Triggers[0]
	scaleDownDelayS := uint(0)
	if trigger.KeepWarmSeconds != nil {
		scaleDownDelayS = *trigger.KeepWarmSeconds
	}

	maxPendingTasks := uint(0)
	if trigger.MaxPendingTasks != nil {
		maxPendingTasks = *trigger.MaxPendingTasks
	}

	autoscalingConfig := types.AutoScaling{}
	if trigger.AutoScaling != nil {
		autoscalingConfig = *trigger.AutoScaling
	}

	autoscalerConfig := types.Autoscaler{}
	if trigger.Autoscaler != nil {
		autoscalerConfig = *trigger.Autoscaler
	}

	if scaleDownDelayS == 0 {
		switch appDeployment.TriggerType {
		case common.TriggerTypeCronJob, common.TriggerTypeWebhook:
			scaleDownDelayS = ActivatorConfig.ScaleDownDelayAsync
		case common.TriggerTypeRestApi, common.TriggerTypeASGI:
			scaleDownDelayS = ActivatorConfig.ScaleDownDelaySync
		}
	}

	if maxPendingTasks == 0 {
		maxPendingTasks = ActivatorConfig.MaxPendingTasks
	}

	workers := uint(1)
	if trigger.Workers != nil {
		workers = *trigger.Workers
	}

	config := DeploymentRequestBucketConfig{
		RequestBucketConfig: RequestBucketConfig{
			AppId:       app.ShortId,
			BucketId:    appDeployment.ExternalId,
			AppConfig:   appConfig,
			Status:      appDeployment.Status,
			IdentityId:  identity.ExternalId,
			TriggerType: appDeployment.TriggerType,
			ImageTag:    appDeployment.ImageTag,
		},
		Version:           appDeployment.Version,
		AutoscalingConfig: autoscalingConfig,
		AutoscalerConfig:  autoscalerConfig,
		ScaleDownDelay:    float32(scaleDownDelayS),
		MaxPendingTasks:   maxPendingTasks,
		Workers:           workers,
	}

	bucket, err := NewDeploymentRequestBucket(config, a)
	if err != nil {
		return nil, err
	}

	a.RequestBuckets[bucketName] = bucket

	log.Printf("Watching app <%s>", bucketName)
	return &bucket, nil
}

func (a *Activator) loadServeRequestBucket(bucketName string) (*types.RequestBucket, error) {
	requestBucket, ok := a.RequestBuckets[bucketName]
	if ok {
		return &requestBucket, nil
	}

	splitBucketName := strings.Split(bucketName, "-")
	if len(splitBucketName) < 2 {
		return nil, errors.New("invalid request bucket name")
	}

	appId := splitBucketName[0]
	serveId := splitBucketName[1]
	appServe, app, err := a.BeamRepo.GetServe(appId, serveId)

	if err != nil || appServe == nil {
		return nil, err
	}

	identity, err := a.BeamRepo.RetrieveUserByPk(app.IdentityId)
	if err != nil || identity == nil {
		return nil, err
	}

	appConfig, err := a.parseAppConfig(appServe.Config)
	if err != nil {
		return nil, err
	}

	trigger := appConfig.Triggers[0]

	config := RequestBucketConfig{
		AppId:       app.ShortId,
		BucketId:    appServe.ExternalId,
		AppConfig:   appConfig,
		IdentityId:  identity.ExternalId,
		TriggerType: trigger.TriggerType,
		ImageTag:    appServe.ImageTag,
	}

	bucket, err := NewServeRequestBucket(config, a)
	if err != nil {
		return nil, err
	}

	a.RequestBuckets[bucketName] = bucket

	log.Printf("Watching app <%s>", bucketName)
	return &bucket, nil
}

// Stop monitoring request bucket
func (a *Activator) unloadRequestBucket(bucketName string) {
	requestBucket, ok := a.RequestBuckets[bucketName]
	if !ok {
		return
	}
	requestBucket.Close()
	delete(a.RequestBuckets, bucketName)
}

// Retrieve the deployment bucket name from Store
func (a *Activator) GetDeploymentRequestBucket(appId string, version *types.RequestedVersion) (*types.RequestBucket, error) {
	if version.Type == types.DefaultRequestedVersion {
		bucketName, _ := a.stateStore.GetDefaultDeploymentBucket(appId)
		bucket, err := a.loadDeploymentRequestBucket(bucketName)
		if err != nil {
			return nil, fmt.Errorf("default bucket <%v> not found in cache or database: %v", bucketName, err)
		}
		return bucket, nil
	}

	if version.Type == types.LatestRequestedVersion {
		deployment, _, _, err := a.BeamRepo.GetDeployment(appId, nil)
		if err != nil {
			return nil, fmt.Errorf("latest version not found in database: %v", err)
		}
		bucketName := common.Names.RequestBucketName(appId, deployment.Version)
		bucket, err := a.loadDeploymentRequestBucket(bucketName)
		if err != nil {
			return nil, fmt.Errorf("bucket <%v> not found in cache or database: %v", bucketName, err)
		}
		return bucket, nil
	}

	// When version is any other value, we assume it is a specific version, so lets get it
	bucketName := common.Names.RequestBucketName(appId, version.Value)
	bucket, err := a.loadDeploymentRequestBucket(bucketName)
	if err != nil {
		return nil, fmt.Errorf("bucket <%v> not found in cache or database: %v", bucketName, err)
	}
	return bucket, nil
}

func (a *Activator) GetServeRequestBucket(appId string, serveId string) (*types.RequestBucket, error) {
	bucketName := common.Names.RequestBucketNameId(appId, serveId)
	bucket, err := a.loadServeRequestBucket(bucketName)
	if err != nil {
		return nil, fmt.Errorf("bucket <%v> not found in cache or database: %v", bucketName, err)
	}
	return bucket, nil
}

// Log request metrics
func (a *Activator) LogRequest(requestBucketName string, startTime time.Time, ctx *gin.Context) {
	a.metricsRepo.BeamDeploymentRequestDuration(requestBucketName, time.Since(startTime))
	a.metricsRepo.BeamDeploymentRequestStatus(requestBucketName, ctx.Writer.Status())
	a.metricsRepo.BeamDeploymentRequestCount(requestBucketName)
}

func (a *Activator) startProxyServer(port string) error {
	log.Printf("Starting proxy server on port %s", port)

	router := gin.Default()

	// appGroup := router.Group("/")
	// apiv1.NewAppGroup(appGroup, a, basicAuthMiddleware(a.BeamRepo, a.stateStore))

	return router.Run(port)
}

func (a *Activator) startInternalServer(port string) error {
	log.Printf("Starting internal server on port %s", port)

	router := gin.New()
	router.Use(common.LoggingMiddlewareGin(), gin.Recovery())

	// appGroup := router.Group("/")
	// apiv1.NewAppGroup(appGroup, a, serviceAuthMiddleware(a.BeamRepo))

	// healthGroup := router.Group("/healthz")
	// apiv1.NewHealthGroup(healthGroup, a.redisClient)

	// scheduleGroup := router.Group("/schedule")
	// apiv1.NewScheduleGroup(scheduleGroup, a.beatService, serviceAuthMiddleware(a.BeamRepo))

	return router.Run(port)
}

// Activator entry point
func (a *Activator) Start() {
	errCh := make(chan error)
	terminationSignal := make(chan os.Signal, 1)
	defer close(errCh)
	defer close(terminationSignal)

	go func() {
		time.Sleep(3 * time.Second)
		err := a.startInternalServer(ActivatorConfig.InternalPort)
		errCh <- fmt.Errorf("internal server error: %v", err)
	}()
	go func() {
		err := a.startProxyServer(ActivatorConfig.ExternalPort)
		errCh <- fmt.Errorf("proxy server error: %v", err)
	}()

	signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)

	select {
	case <-terminationSignal:
		log.Print("Termination signal received. ")
	case err := <-errCh:
		log.Printf("An error has occured: %v ", err)
	}
	log.Println("Shutting down...")

	a.cancelFunc()
	a.redisClient.Close()
}
