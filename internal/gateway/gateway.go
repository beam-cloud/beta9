package gateway

import (
	"context"
	"fmt"
	"log"

	common "github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/integrations/queue"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
	beat "github.com/beam-cloud/beat/pkg"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Gateway struct {
	BaseURL            string
	RequestBuckets     map[string]types.RequestBucket
	RequestBucketNames map[string][]string
	beatService        *beat.BeatService
	eventBus           *common.EventBus
	redisClient        *common.RedisClient
	QueueClient        queue.TaskQueue
	BeamRepo           repository.BeamRepository
	metricsRepo        repository.MetricsStatsdRepository
	Scheduler          *Scheduler

	unloadBucketChan chan string
	keyEventManager  *common.KeyEventManager
	keyEventChan     chan common.KeyEvent
	ctx              context.Context
	cancelFunc       context.CancelFunc
}

type Scheduler struct {
	Client pb.SchedulerClient
	Conn   *grpc.ClientConn
}

func NewSchedulerConnection(host string) (*Scheduler, error) {
	conn, err := grpc.Dial(fmt.Sprintf(host, common.Secrets().Get("BEAM_NAMESPACE")), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := pb.NewSchedulerClient(conn)
	return &Scheduler{Client: client, Conn: conn}, nil
}

func NewGateway() (*Gateway, error) {
	redisClient, err := common.NewRedisClient(common.WithClientName("BeamGateway"))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	gateway := &Gateway{
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

	scheduler, err := NewSchedulerConnection(common.SchedulerHost)
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

	gateway.QueueClient = queue.NewRedisQueueClient(redisClient)
	gateway.BeamRepo = beamRepo
	gateway.metricsRepo = metricsRepo
	gateway.Scheduler = scheduler
	gateway.keyEventManager = keyEventManager
	gateway.beatService = beatService

	go gateway.QueueClient.MonitorTasks(gateway.ctx, beamRepo)
	go gateway.keyEventManager.ListenForPattern(gateway.ctx, common.RedisKeys.SchedulerContainerState(types.DeploymentContainerPrefix), gateway.keyEventChan)
	go gateway.beatService.Run(gateway.ctx)
	// go gateway.eventBus.ReceiveEvents(gateway.ctx)

	return gateway, nil
}

func (g *Gateway) startProxyServer(port string) error {
	log.Printf("Starting proxy server on port %s", port)

	router := gin.Default()

	// appGroup := router.Group("/")
	// apiv1.NewAppGroup(appGroup, a, basicAuthMiddleware(g.BeamRepo, g.stateStore))

	return router.Run(port)
}

func (g *Gateway) startInternalServer(port string) error {
	log.Printf("Starting internal server on port %s", port)

	router := gin.New()
	router.Use(common.LoggingMiddlewareGin(), gin.Recovery())

	// appGroup := router.Group("/")
	// apiv1.NewAppGroup(appGroup, a, serviceAuthMiddleware(g.BeamRepo))

	// healthGroup := router.Group("/healthz")
	// apiv1.NewHealthGroup(healthGroup, g.redisClient)

	// scheduleGroup := router.Group("/schedule")
	// apiv1.NewScheduleGroup(scheduleGroup, g.beatService, serviceAuthMiddleware(g.BeamRepo))

	return router.Run(port)
}

// Gateway entry point
func (g *Gateway) Start() {
	gw, err := NewGatewayService()
	if err != nil {
		return
	}

	gw.StartServer()

	// errCh := make(chan error)
	// terminationSignal := make(chan os.Signal, 1)
	// defer close(errCh)
	// defer close(terminationSignal)

	// go func() {
	// 	time.Sleep(3 * time.Second)
	// 	err := g.startInternalServer(GatewayConfig.InternalPort)
	// 	errCh <- fmt.Errorf("internal server error: %v", err)
	// }()

	// go func() {
	// 	err := g.startProxyServer(GatewayConfig.ExternalPort)
	// 	errCh <- fmt.Errorf("proxy server error: %v", err)
	// }()

	// signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)

	// select {
	// case <-terminationSignal:
	// 	log.Print("Termination signal received. ")
	// case err := <-errCh:
	// 	log.Printf("An error has occured: %v ", err)
	// }
	// log.Println("Shutting down...")

	// g.cancelFunc()
	// g.redisClient.Close()
}
