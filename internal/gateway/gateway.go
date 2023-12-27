package gateway

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/beam-cloud/beam/internal/abstractions/function"
	"github.com/beam-cloud/beam/internal/abstractions/image"
	dmap "github.com/beam-cloud/beam/internal/abstractions/map"
	dqueue "github.com/beam-cloud/beam/internal/abstractions/queue"
	common "github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/storage"
	pb "github.com/beam-cloud/beam/proto"
	beat "github.com/beam-cloud/beat/pkg"
	"google.golang.org/grpc"
)

type Gateway struct {
	pb.UnimplementedSchedulerServer

	BaseURL          string
	beatService      *beat.BeatService
	eventBus         *common.EventBus
	redisClient      *common.RedisClient
	BeamRepo         repository.BeamRepository
	metricsRepo      repository.MetricsStatsdRepository
	Storage          storage.Storage
	Scheduler        *scheduler.Scheduler
	unloadBucketChan chan string
	keyEventManager  *common.KeyEventManager
	keyEventChan     chan common.KeyEvent
	ctx              context.Context
	cancelFunc       context.CancelFunc
}

func NewGateway() (*Gateway, error) {
	redisClient, err := common.NewRedisClient(common.WithClientName("BeamGateway"))
	if err != nil {
		return nil, err
	}

	Scheduler, err := scheduler.NewScheduler()
	if err != nil {
		return nil, err
	}

	Storage, err := storage.NewJuiceFsStorage()
	if err != nil {
		return nil, err
	}

	// Format filesystem
	err = Storage.Format(GatewayConfig.DefaultFilesystemName)
	if err != nil {
		log.Fatalf("Unable to format filesystem: %+v\n", err)
	}

	// Mount filesystem
	err = Storage.Mount(GatewayConfig.DefaultFilesystemPath)
	if err != nil {
		log.Fatalf("Unable to mount filesystem: %+v\n", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	gateway := &Gateway{
		redisClient:      redisClient,
		ctx:              ctx,
		cancelFunc:       cancel,
		keyEventChan:     make(chan common.KeyEvent),
		unloadBucketChan: make(chan string),
		Storage:          Storage,
		Scheduler:        Scheduler,
	}

	beamRepo, err := repository.NewBeamPostgresRepository()
	if err != nil {
		return nil, err
	}

	metricsRepo := repository.NewMetricsStatsdRepository()

	beatService, err := beat.NewBeatService()
	if err != nil {
		return nil, err
	}

	keyEventManager, err := common.NewKeyEventManager(redisClient)
	if err != nil {
		return nil, err
	}

	gateway.BeamRepo = beamRepo
	gateway.metricsRepo = metricsRepo
	gateway.keyEventManager = keyEventManager
	gateway.beatService = beatService

	// go gateway.keyEventManager.ListenForPattern(gateway.ctx, common.RedisKeys.SchedulerContainerState(types.DeploymentContainerPrefix), gateway.keyEventChan)
	go gateway.beatService.Run(gateway.ctx)
	// go gateway.eventBus.ReceiveEvents(gateway.ctx)

	return gateway, nil
}

// Gateway entry point
func (g *Gateway) Start() error {
	listener, err := net.Listen("tcp", GatewayConfig.GrpcServerAddress)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	// Create and register abstractions
	rm, err := dmap.NewRedisMapService(g.redisClient)
	if err != nil {
		return err
	}
	pb.RegisterMapServiceServer(grpcServer, rm)

	rq, err := dqueue.NewRedisSimpleQueueService(g.redisClient)
	if err != nil {
		return err
	}
	pb.RegisterSimpleQueueServiceServer(grpcServer, rq)

	// Register image service
	is, err := image.NewRuncImageService(context.TODO(), g.Scheduler)
	if err != nil {
		return err
	}
	pb.RegisterImageServiceServer(grpcServer, is)

	// Register function service
	fs, err := function.NewRuncFunctionService(context.TODO(), g.redisClient, g.Scheduler, g.keyEventManager)
	if err != nil {
		return err
	}
	pb.RegisterFunctionServiceServer(grpcServer, fs)

	// Register scheduler
	s, err := scheduler.NewSchedulerService()
	if err != nil {
		return err
	}
	pb.RegisterSchedulerServer(grpcServer, s)

	// Register gateway services
	// (catch-all for external gateway grpc endpoints that don't fit into an abstraction yet)
	gws, err := NewGatewayService(g)
	if err != nil {
		return err
	}

	pb.RegisterGatewayServiceServer(grpcServer, gws)

	go func() {
		err := grpcServer.Serve(listener)
		if err != nil {
			log.Printf("Failed to start grpc server: %v\n", err)
		}
	}()

	log.Println("Gateway grpc server running @", GatewayConfig.GrpcServerAddress)

	terminationSignal := make(chan os.Signal, 1)
	defer close(terminationSignal)

	signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)

	<-terminationSignal
	log.Println("Termination signal received. Shutting down...")

	return nil
}
