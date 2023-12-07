package gateway

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	dmap "github.com/beam-cloud/beam/internal/abstractions/map"
	common "github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
	beat "github.com/beam-cloud/beat/pkg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Gateway struct {
	pb.UnimplementedSchedulerServer

	BaseURL     string
	beatService *beat.BeatService
	eventBus    *common.EventBus
	redisClient *common.RedisClient
	BeamRepo    repository.BeamRepository
	metricsRepo repository.MetricsStatsdRepository

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

	ctx, cancel := context.WithCancel(context.Background())
	gateway := &Gateway{
		redisClient:      redisClient,
		ctx:              ctx,
		cancelFunc:       cancel,
		keyEventChan:     make(chan common.KeyEvent),
		unloadBucketChan: make(chan string),
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

	go gateway.keyEventManager.ListenForPattern(gateway.ctx, common.RedisKeys.SchedulerContainerState(types.DeploymentContainerPrefix), gateway.keyEventChan)
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

	// Register scheduler
	s, err := scheduler.NewSchedulerService()
	if err != nil {
		return err
	}

	pb.RegisterSchedulerServer(grpcServer, s)

	// Turn on reflection
	reflection.Register(grpcServer)

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
