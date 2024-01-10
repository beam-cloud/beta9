package gateway

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/beam-cloud/beam/internal/abstractions/function"
	"github.com/beam-cloud/beam/internal/abstractions/image"
	dmap "github.com/beam-cloud/beam/internal/abstractions/map"
	simplequeue "github.com/beam-cloud/beam/internal/abstractions/queue"
	"github.com/beam-cloud/beam/internal/abstractions/taskqueue"
	gatewayservices "github.com/beam-cloud/beam/internal/gateway/services"

	volume "github.com/beam-cloud/beam/internal/abstractions/volume"

	apiv1 "github.com/beam-cloud/beam/internal/api/v1"
	"github.com/beam-cloud/beam/internal/auth"
	common "github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/storage"
	pb "github.com/beam-cloud/beam/proto"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"google.golang.org/grpc"
)

type Gateway struct {
	pb.UnimplementedSchedulerServer
	httpServer     *echo.Echo
	grpcServer     *grpc.Server
	redisClient    *common.RedisClient
	ContainerRepo  repository.ContainerRepository
	BackendRepo    repository.BackendRepository
	BeamRepo       repository.BeamRepository
	metricsRepo    repository.MetricsStatsdRepository
	Storage        storage.Storage
	Scheduler      *scheduler.Scheduler
	ctx            context.Context
	cancelFunc     context.CancelFunc
	baseRouteGroup *echo.Group
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

	Storage, err := storage.NewStorage()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	gateway := &Gateway{
		redisClient: redisClient,
		ctx:         ctx,
		cancelFunc:  cancel,
		Storage:     Storage,
		Scheduler:   Scheduler,
	}

	beamRepo, err := repository.NewBeamPostgresRepository()
	if err != nil {
		return nil, err
	}

	backendRepo, err := repository.NewBackendPostgresRepository()
	if err != nil {
		return nil, err
	}

	containerRepo := repository.NewContainerRedisRepository(redisClient)
	metricsRepo := repository.NewMetricsStatsdRepository()

	gateway.ContainerRepo = containerRepo
	gateway.BackendRepo = backendRepo
	gateway.BeamRepo = beamRepo
	gateway.metricsRepo = metricsRepo

	return gateway, nil
}

func (g *Gateway) initHttp() error {
	g.httpServer = echo.New()
	g.httpServer.HideBanner = true
	g.httpServer.HidePort = true

	g.httpServer.Use(middleware.Logger())
	g.httpServer.Use(middleware.Recover())

	authMiddleware := auth.AuthMiddleware(g.BackendRepo)
	g.baseRouteGroup = g.httpServer.Group(apiv1.HttpServerBaseRoute)

	apiv1.NewHealthGroup(g.baseRouteGroup.Group("/health"), g.redisClient)
	apiv1.NewDeployGroup(g.baseRouteGroup.Group("/deploy", authMiddleware), g.BackendRepo)
	return nil
}

func (g *Gateway) initGrpc() error {
	authInterceptor := auth.NewAuthInterceptor(g.BackendRepo)

	serverOptions := []grpc.ServerOption{
		grpc.UnaryInterceptor(authInterceptor.Unary()),
		grpc.StreamInterceptor(authInterceptor.Stream()),
	}

	g.grpcServer = grpc.NewServer(
		serverOptions...,
	)

	return nil
}

func (g *Gateway) registerServices() error {
	// Register map service
	rm, err := dmap.NewRedisMapService(g.redisClient)
	if err != nil {
		return err
	}
	pb.RegisterMapServiceServer(g.grpcServer, rm)

	// Register simple queue service
	rq, err := simplequeue.NewRedisSimpleQueueService(g.redisClient)
	if err != nil {
		return err
	}
	pb.RegisterSimpleQueueServiceServer(g.grpcServer, rq)

	// Register image service
	is, err := image.NewRuncImageService(g.ctx, g.Scheduler, g.ContainerRepo)
	if err != nil {
		return err
	}
	pb.RegisterImageServiceServer(g.grpcServer, is)

	// Register function service
	fs, err := function.NewRuncFunctionService(g.ctx, g.redisClient, g.BackendRepo, g.ContainerRepo, g.Scheduler, g.baseRouteGroup)
	if err != nil {
		return err
	}
	pb.RegisterFunctionServiceServer(g.grpcServer, fs)

	// Register task queue service
	tq, err := taskqueue.NewRedisTaskQueue(g.ctx, g.redisClient, g.Scheduler, g.ContainerRepo, g.BackendRepo, g.baseRouteGroup)
	if err != nil {
		return err
	}
	pb.RegisterTaskQueueServiceServer(g.grpcServer, tq)

	// Register volume service
	vs, err := volume.NewGlobalVolumeService(g.BackendRepo)
	if err != nil {
		return err
	}
	pb.RegisterVolumeServiceServer(g.grpcServer, vs)

	// Register scheduler
	s, err := scheduler.NewSchedulerService()
	if err != nil {
		return err
	}
	pb.RegisterSchedulerServer(g.grpcServer, s)

	// Register gateway services
	// (catch-all for external gateway grpc endpoints that don't fit into an abstraction)
	gws, err := gatewayservices.NewGatewayService(g.BackendRepo, s.Scheduler)
	if err != nil {
		return err
	}
	pb.RegisterGatewayServiceServer(g.grpcServer, gws)

	return nil
}

// Gateway entry point
func (g *Gateway) Start() error {
	listener, err := net.Listen("tcp", GatewayConfig.GrpcServerAddress)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	err = g.initGrpc()
	if err != nil {
		log.Fatalf("Failed to initialize grpc server: %v", err)
	}

	err = g.initHttp()
	if err != nil {
		log.Fatalf("Failed to initialize http server: %v", err)
	}

	err = g.registerServices()
	if err != nil {
		log.Fatalf("Failed to register services: %v", err)
	}

	go func() {
		err := g.grpcServer.Serve(listener)
		if err != nil {
			log.Printf("Failed to start grpc server: %v\n", err)
		}
	}()

	go func() {
		if err := g.httpServer.Start(GatewayConfig.HttpServerAddress); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start http server: %v", err)
		}
	}()

	log.Println("Gateway http server running @", GatewayConfig.HttpServerAddress)
	log.Println("Gateway grpc server running @", GatewayConfig.GrpcServerAddress)

	terminationSignal := make(chan os.Signal, 1)
	defer close(terminationSignal)

	signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)

	<-terminationSignal
	log.Println("Termination signal received. Shutting down...")

	return nil
}
