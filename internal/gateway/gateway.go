package gateway

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/beam-cloud/beta9/internal/abstractions/function"
	"github.com/beam-cloud/beta9/internal/abstractions/image"
	dmap "github.com/beam-cloud/beta9/internal/abstractions/map"
	simplequeue "github.com/beam-cloud/beta9/internal/abstractions/queue"
	"github.com/beam-cloud/beta9/internal/abstractions/taskqueue"
	gatewayservices "github.com/beam-cloud/beta9/internal/gateway/services"

	volume "github.com/beam-cloud/beta9/internal/abstractions/volume"

	apiv1 "github.com/beam-cloud/beta9/internal/api/v1"
	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/common"

	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/storage"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"google.golang.org/grpc"
)

type Gateway struct {
	pb.UnimplementedSchedulerServer
	config         types.AppConfig
	httpServer     *echo.Echo
	grpcServer     *grpc.Server
	repoManager    *repository.RepositoryManager
	Storage        storage.Storage
	Scheduler      *scheduler.Scheduler
	ctx            context.Context
	cancelFunc     context.CancelFunc
	baseRouteGroup *echo.Group
}

func NewGateway() (*Gateway, error) {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()

	postgresClient, err := repository.NewPostgresClient(config.Database.Postgres)
	if err != nil {
		return nil, err
	}

	redisClient, err := common.NewRedisClient(config.Database.Redis, common.WithClientName("Beta9Gateway"))
	if err != nil {
		return nil, err
	}

	repoManager, err := repository.NewRepositoryManager(postgresClient, redisClient)
	if err != nil {
		return nil, err
	}

	scheduler, err := scheduler.NewScheduler(config, redisClient)
	if err != nil {
		return nil, err
	}

	storage, err := storage.NewStorage(config.Storage)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Gateway{
		ctx:         ctx,
		cancelFunc:  cancel,
		config:      config,
		repoManager: repoManager,
		Storage:     storage,
		Scheduler:   scheduler,
	}, nil
}

func (g *Gateway) initHttp() error {
	g.httpServer = echo.New()
	g.httpServer.HideBanner = true
	g.httpServer.HidePort = true

	g.httpServer.Use(middleware.Logger())
	g.httpServer.Use(middleware.Recover())

	authMiddleware := auth.AuthMiddleware(g.repoManager.Backend)
	g.baseRouteGroup = g.httpServer.Group(apiv1.HttpServerBaseRoute)

	apiv1.NewHealthGroup(g.baseRouteGroup.Group("/health"), g.repoManager.RedisClient)
	apiv1.NewDeployGroup(g.baseRouteGroup.Group("/deploy", authMiddleware), g.repoManager.Backend)
	return nil
}

func (g *Gateway) initGrpc() error {
	authInterceptor := auth.NewAuthInterceptor(g.repoManager.Backend)

	serverOptions := []grpc.ServerOption{
		grpc.UnaryInterceptor(authInterceptor.Unary()),
		grpc.StreamInterceptor(authInterceptor.Stream()),
		grpc.MaxRecvMsgSize(g.config.GatewayService.MaxRecvMsgSize * 1024 * 1024),
		grpc.MaxSendMsgSize(g.config.GatewayService.MaxSendMsgSize * 1024 * 1024),
	}

	g.grpcServer = grpc.NewServer(
		serverOptions...,
	)

	return nil
}

func (g *Gateway) registerServices() error {
	// Register map service
	rm, err := dmap.NewRedisMapService(g.repoManager.RedisClient)
	if err != nil {
		return err
	}
	pb.RegisterMapServiceServer(g.grpcServer, rm)

	// Register simple queue service
	rq, err := simplequeue.NewRedisSimpleQueueService(g.repoManager.RedisClient)
	if err != nil {
		return err
	}
	pb.RegisterSimpleQueueServiceServer(g.grpcServer, rq)

	// Register image service
	is, err := image.NewRuncImageService(g.ctx, g.config.ImageService, g.Scheduler, g.repoManager.Container)
	if err != nil {
		return err
	}
	pb.RegisterImageServiceServer(g.grpcServer, is)

	// Register function service
	fs, err := function.NewRuncFunctionService(g.ctx, g.repoManager, g.Scheduler, g.baseRouteGroup)
	if err != nil {
		return err
	}
	pb.RegisterFunctionServiceServer(g.grpcServer, fs)

	// Register task queue service
	tq, err := taskqueue.NewRedisTaskQueue(g.ctx, g.repoManager, g.Scheduler, g.baseRouteGroup)
	if err != nil {
		return err
	}
	pb.RegisterTaskQueueServiceServer(g.grpcServer, tq)

	// Register volume service
	vs, err := volume.NewGlobalVolumeService(g.repoManager.Backend)
	if err != nil {
		return err
	}
	pb.RegisterVolumeServiceServer(g.grpcServer, vs)

	// Register scheduler
	s, err := scheduler.NewSchedulerService(g.config, g.repoManager.RedisClient)
	if err != nil {
		return err
	}
	pb.RegisterSchedulerServer(g.grpcServer, s)

	// Register gateway services
	// (catch-all for external gateway grpc endpoints that don't fit into an abstraction)
	gws, err := gatewayservices.NewGatewayService(g.repoManager.Backend, s.Scheduler)
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
