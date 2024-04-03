package gateway

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/beam-cloud/beta9/internal/abstractions/endpoint"
	"github.com/beam-cloud/beta9/internal/task"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"google.golang.org/grpc"

	"github.com/beam-cloud/beta9/internal/abstractions/container"
	"github.com/beam-cloud/beta9/internal/abstractions/function"
	"github.com/beam-cloud/beta9/internal/abstractions/image"
	dmap "github.com/beam-cloud/beta9/internal/abstractions/map"
	simplequeue "github.com/beam-cloud/beta9/internal/abstractions/queue"
	"github.com/beam-cloud/beta9/internal/abstractions/taskqueue"
	"github.com/beam-cloud/beta9/internal/network"
	metrics "github.com/beam-cloud/beta9/internal/repository/metrics"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	volume "github.com/beam-cloud/beta9/internal/abstractions/volume"
	apiv1 "github.com/beam-cloud/beta9/internal/api/v1"
	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/common"
	gatewayservices "github.com/beam-cloud/beta9/internal/gateway/services"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/storage"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type Gateway struct {
	pb.UnimplementedSchedulerServer
	config         types.AppConfig
	httpServer     *http.Server
	grpcServer     *grpc.Server
	redisClient    *common.RedisClient
	TaskDispatcher *task.Dispatcher
	ContainerRepo  repository.ContainerRepository
	BackendRepo    repository.BackendRepository
	ProviderRepo   repository.ProviderRepository
	Tailscale      *network.Tailscale
	metricsRepo    repository.MetricsRepository
	Storage        storage.Storage
	Scheduler      *scheduler.Scheduler
	ctx            context.Context
	cancelFunc     context.CancelFunc
	baseRouteGroup *echo.Group
	rootRouteGroup *echo.Group
}

func NewGateway() (*Gateway, error) {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()

	redisClient, err := common.NewRedisClient(config.Database.Redis, common.WithClientName("Beta9Gateway"))
	if err != nil {
		return nil, err
	}

	metricsRepo, err := metrics.NewMetrics(config.Monitoring, string(metrics.MetricsSourceGateway))
	if err != nil {
		return nil, err
	}

	backendRepo, err := repository.NewBackendPostgresRepository(config.Database.Postgres)
	if err != nil {
		return nil, err
	}

	storage, err := storage.NewStorage(config.Storage)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	gateway := &Gateway{
		redisClient: redisClient,
		ctx:         ctx,
		cancelFunc:  cancel,
		Storage:     storage,
	}

	tailscaleRepo := repository.NewTailscaleRedisRepository(redisClient, config)
	tailscale := network.GetOrCreateTailscale(network.TailscaleConfig{
		ControlURL: config.Tailscale.ControlURL,
		AuthKey:    config.Tailscale.AuthKey,
		Debug:      config.Tailscale.Debug,
		Ephemeral:  true,
	}, tailscaleRepo)

	scheduler, err := scheduler.NewScheduler(ctx, config, redisClient, metricsRepo, backendRepo, tailscale)
	if err != nil {
		return nil, err
	}

	containerRepo := repository.NewContainerRedisRepository(redisClient)
	providerRepo := repository.NewProviderRedisRepository(redisClient)
	taskDispatcher, err := task.NewDispatcher(ctx, redisClient)
	if err != nil {
		return nil, err
	}

	gateway.config = config
	gateway.Scheduler = scheduler
	gateway.ContainerRepo = containerRepo
	gateway.ProviderRepo = providerRepo
	gateway.BackendRepo = backendRepo
	gateway.Tailscale = tailscale
	gateway.TaskDispatcher = taskDispatcher
	gateway.metricsRepo = metricsRepo

	return gateway, nil
}

func (g *Gateway) initHttp() error {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Skipper: func(c echo.Context) bool {
			return c.Request().URL.Path == "/api/v1/health"
		},
	}))
	e.Use(middleware.Recover())

	// Accept both HTTP/2 and HTTP/1
	g.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%v", g.config.GatewayService.HTTPPort),
		Handler: h2c.NewHandler(e, &http2.Server{}),
	}

	authMiddleware := auth.AuthMiddleware(g.BackendRepo)
	g.baseRouteGroup = e.Group(apiv1.HttpServerBaseRoute)
	g.rootRouteGroup = e.Group(apiv1.HttpServerRootRoute)

	apiv1.NewHealthGroup(g.baseRouteGroup.Group("/health"), g.redisClient)
	apiv1.NewMachineGroup(g.baseRouteGroup.Group("/machine", authMiddleware), g.ProviderRepo, g.Tailscale, g.config)
	apiv1.NewWorkspaceGroup(g.baseRouteGroup.Group("/workspace", authMiddleware), g.BackendRepo, g.config)
	apiv1.NewTokenGroup(g.baseRouteGroup.Group("/token", authMiddleware), g.BackendRepo, g.config)
	apiv1.NewTaskGroup(g.baseRouteGroup.Group("/task", authMiddleware), g.BackendRepo, g.config)
	apiv1.NewDeploymentGroup(g.baseRouteGroup.Group("/deployment", authMiddleware), g.BackendRepo, g.config)

	return nil
}

func (g *Gateway) initGrpc() error {
	authInterceptor := auth.NewAuthInterceptor(g.BackendRepo)

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
	is, err := image.NewRuncImageService(g.ctx, image.ImageServiceOpts{
		Config:        g.config,
		ContainerRepo: g.ContainerRepo,
		Scheduler:     g.Scheduler,
		Tailscale:     g.Tailscale,
	})
	if err != nil {
		return err
	}
	pb.RegisterImageServiceServer(g.grpcServer, is)

	// Register function service
	fs, err := function.NewRuncFunctionService(g.ctx, function.FunctionServiceOpts{
		Config:        g.config,
		RedisClient:   g.redisClient,
		BackendRepo:   g.BackendRepo,
		ContainerRepo: g.ContainerRepo,
		Scheduler:     g.Scheduler,
		Tailscale:     g.Tailscale,
		RouteGroup:    g.rootRouteGroup,
	})
	if err != nil {
		return err
	}
	pb.RegisterFunctionServiceServer(g.grpcServer, fs)

	// Register task queue service
	tq, err := taskqueue.NewRedisTaskQueueService(g.ctx, taskqueue.TaskQueueServiceOpts{
		Config:         g.config,
		RedisClient:    g.redisClient,
		BackendRepo:    g.BackendRepo,
		ContainerRepo:  g.ContainerRepo,
		Scheduler:      g.Scheduler,
		Tailscale:      g.Tailscale,
		RouteGroup:     g.rootRouteGroup,
		TaskDispatcher: g.TaskDispatcher,
	})
	if err != nil {
		return err
	}
	pb.RegisterTaskQueueServiceServer(g.grpcServer, tq)

	// Register endpoint service
	ws, err := endpoint.NewEndpointService(g.ctx, endpoint.EndpointServiceOpts{
		Config:      g.config,
		RedisClient: g.redisClient,
		Scheduler:   g.Scheduler,
		RouteGroup:  g.rootRouteGroup,
		Tailscale:   g.Tailscale,
	})
	if err != nil {
		return err
	}
	pb.RegisterEndpointServiceServer(g.grpcServer, ws)

	// Register volume service
	vs, err := volume.NewGlobalVolumeService(g.BackendRepo)
	if err != nil {
		return err
	}
	pb.RegisterVolumeServiceServer(g.grpcServer, vs)

	// Register container service
	cs, err := container.NewContainerService(
		g.ctx,
		container.ContainerServiceOpts{
			Config:        g.config,
			BackendRepo:   g.BackendRepo,
			ContainerRepo: g.ContainerRepo,
			Tailscale:     g.Tailscale,
			Scheduler:     g.Scheduler,
			RedisClient:   g.redisClient,
		},
	)
	if err != nil {
		return err
	}
	pb.RegisterContainerServiceServer(g.grpcServer, cs)

	// Register scheduler
	s, err := scheduler.NewSchedulerService(g.Scheduler)
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
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", g.config.GatewayService.GRPCPort))
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
		if err := g.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start http server: %v", err)
		}
	}()

	log.Println("Gateway http server running @", g.config.GatewayService.HTTPPort)
	log.Println("Gateway grpc server running @", g.config.GatewayService.GRPCPort)

	terminationSignal := make(chan os.Signal, 1)
	defer close(terminationSignal)

	signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)

	<-terminationSignal
	log.Println("Termination signal received. Shutting down...")

	return nil
}
