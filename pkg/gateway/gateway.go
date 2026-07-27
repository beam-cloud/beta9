package gateway

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/labstack/echo-contrib/pprof"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/errgroup"

	"github.com/beam-cloud/beta9/pkg/abstractions/endpoint"
	bot "github.com/beam-cloud/beta9/pkg/abstractions/experimental/bot"
	_signal "github.com/beam-cloud/beta9/pkg/abstractions/experimental/signal"
	pod "github.com/beam-cloud/beta9/pkg/abstractions/pod"
	_shell "github.com/beam-cloud/beta9/pkg/abstractions/shell"
	"github.com/beam-cloud/beta9/pkg/clients"

	disk "github.com/beam-cloud/beta9/pkg/abstractions/disk"
	"github.com/beam-cloud/beta9/pkg/abstractions/function"
	"github.com/beam-cloud/beta9/pkg/abstractions/image"
	dmap "github.com/beam-cloud/beta9/pkg/abstractions/map"
	output "github.com/beam-cloud/beta9/pkg/abstractions/output"
	simplequeue "github.com/beam-cloud/beta9/pkg/abstractions/queue"
	"github.com/beam-cloud/beta9/pkg/abstractions/secret"
	"github.com/beam-cloud/beta9/pkg/abstractions/taskqueue"
	volume "github.com/beam-cloud/beta9/pkg/abstractions/volume"
	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	gatewaymiddleware "github.com/beam-cloud/beta9/pkg/gateway/middleware"
	gatewayservices "github.com/beam-cloud/beta9/pkg/gateway/services"
	computesvc "github.com/beam-cloud/beta9/pkg/gateway/services/compute"
	repositoryservices "github.com/beam-cloud/beta9/pkg/gateway/services/repository"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	usage "github.com/beam-cloud/beta9/pkg/repository/usage"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

type Gateway struct {
	pb.UnimplementedSchedulerServer
	Config               types.AppConfig
	httpServer           *http.Server
	grpcServer           *grpc.Server
	healthServer         *health.Server
	RedisClient          *common.RedisClient
	TaskDispatcher       *task.Dispatcher
	TaskRepo             repository.TaskRepository
	WorkspaceRepo        repository.WorkspaceRepository
	ContainerRepo        repository.ContainerRepository
	BackendRepo          repository.BackendRepository
	ComputeRepo          repository.ComputeRepository
	ComputeService       *computesvc.Service
	ProviderRepo         repository.ProviderRepository
	WorkerPoolRepo       repository.WorkerPoolRepository
	EventRepo            repository.EventRepository
	UsageMetricsRepo     repository.UsageMetricsRepository
	Tailscale            *network.Tailscale
	workerRepo           repository.WorkerRepository
	Storage              storage.Storage
	DefaultStorageClient *clients.StorageClient
	Scheduler            *scheduler.Scheduler
	ctx                  context.Context
	cancelFunc           context.CancelFunc
	drainCtx             context.Context
	drainCancelFunc      context.CancelFunc
	baseRouteGroup       *echo.Group
	rootRouteGroup       *echo.Group
	draining             atomic.Bool
}

const (
	gatewayDrainPropagationDelay = 10 * time.Second
	gatewayGRPCShutdownMaxWait   = 5 * time.Second
	gatewayTailscaleStartTimeout = 30 * time.Second
	gatewayAgentWarmupTimeout    = 10 * time.Second
	gatewayAgentKeepalive        = 30 * time.Second
	gatewayLivenessService       = "liveness"
	gatewayReadinessService      = "readiness"
)

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

	usageMetricsRepo, err := usage.NewUsageMetricsRepository(config.Monitoring, string(usage.MetricsSourceGateway))
	if err != nil {
		return nil, err
	}

	eventRepo := repository.NewEventClientRepo(config)

	storage, err := storage.NewStorage(config.Storage, nil)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	drainCtx, drainCancel := context.WithCancel(context.Background())
	gateway := &Gateway{
		RedisClient:     redisClient,
		ctx:             ctx,
		cancelFunc:      cancel,
		drainCtx:        drainCtx,
		drainCancelFunc: drainCancel,
		Storage:         storage,
	}

	backendRepo, err := repository.NewBackendPostgresRepository(config.Database.Postgres, eventRepo)
	if err != nil {
		return nil, err
	}

	if err := gateway.migratePostgres(backendRepo); err != nil {
		return nil, err
	}

	tailscaleRepo := repository.NewTailscaleRedisRepository(redisClient, config)
	tailscale := network.GetOrCreateTailscale(gatewayTailscaleConfig(config), tailscaleRepo)

	workspaceRepo := repository.NewWorkspaceRedisRepository(redisClient)
	computeRepo := repository.NewComputeRedisRepository(redisClient)
	managedPoolRepo := repository.NewManagedPoolRedisRepository(redisClient)

	scheduler, err := scheduler.NewScheduler(ctx, config, redisClient, usageMetricsRepo, backendRepo, workspaceRepo, tailscale)
	if err != nil {
		return nil, err
	}

	storageClient, err := clients.NewDefaultStorageClient(ctx, config)
	if err != nil {
		return nil, err
	}

	containerRepo := repository.NewContainerRedisRepository(redisClient)
	providerRepo := repository.NewProviderRedisRepository(redisClient)
	workerRepo := repository.NewWorkerRedisRepository(redisClient, config.Worker)
	workerPoolRepo := repository.NewWorkerPoolRedisRepository(redisClient)
	taskRepo := repository.NewTaskRedisRepository(redisClient)
	taskDispatcher, err := task.NewDispatcher(ctx, taskRepo)
	if err != nil {
		return nil, err
	}

	gateway.Config = config
	gateway.Scheduler = scheduler
	gateway.TaskRepo = taskRepo
	gateway.WorkspaceRepo = workspaceRepo
	gateway.ContainerRepo = containerRepo
	gateway.ProviderRepo = providerRepo
	gateway.WorkerPoolRepo = workerPoolRepo
	gateway.BackendRepo = backendRepo
	gateway.ComputeRepo = computeRepo
	gateway.Tailscale = tailscale
	gateway.TaskDispatcher = taskDispatcher
	gateway.UsageMetricsRepo = usageMetricsRepo
	gateway.EventRepo = eventRepo
	gateway.workerRepo = workerRepo
	gateway.DefaultStorageClient = storageClient

	keyEventManager, err := common.NewKeyEventManager(redisClient)
	if err != nil {
		return nil, err
	}
	gateway.ComputeService = computesvc.New(computesvc.Options{
		Config:           config,
		BackendRepo:      backendRepo,
		ContainerRepo:    containerRepo,
		Scheduler:        scheduler,
		EventRepo:        eventRepo,
		WorkerRepo:       workerRepo,
		WorkerPoolRepo:   workerPoolRepo,
		UsageMetricsRepo: usageMetricsRepo,
		ComputeRepo:      computeRepo,
		ManagedPoolRepo:  managedPoolRepo,
		KeyEventManager:  keyEventManager,
		RedisClient:      redisClient,
		Tailscale:        tailscale,
	})
	gateway.ComputeService.Start(ctx)

	return gateway, nil
}

type postgresMigrator interface {
	MigrateContext(context.Context) error
}

func (g *Gateway) migratePostgres(backendRepo postgresMigrator) error {
	const lockTTL = 30 * time.Second
	lock := common.NewRedisLock(g.RedisClient)
	return lock.WithLease(g.ctx, "gateway:init:postgres:lock", common.RedisLockOptions{
		TtlS:          int(lockTTL.Seconds()),
		Retries:       480,
		RetryInterval: 500 * time.Millisecond,
	}, backendRepo.MigrateContext)
}

func (g *Gateway) initHttp() error {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	if g.Config.DebugMode {
		pprof.Register(e)
	}

	skipSubdomainRoutes := func(c echo.Context) bool {
		baseDomain := gatewaymiddleware.ParseHostFromURL(g.Config.GatewayService.HTTP.GetExternalURL())
		subdomain := gatewaymiddleware.ParseSubdomain(c.Request().Host, baseDomain)
		return subdomain != ""
	}

	e.Pre(middleware.RemoveTrailingSlashWithConfig(middleware.TrailingSlashConfig{
		Skipper: skipSubdomainRoutes,
	}))

	configureEchoLogger(e, g.Config.GatewayService.HTTP.EnablePrettyLogs)
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: g.Config.GatewayService.HTTP.CORS.AllowedOrigins,
		AllowHeaders: g.Config.GatewayService.HTTP.CORS.AllowedHeaders,
		AllowMethods: g.Config.GatewayService.HTTP.CORS.AllowedMethods,
	}))
	// Subdomain middleware can dispatch requests directly, so drain handling must run first.
	e.Use(g.drainMiddleware)
	e.Use(gatewaymiddleware.Subdomain(g.Config.GatewayService.HTTP.GetExternalURL(), g.BackendRepo, g.RedisClient))
	e.Use(middleware.Recover())
	e.GET("/install/agent", agentInstallScriptHandler())
	e.GET("/install/agent/:os/:arch", agentBinaryHandler())

	// Accept both HTTP/2 and HTTP/1
	g.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%v", g.Config.GatewayService.HTTP.Port),
		Handler: h2c.NewHandler(e, &http2.Server{}),
	}

	authMiddleware := auth.AuthMiddleware(g.BackendRepo, g.WorkspaceRepo)
	g.baseRouteGroup = e.Group(apiv1.HttpServerBaseRoute)
	g.rootRouteGroup = e.Group(apiv1.HttpServerRootRoute)

	apiv1.NewHealthGroup(g.baseRouteGroup.Group("/health"), g.RedisClient, g.BackendRepo, g.isReady)
	apiv1.NewMachineGroup(g.baseRouteGroup.Group("/machine", authMiddleware), g.ProviderRepo, g.Tailscale, g.Config, g.workerRepo)
	apiv1.NewWorkspaceGroup(g.baseRouteGroup.Group("/workspace", authMiddleware), g.BackendRepo, g.WorkspaceRepo, g.DefaultStorageClient, g.Config)
	apiv1.NewTokenGroup(g.baseRouteGroup.Group("/token", authMiddleware), g.BackendRepo, g.WorkspaceRepo, g.Config)
	apiv1.NewTaskGroup(g.baseRouteGroup.Group("/task", authMiddleware), g.RedisClient, g.TaskRepo, g.ContainerRepo, g.EventRepo, g.BackendRepo, g.TaskDispatcher, g.Scheduler, g.Config)
	apiv1.NewEventGroup(g.baseRouteGroup.Group("/events", authMiddleware), g.BackendRepo, g.ContainerRepo, g.EventRepo)
	apiv1.NewLogGroup(g.baseRouteGroup.Group("/logs", authMiddleware), g.BackendRepo, g.ContainerRepo, repository.NewComputeRedisRepository(g.RedisClient), g.EventRepo)
	apiv1.NewMetricsGroup(g.baseRouteGroup.Group("/metrics", authMiddleware), g.BackendRepo, g.EventRepo)
	apiv1.NewContainerGroup(g.baseRouteGroup.Group("/container", authMiddleware), g.BackendRepo, g.ContainerRepo, *g.Scheduler, g.Config)
	apiv1.NewStubGroup(g.baseRouteGroup.Group("/stub", authMiddleware), g.BackendRepo, g.ContainerRepo, g.EventRepo, g.RedisClient, g.Config)
	apiv1.NewConcurrencyLimitGroup(g.baseRouteGroup.Group("/concurrency-limit", authMiddleware), g.BackendRepo, g.WorkspaceRepo)
	apiv1.NewDeploymentGroup(g.baseRouteGroup.Group("/deployment", authMiddleware), g.BackendRepo, g.ContainerRepo, *g.Scheduler, g.RedisClient, g.Config)
	apiv1.NewAppGroup(g.baseRouteGroup.Group("/app", authMiddleware), g.BackendRepo, g.Config, g.ContainerRepo, *g.Scheduler, g.RedisClient)
	apiv1.NewPoolGroup(g.baseRouteGroup.Group("/pools", authMiddleware), g.ComputeService)

	return nil
}

func (g *Gateway) initGrpc() error {
	authInterceptor := auth.NewAuthInterceptor(g.Config, g.BackendRepo, g.WorkspaceRepo)

	serverOptions := []grpc.ServerOption{
		grpc.UnaryInterceptor(authInterceptor.Unary()),
		grpc.StreamInterceptor(authInterceptor.Stream()),
		grpc.MaxRecvMsgSize(g.Config.GatewayService.GRPC.MaxRecvMsgSize * 1024 * 1024),
		grpc.MaxSendMsgSize(g.Config.GatewayService.GRPC.MaxSendMsgSize * 1024 * 1024),
		// Permit client keepalive pings (including without active streams) so
		// long-lived clients like workers can detect a connection broken by a
		// gateway rollout. MinTime must be <= the client's keepalive Time to
		// avoid sending "too_many_pings" GOAWAYs.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		// Server-side keepalive so the gateway also detects and reaps dead client
		// connections rather than holding them open.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    20 * time.Second,
			Timeout: 10 * time.Second,
		}),
	}

	g.grpcServer = grpc.NewServer(
		serverOptions...,
	)

	return nil
}

// initGrpcProxy exposes gRPC services as HTTP endpoints.
func (g *Gateway) initGrpcProxy(grpcAddr string) error {
	ctx, cancel := context.WithCancel(g.ctx)
	g.httpServer.RegisterOnShutdown(func() {
		cancel()
	})
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if err := pb.RegisterPodServiceHandlerFromEndpoint(ctx, mux, grpcAddr, opts); err != nil {
		return err
	}
	if err := pb.RegisterImageServiceHandlerFromEndpoint(ctx, mux, grpcAddr, opts); err != nil {
		return err
	}
	if err := pb.RegisterVolumeServiceHandlerFromEndpoint(ctx, mux, grpcAddr, opts); err != nil {
		return err
	}
	if err := pb.RegisterDiskServiceHandlerFromEndpoint(ctx, mux, grpcAddr, opts); err != nil {
		return err
	}
	if err := pb.RegisterGatewayServiceHandlerFromEndpoint(ctx, mux, grpcAddr, opts); err != nil {
		return err
	}
	// No need to add auth middleware: grpc-gateway maps the 'Authorization' header
	// to gRPC metadata, and the destination gRPC server's interceptor will handle
	// authorization for every request.
	wrappedHandler := gatewaymiddleware.GatewayEvents(g.EventRepo, g.BackendRepo, g.WorkspaceRepo)(http.StripPrefix(apiv1.HttpServerBaseRoute+"/gateway", mux))
	g.baseRouteGroup.Any("/gateway/*", wrappedHandler)
	return nil
}

// Register repository services
func (g *Gateway) registerRepositoryServices() error {
	wr := repositoryservices.NewWorkerRepositoryService(g.ctx, g.workerRepo, g.ContainerRepo, g.BackendRepo, g.ComputeRepo, g.EventRepo, g.RedisClient, g.Config, g.Config.Cache.Coordinator.Token)
	pb.RegisterWorkerRepositoryServiceServer(g.grpcServer, wr)

	cr := repositoryservices.NewContainerRepositoryService(g.ctx, g.ContainerRepo)
	pb.RegisterContainerRepositoryServiceServer(g.grpcServer, cr)

	br := repositoryservices.NewBackendRepositoryService(g.ctx, g.BackendRepo)
	pb.RegisterBackendRepositoryServiceServer(g.grpcServer, br)

	return nil
}

func (g *Gateway) registerServices() error {
	err := g.registerRepositoryServices()
	if err != nil {
		return err
	}

	// Register map service
	rm, err := dmap.NewRedisMapService(g.RedisClient)
	if err != nil {
		return err
	}
	pb.RegisterMapServiceServer(g.grpcServer, rm)

	// Register simple queue service
	rq, err := simplequeue.NewRedisSimpleQueueService(g.RedisClient)
	if err != nil {
		return err
	}
	pb.RegisterSimpleQueueServiceServer(g.grpcServer, rq)

	// Register image service
	is, err := image.NewContainerImageService(g.ctx, image.ImageServiceOpts{
		Config:        g.Config,
		ContainerRepo: g.ContainerRepo,
		Scheduler:     g.Scheduler,
		Tailscale:     g.Tailscale,
		BackendRepo:   g.BackendRepo,
		RedisClient:   g.RedisClient,
	})
	if err != nil {
		return err
	}
	pb.RegisterImageServiceServer(g.grpcServer, is)

	// Register function service
	fs, err := function.NewContainerFunctionService(g.ctx, function.FunctionServiceOpts{
		Config:           g.Config,
		RedisClient:      g.RedisClient,
		BackendRepo:      g.BackendRepo,
		WorkspaceRepo:    g.WorkspaceRepo,
		TaskRepo:         g.TaskRepo,
		ContainerRepo:    g.ContainerRepo,
		Scheduler:        g.Scheduler,
		Tailscale:        g.Tailscale,
		RouteGroup:       g.rootRouteGroup,
		TaskDispatcher:   g.TaskDispatcher,
		EventRepo:        g.EventRepo,
		UsageMetricsRepo: g.UsageMetricsRepo,
	})
	if err != nil {
		return err
	}
	pb.RegisterFunctionServiceServer(g.grpcServer, fs)

	// Register task queue service
	tq, err := taskqueue.NewRedisTaskQueueService(g.ctx, taskqueue.TaskQueueServiceOpts{
		Config:           g.Config,
		RedisClient:      g.RedisClient,
		BackendRepo:      g.BackendRepo,
		WorkspaceRepo:    g.WorkspaceRepo,
		TaskRepo:         g.TaskRepo,
		ContainerRepo:    g.ContainerRepo,
		Scheduler:        g.Scheduler,
		Tailscale:        g.Tailscale,
		RouteGroup:       g.rootRouteGroup,
		TaskDispatcher:   g.TaskDispatcher,
		EventRepo:        g.EventRepo,
		UsageMetricsRepo: g.UsageMetricsRepo,
	})
	if err != nil {
		return err
	}
	pb.RegisterTaskQueueServiceServer(g.grpcServer, tq)

	// Register endpoint service
	ws, err := endpoint.NewHTTPEndpointService(g.ctx, endpoint.EndpointServiceOpts{
		Config:           g.Config,
		ContainerRepo:    g.ContainerRepo,
		BackendRepo:      g.BackendRepo,
		WorkspaceRepo:    g.WorkspaceRepo,
		TaskRepo:         g.TaskRepo,
		RedisClient:      g.RedisClient,
		Scheduler:        g.Scheduler,
		RouteGroup:       g.rootRouteGroup,
		Tailscale:        g.Tailscale,
		TaskDispatcher:   g.TaskDispatcher,
		EventRepo:        g.EventRepo,
		UsageMetricsRepo: g.UsageMetricsRepo,
	})
	if err != nil {
		return err
	}
	pb.RegisterEndpointServiceServer(g.grpcServer, ws)

	// Register volume service
	vs, err := volume.NewGlobalVolumeService(g.Config.FileService, g.Config.Storage.WorkspaceStorage, g.BackendRepo, g.WorkspaceRepo, g.RedisClient, g.rootRouteGroup)
	if err != nil {
		return err
	}
	pb.RegisterVolumeServiceServer(g.grpcServer, vs)

	// Register disk service
	ds, err := disk.NewGlobalDiskService(g.BackendRepo, g.WorkspaceRepo, g.baseRouteGroup)
	if err != nil {
		return err
	}
	pb.RegisterDiskServiceServer(g.grpcServer, ds)

	// Register pod service
	ps, err := pod.NewPodService(
		g.ctx,
		pod.PodServiceOpts{
			Config:         g.Config,
			BackendRepo:    g.BackendRepo,
			ContainerRepo:  g.ContainerRepo,
			ComputeRepo:    g.ComputeRepo,
			Tailscale:      g.Tailscale,
			Scheduler:      g.Scheduler,
			RedisClient:    g.RedisClient,
			EventRepo:      g.EventRepo,
			RouteGroup:     g.rootRouteGroup,
			WorkspaceRepo:  g.WorkspaceRepo,
			WorkerRepo:     g.workerRepo,
			WorkerPoolRepo: g.WorkerPoolRepo,
			TaskDispatcher: g.TaskDispatcher,
			DrainContext:   g.drainCtx,
		},
	)
	if err != nil {
		return err
	}
	pb.RegisterPodServiceServer(g.grpcServer, ps)

	// Register output service
	o, err := output.NewOutputRedisService(g.Config, g.RedisClient, g.BackendRepo, g.rootRouteGroup)
	if err != nil {
		return err
	}
	pb.RegisterOutputServiceServer(g.grpcServer, o)

	// Register Secret service
	secretService := secret.NewSecretService(g.BackendRepo, g.WorkspaceRepo, g.rootRouteGroup)
	pb.RegisterSecretServiceServer(g.grpcServer, secretService)

	// Register Signal service
	signalService, err := _signal.NewRedisSignalService(g.RedisClient)
	if err != nil {
		return err
	}
	pb.RegisterSignalServiceServer(g.grpcServer, signalService)

	// Register Bot service
	botService, err := bot.NewPetriBotService(g.ctx,
		bot.BotServiceOpts{
			Config:         g.Config,
			ContainerRepo:  g.ContainerRepo,
			BackendRepo:    g.BackendRepo,
			WorkspaceRepo:  g.WorkspaceRepo,
			TaskRepo:       g.TaskRepo,
			RedisClient:    g.RedisClient,
			Scheduler:      g.Scheduler,
			RouteGroup:     g.rootRouteGroup,
			Tailscale:      g.Tailscale,
			TaskDispatcher: g.TaskDispatcher,
			EventRepo:      g.EventRepo,
		})
	if err != nil {
		return err
	}
	pb.RegisterBotServiceServer(g.grpcServer, botService)

	// Register shell service
	ss, err := _shell.NewSSHShellService(g.ctx, _shell.ShellServiceOpts{
		Config:         g.Config,
		RedisClient:    g.RedisClient,
		Scheduler:      g.Scheduler,
		BackendRepo:    g.BackendRepo,
		ComputeRepo:    g.ComputeRepo,
		WorkspaceRepo:  g.WorkspaceRepo,
		ContainerRepo:  g.ContainerRepo,
		WorkerRepo:     g.workerRepo,
		WorkerPoolRepo: g.WorkerPoolRepo,
		Tailscale:      g.Tailscale,
		EventRepo:      g.EventRepo,
		RouteGroup:     g.rootRouteGroup,
	})
	if err != nil {
		return err
	}
	pb.RegisterShellServiceServer(g.grpcServer, ss)

	// Register scheduler
	s, err := scheduler.NewSchedulerService(g.Scheduler)
	if err != nil {
		return err
	}
	pb.RegisterSchedulerServer(g.grpcServer, s)

	// Register gateway services
	// (catch-all for external gateway grpc endpoints that don't fit into an abstraction)
	gws, err := gatewayservices.NewGatewayService(&gatewayservices.GatewayServiceOpts{
		Ctx:              g.ctx,
		Config:           g.Config,
		BackendRepo:      g.BackendRepo,
		WorkspaceRepo:    g.WorkspaceRepo,
		ContainerRepo:    g.ContainerRepo,
		ProviderRepo:     g.ProviderRepo,
		Scheduler:        g.Scheduler,
		TaskDispatcher:   g.TaskDispatcher,
		RedisClient:      g.RedisClient,
		EventRepo:        g.EventRepo,
		WorkerRepo:       g.workerRepo,
		WorkerPoolRepo:   g.WorkerPoolRepo,
		ComputeRepo:      g.ComputeRepo,
		ComputeService:   g.ComputeService,
		UsageMetricsRepo: g.UsageMetricsRepo,
		Tailscale:        g.Tailscale,
	})
	if err != nil {
		return err
	}
	pb.RegisterGatewayServiceServer(g.grpcServer, gws)

	g.registerHealthService()

	return nil
}

// Gateway entry point
func (g *Gateway) Start() error {
	var err error
	if g.Config.Tailscale.Enabled {
		ctx, cancel := context.WithTimeout(g.ctx, gatewayTailscaleStartTimeout)
		err := g.Tailscale.Start(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("failed to connect gateway to tailnet: %w", err)
		}
		warmupCtx, cancel := context.WithTimeout(g.ctx, gatewayAgentWarmupTimeout)
		g.Tailscale.WarmPeers(warmupCtx, types.AgentTailnetHostnamePrefix)
		cancel()
		go g.Tailscale.KeepPeersAlive(g.ctx, types.AgentTailnetHostnamePrefix, gatewayAgentKeepalive)
	}

	if g.Config.Monitoring.Telemetry.Enabled {
		_, err = common.SetupTelemetry(g.ctx, types.DefaultGatewayServiceName, g.Config)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to setup telemetry")
		}
	}

	err = g.initGrpc()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize grpc server")
	}

	err = g.initHttp()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize http server")
	}

	err = g.initGrpcProxy(fmt.Sprintf(":%d", g.Config.GatewayService.GRPC.Port))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize grpc gateway")
	}

	err = g.registerServices()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to register services")
	}

	if g.Config.DebugMode {
		reflection.Register(g.grpcServer)
	}

	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", g.Config.GatewayService.GRPC.Port))
		if err != nil {
			log.Fatal().Err(err).Msg("failed to listen")
		}

		if err := g.grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Fatal().Err(err).Msg("failed to start grpc server")
		}
	}()

	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", g.Config.GatewayService.HTTP.Port))
		if err != nil {
			log.Fatal().Err(err).Msg("failed to listen")
		}

		if err := g.httpServer.Serve(lis); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("failed to start http server")
		}
	}()

	log.Info().Int("port", g.Config.GatewayService.HTTP.Port).Msg("gateway http server running")
	log.Info().Int("port", g.Config.GatewayService.GRPC.Port).Msg("gateway grpc server running")

	terminationSignal := make(chan os.Signal, 1)
	signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(terminationSignal)
	<-terminationSignal
	log.Info().Msg("termination signal received. shutting down...")
	g.startDraining()
	waitForDrainPropagation(g.ctx)
	g.shutdown()

	return nil
}

func (g *Gateway) isReady() bool {
	return !g.draining.Load()
}

func (g *Gateway) registerHealthService() {
	hs := g.newHealthServer()
	healthpb.RegisterHealthServer(g.grpcServer, hs)
}

func (g *Gateway) newHealthServer() *health.Server {
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus(gatewayLivenessService, healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus(gatewayReadinessService, healthpb.HealthCheckResponse_SERVING)
	g.healthServer = hs

	if g.ctx != nil {
		go func() {
			<-g.ctx.Done()
			hs.Shutdown()
		}()
	}
	return hs
}

func (g *Gateway) drainMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Response().Before(func() {
			if g.draining.Load() {
				c.Response().Header().Set(echo.HeaderConnection, "close")
			}
		})
		return next(c)
	}
}

func (g *Gateway) startDraining() {
	if !g.draining.CompareAndSwap(false, true) {
		return
	}

	if g.httpServer != nil {
		g.httpServer.SetKeepAlivesEnabled(false)
	}
	if g.healthServer != nil {
		g.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		g.healthServer.SetServingStatus(gatewayReadinessService, healthpb.HealthCheckResponse_NOT_SERVING)
	}
	if g.drainCancelFunc != nil {
		g.drainCancelFunc()
	}
	log.Info().Msg("gateway entering drain mode")
}

func waitForDrainPropagation(ctx context.Context) {
	timer := time.NewTimer(gatewayDrainPropagationDelay)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

func grpcGracefulStopTimeout(shutdownTimeout time.Duration) time.Duration {
	if shutdownTimeout <= 0 || shutdownTimeout > gatewayGRPCShutdownMaxWait {
		return gatewayGRPCShutdownMaxWait
	}
	return shutdownTimeout
}

// Shutdown gracefully shuts down the gateway.
// This function is blocking and will only return when the gateway has been shut down.
func (g *Gateway) shutdown() {
	timeout := g.Config.GatewayService.ShutdownTimeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return g.httpServer.Shutdown(ctx)
	})

	eg.Go(func() error {
		done := make(chan struct{})
		go func() {
			g.grpcServer.GracefulStop()
			close(done)
		}()

		timer := time.NewTimer(grpcGracefulStopTimeout(timeout))
		defer timer.Stop()

		select {
		case <-timer.C:
			g.grpcServer.Stop()
			return nil
		case <-ctx.Done():
			g.grpcServer.Stop()
			return nil
		case <-done:
			return nil
		}
	})

	if err := eg.Wait(); err != nil {
		log.Warn().Err(err).Msg("gateway shutdown completed with errors")
	}

	g.cancelFunc()
}
