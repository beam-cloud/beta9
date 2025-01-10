package gateway

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/labstack/echo-contrib/pprof"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/beam-cloud/beta9/pkg/abstractions/container"
	"github.com/beam-cloud/beta9/pkg/abstractions/endpoint"
	bot "github.com/beam-cloud/beta9/pkg/abstractions/experimental/bot"
	_signal "github.com/beam-cloud/beta9/pkg/abstractions/experimental/signal"
	_shell "github.com/beam-cloud/beta9/pkg/abstractions/shell"

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
	gatewayMiddleware "github.com/beam-cloud/beta9/pkg/gateway/middleware"
	gatewayservices "github.com/beam-cloud/beta9/pkg/gateway/services"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	metrics "github.com/beam-cloud/beta9/pkg/repository/metrics"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type Gateway struct {
	pb.UnimplementedSchedulerServer
	Config         types.AppConfig
	httpServer     *http.Server
	grpcServer     *grpc.Server
	RedisClient    *common.RedisClient
	TaskDispatcher *task.Dispatcher
	TaskRepo       repository.TaskRepository
	WorkspaceRepo  repository.WorkspaceRepository
	ContainerRepo  repository.ContainerRepository
	BackendRepo    repository.BackendRepository
	ProviderRepo   repository.ProviderRepository
	EventRepo      repository.EventRepository
	Tailscale      *network.Tailscale
	metricsRepo    repository.MetricsRepository
	workerRepo     repository.WorkerRepository
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

	eventRepo := repository.NewTCPEventClientRepo(config.Monitoring.FluentBit.Events)

	storage, err := storage.NewStorage(config.Storage)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	gateway := &Gateway{
		RedisClient: redisClient,
		ctx:         ctx,
		cancelFunc:  cancel,
		Storage:     storage,
	}

	backendRepo, err := repository.NewBackendPostgresRepository(config.Database.Postgres, eventRepo)
	if err != nil {
		return nil, err
	}

	if release, err := gateway.initLock("postgres"); err == nil {
		defer release()
		if err = backendRepo.Migrate(); err != nil {
			return nil, err
		}
	}

	tailscaleRepo := repository.NewTailscaleRedisRepository(redisClient, config)
	tailscale := network.GetOrCreateTailscale(network.TailscaleConfig{
		ControlURL: config.Tailscale.ControlURL,
		AuthKey:    config.Tailscale.AuthKey,
		Debug:      config.Tailscale.Debug,
		Ephemeral:  true,
	}, tailscaleRepo)

	workspaceRepo := repository.NewWorkspaceRedisRepository(redisClient)

	scheduler, err := scheduler.NewScheduler(ctx, config, redisClient, metricsRepo, backendRepo, workspaceRepo, tailscale)
	if err != nil {
		return nil, err
	}

	containerRepo := repository.NewContainerRedisRepository(redisClient)
	providerRepo := repository.NewProviderRedisRepository(redisClient)
	workerRepo := repository.NewWorkerRedisRepository(redisClient, config.Worker)
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
	gateway.BackendRepo = backendRepo
	gateway.Tailscale = tailscale
	gateway.TaskDispatcher = taskDispatcher
	gateway.metricsRepo = metricsRepo
	gateway.EventRepo = eventRepo
	gateway.workerRepo = workerRepo

	return gateway, nil
}

func (g *Gateway) initLock(name string) (func(), error) {
	lockKey := fmt.Sprintf("gateway:init:%v:lock", name)
	lock := common.NewRedisLock(g.RedisClient)

	if err := lock.Acquire(g.ctx, lockKey, common.RedisLockOptions{TtlS: 10, Retries: 1}); err != nil {
		return nil, err
	}

	return func() {
		if err := lock.Release(lockKey); err != nil {
			log.Error().Str("lock_key", lockKey).Err(err).Msg("failed to release init lock")
		}
	}, nil
}

func (g *Gateway) initHttp() error {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	if g.Config.DebugMode {
		pprof.Register(e)
	}

	e.Pre(middleware.RemoveTrailingSlash())
	configureEchoLogger(e, g.Config.GatewayService.HTTP.EnablePrettyLogs)
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: g.Config.GatewayService.HTTP.CORS.AllowedOrigins,
		AllowHeaders: g.Config.GatewayService.HTTP.CORS.AllowedHeaders,
		AllowMethods: g.Config.GatewayService.HTTP.CORS.AllowedMethods,
	}))
	e.Use(gatewayMiddleware.Subdomain(g.Config.GatewayService.HTTP.GetExternalURL(), g.BackendRepo, g.RedisClient))
	e.Use(middleware.Recover())

	// Accept both HTTP/2 and HTTP/1
	g.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%v", g.Config.GatewayService.HTTP.Port),
		Handler: h2c.NewHandler(e, &http2.Server{}),
	}

	authMiddleware := auth.AuthMiddleware(g.BackendRepo, g.WorkspaceRepo)
	g.baseRouteGroup = e.Group(apiv1.HttpServerBaseRoute)
	g.rootRouteGroup = e.Group(apiv1.HttpServerRootRoute)

	apiv1.NewHealthGroup(g.baseRouteGroup.Group("/health"), g.RedisClient, g.BackendRepo)
	apiv1.NewMachineGroup(g.baseRouteGroup.Group("/machine", authMiddleware), g.ProviderRepo, g.Tailscale, g.Config)
	apiv1.NewWorkspaceGroup(g.baseRouteGroup.Group("/workspace", authMiddleware), g.BackendRepo, g.Config)
	apiv1.NewTokenGroup(g.baseRouteGroup.Group("/token", authMiddleware), g.BackendRepo, g.Config)
	apiv1.NewTaskGroup(g.baseRouteGroup.Group("/task", authMiddleware), g.RedisClient, g.TaskRepo, g.ContainerRepo, g.BackendRepo, g.TaskDispatcher, g.Config)
	apiv1.NewContainerGroup(g.baseRouteGroup.Group("/container", authMiddleware), g.BackendRepo, g.ContainerRepo, *g.Scheduler, g.Config)
	apiv1.NewStubGroup(g.baseRouteGroup.Group("/stub", authMiddleware), g.BackendRepo, g.Config)
	apiv1.NewConcurrencyLimitGroup(g.baseRouteGroup.Group("/concurrency-limit", authMiddleware), g.BackendRepo, g.WorkspaceRepo)
	apiv1.NewDeploymentGroup(g.baseRouteGroup.Group("/deployment", authMiddleware), g.BackendRepo, g.ContainerRepo, *g.Scheduler, g.RedisClient, g.Config)

	return nil
}

func (g *Gateway) initGrpc() error {
	authInterceptor := auth.NewAuthInterceptor(g.BackendRepo, g.WorkspaceRepo)

	serverOptions := []grpc.ServerOption{
		grpc.UnaryInterceptor(authInterceptor.Unary()),
		grpc.StreamInterceptor(authInterceptor.Stream()),
		grpc.MaxRecvMsgSize(g.Config.GatewayService.GRPC.MaxRecvMsgSize * 1024 * 1024),
		grpc.MaxSendMsgSize(g.Config.GatewayService.GRPC.MaxSendMsgSize * 1024 * 1024),
	}

	g.grpcServer = grpc.NewServer(
		serverOptions...,
	)

	return nil
}

func (g *Gateway) registerServices() error {
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
	is, err := image.NewRuncImageService(g.ctx, image.ImageServiceOpts{
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
	fs, err := function.NewRuncFunctionService(g.ctx, function.FunctionServiceOpts{
		Config:         g.Config,
		RedisClient:    g.RedisClient,
		BackendRepo:    g.BackendRepo,
		WorkspaceRepo:  g.WorkspaceRepo,
		TaskRepo:       g.TaskRepo,
		ContainerRepo:  g.ContainerRepo,
		Scheduler:      g.Scheduler,
		Tailscale:      g.Tailscale,
		RouteGroup:     g.rootRouteGroup,
		TaskDispatcher: g.TaskDispatcher,
		EventRepo:      g.EventRepo,
	})
	if err != nil {
		return err
	}
	pb.RegisterFunctionServiceServer(g.grpcServer, fs)

	// Register task queue service
	tq, err := taskqueue.NewRedisTaskQueueService(g.ctx, taskqueue.TaskQueueServiceOpts{
		Config:         g.Config,
		RedisClient:    g.RedisClient,
		BackendRepo:    g.BackendRepo,
		WorkspaceRepo:  g.WorkspaceRepo,
		TaskRepo:       g.TaskRepo,
		ContainerRepo:  g.ContainerRepo,
		Scheduler:      g.Scheduler,
		Tailscale:      g.Tailscale,
		RouteGroup:     g.rootRouteGroup,
		TaskDispatcher: g.TaskDispatcher,
		EventRepo:      g.EventRepo,
	})
	if err != nil {
		return err
	}
	pb.RegisterTaskQueueServiceServer(g.grpcServer, tq)

	// Register endpoint service
	ws, err := endpoint.NewHTTPEndpointService(g.ctx, endpoint.EndpointServiceOpts{
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
	pb.RegisterEndpointServiceServer(g.grpcServer, ws)

	// Register volume service
	vs, err := volume.NewGlobalVolumeService(g.Config.FileService, g.BackendRepo, g.WorkspaceRepo, g.RedisClient, g.rootRouteGroup)
	if err != nil {
		return err
	}
	pb.RegisterVolumeServiceServer(g.grpcServer, vs)

	// Register container service
	cs, err := container.NewContainerService(
		g.ctx,
		container.ContainerServiceOpts{
			Config:        g.Config,
			BackendRepo:   g.BackendRepo,
			ContainerRepo: g.ContainerRepo,
			Tailscale:     g.Tailscale,
			Scheduler:     g.Scheduler,
			RedisClient:   g.RedisClient,
			EventRepo:     g.EventRepo,
		},
	)
	if err != nil {
		return err
	}
	pb.RegisterContainerServiceServer(g.grpcServer, cs)

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
		Config:        g.Config,
		RedisClient:   g.RedisClient,
		Scheduler:     g.Scheduler,
		BackendRepo:   g.BackendRepo,
		WorkspaceRepo: g.WorkspaceRepo,
		ContainerRepo: g.ContainerRepo,
		Tailscale:     g.Tailscale,
		EventRepo:     g.EventRepo,
		RouteGroup:    g.rootRouteGroup,
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
		Config:         g.Config,
		BackendRepo:    g.BackendRepo,
		ContainerRepo:  g.ContainerRepo,
		ProviderRepo:   g.ProviderRepo,
		Scheduler:      g.Scheduler,
		TaskDispatcher: g.TaskDispatcher,
		RedisClient:    g.RedisClient,
		EventRepo:      g.EventRepo,
		WorkerRepo:     g.workerRepo,
	})
	if err != nil {
		return err
	}
	pb.RegisterGatewayServiceServer(g.grpcServer, gws)

	return nil
}

// Gateway entry point
func (g *Gateway) Start() error {
	var err error

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

	err = g.registerServices()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to register services")
	}

	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", g.Config.GatewayService.GRPC.Port))
		if err != nil {
			log.Fatal().Err(err).Msg("failed to listen")
		}

		if err := g.grpcServer.Serve(lis); err != nil {
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
	<-terminationSignal
	log.Info().Msg("termination signal received. shutting down...")
	g.shutdown()

	return nil
}

// Shutdown gracefully shuts down the gateway.
// This function is blocking and will only return when the gateway has been shut down.
func (g *Gateway) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), g.Config.GatewayService.ShutdownTimeout)
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

		select {
		case <-ctx.Done():
			g.grpcServer.Stop()
			return ctx.Err()
		case <-done:
			return nil
		}
	})

	g.cancelFunc()

	if err := eg.Wait(); err != nil {
		log.Fatal().Err(err).Msg("failed to shutdown gateway")
	}
}
