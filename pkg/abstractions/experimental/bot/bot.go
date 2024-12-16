package bot

import (
	"encoding/json"
	"errors"
	"fmt"

	"context"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	botRoutePrefix string = "/bot"
)

type BotServiceOpts struct {
	Config         types.AppConfig
	RedisClient    *common.RedisClient
	BackendRepo    repository.BackendRepository
	WorkspaceRepo  repository.WorkspaceRepository
	TaskRepo       repository.TaskRepository
	ContainerRepo  repository.ContainerRepository
	Scheduler      *scheduler.Scheduler
	RouteGroup     *echo.Group
	Tailscale      *network.Tailscale
	TaskDispatcher *task.Dispatcher
	EventRepo      repository.EventRepository
}

type BotService interface {
	pb.BotServiceServer
	PushBotEvent(ctx context.Context, in *pb.PushBotEventRequest) (*pb.PushBotEventResponse, error)
	PushBotEventBlocking(ctx context.Context, in *pb.PushBotEventBlockingRequest) (*pb.PushBotEventBlockingResponse, error)
	PopBotTask(ctx context.Context, in *pb.PopBotTaskRequest) (*pb.PopBotTaskResponse, error)
	PushBotMarkers(ctx context.Context, in *pb.PushBotMarkersRequest) (*pb.PushBotMarkersResponse, error)
}

type PetriBotService struct {
	pb.UnimplementedBotServiceServer
	ctx             context.Context
	config          types.AppConfig
	rdb             *common.RedisClient
	keyEventManager *common.KeyEventManager
	keyEventChan    chan common.KeyEvent
	routeGroup      *echo.Group
	scheduler       *scheduler.Scheduler
	backendRepo     repository.BackendRepository
	workspaceRepo   repository.WorkspaceRepository
	containerRepo   repository.ContainerRepository
	eventRepo       repository.EventRepository
	taskRepo        repository.TaskRepository
	tailscale       *network.Tailscale
	taskDispatcher  *task.Dispatcher
	botInstances    *common.SafeMap[*botInstance]
	botStateManager *botStateManager
}

func NewPetriBotService(ctx context.Context, opts BotServiceOpts) (BotService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	pbs := &PetriBotService{
		ctx:             ctx,
		config:          opts.Config,
		rdb:             opts.RedisClient,
		keyEventManager: keyEventManager,
		keyEventChan:    make(chan common.KeyEvent),
		routeGroup:      opts.RouteGroup,
		scheduler:       opts.Scheduler,
		backendRepo:     opts.BackendRepo,
		workspaceRepo:   opts.WorkspaceRepo,
		containerRepo:   opts.ContainerRepo,
		taskRepo:        opts.TaskRepo,
		tailscale:       opts.Tailscale,
		taskDispatcher:  opts.TaskDispatcher,
		eventRepo:       opts.EventRepo,
		botInstances:    common.NewSafeMap[*botInstance](),
		botStateManager: newBotStateManager(opts.RedisClient),
	}

	// Listen for container events with a bot container prefix
	go keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerState(botContainerPrefix), pbs.keyEventChan)
	go pbs.handleBotContainerEvents(ctx)

	// Register task dispatcher
	pbs.taskDispatcher.Register(string(types.ExecutorBot), pbs.botTaskFactory)

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(pbs.backendRepo, pbs.workspaceRepo)
	registerBotRoutes(pbs.routeGroup.Group(botRoutePrefix, authMiddleware), pbs)

	return pbs, nil
}

func (pbs *PetriBotService) botTaskFactory(ctx context.Context, msg types.TaskMessage) (types.TaskInterface, error) {
	return &BotTask{
		pbs: pbs,
		msg: &msg,
	}, nil
}

func (pbs *PetriBotService) isPublic(stubId string) (*types.Workspace, error) {
	instance, err := pbs.getOrCreateBotInstance(stubId)
	if err != nil {
		return nil, err
	}

	if instance.botConfig.Authorized {
		return nil, errors.New("unauthorized")
	}

	return instance.workspace, nil
}

func (pbs *PetriBotService) handleBotContainerEvents(ctx context.Context) {
	for {
		select {
		case event := <-pbs.keyEventChan:
			operation := event.Operation
			containerId := fmt.Sprintf("%s%s", botContainerPrefix, event.Key)

			container, err := parseContainerId(containerId)
			if err != nil {
				continue
			}

			switch operation {
			case common.KeyOperationSet, common.KeyOperationHSet:
				_, err := pbs.getOrCreateBotInstance(container.StubId)
				if err != nil {
					continue
				}

			case common.KeyOperationDel, common.KeyOperationExpired:
				// Do nothing
			}

		case <-ctx.Done():
			return
		}
	}
}

func (pbs *PetriBotService) getOrCreateBotInstance(stubId string) (*botInstance, error) {
	instance, exists := pbs.botInstances.Get(stubId)
	if exists {
		return instance, nil
	}

	stub, err := pbs.backendRepo.GetStubByExternalId(pbs.ctx, stubId)
	if err != nil {
		return nil, errors.New("invalid stub id")
	}

	var stubConfig *types.StubConfigV1 = &types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), stubConfig)
	if err != nil {
		return nil, err
	}

	var botConfig BotConfig
	err = json.Unmarshal(stubConfig.Extra, &botConfig)
	if err != nil {
		return nil, err
	}

	token, err := pbs.backendRepo.RetrieveActiveToken(pbs.ctx, stub.Workspace.Id)
	if err != nil {
		return nil, err
	}

	instance, err = newBotInstance(pbs.ctx, botInstanceOpts{
		AppConfig:      pbs.config,
		Scheduler:      pbs.scheduler,
		Token:          token,
		Stub:           stub,
		StubConfig:     stubConfig,
		BotConfig:      botConfig,
		StateManager:   pbs.botStateManager,
		TaskDispatcher: pbs.taskDispatcher,
		ContainerRepo:  pbs.containerRepo,
		BackendRepo:    pbs.backendRepo,
	})
	if err != nil {
		return nil, err
	}

	log.Info().Str("stub_id", instance.stub.ExternalId).Interface("bot_config", instance.botConfig).Msg("created bot instance")
	pbs.botInstances.Set(stubId, instance)

	// Monitor and then clean up the instance once it's done
	go instance.Start()
	go func(i *botInstance) {
		<-i.ctx.Done()
		pbs.botInstances.Delete(stubId)
	}(instance)

	return instance, nil
}

func (s *PetriBotService) PushBotEvent(ctx context.Context, in *pb.PushBotEventRequest) (*pb.PushBotEventResponse, error) {
	instance, err := s.getOrCreateBotInstance(in.StubId)
	if err != nil {
		return &pb.PushBotEventResponse{Ok: false}, nil
	}

	err = instance.botStateManager.pushEvent(instance.workspace.Name, instance.stub.ExternalId, in.SessionId, &BotEvent{
		Type:     BotEventType(in.EventType),
		Value:    in.EventValue,
		Metadata: in.Metadata,
	})
	if err != nil {
		return &pb.PushBotEventResponse{Ok: false}, nil
	}

	return &pb.PushBotEventResponse{Ok: true}, nil
}

func (s *PetriBotService) PushBotEventBlocking(ctx context.Context, in *pb.PushBotEventBlockingRequest) (*pb.PushBotEventBlockingResponse, error) {
	instance, err := s.getOrCreateBotInstance(in.StubId)
	if err != nil {
		return &pb.PushBotEventBlockingResponse{Ok: false}, nil
	}

	pairId := uuid.New().String()
	err = instance.botStateManager.pushEvent(instance.workspace.Name, instance.stub.ExternalId, in.SessionId, &BotEvent{
		PairId:   pairId,
		Type:     BotEventType(in.EventType),
		Value:    in.EventValue,
		Metadata: in.Metadata,
	})
	if err != nil {
		return &pb.PushBotEventBlockingResponse{Ok: false}, nil
	}

	ctxWithTimeout, cancel := common.GetTimeoutContext(ctx, int(in.TimeoutSeconds))
	defer cancel()

	eventPair, err := s.botStateManager.waitForEventPair(ctxWithTimeout, instance.workspace.Name, instance.stub.ExternalId, in.SessionId, pairId)
	if err != nil {
		return &pb.PushBotEventBlockingResponse{Ok: false}, nil
	}

	return &pb.PushBotEventBlockingResponse{Ok: true, Event: &pb.BotEvent{
		Type:     string(eventPair.Response.Type),
		Value:    eventPair.Response.Value,
		Metadata: eventPair.Response.Metadata,
	}}, nil
}

func (s *PetriBotService) PushBotMarkers(ctx context.Context, in *pb.PushBotMarkersRequest) (*pb.PushBotMarkersResponse, error) {
	instance, err := s.getOrCreateBotInstance(in.StubId)
	if err != nil {
		return &pb.PushBotMarkersResponse{Ok: false}, nil
	}

	for locationName, markerList := range in.Markers {
		for _, marker := range markerList.Markers {
			fields := []MarkerField{}
			for _, field := range marker.Fields {
				fields = append(fields, MarkerField{
					FieldName:  field.FieldName,
					FieldValue: field.FieldValue,
				})
			}

			marker := Marker{
				LocationName: marker.LocationName,
				Fields:       fields,
				SourceTaskId: in.SourceTaskId,
			}

			err = s.botStateManager.pushMarker(instance.workspace.Name, instance.stub.ExternalId, in.SessionId, locationName, marker)
			if err != nil {
				log.Error().Str("stub_id", instance.stub.ExternalId).Err(err).Msg("failed to push marker")
				continue
			}
		}
	}

	return &pb.PushBotMarkersResponse{Ok: true}, nil
}

func (s *PetriBotService) PopBotTask(ctx context.Context, in *pb.PopBotTaskRequest) (*pb.PopBotTaskResponse, error) {
	instance, err := s.getOrCreateBotInstance(in.StubId)
	if err != nil {
		return &pb.PopBotTaskResponse{Ok: false}, nil
	}

	markers, err := s.botStateManager.popTask(instance.workspace.Name, instance.stub.ExternalId, in.SessionId, in.TransitionName, in.TaskId)
	if err != nil {
		return &pb.PopBotTaskResponse{Ok: false}, nil
	}

	markerMap := map[string]*pb.PopBotTaskResponse_MarkerList{}
	for _, marker := range markers {
		if _, ok := markerMap[marker.LocationName]; !ok {
			markerMap[marker.LocationName] = &pb.PopBotTaskResponse_MarkerList{}
		}

		fields := []*pb.MarkerField{}
		for _, field := range marker.Fields {
			fields = append(fields, &pb.MarkerField{
				FieldName:  field.FieldName,
				FieldValue: field.FieldValue,
			})
		}

		markerMap[marker.LocationName].Markers = append(markerMap[marker.LocationName].Markers, &pb.Marker{
			LocationName: marker.LocationName,
			Fields:       fields,
		})
	}

	return &pb.PopBotTaskResponse{Ok: true, Markers: markerMap}, nil
}

var Keys = &keys{}

type keys struct{}

var (
	botLock             string = "bot:%s:%s:session_state_lock:%s"
	botInputBuffer      string = "bot:%s:%s:input_buffer:%s"
	botEventPair        string = "bot:%s:%s:event_pair:%s:%s"
	botEventBuffer      string = "bot:%s:%s:event_buffer:%s"
	botEventHistory     string = "bot:%s:%s:event_history:%s"
	botSessionIndex     string = "bot:%s:%s:session_index"
	botSessionState     string = "bot:%s:%s:session_state:%s"
	botSessionKeepAlive string = "bot:%s:%s:session_keep_alive:%s"
	botMarkers          string = "bot:%s:%s:markers:%s:%s"
	botTaskIndex        string = "bot:%s:%s:task_index:%s:%s"
	botTransitionTask   string = "bot:%s:%s:transition_task:%s:%s:%s"
)

func (k *keys) botLock(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botLock, workspaceName, stubId, sessionId)
}

func (k *keys) botSessionIndex(workspaceName, stubId string) string {
	return fmt.Sprintf(botSessionIndex, workspaceName, stubId)
}

func (k *keys) botInputBuffer(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botInputBuffer, workspaceName, stubId, sessionId)
}

func (k *keys) botEventPair(workspaceName, stubId, sessionId, pairId string) string {
	return fmt.Sprintf(botEventPair, workspaceName, stubId, sessionId, pairId)
}

func (k *keys) botEventBuffer(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botEventBuffer, workspaceName, stubId, sessionId)
}

func (k *keys) botEventHistory(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botEventHistory, workspaceName, stubId, sessionId)
}

func (k *keys) botSessionState(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botSessionState, workspaceName, stubId, sessionId)
}

func (k *keys) botSessionKeepAlive(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botSessionKeepAlive, workspaceName, stubId, sessionId)
}

func (k *keys) botMarkers(workspaceName, stubId, sessionId, locationName string) string {
	return fmt.Sprintf(botMarkers, workspaceName, stubId, sessionId, locationName)
}

func (k *keys) botTaskIndex(workspaceName, stubId, sessionId string) string {
	return fmt.Sprintf(botTaskIndex, workspaceName, stubId, sessionId)
}

func (k *keys) botTransitionTask(workspaceName, stubId, sessionId, transitionName, taskId string) string {
	return fmt.Sprintf(botTransitionTask, workspaceName, stubId, sessionId, transitionName, taskId)
}
