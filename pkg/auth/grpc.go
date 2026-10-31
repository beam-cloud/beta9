package auth

import (
	"context"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"

	"github.com/beam-cloud/beta9/pkg/repository"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const authContextKey = "auth"

type AuthInfo struct {
	Workspace *types.Workspace
	Token     *types.Token
	// OAuth is present only after resource-server introspection. The backing
	// workspace token never leaves the server; scopes constrain MCP dispatch.
	OAuth *OAuthGrant
	// Request attribution.
	Actor types.EventActor
}

// RequestAuthorizer lets a service enforce resource permissions after token
// authentication, before either a unary handler or a stream receives a request.
type RequestAuthorizer interface {
	AuthorizeRequest(context.Context, interface{}) error
}

type OAuthGrant struct {
	ClientID string
	Subject  string
	Resource string
	Scopes   []string
}

const (
	CallerHeader       = "x-beta9-caller"
	AgentSessionHeader = "x-beta9-agent-session"
)

func actorFromMetadata(md metadata.MD) types.EventActor {
	actor := types.EventActor{}
	if v := md.Get(CallerHeader); len(v) > 0 {
		actor.Caller = v[0]
	}
	if v := md.Get(AgentSessionHeader); len(v) > 0 {
		actor.AgentSession = v[0]
	}
	return actor
}

// TokenId is zero for requests without a token.
func (ai *AuthInfo) TokenId() uint {
	if ai.Token == nil {
		return 0
	}
	return ai.Token.Id
}

func AuthInfoFromContext(ctx context.Context) (*AuthInfo, bool) {
	authInfo, ok := ctx.Value(authContextKey).(*AuthInfo)
	return authInfo, ok
}

type AuthInterceptor struct {
	unauthenticatedMethods map[string]bool
	backendRepo            repository.BackendRepository
	workspaceRepo          repository.WorkspaceRepository
}

func NewAuthInterceptor(config types.AppConfig, backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository) *AuthInterceptor {
	return &AuthInterceptor{
		backendRepo:   backendRepo,
		workspaceRepo: workspaceRepo,
		unauthenticatedMethods: map[string]bool{
			pb.GatewayService_Authorize_FullMethodName:                       true,
			pb.WorkerRepositoryService_PublishWorkerTelemetry_FullMethodName: true,
			// Replica harness calls carry no workspace token; the service
			// authorizes them with the per-replica secret.
			pb.EndpointHarnessService_Register_FullMethodName:                          true,
			pb.EndpointHarnessService_WatchConfig_FullMethodName:                       true,
			pb.EndpointHarnessService_AckConfig_FullMethodName:                         true,
			pb.EndpointHarnessService_Heartbeat_FullMethodName:                         true,
			pb.EndpointHarnessService_PublishEvents_FullMethodName:                     true,
			pb.GatewayService_JoinAgent_FullMethodName:                                 true,
			pb.GatewayService_ListAgentRoutes_FullMethodName:                           true,
			pb.GatewayService_RequestAgentTransportCredential_FullMethodName:           true,
			pb.GatewayService_GetAgentPoolVirtualization_FullMethodName:                true,
			pb.GatewayService_CreateNodeEnrollment_FullMethodName:                      true,
			pb.GatewayService_DeleteNodeEnrollment_FullMethodName:                      true,
			pb.GatewayService_StreamAgent_FullMethodName:                               true,
			pb.GatewayService_StreamAgentTelemetry_FullMethodName:                      true,
			pb.GatewayService_UpdateAgentRouteStatus_FullMethodName:                    true,
			pb.GatewayService_UpdateAgentSSHStatus_FullMethodName:                      true,
			"/grpc.health.v1.Health/Check":                                             true,
			"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo":                config.DebugMode,
			pb.WorkerRepositoryService_RegisterCacheHost_FullMethodName:                true,
			pb.WorkerRepositoryService_UnregisterCacheHost_FullMethodName:              true,
			pb.WorkerRepositoryService_ListCacheHosts_FullMethodName:                   true,
			pb.WorkerRepositoryService_SetCacheClientLock_FullMethodName:               true,
			pb.WorkerRepositoryService_RemoveCacheClientLock_FullMethodName:            true,
			pb.WorkerRepositoryService_SetCacheStoreFromContentLock_FullMethodName:     true,
			pb.WorkerRepositoryService_RemoveCacheStoreFromContentLock_FullMethodName:  true,
			pb.WorkerRepositoryService_RefreshCacheStoreFromContentLock_FullMethodName: true,
			pb.WorkerRepositoryService_SetCacheFsNode_FullMethodName:                   true,
			pb.WorkerRepositoryService_GetCacheFsNode_FullMethodName:                   true,
			pb.WorkerRepositoryService_AddCacheFsNodeChild_FullMethodName:              true,
			pb.WorkerRepositoryService_RemoveCacheFsNode_FullMethodName:                true,
			pb.WorkerRepositoryService_RemoveCacheFsNodeChild_FullMethodName:           true,
			pb.WorkerRepositoryService_GetCacheFsNodeChildren_FullMethodName:           true,
			pb.WorkerRepositoryService_AddRecentCacheStub_FullMethodName:               true,
			pb.WorkerRepositoryService_ListRecentCacheStubs_FullMethodName:             true,
			pb.WorkerRepositoryService_AcquireCacheReconcileLock_FullMethodName:        true,
			pb.WorkerRepositoryService_ReleaseCacheReconcileLock_FullMethodName:        true,
			pb.WorkerRepositoryService_GetCacheOriginCredentials_FullMethodName:        true,
			pb.WorkerRepositoryService_PruneStaleCacheCheckpoints_FullMethodName:       true,
		},
	}
}

func (ai *AuthInterceptor) getToken(ctx context.Context, tokenKey string) (*types.Token, *types.Workspace, error) {
	token, workspace, err := ai.workspaceRepo.AuthorizeToken(tokenKey)
	if err != nil {
		token, workspace, err = ai.backendRepo.AuthorizeToken(ctx, tokenKey)
		if err != nil {
			return nil, nil, err
		}

		err = ai.workspaceRepo.SetAuthorizationToken(token, workspace)
		if err != nil {
			return nil, nil, err
		}
	} else if err := validateCachedToken(ctx, ai.backendRepo, token); err != nil {
		return nil, nil, err
	}

	if !workspace.StorageAvailable() {
		if err := ensureWorkspaceStorage(ctx, workspace); err != nil {
			return nil, nil, err
		}
		_ = ai.workspaceRepo.SetAuthorizationToken(token, workspace)
	}

	return token, workspace, nil
}

func (ai *AuthInterceptor) isAuthRequired(method string) bool {
	return !ai.unauthenticatedMethods[method]
}

func (ai *AuthInterceptor) validateToken(ctx context.Context, md metadata.MD) (*AuthInfo, bool) {
	if len(md["authorization"]) == 0 {
		return nil, false
	}

	tokenKey := strings.TrimPrefix(md["authorization"][0], "Bearer ")

	var token *types.Token
	var workspace *types.Workspace
	var err error

	token, workspace, err = ai.getToken(ctx, tokenKey)
	if err != nil {
		return nil, false
	}

	// For now, restricted tokens should not be allowed to access grpc calls
	if !token.ValidAt(time.Now()) {
		return nil, false
	}
	return &AuthInfo{
		Token:     token,
		Workspace: workspace,
		Actor:     actorFromMetadata(md),
	}, true
}

type wrappedStream struct {
	grpc.ServerStream
	ctx       context.Context
	authorize func(interface{}) error
	validate  func() error
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

func (w *wrappedStream) SendMsg(resp interface{}) error {
	if w.validate != nil {
		if err := w.validate(); err != nil {
			return err
		}
	}
	return w.ServerStream.SendMsg(resp)
}

func (w *wrappedStream) RecvMsg(req interface{}) error {
	if w.validate != nil {
		if err := w.validate(); err != nil {
			return err
		}
	}
	if w.validate == nil {
		return w.ServerStream.RecvMsg(req)
	}
	received := make(chan error, 1)
	go func() { received <- w.ServerStream.RecvMsg(req) }()
	select {
	case <-w.ctx.Done():
		return status.Error(codes.Unauthenticated, "credential is no longer active")
	case err := <-received:
		if err != nil {
			return err
		}
	}
	if err := w.validate(); err != nil {
		return err
	}
	if w.authorize != nil {
		return w.authorize(req)
	}
	return nil
}

func (ai *AuthInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(stream.Context())
		if !ok {
			return status.Errorf(codes.Unauthenticated, "invalid or missing token")
		}

		authInfo, valid := ai.validateToken(stream.Context(), md)
		if !valid {
			if !ai.isAuthRequired(info.FullMethod) {
				return handler(srv, stream)
			}
			return status.Errorf(codes.Unauthenticated, "invalid or missing token")
		}

		if ai.isAuthRequired(info.FullMethod) {
			if err := authorizeMethod(authInfo, info.FullMethod); err != nil {
				return err
			}
		}

		streamCtx, cancel, validate := ai.guardStream(stream.Context(), authInfo.Token)
		defer cancel()

		// Create a new context with the AuthInfo
		ctxWithAuth := ai.newContextWithAuth(streamCtx, authInfo)

		// Create a new wrapped stream with the new context
		wrappedStr := &wrappedStream{
			ServerStream: stream,
			ctx:          ctxWithAuth,
			validate:     validate,
		}
		if service, ok := srv.(RequestAuthorizer); ok && ai.isAuthRequired(info.FullMethod) {
			wrappedStr.authorize = func(req interface{}) error {
				return service.AuthorizeRequest(ctxWithAuth, req)
			}
		}

		return handler(srv, wrappedStr)
	}
}

func (ai *AuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "invalid or missing token")
		}

		authInfo, valid := ai.validateToken(ctx, md)
		if !valid {
			if !ai.isAuthRequired(info.FullMethod) {
				return handler(ctx, req)
			}

			return nil, status.Errorf(codes.Unauthenticated, "invalid or missing token")
		}

		if ai.isAuthRequired(info.FullMethod) {
			if err := authorizeMethod(authInfo, info.FullMethod); err != nil {
				return nil, err
			}
		}

		// Attach the auth info to context
		ctx = ai.newContextWithAuth(ctx, authInfo)
		if service, ok := info.Server.(RequestAuthorizer); ok && ai.isAuthRequired(info.FullMethod) {
			if err := service.AuthorizeRequest(ctx, req); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

func (ai *AuthInterceptor) newContextWithAuth(ctx context.Context, authInfo *AuthInfo) context.Context {
	return ContextWithAuthInfo(ctx, authInfo)
}

func ContextWithAuthInfo(ctx context.Context, authInfo *AuthInfo) context.Context {
	return context.WithValue(ctx, authContextKey, authInfo)
}

func HasPermission(authInfo *AuthInfo) bool {
	if authInfo == nil || authInfo.Token == nil {
		return false
	}
	switch authInfo.Token.TokenType {
	case types.TokenTypeClusterAdmin, types.TokenTypeWorkspacePrimary, types.TokenTypeWorkspace, types.TokenTypePlatformDeployer:
		return true
	}
	return false
}

// HasInteractivePermission limits user-controlled interactive sessions to
// user and administrator credentials. Worker, machine, and workload tokens
// must never be able to turn their infrastructure access into an interactive
// root shell.
func HasInteractivePermission(authInfo *AuthInfo) bool {
	if authInfo == nil || authInfo.Token == nil {
		return false
	}

	switch authInfo.Token.TokenType {
	case types.TokenTypeClusterAdmin,
		types.TokenTypeWorkspacePrimary,
		types.TokenTypeWorkspace:
		return true
	default:
		return false
	}
}
