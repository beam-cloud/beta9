package auth

import (
	"context"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"

	"github.com/beam-cloud/beta9/pkg/repository"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var authContextKey = "auth"

type AuthInfo struct {
	Workspace *types.Workspace
	Token     *types.Token
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
			pb.GatewayService_JoinAgent_FullMethodName:                       true,
			pb.GatewayService_ListAgentRoutes_FullMethodName:                 true,
			pb.GatewayService_RequestAgentTransportCredential_FullMethodName: true,
			pb.GatewayService_StreamAgent_FullMethodName:                     true,
			pb.GatewayService_StreamAgentTelemetry_FullMethodName:            true,
			pb.GatewayService_UpdateAgentRouteStatus_FullMethodName:          true,
			// Marketplace browse is public: offers expose only what the authed UI
			// already shows (listing id, seller workspace external id, GPU specs) —
			// no join tokens or machine internals. Seller/management RPCs stay
			// auth-required and also self-check workspace auth in their handlers.
			pb.GatewayService_ListMarketplaceOffers_FullMethodName:                     true,
			pb.GatewayService_GetMarketplaceOffer_FullMethodName:                       true,
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
			pb.WorkerRepositoryService_MarkCacheStubReported_FullMethodName:            true,
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
	if !token.Active || token.DisabledByClusterAdmin {
		return nil, false
	}
	return &AuthInfo{
		Token:     token,
		Workspace: workspace,
	}, true
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
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

		// Create a new context with the AuthInfo
		ctxWithAuth := ai.newContextWithAuth(stream.Context(), authInfo)

		// Create a new wrapped stream with the new context
		wrappedStr := &wrappedStream{
			ServerStream: stream,
			ctx:          ctxWithAuth,
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

		// Attach the auth info to context
		ctx = ai.newContextWithAuth(ctx, authInfo)
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
	return authInfo != nil && authInfo.Token != nil && authInfo.Token.TokenType != types.TokenTypeWorkspaceRestricted
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
