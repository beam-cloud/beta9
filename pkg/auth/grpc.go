package auth

import (
	"context"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"

	"github.com/beam-cloud/beta9/pkg/repository"
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
			"/gateway.GatewayService/Authorize":                         true,
			"/grpc.health.v1.Health/Check":                              true,
			"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo": config.DebugMode,
		},
	}
}

func (ai *AuthInterceptor) isAuthRequired(method string) bool {
	return !ai.unauthenticatedMethods[method]
}

func (ai *AuthInterceptor) validateToken(md metadata.MD) (*AuthInfo, bool) {
	if len(md["authorization"]) == 0 {
		return nil, false
	}

	tokenKey := strings.TrimPrefix(md["authorization"][0], "Bearer ")

	var token *types.Token
	var workspace *types.Workspace
	var err error

	token, workspace, err = ai.workspaceRepo.AuthorizeToken(tokenKey)
	if err != nil {
		token, workspace, err = ai.backendRepo.AuthorizeToken(context.TODO(), tokenKey)
		if err != nil {
			return nil, false
		}

		err = ai.workspaceRepo.SetAuthorizationToken(token, workspace)
		if err != nil {
			return nil, false
		}
	}

	// For now, restricted tokens should not be allowed to access grpc calls
	if !token.Active || token.DisabledByClusterAdmin || token.TokenType == types.TokenTypeWorkspaceRestricted {
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

		authInfo, valid := ai.validateToken(md)
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

		authInfo, valid := ai.validateToken(md)
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
	return context.WithValue(ctx, authContextKey, authInfo)
}
