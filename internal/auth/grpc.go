package auth

import (
	"context"
	"log"
	"strings"

	"github.com/beam-cloud/beam/internal/types"

	"github.com/beam-cloud/beam/internal/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var authContextKey = "auth"

type AuthInfo struct {
	Context types.Context
	Token   types.Token
}

func AuthInfoFromContext(ctx context.Context) (AuthInfo, bool) {
	authInfo, ok := ctx.Value(authContextKey).(AuthInfo)
	return authInfo, ok
}

type AuthInterceptor struct {
	unauthenticatedMethods map[string]bool
	backendRepo            repository.BackendRepository
}

func NewAuthInterceptor(backendRepo repository.BackendRepository) *AuthInterceptor {
	return &AuthInterceptor{
		backendRepo: backendRepo,
		unauthenticatedMethods: map[string]bool{
			"/gateway.GatewayService/Authorize": true,
		},
	}
}

func (ai *AuthInterceptor) isAuthRequired(method string) bool {
	_, ok := ai.unauthenticatedMethods[method]
	return !ok
}

func (ai *AuthInterceptor) validateToken(md metadata.MD) (string, bool) {
	if len(md["authorization"]) == 0 {
		return "", false
	}

	tokenStr := strings.TrimPrefix(md["authorization"][0], "Bearer ")
	log.Println("tokenstr: ", tokenStr)

	return "", true
}

func (ai *AuthInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(stream.Context())
		if !ok {
			return status.Errorf(codes.Unauthenticated, "invalid or missing token")
		}

		_, valid := ai.validateToken(md)
		if !valid {
			if !ai.isAuthRequired(info.FullMethod) {
				return handler(srv, stream)
			}

			return status.Errorf(codes.Unauthenticated, "invalid or missing token")
		}

		return handler(srv, stream)
	}
}

func (ai *AuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "invalid or missing token")
		}

		_, valid := ai.validateToken(md)
		if !valid {
			if !ai.isAuthRequired(info.FullMethod) {
				return handler(ctx, req)
			}

			return nil, status.Errorf(codes.Unauthenticated, "invalid or missing token")
		}

		authInfo := AuthInfo{
			Token:   types.Token{},
			Context: types.Context{},
		}

		// Attach the auth info to context
		ctx = ai.newContextWithAuth(ctx, authInfo)
		return handler(ctx, req)
	}
}

func (ai *AuthInterceptor) newContextWithAuth(ctx context.Context, authInfo AuthInfo) context.Context {
	return context.WithValue(ctx, authContextKey, authInfo)
}
