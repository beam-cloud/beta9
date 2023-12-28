package auth

import (
	"context"
	"log"
	"strings"

	"github.com/beam-cloud/beam/internal/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type AuthInterceptor struct {
	unauthenticatedMethods map[string]bool
	backendRepo            repository.BackendRepository
}

func NewAuthInterceptor(backendRepo repository.BackendRepository) *AuthInterceptor {
	return &AuthInterceptor{
		backendRepo: backendRepo,
		unauthenticatedMethods: map[string]bool{
			"/gateway.GatewayService/Configure": true,
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
	return strings.TrimPrefix(md["authorization"][0], "Bearer "), true
}

func (ai *AuthInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !ai.isAuthRequired(info.FullMethod) {
			return handler(srv, stream)
		}

		md, ok := metadata.FromIncomingContext(stream.Context())
		if !ok {
			return handler(srv, stream)
		}

		// TODO: Validate the token
		_, valid := ai.validateToken(md)
		if !valid {
			// Handle invalid token case
		}

		return handler(srv, stream)
	}
}

var authKey = "auth"

type AuthInfo struct {
	Token    string
	UserName string
}

func (ai *AuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return handler(ctx, req)
		}

		// TODO: Validate the token
		token, valid := ai.validateToken(md)
		if !valid {
			// Handle invalid token case
		}

		if !ai.isAuthRequired(info.FullMethod) {
			log.Println("bypassing auth...")
			return handler(ctx, req)
		}

		authInfo := AuthInfo{
			Token:    token,
			UserName: "exampleUserName",
		}

		// Attach the auth info to context
		ctx = ai.newContextWithAuth(ctx, authInfo)
		return handler(ctx, req)
	}
}

func (ai *AuthInterceptor) newContextWithAuth(ctx context.Context, authInfo AuthInfo) context.Context {
	return context.WithValue(ctx, authKey, authInfo)
}

func AuthInfoFromContext(ctx context.Context) (AuthInfo, bool) {
	authInfo, ok := ctx.Value(authKey).(AuthInfo)
	return authInfo, ok
}
