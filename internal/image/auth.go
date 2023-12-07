package image

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type AuthInterceptor interface {
	Unary() grpc.UnaryServerInterceptor
}

type TokenAuthInterceptor struct {
	Token string
}

func NewTokenAuthInterceptor(apiToken string) AuthInterceptor {
	return &TokenAuthInterceptor{
		Token: apiToken,
	}
}

func (i *TokenAuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		err := i.authenticateClient(ctx)
		if err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func (interceptor *TokenAuthInterceptor) authenticateClient(ctx context.Context) error {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if len(md["authorization"]) > 0 {
			bearerSchema, token := split(md["authorization"][0], " ")

			if bearerSchema != "Bearer" {
				return status.Error(codes.Unauthenticated, "invalid auth schema")
			}

			if token != interceptor.Token {
				return status.Error(codes.Unauthenticated, "invalid token")
			}

			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "missing auth token")
}

func split(input, delimiter string) (string, string) {
	data := strings.Split(input, delimiter)
	if len(data) > 1 {
		return data[0], data[1]
	}
	return data[0], ""
}
