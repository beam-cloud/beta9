package common

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	LogOffsetHeader  = "x-beta9-log-offset"
	RequestIDHeader  = "x-beta9-request-id"
	RequestAckHeader = "x-beta9-request-ack"
)

// GRPCClientAuthInterceptor adds an authorization header to the outgoing context for unary calls
func GRPCClientAuthInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		newCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			for _, key := range []string{RequestIDHeader, RequestAckHeader} {
				for _, value := range md.Get(key) {
					newCtx = metadata.AppendToOutgoingContext(newCtx, key, value)
				}
			}
		}
		return invoker(newCtx, method, req, reply, cc, opts...)
	}
}

// GRPCClientAuthStreamInterceptor adds an authorization header to the outgoing context for streaming calls
func GRPCClientAuthStreamInterceptor(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		newCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			for _, value := range md.Get(LogOffsetHeader) {
				newCtx = metadata.AppendToOutgoingContext(newCtx, LogOffsetHeader, value)
			}
		}
		return streamer(newCtx, desc, cc, method, opts...)
	}
}

// GRPCClientRetryInterceptor retries the call on some gRPC errors
func GRPCClientRetryInterceptor(maxRetries int, delay time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		retryDelay := delay
		for i := 0; i < maxRetries; i++ {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				return nil
			}

			st, ok := status.FromError(err)
			if !ok || (st.Code() != codes.Unavailable && st.Code() != codes.ResourceExhausted) {
				return err
			}

			if i == maxRetries-1 {
				break
			}

			timer := time.NewTimer(retryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			retryDelay *= 2
		}

		return errors.New("max retries reached")
	}
}
