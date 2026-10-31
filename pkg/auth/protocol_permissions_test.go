package auth

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type protocolBackend struct {
	repository.BackendRepository
	token  *types.Token
	active bool
}

func (b *protocolBackend) AuthorizeToken(context.Context, string) (*types.Token, *types.Workspace, error) {
	if !b.active {
		return nil, nil, errors.New("revoked")
	}
	return b.token, &types.Workspace{Storage: &types.WorkspaceStorage{Id: uintPointer(1)}}, nil
}
func (b *protocolBackend) TokenIsActive(context.Context, string, string) (bool, error) {
	return b.active, nil
}

type protocolCache struct {
	repository.WorkspaceRepository
	token *types.Token
}

func (c *protocolCache) AuthorizeToken(string) (*types.Token, *types.Workspace, error) {
	return c.token, &types.Workspace{Storage: &types.WorkspaceStorage{Id: uintPointer(1)}}, nil
}
func (*protocolCache) SetAuthorizationToken(*types.Token, *types.Workspace) error { return nil }

type protocolStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *protocolStream) Context() context.Context  { return s.ctx }
func (s *protocolStream) SendMsg(interface{}) error { return nil }

func TestProtocolCredentialRoles(t *testing.T) {
	for _, role := range []string{types.TokenTypeClusterAdmin, types.TokenTypeWorkspacePrimary, types.TokenTypeWorkspace, types.TokenTypeWorkspaceRestricted, types.TokenTypeWorker, types.TokenTypeWorkerPrivate, types.TokenTypePlatformDeployer, "unknown"} {
		t.Run(role, func(t *testing.T) {
			token := &types.Token{ExternalId: "token", Key: "key", TokenType: role, Active: true, Reusable: true}
			backend := &protocolBackend{token: token, active: true}
			cache := &protocolCache{token: token}
			allowed := role == types.TokenTypeClusterAdmin || role == types.TokenTypeWorkspacePrimary || role == types.TokenTypeWorkspace
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer key"))
			interceptor := NewAuthInterceptor(types.AppConfig{}, backend, cache)
			called := false
			_, err := interceptor.Unary()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: pb.GatewayService_ListTokens_FullMethodName}, func(context.Context, interface{}) (interface{}, error) { called = true; return nil, nil })
			require.Equal(t, allowed, called)
			if !allowed {
				require.Equal(t, codes.PermissionDenied, status.Code(err))
			}
			called = false
			err = interceptor.Stream()(nil, &protocolStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: pb.GatewayService_ListTokens_FullMethodName}, func(interface{}, grpc.ServerStream) error { called = true; return nil })
			require.Equal(t, allowed, called)
			if !allowed {
				require.Equal(t, codes.PermissionDenied, status.Code(err))
			}
			e := echo.New()
			e.Use(AuthMiddleware(backend, cache))
			e.GET("/api/v1/token/:workspaceId", func(c echo.Context) error { return c.NoContent(http.StatusNoContent) })
			request := httptest.NewRequest("GET", "/api/v1/token/workspace", nil)
			request.Header.Set("Authorization", "Bearer key")
			response := httptest.NewRecorder()
			e.ServeHTTP(response, request)
			expected := http.StatusForbidden
			if allowed {
				expected = http.StatusNoContent
			}
			require.Equal(t, expected, response.Code)
			backend.active = false
			// A cached authorization record must never keep a revoked token alive.
			response = httptest.NewRecorder()
			e.ServeHTTP(response, request)
			require.Equal(t, http.StatusUnauthorized, response.Code)
			_, err = interceptor.Unary()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: pb.GatewayService_ListTokens_FullMethodName}, func(context.Context, interface{}) (interface{}, error) {
				t.Fatal("revoked handler dispatched")
				return nil, nil
			})
			require.Equal(t, codes.Unauthenticated, status.Code(err))
		})
	}
}

func TestActiveStreamChecksRevocationBeforeSend(t *testing.T) {
	token := &types.Token{ExternalId: "token", Key: "key", TokenType: types.TokenTypeWorker, Active: true, Reusable: true}
	backend := &protocolBackend{token: token, active: true}
	interceptor := NewAuthInterceptor(types.AppConfig{}, backend, &protocolCache{token: token})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer key"))
	err := interceptor.Stream()(nil, &protocolStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: pb.WorkerRepositoryService_GetNextContainerRequest_FullMethodName}, func(_ interface{}, stream grpc.ServerStream) error {
		require.NoError(t, stream.SendMsg(nil))
		backend.active = false
		require.Equal(t, codes.Unauthenticated, status.Code(stream.SendMsg(nil)))
		require.ErrorIs(t, stream.Context().Err(), context.Canceled)
		return nil
	})
	require.NoError(t, err)
}

func uintPointer(v uint) *uint { return &v }

type blockedProtocolStream struct {
	protocolStream
	stop chan struct{}
}

func (s *blockedProtocolStream) RecvMsg(interface{}) error { <-s.stop; return io.EOF }

func TestCancelledCredentialStopsWaitingForStreamInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	source := &blockedProtocolStream{protocolStream: protocolStream{ctx: ctx}, stop: make(chan struct{})}
	defer close(source.stop)
	stream := &wrappedStream{ServerStream: source, ctx: ctx, validate: func() error { return nil }}
	done := make(chan error, 1)
	go func() { done <- stream.RecvMsg(nil) }()
	cancel()
	select {
	case err := <-done:
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	case <-time.After(time.Second):
		t.Fatal("revoked stream remained blocked")
	}
}
