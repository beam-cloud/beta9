package auth

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func TestAuthInterceptorLetsCacheRepositoryMethodsSelfAuthenticate(t *testing.T) {
	interceptor := NewAuthInterceptor(types.AppConfig{}, nil, nil)

	for _, method := range []string{
		pb.WorkerRepositoryService_RegisterCacheHost_FullMethodName,
		pb.WorkerRepositoryService_UnregisterCacheHost_FullMethodName,
		pb.WorkerRepositoryService_ListCacheHosts_FullMethodName,
		pb.WorkerRepositoryService_SetCacheClientLock_FullMethodName,
		pb.WorkerRepositoryService_GetCacheFsNodeChildren_FullMethodName,
	} {
		if interceptor.isAuthRequired(method) {
			t.Fatalf("method %s requires interceptor auth, want service-level cache auth", method)
		}
	}
}

func TestAuthInterceptorStillRequiresAuthForNonCacheRepositoryMethods(t *testing.T) {
	interceptor := NewAuthInterceptor(types.AppConfig{}, nil, nil)

	for _, method := range []string{
		pb.WorkerRepositoryService_GetNextContainerRequest_FullMethodName,
		pb.WorkerRepositoryService_SetImagePullLock_FullMethodName,
		pb.WorkerRepositoryService_SetNetworkLock_FullMethodName,
	} {
		if !interceptor.isAuthRequired(method) {
			t.Fatalf("method %s does not require interceptor auth", method)
		}
	}
}
