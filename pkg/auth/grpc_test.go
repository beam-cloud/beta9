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
		pb.WorkerRepositoryService_RemoveCacheClientLock_FullMethodName,
		pb.WorkerRepositoryService_SetCacheStoreFromContentLock_FullMethodName,
		pb.WorkerRepositoryService_RemoveCacheStoreFromContentLock_FullMethodName,
		pb.WorkerRepositoryService_RefreshCacheStoreFromContentLock_FullMethodName,
		pb.WorkerRepositoryService_SetCacheFsNode_FullMethodName,
		pb.WorkerRepositoryService_GetCacheFsNode_FullMethodName,
		pb.WorkerRepositoryService_AddCacheFsNodeChild_FullMethodName,
		pb.WorkerRepositoryService_RemoveCacheFsNode_FullMethodName,
		pb.WorkerRepositoryService_RemoveCacheFsNodeChild_FullMethodName,
		pb.WorkerRepositoryService_GetCacheFsNodeChildren_FullMethodName,
	} {
		if interceptor.isAuthRequired(method) {
			t.Fatalf("method %s requires interceptor auth, want service-level cache auth", method)
		}
	}
}

// Marketplace browse (search + direct share links) is public; every other
// marketplace RPC (seller/management) still requires interceptor auth.
func TestAuthInterceptorAllowsPublicMarketplaceBrowse(t *testing.T) {
	interceptor := NewAuthInterceptor(types.AppConfig{}, nil, nil)

	for _, method := range []string{
		pb.GatewayService_ListMarketplaceOffers_FullMethodName,
		pb.GatewayService_GetMarketplaceOffer_FullMethodName,
	} {
		if interceptor.isAuthRequired(method) {
			t.Fatalf("method %s requires auth, want public marketplace browse", method)
		}
	}

	for _, method := range []string{
		pb.GatewayService_ListMarketplaceListings_FullMethodName,
		pb.GatewayService_CreateMarketplaceListing_FullMethodName,
		pb.GatewayService_UpdateMarketplaceListing_FullMethodName,
		pb.GatewayService_DeleteMarketplaceListing_FullMethodName,
		pb.GatewayService_GetMarketplaceJoinCommand_FullMethodName,
		pb.GatewayService_ListMarketplaceMachines_FullMethodName,
		pb.GatewayService_ListMachineContainers_FullMethodName,
	} {
		if !interceptor.isAuthRequired(method) {
			t.Fatalf("method %s does not require auth, want seller RPCs gated", method)
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
