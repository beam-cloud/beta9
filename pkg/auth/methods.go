package auth

import (
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Method permissions are separate from resource authorization in each service.
var workerMethods = map[string]bool{
	pb.WorkerRepositoryService_ReportWorkerUsage_FullMethodName:                true,
	pb.WorkerRepositoryService_PublishWorkerTelemetry_FullMethodName:           true,
	pb.WorkerRepositoryService_GetNextContainerRequest_FullMethodName:          true,
	pb.WorkerRepositoryService_PushContainerLifecycleEvents_FullMethodName:     true,
	pb.WorkerRepositoryService_GetWorkerMeteringConfig_FullMethodName:          true,
	pb.WorkerRepositoryService_GetWorkerServiceProxyConfig_FullMethodName:      true,
	pb.WorkerRepositoryService_StreamWorkerEvents_FullMethodName:               true,
	pb.WorkerRepositoryService_SetImagePullLock_FullMethodName:                 true,
	pb.WorkerRepositoryService_RemoveImagePullLock_FullMethodName:              true,
	pb.WorkerRepositoryService_AddContainerToWorker_FullMethodName:             true,
	pb.WorkerRepositoryService_ClaimContainer_FullMethodName:                   true,
	pb.WorkerRepositoryService_RemoveContainerFromWorker_FullMethodName:        true,
	pb.WorkerRepositoryService_GetWorkerById_FullMethodName:                    true,
	pb.WorkerRepositoryService_ToggleWorkerAvailable_FullMethodName:            true,
	pb.WorkerRepositoryService_DisableWorker_FullMethodName:                    true,
	pb.WorkerRepositoryService_RemoveWorker_FullMethodName:                     true,
	pb.WorkerRepositoryService_UpdateWorkerCapacity_FullMethodName:             true,
	pb.WorkerRepositoryService_SetWorkerKeepAlive_FullMethodName:               true,
	pb.WorkerRepositoryService_RegisterCacheHost_FullMethodName:                true,
	pb.WorkerRepositoryService_UnregisterCacheHost_FullMethodName:              true,
	pb.WorkerRepositoryService_ListCacheHosts_FullMethodName:                   true,
	pb.WorkerRepositoryService_SetCacheClientLock_FullMethodName:               true,
	pb.WorkerRepositoryService_RemoveCacheClientLock_FullMethodName:            true,
	pb.WorkerRepositoryService_SetCacheStoreFromContentLock_FullMethodName:     true,
	pb.WorkerRepositoryService_RemoveCacheStoreFromContentLock_FullMethodName:  true,
	pb.WorkerRepositoryService_RefreshCacheStoreFromContentLock_FullMethodName: true,
	pb.WorkerRepositoryService_AddRecentCacheStub_FullMethodName:               true,
	pb.WorkerRepositoryService_ListRecentCacheStubs_FullMethodName:             true,
	pb.WorkerRepositoryService_MarkCacheStubReported_FullMethodName:            true,
	pb.WorkerRepositoryService_AcquireCacheReconcileLock_FullMethodName:        true,
	pb.WorkerRepositoryService_ReleaseCacheReconcileLock_FullMethodName:        true,
	pb.WorkerRepositoryService_GetCacheOriginCredentials_FullMethodName:        true,
	pb.WorkerRepositoryService_GetContainerRuntimeCredentials_FullMethodName:   true,
	pb.WorkerRepositoryService_PruneStaleCacheCheckpoints_FullMethodName:       true,
	pb.WorkerRepositoryService_SetCacheFsNode_FullMethodName:                   true,
	pb.WorkerRepositoryService_GetCacheFsNode_FullMethodName:                   true,
	pb.WorkerRepositoryService_AddCacheFsNodeChild_FullMethodName:              true,
	pb.WorkerRepositoryService_RemoveCacheFsNode_FullMethodName:                true,
	pb.WorkerRepositoryService_RemoveCacheFsNodeChild_FullMethodName:           true,
	pb.WorkerRepositoryService_GetCacheFsNodeChildren_FullMethodName:           true,
	pb.WorkerRepositoryService_SetNetworkLock_FullMethodName:                   true,
	pb.WorkerRepositoryService_RemoveNetworkLock_FullMethodName:                true,
	pb.WorkerRepositoryService_SetContainerIp_FullMethodName:                   true,
	pb.WorkerRepositoryService_MoveContainerIp_FullMethodName:                  true,
	pb.WorkerRepositoryService_GetContainerIp_FullMethodName:                   true,
	pb.WorkerRepositoryService_GetContainerIps_FullMethodName:                  true,
	pb.WorkerRepositoryService_GetContainerIpAssignments_FullMethodName:        true,
	pb.WorkerRepositoryService_RemoveContainerIp_FullMethodName:                true,
	pb.ContainerRepositoryService_GetContainerState_FullMethodName:             true,
	pb.ContainerRepositoryService_DeleteContainerState_FullMethodName:          true,
	pb.ContainerRepositoryService_UpdateContainerStatus_FullMethodName:         true,
	pb.ContainerRepositoryService_SetContainerExitCode_FullMethodName:          true,
	pb.ContainerRepositoryService_SetContainerAddress_FullMethodName:           true,
	pb.ContainerRepositoryService_SetContainerAddressMap_FullMethodName:        true,
	pb.ContainerRepositoryService_GetContainerAddressMap_FullMethodName:        true,
	pb.ContainerRepositoryService_SetWorkerAddress_FullMethodName:              true,
	pb.BackendRepositoryService_GetCheckpointById_FullMethodName:               true,
	pb.BackendRepositoryService_GetLatestCheckpointByStubId_FullMethodName:     true,
	pb.BackendRepositoryService_ListCheckpoints_FullMethodName:                 true,
	pb.BackendRepositoryService_CreateCheckpoint_FullMethodName:                true,
	pb.BackendRepositoryService_UpdateCheckpoint_FullMethodName:                true,
	pb.BackendRepositoryService_CreateDiskSnapshot_FullMethodName:              true,
	pb.BackendRepositoryService_FailDiskSnapshot_FullMethodName:                true,
	pb.BackendRepositoryService_GetLatestDiskSnapshot_FullMethodName:           true,
	pb.BackendRepositoryService_GetDiskSnapshot_FullMethodName:                 true,
	pb.BackendRepositoryService_GetDiskSnapshotDownloadURL_FullMethodName:      true,
	pb.ThunderService_CreateClientEnrollment_FullMethodName:                    true,
	pb.ThunderService_DeleteClientEnrollment_FullMethodName:                    true,
}

var runtimeMethods = map[string]bool{
	pb.FunctionService_FunctionInvoke_FullMethodName:       true,
	pb.FunctionService_FunctionGetArgs_FullMethodName:      true,
	pb.FunctionService_FunctionSetResult_FullMethodName:    true,
	pb.FunctionService_FunctionMonitor_FullMethodName:      true,
	pb.FunctionService_FunctionSchedule_FullMethodName:     true,
	pb.TaskQueueService_TaskQueuePut_FullMethodName:        true,
	pb.TaskQueueService_TaskQueuePop_FullMethodName:        true,
	pb.TaskQueueService_TaskQueueMonitor_FullMethodName:    true,
	pb.TaskQueueService_TaskQueueComplete_FullMethodName:   true,
	pb.TaskQueueService_TaskQueueLength_FullMethodName:     true,
	pb.TaskQueueService_StartTaskQueueServe_FullMethodName: true,
	pb.MapService_MapSet_FullMethodName:                    true,
	pb.MapService_MapGet_FullMethodName:                    true,
	pb.MapService_MapDelete_FullMethodName:                 true,
	pb.MapService_MapCount_FullMethodName:                  true,
	pb.MapService_MapKeys_FullMethodName:                   true,
	pb.SimpleQueueService_SimpleQueuePut_FullMethodName:    true,
	pb.SimpleQueueService_SimpleQueuePop_FullMethodName:    true,
	pb.SimpleQueueService_SimpleQueuePeek_FullMethodName:   true,
	pb.SimpleQueueService_SimpleQueueEmpty_FullMethodName:  true,
	pb.SimpleQueueService_SimpleQueueSize_FullMethodName:   true,
	pb.SignalService_SignalSet_FullMethodName:              true,
	pb.SignalService_SignalClear_FullMethodName:            true,
	pb.SignalService_SignalMonitor_FullMethodName:          true,
	pb.GatewayService_Authorize_FullMethodName:             true,
	pb.GatewayService_SignPayload_FullMethodName:           true,
	pb.GatewayService_StartTask_FullMethodName:             true,
	pb.GatewayService_EndTask_FullMethodName:               true,
}

func authorizeMethod(info *AuthInfo, method string) error {
	if info == nil || info.Token == nil {
		return status.Error(codes.Unauthenticated, "authentication required")
	}
	switch info.Token.TokenType {
	case types.TokenTypeWorker, types.TokenTypeWorkerPrivate:
		if workerMethods[method] {
			return nil
		}
	case types.TokenTypeWorkspaceRestricted:
		if runtimeMethods[method] {
			return nil
		}
	case types.TokenTypeClusterAdmin, types.TokenTypeWorkspacePrimary, types.TokenTypeWorkspace:
		if !workerMethods[method] {
			return nil
		}
	case types.TokenTypePlatformDeployer:
		switch method {
		case pb.GatewayService_Authorize_FullMethodName, pb.ImageService_VerifyImageBuild_FullMethodName, pb.ImageService_BuildImage_FullMethodName,
			pb.GatewayService_HeadObject_FullMethodName, pb.GatewayService_CreateObject_FullMethodName, pb.GatewayService_PutObjectStream_FullMethodName,
			pb.GatewayService_GetOrCreateStub_FullMethodName, pb.GatewayService_DeployStub_FullMethodName, pb.GatewayService_GetURL_FullMethodName:
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "credential is not permitted for this operation")
}
