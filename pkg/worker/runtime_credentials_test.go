package worker

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestMergeRuntimeEnvReplacesVendedKeys(t *testing.T) {
	env := mergeRuntimeEnv(
		[]string{"SAFE=value", "BETA9_TOKEN=old", "SECRET=old"},
		[]string{"BETA9_TOKEN=new", "SECRET=new"},
	)

	require.Equal(t, []string{"SAFE=value", "BETA9_TOKEN=new", "SECRET=new"}, env)
}

func TestApplyMountCredentialsMatchesPathAndBucket(t *testing.T) {
	request := &types.ContainerRequest{
		Mounts: []types.Mount{
			{
				MountPath: "/mnt/data",
				MountPointConfig: &types.MountPointConfig{
					BucketName: "bucket-a",
				},
			},
			{
				MountPath: "/mnt/data",
				MountPointConfig: &types.MountPointConfig{
					BucketName: "bucket-b",
				},
			},
		},
	}

	applyMountCredentials(request, []*pb.RuntimeMountCredentials{
		{
			MountPath: "/mnt/data",
			Config: &pb.MountPointConfig{
				BucketName: "bucket-b",
				AccessKey:  "access-b",
				SecretKey:  "secret-b",
			},
		},
	})

	require.Empty(t, request.Mounts[0].MountPointConfig.AccessKey)
	require.Equal(t, "access-b", request.Mounts[1].MountPointConfig.AccessKey)
	require.Equal(t, "secret-b", request.Mounts[1].MountPointConfig.SecretKey)
}

func TestHydrateRuntimeCredentialsForBuildOnlyRequestsWorkspaceStorage(t *testing.T) {
	storageID := uint(1)
	dockerfile := "FROM alpine"
	buildCtxObject := "build-context"
	repo := &fakeRuntimeCredentialsWorkerRepo{
		resp: &pb.GetContainerRuntimeCredentialsResponse{
			Ok: true,
			WorkspaceStorage: &pb.CacheWorkspaceStorageCredentials{
				EndpointUrl: "https://storage.example",
				Region:      "us-east-1",
				BucketName:  "bucket",
				AccessKey:   "access",
				SecretKey:   "secret",
			},
		},
	}
	request := &types.ContainerRequest{
		ContainerId:          "build-1",
		WorkspaceId:          "workspace-1",
		StubId:               "stub-1",
		RuntimeSecretNames:   []string{"SECRET"},
		RuntimeTokenRequired: true,
		Workspace: types.Workspace{
			Storage: &types.WorkspaceStorage{Id: &storageID},
		},
		BuildOptions: types.BuildOptions{
			Dockerfile:     &dockerfile,
			BuildCtxObject: &buildCtxObject,
		},
	}
	worker := &Worker{workerRepoClient: repo}

	require.NoError(t, worker.hydrateRuntimeCredentials(context.Background(), request))

	require.NotNil(t, repo.lastReq)
	require.True(t, repo.lastReq.WorkspaceStorage)
	require.False(t, repo.lastReq.RuntimeToken)
	require.Empty(t, repo.lastReq.SecretNames)
	require.Empty(t, repo.lastReq.MountCredentials)
	require.Equal(t, "access", *request.Workspace.Storage.AccessKey)
	require.Equal(t, "secret", *request.Workspace.Storage.SecretKey)
	require.Equal(t, "https://storage.example", *request.Workspace.Storage.EndpointUrl)
}

type fakeRuntimeCredentialsWorkerRepo struct {
	pb.WorkerRepositoryServiceClient
	lastReq *pb.GetContainerRuntimeCredentialsRequest
	resp    *pb.GetContainerRuntimeCredentialsResponse
	err     error
}

func (f *fakeRuntimeCredentialsWorkerRepo) GetContainerRuntimeCredentials(ctx context.Context, req *pb.GetContainerRuntimeCredentialsRequest, opts ...grpc.CallOption) (*pb.GetContainerRuntimeCredentialsResponse, error) {
	f.lastReq = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}
