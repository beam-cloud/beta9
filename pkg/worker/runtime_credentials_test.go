package worker

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
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

func TestClaimContainerForBuildOnlyRequestsWorkspaceStorage(t *testing.T) {
	storageID := uint(1)
	dockerfile := "FROM alpine"
	buildCtxObject := "build-context"
	repo := &fakeWorkerRepoClient{
		claim: &pb.ClaimContainerResponse{
			Ok:      true,
			Claimed: true,
			Credentials: &pb.GetContainerRuntimeCredentialsResponse{
				Ok: true,
				WorkspaceStorage: &pb.CacheWorkspaceStorageCredentials{
					EndpointUrl: "https://storage.example",
					Region:      "us-east-1",
					BucketName:  "bucket",
					AccessKey:   "access",
					SecretKey:   "secret",
				},
			},
		},
	}
	request := &types.ContainerRequest{
		ContainerId:          "build-1",
		WorkspaceId:          "workspace-1",
		StubId:               "stub-1",
		DeliveryToken:        "token-1",
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
	worker := &Worker{workerId: "worker-1", workerRepoClient: repo}

	claimed, err := worker.claimContainer(context.Background(), request)
	require.NoError(t, err)
	require.True(t, claimed)

	require.Equal(t, "worker-1", repo.lastClaim.WorkerId)
	require.Equal(t, "token-1", repo.lastClaim.DeliveryToken)
	creds := repo.lastClaim.Credentials
	require.NotNil(t, creds)
	require.True(t, creds.WorkspaceStorage)
	require.False(t, creds.RuntimeToken)
	require.Empty(t, creds.SecretNames)
	require.Empty(t, creds.MountCredentials)
	require.Equal(t, "access", *request.Workspace.Storage.AccessKey)
	require.Equal(t, "secret", *request.Workspace.Storage.SecretKey)
	require.Equal(t, "https://storage.example", *request.Workspace.Storage.EndpointUrl)
}

func TestClaimContainerHydratesRuntimeTokenAndSecrets(t *testing.T) {
	repo := &fakeWorkerRepoClient{
		claim: &pb.ClaimContainerResponse{
			Ok:      true,
			Claimed: true,
			Credentials: &pb.GetContainerRuntimeCredentialsResponse{
				Ok:  true,
				Env: []string{"BETA9_TOKEN=restricted-runtime-token", "SECRET=runtime-secret"},
			},
		},
	}
	request := &types.ContainerRequest{
		ContainerId:          "container-1",
		WorkspaceId:          "workspace-1",
		StubId:               "stub-1",
		Env:                  []string{"BETA9_TOKEN=placeholder"},
		RuntimeSecretNames:   []string{"SECRET"},
		RuntimeTokenRequired: true,
	}
	worker := &Worker{workerRepoClient: repo}

	claimed, err := worker.claimContainer(context.Background(), request)
	require.NoError(t, err)
	require.True(t, claimed)

	require.True(t, repo.lastClaim.Credentials.RuntimeToken)
	require.Equal(t, []string{"SECRET"}, repo.lastClaim.Credentials.SecretNames)
	require.Equal(t, []string{"BETA9_TOKEN=restricted-runtime-token", "SECRET=runtime-secret"}, request.Env)
}

func TestClaimContainerSkipsCredentialsForCompleteRequests(t *testing.T) {
	repo := &fakeWorkerRepoClient{claim: &pb.ClaimContainerResponse{Ok: true, Claimed: true}}
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		WorkspaceId: "workspace-1",
		StubId:      "stub-1",
		Env:         []string{"BETA9_TOKEN=inline-token", "SECRET=inline-secret"},
	}
	worker := &Worker{workerRepoClient: repo}

	claimed, err := worker.claimContainer(context.Background(), request)
	require.NoError(t, err)
	require.True(t, claimed)

	require.Nil(t, repo.lastClaim.Credentials)
	require.Equal(t, []string{"BETA9_TOKEN=inline-token", "SECRET=inline-secret"}, request.Env)
}

func TestClaimContainerReportsRejectedClaim(t *testing.T) {
	repo := &fakeWorkerRepoClient{claim: &pb.ClaimContainerResponse{Ok: false, Claimed: false, ErrorMsg: "container already claimed"}}
	worker := &Worker{workerRepoClient: repo}

	claimed, err := worker.claimContainer(context.Background(), &types.ContainerRequest{ContainerId: "container-1"})
	require.EqualError(t, err, "container already claimed")
	require.False(t, claimed)
}
