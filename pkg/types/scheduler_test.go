package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerStartConcurrencyUsesPoolAndRuntime(t *testing.T) {
	worker := &Worker{
		TotalCpu: 16000,
		PoolName: "default",
		Runtime:  ContainerRuntimeRunc.String(),
	}
	config := WorkerConfig{
		Pools: map[string]WorkerPoolConfig{
			"default": {
				ContainerRuntime:          ContainerRuntimeRunc.String(),
				ContainerStartConcurrency: 32,
			},
		},
	}

	require.Equal(t, 32, WorkerStartConcurrency(config, worker))
}

func TestWorkerStartConcurrencyCapsByWorkerCPU(t *testing.T) {
	worker := &Worker{
		TotalCpu: 1000,
		PoolName: "default",
		Runtime:  ContainerRuntimeRunc.String(),
	}
	config := WorkerConfig{
		Pools: map[string]WorkerPoolConfig{
			"default": {
				ContainerRuntime:          ContainerRuntimeRunc.String(),
				ContainerStartConcurrency: 32,
			},
		},
	}

	require.Equal(t, 2, WorkerStartConcurrency(config, worker))
}

func TestWorkerProtoRoundTripPreservesRuntime(t *testing.T) {
	worker := &Worker{
		Id:      "worker-1",
		Runtime: ContainerRuntimeRunc.String(),
	}

	require.Equal(t, ContainerRuntimeRunc.String(), NewWorkerFromProto(worker.ToProto()).Runtime)
}

func TestPrivateWorkerRequestRemovesControlPlaneCredentials(t *testing.T) {
	signingKey := "workspace-signing-key"
	storageID := uint(1)
	storageSecret := "storage-secret"
	mountConfig := &MountPointConfig{
		BucketName: "external-bucket",
		AccessKey:  "external-access",
		SecretKey:  "external-secret",
	}
	stubConfig, err := json.Marshal(StubConfigV1{
		Secrets: []Secret{{Name: "SECRET"}},
	})
	require.NoError(t, err)

	request := &ContainerRequest{
		ContainerId: "container-1",
		EntryPoint:  []string{"python"},
		Env:         []string{"BETA9_TOKEN=user-token", "SECRET=value", "SAFE=value"},
		ImageId:     "image-1",
		Stub: StubWithRelated{
			Stub: Stub{Config: string(stubConfig)},
		},
		Workspace: Workspace{
			Name:       "workspace",
			SigningKey: &signingKey,
			Storage: &WorkspaceStorage{
				Id:        &storageID,
				SecretKey: &storageSecret,
			},
		},
		Mounts: []Mount{{
			MountPath:        "/mnt/data",
			MountPointConfig: mountConfig,
		}},
		ImageCredentials:         "image-creds",
		BuildRegistryCredentials: "build-creds",
		BuildOptions: BuildOptions{
			SourceImageCreds: "source-image-creds",
			BuildSecrets:     []string{"SECRET=value"},
		},
	}

	privateRequest := request.PrivateWorkerRequest()

	require.Nil(t, privateRequest.Workspace.SigningKey)
	require.Empty(t, privateRequest.ImageCredentials)
	require.Empty(t, privateRequest.BuildRegistryCredentials)
	require.Empty(t, privateRequest.BuildOptions.SourceImageCreds)
	require.Empty(t, privateRequest.BuildOptions.BuildSecrets)
	require.Equal(t, []string{"SAFE=value"}, privateRequest.Env)
	require.NotNil(t, privateRequest.Workspace.Storage)
	require.Nil(t, privateRequest.Workspace.Storage.SecretKey)
	require.NotSame(t, request.Mounts[0].MountPointConfig, privateRequest.Mounts[0].MountPointConfig)
	require.Empty(t, privateRequest.Mounts[0].MountPointConfig.AccessKey)
	require.Empty(t, privateRequest.Mounts[0].MountPointConfig.SecretKey)

	privateRequest.Mounts[0].MountPointConfig.SecretKey = "changed"
	require.Equal(t, "external-secret", request.Mounts[0].MountPointConfig.SecretKey)

	proto := privateRequest.ToProto()
	require.Empty(t, proto.Workspace.SigningKey)
	require.Empty(t, proto.Workspace.Storage.AccessKey)
	require.Empty(t, proto.Workspace.Storage.SecretKey)
}
