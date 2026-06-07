package worker

import (
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
