package abstractions

import (
	"path"

	"github.com/beam-cloud/beta9/pkg/types"
)

func ConfigureContainerRequestMounts(stubObjectId string, workspaceName string, config types.StubConfigV1, stubId string) []types.Mount {
	mounts := []types.Mount{
		{
			LocalPath: path.Join(types.DefaultExtractedObjectPath, workspaceName, stubObjectId),
			MountPath: types.WorkerUserCodeVolume,
			ReadOnly:  true,
		},
		{
			LocalPath: path.Join(types.DefaultOutputsPath, workspaceName, stubId),
			MountPath: types.WorkerUserOutputVolume,
			ReadOnly:  false,
		},
	}

	for _, v := range config.Volumes {
		mounts = append(mounts, types.Mount{
			LocalPath: path.Join(types.DefaultVolumesPath, workspaceName, v.Id),
			LinkPath:  path.Join(types.DefaultExtractedObjectPath, workspaceName, stubObjectId, v.MountPath),
			MountPath: path.Join(types.ContainerVolumePath, v.MountPath),
			ReadOnly:  false,
		})
	}

	for _, s := range config.CloudBuckets {
		mounts = append(mounts, types.Mount{
			LocalPath: path.Join(types.ContainerCloudBucketPath, workspaceName, s.MountPath),
			LinkPath:  path.Join(types.DefaultExtractedObjectPath, workspaceName, stubObjectId, s.MountPath),
			MountPath: path.Join(types.ContainerVolumePath, s.MountPath),
			ReadOnly:  false,
			MountPointConfig: &types.MountPointConfig{
				S3Bucket:  s.BucketName,
				AccessKey: s.Config.AccessKey,
				SecretKey: s.Config.Secret,
				BucketURL: s.Config.Endpoint,
			},
		})
	}

	return mounts
}
