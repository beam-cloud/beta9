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
		mount := types.Mount{
			LocalPath: path.Join(types.DefaultVolumesPath, workspaceName, v.Id),
			LinkPath:  path.Join(types.DefaultExtractedObjectPath, workspaceName, stubObjectId, v.MountPath),
			MountPath: path.Join(types.ContainerVolumePath, v.MountPath),
			ReadOnly:  false,
		}

		if v.Config != nil {
			mount.MountPointConfig = &types.MountPointConfig{
				S3Bucket:  v.Config.BucketName,
				AccessKey: v.Config.AccessKey,
				SecretKey: v.Config.SecretKey,
				BucketURL: v.Config.BucketUrl,
			}
			mount.LocalPath = path.Join(types.DefaultExternalVolumesPath, workspaceName, v.Id)
		}

		mounts = append(mounts, mount)
	}

	return mounts
}
