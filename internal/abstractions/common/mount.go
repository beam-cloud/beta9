package common

import (
	"path"

	"github.com/beam-cloud/beta9/internal/types"
)

func ConfigureContainerRequestMounts(stubObjectId string, workspaceName string, config types.StubConfigV1) []types.Mount {
	mounts := []types.Mount{
		{
			LocalPath: path.Join(types.DefaultExtractedObjectPath, workspaceName, stubObjectId),
			MountPath: types.WorkerUserCodeVolume,
			ReadOnly:  true,
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

	return mounts
}
