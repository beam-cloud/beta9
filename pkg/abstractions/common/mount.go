package abstractions

import (
	"path"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

const defaultExternalVolumesPath string = "/data/external-volumes"

func ConfigureContainerRequestMounts(stubObjectId string, workspace *types.Workspace, config types.StubConfigV1, stubId string) ([]types.Mount, error) {
	signingKey, err := common.ParseSigningKey(*workspace.SigningKey)
	if err != nil {
		return nil, err
	}

	mounts := []types.Mount{
		{
			LocalPath: path.Join(types.DefaultExtractedObjectPath, workspace.Name, stubObjectId),
			MountPath: types.WorkerUserCodeVolume,
			ReadOnly:  true,
		},
		{
			LocalPath: path.Join(types.DefaultOutputsPath, workspace.Name, stubId),
			MountPath: types.WorkerUserOutputVolume,
			ReadOnly:  false,
		},
	}

	for _, v := range config.Volumes {
		mount := types.Mount{
			LocalPath: path.Join(types.DefaultVolumesPath, workspace.Name, v.Id),
			LinkPath:  path.Join(types.DefaultExtractedObjectPath, workspace.Name, stubObjectId, v.MountPath),
			MountPath: path.Join(types.ContainerVolumePath, v.MountPath),
			ReadOnly:  false,
		}

		if v.Config != nil {
			decryptedSecrets, err := common.DecryptAllSecrets(signingKey, []string{v.Config.AccessKey, v.Config.SecretKey, v.Config.BucketUrl})
			if err != nil {
				return nil, err
			}

			mount.MountPointConfig = &types.MountPointConfig{
				S3Bucket:  v.Config.BucketName,
				AccessKey: decryptedSecrets[0],
				SecretKey: decryptedSecrets[1],
				BucketURL: decryptedSecrets[2],
			}
			mount.LocalPath = path.Join(defaultExternalVolumesPath, workspace.Name, v.Id)
		}

		mounts = append(mounts, mount)
	}

	return mounts, nil
}
