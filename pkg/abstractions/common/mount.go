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
			MountType: types.MountTypeJuiceFS,
		},
		{
			LocalPath: path.Join(types.DefaultOutputsPath, workspace.Name, stubId),
			MountPath: types.WorkerUserOutputVolume,
			ReadOnly:  false,
			MountType: types.MountTypeJuiceFS,
		},
	}

	for _, v := range config.Volumes {
		mount := types.Mount{
			LocalPath: path.Join(types.DefaultVolumesPath, workspace.Name, v.Id),
			LinkPath:  path.Join(types.DefaultExtractedObjectPath, workspace.Name, stubObjectId, v.MountPath),
			MountPath: path.Join(types.ContainerVolumePath, v.MountPath),
			ReadOnly:  false,
			MountType: types.MountTypeJuiceFS,
		}

		if v.Config != nil {
			secrets := []string{v.Config.AccessKey, v.Config.SecretKey}
			if v.Config.BucketUrl != "" {
				secrets = append(secrets, v.Config.BucketUrl)
			}
			decryptedSecrets, err := common.DecryptAllSecrets(signingKey, secrets)
			if err != nil {
				return nil, err
			}

			mount.MountPointConfig = &types.MountPointConfig{
				S3Bucket:  v.Config.BucketName,
				AccessKey: decryptedSecrets[0],
				SecretKey: decryptedSecrets[1],
			}
			if v.Config.BucketUrl != "" {
				mount.MountPointConfig.BucketURL = decryptedSecrets[2]
			}
			mount.LocalPath = path.Join(defaultExternalVolumesPath, workspace.Name, v.Id)
			mount.MountType = types.MountTypeMountPoint
		}

		mounts = append(mounts, mount)
	}

	return mounts, nil
}
