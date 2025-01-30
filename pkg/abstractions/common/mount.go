package abstractions

import (
	"path"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

const defaultExternalVolumesPath string = "/tmp/external-volumes"

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
			secrets := []string{v.Config.AccessKey, v.Config.SecretKey}
			decryptedSecrets, err := common.DecryptAllSecrets(signingKey, secrets)
			if err != nil {
				return nil, err
			}

			mount.MountPointConfig = &types.MountPointConfig{
				BucketName:  v.Config.BucketName,
				AccessKey:   decryptedSecrets[0],
				SecretKey:   decryptedSecrets[1],
				EndpointURL: v.Config.EndpointUrl,
				Region:      v.Config.Region,
				ReadOnly:    v.Config.ReadOnly,
			}
			mount.LocalPath = path.Join(defaultExternalVolumesPath, workspace.Name, v.Id)
			mount.MountType = storage.StorageModeMountPoint
		}

		mounts = append(mounts, mount)
	}

	return mounts, nil
}
