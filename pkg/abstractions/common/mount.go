package abstractions

import (
	"path"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

const defaultExternalVolumesPath string = "/tmp/external-volumes"

func ConfigureContainerRequestMounts(containerId, stubObjectId string, workspace *types.Workspace, config types.StubConfigV1, stubId string) ([]types.Mount, error) {
	secretKey, err := common.ParseSecretKey(*workspace.SigningKey)
	if err != nil {
		return nil, err
	}

	mounts := []types.Mount{
		{
			LocalPath: path.Join(types.DefaultObjectPath, workspace.Name, stubObjectId),
			MountPath: types.WorkerUserCodeVolume,
			ReadOnly:  false,
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
			LinkPath:  path.Join(types.TempContainerWorkspace(containerId), v.MountPath),
			MountPath: path.Join(types.WorkerContainerVolumePath, v.MountPath),
			ReadOnly:  false,
		}

		if v.Config != nil {
			secrets := []string{v.Config.AccessKey, v.Config.SecretKey}
			decryptedSecrets, err := common.DecryptAllSecrets(secretKey, secrets)
			if err != nil {
				return nil, err
			}

			mount.MountPointConfig = &types.MountPointConfig{
				BucketName:     v.Config.BucketName,
				AccessKey:      decryptedSecrets[0],
				SecretKey:      decryptedSecrets[1],
				EndpointURL:    v.Config.EndpointUrl,
				Region:         v.Config.Region,
				ReadOnly:       v.Config.ReadOnly,
				ForcePathStyle: v.Config.ForcePathStyle,
			}
			mount.LocalPath = path.Join(defaultExternalVolumesPath, workspace.Name, v.Id)
			mount.MountType = storage.StorageModeMountPoint
		}

		// NOTE: This is a hack to support the case where the mount path is an absolute path.
		// Currently, if a user specifies a mount path like '/my-mount-path', we're mounting it
		// to <WORKDIR>/my-mount-path. This is not the desired behavior, so here we're adding an
		// extra mount where the user would expect the mount path to be. For existing users, we will
		// keep the existing behavior, but for new users that expect the mount path to be an absolute
		// path, we will add an extra mount where they would expect it to be.
		if path.IsAbs(v.MountPath) {
			rootMount := mount
			rootMount.LinkPath = ""
			rootMount.MountPath = v.MountPath
			mounts = append(mounts, rootMount)
		}

		mounts = append(mounts, mount)
	}

	return mounts, nil
}
