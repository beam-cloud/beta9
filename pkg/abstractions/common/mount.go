package abstractions

import (
	"os"
	"path"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

const defaultExternalVolumesPath string = "/tmp/external-volumes"

func ConfigureContainerRequestMounts(containerId string, stub *types.StubWithRelated, workspace *types.Workspace, config types.StubConfigV1) ([]types.Mount, error) {
	secretKey, err := common.ParseSecretKey(*workspace.SigningKey)
	if err != nil {
		return nil, err
	}

	mounts := []types.Mount{
		{
			LocalPath: path.Join(types.DefaultObjectPath, workspace.Name, stub.Object.ExternalId),
			MountPath: types.WorkerUserCodeVolume,
			ReadOnly:  false,
		},
	}
	if stub.Type.Kind() != types.StubTypeSandbox {
		mounts = append(mounts, types.Mount{
			LocalPath: path.Join(types.DefaultOutputsPath, workspace.Name, stub.ExternalId),
			MountPath: types.WorkerUserOutputVolume,
			ReadOnly:  false,
		})
	}

	for _, v := range config.Volumes {
		localPath := path.Join(types.DefaultVolumesPath, workspace.Name, v.Id)
		subPath := v.GetSubPath()
		if subPath != "" {
			localPath = path.Join(localPath, subPath)
			if err := os.MkdirAll(localPath, 0755); err != nil {
				return nil, err
			}
		}

		mount := types.Mount{
			LocalPath:       localPath,
			LinkPath:        path.Join(types.TempContainerWorkspace(containerId), v.MountPath),
			MountPath:       path.Join(types.WorkerContainerVolumePath, v.MountPath),
			ReadOnly:        false,
			SubPath:         subPath,
			MaxStorageBytes: v.GetMaxStorageBytes(),
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
			localPath = path.Join(defaultExternalVolumesPath, workspace.Name, v.Id)
			if subPath != "" {
				localPath = path.Join(localPath, subPath)
				if err := os.MkdirAll(localPath, 0755); err != nil {
					return nil, err
				}
			}
			mount.LocalPath = localPath
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

	for _, disk := range config.Disks {
		if disk == nil || disk.Name == "" || disk.MountPath == "" {
			continue
		}

		mounts = append(mounts, types.Mount{
			LocalPath:   path.Join(types.DefaultDurableDisksPath, workspace.Name, types.SafeDurableDiskName(disk.Name)),
			MountPath:   disk.MountPath,
			ReadOnly:    disk.ReadOnly,
			MountType:   types.StorageModeDurableDisk,
			DurableDisk: types.NewDurableDiskMountConfigFromProto(disk),
		})
	}

	return mounts, nil
}
