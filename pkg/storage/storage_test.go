package storage

import (
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestNewStorageUsesWorkspaceMetadata(t *testing.T) {
	bucket, access, secret, endpoint, region := "workspace-bucket", "access", "secret", "https://s3.example.com", "us-east-1"
	metadata := &types.WorkspaceStorage{BucketName: &bucket, AccessKey: &access, SecretKey: &secret, EndpointUrl: &endpoint, Region: &region}
	config := types.WorkspaceStorageConfig{Geese: types.GeeseConfig{Debug: true, AccessKey: "unused-global-key"}}
	for _, mode := range []string{StorageModeGeese, StorageModeMountPoint} {
		t.Run(mode, func(t *testing.T) {
			backend, err := NewStorage(mode, config, metadata, nil)
			require.NoError(t, err)
			require.Equal(t, mode, backend.Mode())
			switch backend := backend.(type) {
			case *GeeseStorage:
				require.Equal(t, types.GeeseConfig{Debug: true, BucketName: bucket, AccessKey: access, SecretKey: secret, EndpointUrl: endpoint, Region: region}, backend.config)
			case *MountPointStorage:
				require.Equal(t, types.MountPointConfig{BucketName: bucket, AccessKey: access, SecretKey: secret, EndpointURL: endpoint, Region: region}, backend.config)
			}
			_, err = NewStorage(mode, config, nil, nil)
			require.ErrorContains(t, err, "metadata is required")
			_, err = NewStorage(mode, config, &types.WorkspaceStorage{BucketName: &bucket}, nil)
			require.ErrorContains(t, err, "metadata is incomplete")
		})
	}
}

func TestMountInfoContains(t *testing.T) {
	mountInfo := strings.NewReader(strings.Join([]string{
		"31 23 0:27 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw",
		"74 34 0:65 / /storage/workspace\\040name rw,nosuid,nodev,relatime - fuse.geesefs geesefs rw,user_id=0,group_id=0",
		"75 34 0:66 / /storage/other rw,nosuid,nodev,relatime - fuse.geesefs geesefs rw,user_id=0,group_id=0",
	}, "\n"))

	if !mountInfoContains(mountInfo, "/storage/workspace name") {
		t.Fatal("expected escaped mountinfo path to match requested mount point")
	}
}

func TestMountInfoContainsDoesNotMatchPrefix(t *testing.T) {
	mountInfo := strings.NewReader("75 34 0:66 / /storage/workspace-extra rw,nosuid,nodev,relatime - fuse.geesefs geesefs rw\n")

	if mountInfoContains(mountInfo, "/storage/workspace") {
		t.Fatal("did not expect mountinfo prefix to match requested mount point")
	}
}
