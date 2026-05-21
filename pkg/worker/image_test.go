package worker

import (
	"testing"

	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestArchivePathForMountUsesCacheFSForLocalArchive(t *testing.T) {
	meta := &clipCommon.ClipArchiveMetadata{}

	require.Equal(t, "/cache/images/image.clip", archivePathForMount(meta, "/images/cache/image.clip", "/cache/images/image.clip"))
}

func TestArchivePathForMountKeepsArchivePathForRemoteArchive(t *testing.T) {
	meta := &clipCommon.ClipArchiveMetadata{
		Header: clipCommon.ClipArchiveHeader{StorageInfoLength: 1},
		StorageInfo: clipCommon.S3StorageInfo{
			Bucket: "images",
			Key:    "image.clip",
		},
	}

	require.Equal(t, "/images/cache/image.clip", archivePathForMount(meta, "/images/cache/image.clip", "/cache/images/image.clip"))
}

func TestArchivePathForMountKeepsArchivePathWithoutCacheFSPath(t *testing.T) {
	meta := &clipCommon.ClipArchiveMetadata{}

	require.Equal(t, "/images/cache/image.clip", archivePathForMount(meta, "/images/cache/image.clip", ""))
}
