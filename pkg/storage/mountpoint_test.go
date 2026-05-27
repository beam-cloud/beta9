package storage

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestNewMountpointCmdArgsRunsForeground(t *testing.T) {
	s := &MountPointStorage{config: types.MountPointConfig{BucketName: "bucket"}}

	args := newMountpointCmdArgs(s, "/mnt/test")
	for _, arg := range args {
		if arg == "--foreground" {
			return
		}
	}

	t.Fatalf("expected mountpoint args to include --foreground, got %v", args)
}
