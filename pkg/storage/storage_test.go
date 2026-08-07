package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

// A worker mounts one of these per workspace while containers are already
// running on it, so a mount it cannot complete has to come back as an error.
// Exiting instead would take every unrelated container on the worker with it,
// and this test would not fail so much as kill the package's test binary.
func TestNewStorageReturnsMountFailuresInsteadOfExiting(t *testing.T) {
	// MkdirAll cannot build a directory underneath a regular file, which is a
	// mount failure that needs no S3 and no fuse to reproduce.
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocked, nil, 0644); err != nil {
		t.Fatalf("failed to seed the test: %v", err)
	}

	storage, err := NewStorage(types.StorageConfig{
		Mode:           StorageModeMountPoint,
		FilesystemName: "workspace",
		FilesystemPath: filepath.Join(blocked, "workspace"),
	}, nil)

	if err == nil {
		t.Fatal("expected a mount failure to be returned")
	}
	if storage != nil {
		t.Fatal("expected no storage to be handed back alongside the error")
	}
	if !strings.Contains(err.Error(), "unable to mount filesystem") {
		t.Fatalf("expected the error to say the mount failed, got %v", err)
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
