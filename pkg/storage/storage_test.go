package storage

import (
	"strings"
	"testing"
)

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
