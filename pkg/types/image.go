package types

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SnapshotImageID names the image a sandbox's filesystem is saved as.
func SnapshotImageID(stubID string, at time.Time) string {
	return fmt.Sprintf("%s-%d", stubID, at.Unix())
}

// SnapshotImageStub returns the stub a filesystem snapshot image was taken
// from. Built images are named by a hash of their build and have none.
func SnapshotImageStub(imageID string) (string, bool) {
	i := strings.LastIndexByte(imageID, '-')
	if i <= 0 {
		return "", false
	}
	if at, err := strconv.ParseUint(imageID[i+1:], 10, 63); err != nil || at == 0 {
		return "", false
	}
	return imageID[:i], true
}
