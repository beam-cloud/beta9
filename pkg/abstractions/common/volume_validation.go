package abstractions

import (
	"fmt"
	"strings"

	pb "github.com/beam-cloud/beta9/proto"
)

// ValidateVolumeMount checks fields that are used to construct worker paths.
// Cloud buckets do not have a volume record and the SDK leaves their ID empty.
func ValidateVolumeMount(volume *pb.Volume) error {
	if volume == nil {
		return fmt.Errorf("volume must not be nil")
	}
	if (volume.Id == "" && volume.Config == nil) || volume.Id == "." || volume.Id == ".." || strings.ContainsAny(volume.Id, "/\\\x00") {
		return fmt.Errorf("invalid volume ID")
	}
	if volume.MountPath == "" || strings.ContainsRune(volume.MountPath, '\x00') {
		return fmt.Errorf("invalid volume mount path")
	}
	for _, part := range strings.Split(volume.MountPath, "/") {
		if part == ".." {
			return fmt.Errorf("volume mount path must not contain parent directory traversal")
		}
	}
	return nil
}
