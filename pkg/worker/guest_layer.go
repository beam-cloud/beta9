package worker

import (
	"context"
	"os"

	"github.com/beam-cloud/beta9/pkg/runtime"
)

// guestTreeExcludes are the guest paths that are mounts or plumbing rather
// than container state and must not end up in an image or checkpoint.
var (
	guestUpperExcludes = []string{".beam"}
	guestRootExcludes  = []string{".beam", "proc", "sys", "dev"}
)

// containerUpperDir returns the container's writable layer as a host
// directory. For host-overlay runtimes that is the overlay upper dir; for a
// runtime that keeps its writable layer inside the guest it is exported to a
// temporary directory first. cleanup removes any export and is always safe
// to call.
func containerUpperDir(ctx context.Context, instance *ContainerInstance) (string, func(), error) {
	gfs, ok := guestFS(instance)
	if !ok {
		return instance.Overlay.TopLayerUpperDir(), func() {}, nil
	}
	return exportGuestTree(ctx, gfs, instance.Id, runtime.GuestUpperDir, guestUpperExcludes)
}

// containerRootDir is the merged-root counterpart of containerUpperDir, for
// images without an OCI base to layer on.
func containerRootDir(ctx context.Context, instance *ContainerInstance) (string, func(), error) {
	gfs, ok := guestFS(instance)
	if !ok {
		return instance.Overlay.TopLayerPath(), func() {}, nil
	}
	return exportGuestTree(ctx, gfs, instance.Id, "/", guestRootExcludes)
}

func exportGuestTree(ctx context.Context, gfs runtime.GuestFilesystem, containerID, guestPath string, exclude []string) (string, func(), error) {
	dir, err := os.MkdirTemp("", "guest-tree-"+containerID+"-")
	if err != nil {
		return "", func() {}, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	if err := gfs.ExportGuestTree(ctx, containerID, guestPath, dir, exclude); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return dir, cleanup, nil
}
