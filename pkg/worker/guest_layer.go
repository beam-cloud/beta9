package worker

import (
	"context"
	"fmt"
	"os"

	"github.com/beam-cloud/beta9/pkg/runtime"
)

// Guest paths that are mounts or plumbing rather than container state; they
// must not end up in an image or checkpoint.
var (
	guestUpperExcludes = []string{".beam"}
	guestRootExcludes  = []string{".beam", "proc", "sys", "dev"}
)

// containerUpperDir returns the container's writable layer as a host
// directory, exporting it to a temp dir when the runtime keeps it in the
// guest. cleanup is always safe to call.
func containerUpperDir(ctx context.Context, instance *ContainerInstance) (string, func(), error) {
	gfs, ok := guestFS(instance)
	if !ok {
		return instance.Overlay.TopLayerUpperDir(), func() {}, nil
	}
	return exportGuestTree(ctx, gfs, instance.Id, runtime.GuestUpperDir, guestUpperExcludes)
}

type scratchUpperMounter interface {
	MountScratchUpper(containerID string) (string, func(), error)
}

// containerExitUpperDir returns the container's writable layer after the container or VM
// has finished running. For microVMs whose guest agent is no longer running, it mounts
// the host scratch disk directly. For standard containers, it reads the host overlay upper dir.
func containerExitUpperDir(ctx context.Context, instance *ContainerInstance) (string, func(), error) {
	if instance == nil {
		return "", func() {}, fmt.Errorf("container instance is nil")
	}
	if mvm, ok := instance.Runtime.(scratchUpperMounter); ok {
		// The mount is read-only; the guest's .beam plumbing is skipped when the
		// layer is packed (see packOverlayLayersWithTarget).
		return mvm.MountScratchUpper(instance.Id)
	}
	if instance.Overlay != nil {
		return instance.Overlay.TopLayerUpperDir(), func() {}, nil
	}
	return containerUpperDir(ctx, instance)
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
