package worker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func TestImageNoXattrsRequiresActualCLIPMount(t *testing.T) {
	for _, tc := range []struct {
		name, runtime, mountedImage string
		want                        bool
	}{
		{"mounted CLIP", "microvm", "image", true},
		{"different image", "microvm", "other", false},
		{"native image", "microvm", "", false},
		{"shared kernel", "runc", "image", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mounts := common.NewSafeMap[*fuse.Server]()
			if tc.mountedImage != "" {
				mounts.Set(tc.mountedImage, &fuse.Server{})
			}
			worker := &Worker{runtime: &mockRuntime{name: tc.runtime}, imageClient: &ImageClient{mountedFuseServers: mounts}}
			spec := &specs.Spec{Annotations: map[string]string{"unrelated": "preserved"}}
			require.NoError(t, worker.prepareSandboxProcess(&types.ContainerRequest{ImageId: "image"}, spec))
			require.Equal(t, tc.want, spec.Annotations[runtime.MicroVMImageNoXattrsAnnotation] == "true")
			require.Equal(t, "preserved", spec.Annotations["unrelated"])
		})
	}
}

func TestDirectImageAnnotationOnlyComesFromCurrentCLIPOverlay(t *testing.T) {
	for _, v := range []struct {
		name, mounted, root string
		want                bool
	}{
		{"current CLIP", "image", "image", true},
		{"legacy rootfs", "image", "image/rootfs", true},
		{"different image", "other", "image", false},
		{"no mount", "", "image", false},
		{"foreign overlay", "image", "foreign", false},
		{"symlink root", "image", "image", false},
	} {
		t.Run(v.name, func(t *testing.T) {
			base := t.TempDir()
			mounts := common.NewSafeMap[*fuse.Server]()
			if v.mounted != "" {
				mounts.Set(v.mounted, &fuse.Server{})
			}
			if v.name == "symlink root" {
				require.NoError(t, os.Symlink(t.TempDir(), filepath.Join(base, v.root)))
			} else {
				require.NoError(t, os.MkdirAll(filepath.Join(base, v.root), 0o755))
			}
			instances := common.NewSafeMap[*ContainerInstance]()
			request := &types.ContainerRequest{ContainerId: "owned", ImageId: "image"}
			instances.Set("owned", &ContainerInstance{Overlay: common.NewContainerOverlay(request, filepath.Join(base, v.root), t.TempDir())})
			worker := &Worker{runtime: &mockRuntime{name: "microvm"}, containerInstances: instances, imageClient: &ImageClient{mountedFuseServers: mounts, imageMountPath: base}}
			spec := &specs.Spec{Annotations: map[string]string{runtime.MicroVMImageRootAnnotation: "/image-supplied/forged"}}
			require.NoError(t, worker.prepareSandboxProcess(request, spec))
			if v.want {
				require.Equal(t, filepath.Join(base, v.root), spec.Annotations[runtime.MicroVMImageRootAnnotation])
			} else {
				require.Empty(t, spec.Annotations[runtime.MicroVMImageRootAnnotation])
			}
		})
	}
}
