package common

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	types "github.com/beam-cloud/beta9/pkg/types"
)

type ContainerOverlay struct {
	request         *types.ContainerRequest
	containerId     string
	layers          []ContainerOverlayLayer
	root            string
	overlayPath     string
	persistentUpper string
	persistentWork  string
}

type ContainerOverlayLayer struct {
	index  int
	lower  string
	upper  string
	work   string
	merged string
	// volatile mounts the layer with overlayfs' "volatile" option: fsync and
	// syncfs on the merged tree return without flushing the upper to disk.
	// The upper is still written back by the kernel as usual, so the only
	// thing given up is durability across a host crash, which an ephemeral
	// container layer does not have anyway (the container is gone with the
	// host). This is what makes writes to / behave like a memory-backed
	// rootfs (gVisor's tmpfs overlay does the same) instead of paying an
	// EBS/NVMe round trip on every fsync. Never used for durable-disk uppers.
	volatile bool
}

// overlayVolatileUnsupported is set once the kernel rejects a volatile mount
// (overlayfs "volatile" landed in Linux 5.10) so later mounts skip the retry.
var overlayVolatileUnsupported atomic.Bool

func NewContainerOverlay(request *types.ContainerRequest, rootPath string, overlayPath string) *ContainerOverlay {
	return &ContainerOverlay{
		request:     request,
		containerId: request.ContainerId,
		layers:      []ContainerOverlayLayer{},
		root:        rootPath,
		overlayPath: overlayPath,
	}
}

func (co *ContainerOverlay) Setup() error {
	if co.persistentUpper != "" {
		return co.addPersistentLayer()
	}
	// Right now, we are just adding an empty layer to the top of the rootfs
	// In the future, though, we can add additional layers on top of that
	return co.AddEmptyLayer()
}

// SetupWithWritable mounts the overlay with its writable layer on a
// persistent filesystem (e.g. a qcow-backed durable disk) instead of worker
// scratch space, so the container's entire root filesystem delta survives
// snapshots and restores. Cleanup never removes upper or work.
func (co *ContainerOverlay) SetupWithWritable(upperDir, workDir string) error {
	if len(co.layers) != 0 {
		return fmt.Errorf("container overlay is already mounted")
	}
	if !filepath.IsAbs(upperDir) || !filepath.IsAbs(workDir) || upperDir == workDir {
		return fmt.Errorf("persistent overlay requires distinct absolute upper and work paths")
	}
	co.persistentUpper = filepath.Clean(upperDir)
	co.persistentWork = filepath.Clean(workDir)
	return co.addPersistentLayer()
}

func (co *ContainerOverlay) addPersistentLayer() error {
	if err := os.MkdirAll(co.persistentUpper, 0755); err != nil {
		return err
	}
	// A restored work directory can hold stale kernel state; overlayfs
	// requires it to be recreated for every mount.
	if err := os.RemoveAll(co.persistentWork); err != nil {
		return err
	}
	if err := os.Mkdir(co.persistentWork, 0755); err != nil {
		return err
	}
	for _, dir := range []string{"workspace", "volumes"} {
		if err := os.MkdirAll(filepath.Join(co.persistentUpper, dir), 0755); err != nil {
			return err
		}
	}

	mergedDir := filepath.Join(co.overlayPath, co.containerId, "layer-0", "merged")
	if err := os.MkdirAll(mergedDir, 0755); err != nil {
		return err
	}
	layer := ContainerOverlayLayer{
		lower:  co.root,
		upper:  co.persistentUpper,
		work:   co.persistentWork,
		merged: mergedDir,
		index:  0,
	}
	if err := co.mount(&layer); err != nil {
		return err
	}
	co.layers = append(co.layers, layer)
	return nil
}

func (co *ContainerOverlay) AddEmptyLayer() error {
	index := 0
	lowerDir := co.root
	if len(co.layers) > 0 {
		index = len(co.layers)
		lowerDir = co.layers[index-1].merged
	}

	layerDir := filepath.Join(co.overlayPath, co.containerId, fmt.Sprintf("layer-%d", index))

	workDir, err := freshWorkDir(layerDir)
	if err != nil {
		return err
	}

	upperDir := filepath.Join(layerDir, "upper")
	err = os.MkdirAll(upperDir, 0755)
	if err != nil {
		return err
	}

	// Create required directories in the upper layer
	// This ensures they exist in the container filesystem regardless of the base image
	for _, dir := range []string{"workspace", "volumes"} {
		requiredDir := filepath.Join(upperDir, dir)
		if err := os.MkdirAll(requiredDir, 0755); err != nil {
			log.Warn().Err(err).Str("path", requiredDir).Msg("failed to create required directory in upper layer")
		}
	}

	mergedDir := filepath.Join(layerDir, "merged")
	err = os.MkdirAll(mergedDir, 0755)
	if err != nil {
		return err
	}

	layer := ContainerOverlayLayer{
		lower:    lowerDir,
		upper:    upperDir,
		work:     workDir,
		merged:   mergedDir,
		index:    index,
		volatile: true,
	}

	err = co.mount(&layer)
	if err != nil {
		return err
	}

	co.layers = append(co.layers, layer)

	return nil
}

func (co *ContainerOverlay) AddLayer(upperDir string) error {
	index := 0
	lowerDir := co.root
	if len(co.layers) > 0 {
		index = len(co.layers)
		lowerDir = co.layers[index-1].merged
	}

	layerDir := filepath.Join(co.overlayPath, co.containerId, fmt.Sprintf("layer-%d", index))

	workDir, err := freshWorkDir(layerDir)
	if err != nil {
		return err
	}

	mergedDir := filepath.Join(layerDir, "merged")
	err = os.MkdirAll(mergedDir, 0755)
	if err != nil {
		return err
	}

	layer := ContainerOverlayLayer{
		lower:    lowerDir,
		upper:    upperDir,
		work:     workDir,
		merged:   mergedDir,
		index:    index,
		volatile: true,
	}

	err = co.mount(&layer)
	if err != nil {
		return err
	}

	co.layers = append(co.layers, layer)

	return nil
}

func (co *ContainerOverlay) cleanupLayers() error {
	for len(co.layers) > 0 {
		// Get the last layer index
		i := len(co.layers) - 1
		layer := co.layers[i]

		log.Info().Str("layer_path", layer.merged).Msg("unmounting layer")
		if err := exec.Command("umount", "-f", layer.merged).Run(); err != nil {
			log.Error().Str("layer_path", layer.merged).Err(err).Msg("unable to unmount layer")
			return err
		}

		layerDir := filepath.Join(co.overlayPath, co.containerId, fmt.Sprintf("layer-%d", i))
		if err := os.RemoveAll(layerDir); err != nil {
			return err
		}

		// Remove the layer from the slice
		co.layers = co.layers[:i]
	}
	return nil
}

func (co *ContainerOverlay) Cleanup() error {
	if err := co.cleanupLayers(); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Join(co.overlayPath, co.containerId))
}

// Reset rebuilds the writable layer while preserving bundle state that may
// share the container root, such as config.json and checkpoint signal mounts.
func (co *ContainerOverlay) Reset() error {
	if err := co.cleanupLayers(); err != nil {
		return err
	}
	if err := co.Setup(); err != nil {
		_ = co.cleanupLayers()
		return err
	}
	return nil
}

// ResetWithUpper rebuilds the writable layer from seed while it is unmounted.
// Checkpoint upper directories can contain overlay whiteouts and opaque xattrs
// that must not be copied through the merged filesystem.
func (co *ContainerOverlay) ResetWithUpper(seed func(string) error) error {
	if seed == nil {
		return fmt.Errorf("upper layer seed is required")
	}
	if err := co.cleanupLayers(); err != nil {
		return err
	}

	if co.persistentUpper != "" {
		if err := os.RemoveAll(co.persistentUpper); err != nil {
			return err
		}
		if err := os.MkdirAll(co.persistentUpper, 0755); err != nil {
			return err
		}
		if err := seed(co.persistentUpper); err != nil {
			return err
		}
		return co.Setup()
	}

	layerDir := filepath.Join(co.overlayPath, co.containerId, "layer-0")
	if err := os.RemoveAll(layerDir); err != nil {
		return err
	}
	upperDir := filepath.Join(layerDir, "upper")
	if err := os.MkdirAll(upperDir, 0755); err != nil {
		return err
	}
	if err := seed(upperDir); err != nil {
		_ = os.RemoveAll(layerDir)
		return err
	}
	if err := co.Setup(); err != nil {
		_ = co.cleanupLayers()
		_ = os.RemoveAll(layerDir)
		return err
	}
	return nil
}

// RootPath is the immutable image root under every layer.
func (co *ContainerOverlay) RootPath() string {
	return co.root
}

func (co *ContainerOverlay) TopLayerPath() string {
	if len(co.layers) == 0 {
		return co.root
	}

	i := len(co.layers) - 1
	layer := co.layers[i]

	return layer.merged
}

// TopLayerUpperDir returns the writable upper directory of the top layer,
// which may live on a persistent disk rather than beside the merged path.
func (co *ContainerOverlay) TopLayerUpperDir() string {
	if len(co.layers) == 0 {
		// Matches the layout AddEmptyLayer would create; kept for callers that
		// resolve the path before the overlay is mounted.
		return filepath.Join(filepath.Dir(co.root), "upper")
	}
	return co.layers[len(co.layers)-1].upper
}

func (co *ContainerOverlay) OverlayPath() string {
	return co.overlayPath
}

// freshWorkDir recreates <layerDir>/work. overlayfs needs an empty work dir,
// and a work dir left by a previous volatile mount carries an
// incompat/volatile marker that makes the kernel refuse the next mount.
func freshWorkDir(layerDir string) (string, error) {
	workDir := filepath.Join(layerDir, "work")
	if err := os.RemoveAll(workDir); err != nil {
		return "", err
	}
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return "", err
	}
	return workDir, nil
}

func (co *ContainerOverlay) mount(layer *ContainerOverlayLayer) error {
	startTime := time.Now()

	mntOptions := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", layer.lower, layer.upper, layer.work)
	volatile := layer.volatile && !overlayVolatileUnsupported.Load()
	if volatile {
		out, err := exec.Command("mount", "-t", "overlay", "overlay", "-o", mntOptions+",volatile", layer.merged).CombinedOutput()
		if err == nil {
			log.Info().Str("container_id", co.containerId).Int("layer_index", layer.index).Bool("volatile", true).Dur("duration", time.Since(startTime)).Msg("mounted kernel overlay layer")
			return nil
		}
		// Pre-5.10 kernels reject the option with EINVAL; everything else is
		// a real failure that a retry without it would only mask.
		msg := strings.ToLower(string(out))
		if !strings.Contains(msg, "invalid argument") && !strings.Contains(msg, "bad option") {
			return fmt.Errorf("mount overlay: %w: %s", err, strings.TrimSpace(string(out)))
		}
		volatileOut := strings.TrimSpace(string(out))
		if _, err := freshWorkDir(filepath.Dir(layer.work)); err != nil {
			return err
		}
		if out, err := exec.Command("mount", "-t", "overlay", "overlay", "-o", mntOptions, layer.merged).CombinedOutput(); err != nil {
			return fmt.Errorf("mount overlay: %w: %s", err, strings.TrimSpace(string(out)))
		}
		// The same mount succeeded without the option, so the option is
		// what this kernel rejects: stop trying it.
		overlayVolatileUnsupported.Store(true)
		log.Warn().Str("container_id", co.containerId).Str("output", volatileOut).Msg("kernel rejected volatile overlay mount; using synced overlay mounts")
		return nil
	}

	if out, err := exec.Command("mount", "-t", "overlay", "overlay", "-o", mntOptions, layer.merged).CombinedOutput(); err != nil {
		return fmt.Errorf("mount overlay: %w: %s", err, strings.TrimSpace(string(out)))
	}

	log.Info().Str("container_id", co.containerId).Int("layer_index", layer.index).Bool("volatile", false).Dur("duration", time.Since(startTime)).Msg("mounted kernel overlay layer")
	return nil
}
