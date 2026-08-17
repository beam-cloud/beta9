package common

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
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
	index      int
	lower      string
	upper      string
	work       string
	merged     string
	persistent bool
}

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
	if co.persistentUpper != "" || co.persistentWork != "" {
		return co.addPersistentLayer(co.persistentUpper, co.persistentWork)
	}
	// Right now, we are just adding an empty layer to the top of the rootfs
	// In the future, though, we can add additional layers on top of that
	return co.AddEmptyLayer()
}

// SetupWithWritable mounts an overlay whose writable state lives on a
// persistent block filesystem while merged remains disposable worker state.
// Cleanup and Reset never remove upper or work.
func (co *ContainerOverlay) SetupWithWritable(upperDir, workDir string) error {
	if len(co.layers) != 0 {
		return fmt.Errorf("container overlay is already mounted")
	}
	if !filepath.IsAbs(upperDir) || !filepath.IsAbs(workDir) {
		return fmt.Errorf("persistent overlay upper and work paths must be absolute")
	}
	upperDir = filepath.Clean(upperDir)
	workDir = filepath.Clean(workDir)
	if upperDir == workDir {
		return fmt.Errorf("persistent overlay upper and work paths must differ")
	}
	if filepath.Dir(upperDir) != filepath.Dir(workDir) || filepath.Base(upperDir) != "upper" || filepath.Base(workDir) != "work" {
		return fmt.Errorf("persistent overlay writable paths must be sibling overlay/upper and overlay/work directories")
	}
	transientRoot := filepath.Join(co.overlayPath, co.containerId)
	if overlayPathsOverlap(transientRoot, upperDir) || overlayPathsOverlap(transientRoot, workDir) {
		return fmt.Errorf("persistent overlay writable paths must be outside transient overlay path %q", transientRoot)
	}
	co.persistentUpper = upperDir
	co.persistentWork = workDir
	return co.addPersistentLayer(upperDir, workDir)
}

func (co *ContainerOverlay) addPersistentLayer(upperDir, workDir string) error {
	if upperDir == "" || workDir == "" {
		return fmt.Errorf("persistent overlay upper and work paths are required")
	}
	if len(co.layers) != 0 {
		return fmt.Errorf("persistent overlay supports exactly one writable layer")
	}
	if err := preparePersistentOverlayWritable(upperDir, workDir); err != nil {
		return err
	}
	upperInfo, err := os.Stat(upperDir)
	if err != nil {
		return err
	}
	workInfo, err := os.Stat(workDir)
	if err != nil {
		return err
	}
	upperStat, upperOK := upperInfo.Sys().(*syscall.Stat_t)
	workStat, workOK := workInfo.Sys().(*syscall.Stat_t)
	if !upperOK || !workOK || upperStat.Dev != workStat.Dev {
		return fmt.Errorf("persistent overlay upper and work must share a filesystem")
	}
	for _, dir := range []string{"workspace", "volumes"} {
		if err := os.MkdirAll(filepath.Join(upperDir, dir), 0755); err != nil {
			return fmt.Errorf("create persistent root directory %q: %w", dir, err)
		}
	}
	layerDir := filepath.Join(co.overlayPath, co.containerId, "layer-0")
	mergedDir := filepath.Join(layerDir, "merged")
	if err := os.MkdirAll(mergedDir, 0755); err != nil {
		return err
	}
	layer := ContainerOverlayLayer{
		lower: co.root, upper: upperDir, work: workDir, merged: mergedDir,
		index: 0, persistent: true,
	}
	if err := co.mount(&layer); err != nil {
		return err
	}
	co.layers = append(co.layers, layer)
	return nil
}

// preparePersistentOverlayWritable keeps the snapshotted upper intact and
// recreates only OverlayFS's scratch work directory. A restored work directory
// may contain stale kernel entries and must never be reused across mounts.
func preparePersistentOverlayWritable(upperDir, workDir string) error {
	parent := filepath.Dir(upperDir)
	if parent != filepath.Dir(workDir) || filepath.Base(upperDir) != "upper" || filepath.Base(workDir) != "work" {
		return fmt.Errorf("persistent overlay writable paths are not canonical siblings")
	}
	if err := os.MkdirAll(parent, 0755); err != nil {
		return err
	}
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		return err
	}
	if parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() {
		return fmt.Errorf("persistent overlay parent %q is not a real directory", parent)
	}
	if info, err := os.Lstat(upperDir); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("persistent overlay upper %q is not a real directory", upperDir)
		}
	} else if !os.IsNotExist(err) {
		return err
	} else if err := os.Mkdir(upperDir, 0755); err != nil {
		return err
	}
	if info, err := os.Lstat(workDir); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("persistent overlay work %q is not a real directory", workDir)
		}
		if err := os.RemoveAll(workDir); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Mkdir(workDir, 0755)
}

func overlayPathsOverlap(a, b string) bool {
	rel, err := filepath.Rel(a, b)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return true
	}
	rel, err = filepath.Rel(b, a)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func (co *ContainerOverlay) AddEmptyLayer() error {
	index := 0
	lowerDir := co.root
	if len(co.layers) > 0 {
		index = len(co.layers)
		lowerDir = co.layers[index-1].merged
	}

	layerDir := filepath.Join(co.overlayPath, co.containerId, fmt.Sprintf("layer-%d", index))

	workDir := filepath.Join(layerDir, "work")
	err := os.MkdirAll(workDir, 0755)
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
		lower:  lowerDir,
		upper:  upperDir,
		work:   workDir,
		merged: mergedDir,
		index:  index,
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

	workDir := filepath.Join(layerDir, "work")
	err := os.MkdirAll(workDir, 0755)
	if err != nil {
		return err
	}

	mergedDir := filepath.Join(layerDir, "merged")
	err = os.MkdirAll(mergedDir, 0755)
	if err != nil {
		return err
	}

	layer := ContainerOverlayLayer{
		lower:  lowerDir,
		upper:  upperDir,
		work:   workDir,
		merged: mergedDir,
		index:  index,
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

func (co *ContainerOverlay) TopLayerPath() string {
	if len(co.layers) == 0 {
		return co.root
	}

	i := len(co.layers) - 1
	layer := co.layers[i]

	return layer.merged
}

func (co *ContainerOverlay) OverlayPath() string {
	return co.overlayPath
}

func (co *ContainerOverlay) mount(layer *ContainerOverlayLayer) error {
	startTime := time.Now()

	mntOptions := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", layer.lower, layer.upper, layer.work)
	if err := exec.Command("mount", "-t", "overlay", "overlay", "-o", mntOptions, layer.merged).Run(); err != nil {
		return err
	}

	log.Info().Str("container_id", co.containerId).Int("layer_index", layer.index).Dur("duration", time.Since(startTime)).Msg("mounted kernel overlay layer")
	return nil
}
