package common

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"
)

type ContainerOverlay struct {
	containerId string
	layers      []ContainerOverlayLayer
	root        string
	overlayPath string
}

type ContainerOverlayLayer struct {
	index  int
	lower  string
	upper  string
	work   string
	merged string
}

func NewContainerOverlay(containerId string, rootPath string, overlayPath string) *ContainerOverlay {
	return &ContainerOverlay{
		containerId: containerId,
		layers:      []ContainerOverlayLayer{},
		root:        rootPath,
		overlayPath: overlayPath,
	}
}

func (co *ContainerOverlay) Setup() error {
	// Right now, we are just adding an empty layer to the top of the rootfs
	// In the future, though, we can add additional layers on top of that
	return co.AddEmptyLayer()
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

func (co *ContainerOverlay) Cleanup() error {
	var err error = nil
	for len(co.layers) > 0 {
		// Get the last layer index
		i := len(co.layers) - 1
		layer := co.layers[i]

		log.Info().Str("layer_path", layer.merged).Msg("unmounting layer")
		err := exec.Command("umount", "-f", layer.merged).Run()
		if err != nil {
			log.Error().Str("layer_path", layer.merged).Err(err).Msg("unable to unmount layer")
			return err
		}

		layerDir := filepath.Join(co.overlayPath, co.containerId, fmt.Sprintf("layer-%d", i))
		err = os.RemoveAll(layerDir)
		if err != nil {
			return err
		}

		// Remove the layer from the slice
		co.layers = co.layers[:i]
	}

	err = os.RemoveAll(filepath.Join(co.overlayPath, co.containerId))
	return err
}

func (co *ContainerOverlay) TopLayerPath() string {
	if len(co.layers) == 0 {
		return co.root
	}

	i := len(co.layers) - 1
	layer := co.layers[i]

	return layer.merged
}

func (co *ContainerOverlay) mount(layer *ContainerOverlayLayer) error {
	startTime := time.Now()

	mntOptions := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", layer.lower, layer.upper, layer.work)
	err := exec.Command("mount", "-t", "overlay", "overlay", "-o", mntOptions, layer.merged).Run()
	if err != nil {
		return err
	}

	log.Info().Str("container_id", co.containerId).Int("layer_index", layer.index).Dur("duration", time.Since(startTime)).Msg("mounted layer")
	return nil
}
