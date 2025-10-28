package common

import (
    "bufio"
    "errors"
    "fmt"
    "os"
    "os/exec"
    "path/filepath"
    "strings"
    "time"

    "github.com/rs/zerolog/log"

    types "github.com/beam-cloud/beta9/pkg/types"
)

type ContainerOverlay struct {
	request     *types.ContainerRequest
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

    // Use kernel overlayfs (v1 behavior) and allow exec
    mntOptions := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s,exec", layer.lower, layer.upper, layer.work)
    if err := exec.Command("mount", "-t", "overlay", "overlay", "-o", mntOptions, layer.merged).Run(); err != nil {
        return err
    }
    log.Info().Str("container_id", co.containerId).Int("layer_index", layer.index).Dur("duration", time.Since(startTime)).Msg("mounted layer (kernel overlay)")
    return nil
}

// lowerIsFuse returns true if the path resides on a FUSE filesystem
func lowerIsFuse(path string) bool {
    // Quick heuristic: our ClipFS FUSE mounts live under /images/mnt/
    if strings.HasPrefix(path, "/images/mnt/") {
        return true
    }
    f, err := os.Open("/proc/mounts")
    if err != nil {
        return false
    }
    defer f.Close()
    scanner := bufio.NewScanner(f)
    for scanner.Scan() {
        line := scanner.Text()
        // format: <src> <target> <fstype> <options> ...
        fields := strings.Fields(line)
        if len(fields) < 3 {
            continue
        }
        target := fields[1]
        fstype := fields[2]
        if strings.HasPrefix(path, target) && strings.Contains(strings.ToLower(fstype), "fuse") {
            return true
        }
    }
    return false
}

func fuseOverlayfsAvailable() bool {
    _, err := exec.LookPath("fuse-overlayfs")
    return err == nil
}
