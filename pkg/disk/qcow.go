package disk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

// Fixed qcow2 image options. Every layer in a chain is created with the same
// options so sealed layers are byte-reproducible across hosts.
const qcowCreateOpts = "compat=1.1,cluster_size=65536,lazy_refcounts=off"

// formatQcowBase prepares a fresh filesystem offline. No kernel NBD device,
// daemon or udev probe participates. Only a complete, synced image replaces
// the empty head, so a failed or interrupted attempt can be repeated safely.
func (m *Manager) formatQcowBase(ctx context.Context, headPath string, sizeBytes int64) error {
	raw, err := os.CreateTemp(filepath.Dir(headPath), ".format-*.raw")
	if err != nil {
		return err
	}
	defer os.Remove(raw.Name())
	defer raw.Close()
	if err := raw.Truncate(sizeBytes); err != nil {
		return err
	}
	if err := raw.Close(); err != nil {
		return err
	}
	if err := m.formatExt4(ctx, raw.Name()); err != nil {
		return err
	}
	converted := raw.Name() + ".qcow2"
	defer os.Remove(converted)
	if err := m.convertQcow(ctx, raw.Name(), converted, "raw"); err != nil {
		return fmt.Errorf("convert formatted filesystem: %w", err)
	}
	file, err := os.OpenFile(converted, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	return replaceFileAtomic(file, headPath)
}

// runner executes an external binary and returns combined output. It exists so
// tests can intercept process execution.
type runner func(ctx context.Context, name string, args ...string) ([]byte, error)

func execRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	output, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("%s %v: %w: %s", name, args, err, string(output))
	}
	return output, nil
}

// createQcowBase creates an empty parentless qcow2 image.
func (m *Manager) createQcowBase(ctx context.Context, path string, virtualSizeBytes int64) error {
	_, err := m.run(ctx, m.binaries.QemuImg,
		"create", "-q", "-f", "qcow2", "-o", qcowCreateOpts,
		path, strconv.FormatInt(virtualSizeBytes, 10))
	return err
}

// createQcowOverlay creates an empty qcow2 overlay backed by parentPath.
func (m *Manager) createQcowOverlay(ctx context.Context, path, parentPath string, virtualSizeBytes int64) error {
	_, err := m.run(ctx, m.binaries.QemuImg,
		"create", "-q", "-f", "qcow2", "-o", qcowCreateOpts,
		"-F", "qcow2", "-b", parentPath,
		path, strconv.FormatInt(virtualSizeBytes, 10))
	return err
}

// rebaseQcow repoints an image's backing reference without copying data.
// An empty parentPath detaches the image from any backing file.
func (m *Manager) rebaseQcow(ctx context.Context, path, parentPath string) error {
	args := []string{"rebase", "-q", "-u", "-f", "qcow2"}
	if parentPath != "" {
		args = append(args, "-F", "qcow2")
	}
	args = append(args, "-b", parentPath, path)
	_, err := m.run(ctx, m.binaries.QemuImg, args...)
	return err
}

// flattenQcow collapses an image and its entire backing chain into a single
// parentless image, skipping zero regions so the output stays sparse.
func (m *Manager) flattenQcow(ctx context.Context, sourcePath, destPath string) error {
	stop := reportFileGrowth(ctx, destPath)
	defer stop()
	return m.convertQcow(ctx, sourcePath, destPath, "qcow2")
}

func (m *Manager) convertQcow(ctx context.Context, sourcePath, destPath, sourceFormat string) error {
	_, err := m.run(ctx, m.binaries.QemuImg,
		"convert", "-q", "-f", sourceFormat, "-O", "qcow2",
		"-o", qcowCreateOpts, "-S", "4k",
		sourcePath, destPath)
	return err
}

type qcowInfo struct {
	VirtualSize int64  `json:"virtual-size"`
	ActualSize  int64  `json:"actual-size"`
	BackingFile string `json:"backing-filename"`
}

func (m *Manager) queryQcowInfo(ctx context.Context, path string) (*qcowInfo, error) {
	// --force-share tolerates the file being open read-only inside QSD.
	output, err := m.run(ctx, m.binaries.QemuImg, "info", "--output=json", "--force-share", path)
	if err != nil {
		return nil, err
	}
	info := &qcowInfo{}
	if err := json.Unmarshal(output, info); err != nil {
		return nil, fmt.Errorf("parse qemu-img info for %s: %w", path, err)
	}
	return info, nil
}
