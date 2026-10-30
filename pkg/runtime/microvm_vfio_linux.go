//go:build linux

package runtime

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

var microVMPCIAddress = regexp.MustCompile(`^[0-9a-f]{4}:[0-9a-f]{2}:[01][0-9a-f]\.[0-7]$`)

const microVMVFIODevicesEnv = "BEAM_MICROVM_VFIO_DEVICES"

type microVMVFIOHost struct {
	devices                    []string
	pciRoot, devRoot, lockRoot string
}

type microVMVFIO struct {
	paths []string
	locks []*os.File
	once  sync.Once
}

// acquire accepts only complete, host-authorized IOMMU groups already bound to
// vfio-pci. Driver binding remains an explicit host provisioning operation.
func (h microVMVFIOHost) acquire(value string) (_ *microVMVFIO, err error) {
	vfio := &microVMVFIO{}
	defer func() {
		if err != nil {
			vfio.close()
		}
	}()
	if value == "" {
		return vfio, nil
	}
	if h.pciRoot == "" {
		h.pciRoot = "/sys/bus/pci/devices"
	}
	if h.devRoot == "" {
		h.devRoot = "/dev/vfio"
	}
	if h.lockRoot == "" {
		h.lockRoot = "/run/lock/beam-vfio"
	}
	devices := strings.Split(strings.ToLower(value), ",")
	for i, device := range devices {
		if !microVMPCIAddress.MatchString(device) || slices.Contains(devices[:i], device) {
			return nil, fmt.Errorf("invalid or duplicate VFIO PCI address %q; use domain:bus:slot.function", device)
		}
		if !slices.ContainsFunc(h.devices, func(allowed string) bool { return strings.EqualFold(allowed, device) }) {
			return nil, fmt.Errorf("VFIO device %s is not authorized by the host", device)
		}
	}
	groups := map[string]string{}
	for _, device := range devices {
		path := filepath.Join(h.pciRoot, device)
		driver, err := filepath.EvalSymlinks(filepath.Join(path, "driver"))
		if err != nil || filepath.Base(driver) != "vfio-pci" {
			return nil, fmt.Errorf("VFIO device %s must be bound to vfio-pci before launch", device)
		}
		group, err := filepath.EvalSymlinks(filepath.Join(path, "iommu_group"))
		if err != nil {
			return nil, fmt.Errorf("VFIO device %s requires an IOMMU group: %w", device, err)
		}
		id := filepath.Base(group)
		if n, err := strconv.Atoi(id); err != nil || n < 0 || strconv.Itoa(n) != id {
			return nil, fmt.Errorf("invalid IOMMU group %q for %s", id, device)
		}
		groups[id] = group
		vfio.paths = append(vfio.paths, path)
	}
	for _, id := range slices.Sorted(maps.Keys(groups)) {
		group := groups[id]
		members, err := os.ReadDir(filepath.Join(group, "devices"))
		if err != nil {
			return nil, fmt.Errorf("read IOMMU group %s: %w", id, err)
		}
		if len(members) == 0 {
			return nil, fmt.Errorf("IOMMU group %s has no devices", id)
		}
		for _, member := range members {
			if !slices.Contains(devices, member.Name()) {
				return nil, fmt.Errorf("IOMMU group %s also contains %s; authorize and assign the complete group", id, member.Name())
			}
		}
		if err := os.MkdirAll(h.lockRoot, 0o700); err != nil {
			return nil, fmt.Errorf("create VFIO lock directory: %w", err)
		}
		lock, err := os.OpenFile(filepath.Join(h.lockRoot, id+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return nil, fmt.Errorf("open VFIO group %s lock: %w", id, err)
		}
		vfio.locks = append(vfio.locks, lock)
		if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			return nil, fmt.Errorf("VFIO group %s is already in use: %w", id, err)
		}
		for _, node := range []string{"vfio", id} {
			file, err := os.OpenFile(filepath.Join(h.devRoot, node), os.O_RDWR, 0)
			if err != nil {
				return nil, fmt.Errorf("open VFIO %s: %w", node, err)
			}
			_ = file.Close()
		}
	}
	return vfio, nil
}

func (v *microVMVFIO) close() {
	if v != nil {
		v.once.Do(func() {
			for _, lock := range v.locks {
				_ = lock.Close()
			}
		})
	}
}
