//go:build linux

package runtime

import (
	"errors"
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
	devices          []string
	pciRoot, devRoot string
}

type microVMVFIO struct {
	paths []string
	locks []*os.File
	once  sync.Once
}

type microVMVFIOGroup struct {
	path    string
	devices []string
	gpus    int
}

// acquire reserves one complete GPU group already bound to vfio-pci. An empty
// selection takes the first available group from the host's inventory.
func (h microVMVFIOHost) acquire(value string) (_ *microVMVFIO, err error) {
	vfio := &microVMVFIO{}
	defer func() {
		if err != nil {
			vfio.close()
		}
	}()
	automatic := value == ""
	if automatic {
		value = strings.Join(h.devices, ",")
	}
	if value == "" {
		return vfio, nil
	}
	if h.pciRoot == "" {
		h.pciRoot = "/sys/bus/pci/devices"
	}
	if h.devRoot == "" {
		h.devRoot = "/dev/vfio"
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
	groups := map[string]*microVMVFIOGroup{}
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
		if groups[id] == nil {
			groups[id] = &microVMVFIOGroup{path: group}
		}
		groups[id].devices = append(groups[id].devices, device)
		class, err := os.ReadFile(filepath.Join(path, "class"))
		if err != nil {
			return nil, fmt.Errorf("read VFIO device %s class: %w", device, err)
		}
		if strings.HasPrefix(strings.TrimSpace(string(class)), "0x03") {
			groups[id].gpus++
		}
	}
	if !automatic && len(groups) != 1 {
		return nil, fmt.Errorf("VFIO selection must contain exactly one GPU IOMMU group")
	}
	for _, id := range slices.Sorted(maps.Keys(groups)) {
		group := groups[id]
		if group.gpus != 1 {
			if automatic {
				continue
			}
			return nil, fmt.Errorf("VFIO group %s must contain exactly one GPU, found %d", id, group.gpus)
		}
		members, err := os.ReadDir(filepath.Join(group.path, "devices"))
		if err != nil {
			return nil, fmt.Errorf("read IOMMU group %s: %w", id, err)
		}
		if len(members) != len(group.devices) {
			return nil, fmt.Errorf("IOMMU group %s requires its complete set of devices", id)
		}
		for _, member := range members {
			if !slices.Contains(group.devices, member.Name()) {
				return nil, fmt.Errorf("IOMMU group %s also contains %s; authorize and assign the complete group", id, member.Name())
			}
		}
		// Host-networked workers share the group's sysfs inode.
		lock, err := os.Open(group.path)
		if err != nil {
			return nil, fmt.Errorf("open VFIO group %s lock: %w", id, err)
		}
		if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			_ = lock.Close()
			if automatic && errors.Is(err, unix.EWOULDBLOCK) {
				continue
			}
			return nil, fmt.Errorf("VFIO group %s is already in use: %w", id, err)
		}
		vfio.locks = append(vfio.locks, lock)
		for _, node := range []string{"vfio", id} {
			file, err := os.OpenFile(filepath.Join(h.devRoot, node), os.O_RDWR, 0)
			if err != nil {
				return nil, fmt.Errorf("open VFIO %s: %w", node, err)
			}
			_ = file.Close()
		}
		for _, device := range group.devices {
			vfio.paths = append(vfio.paths, filepath.Join(h.pciRoot, device))
		}
		return vfio, nil
	}
	return nil, fmt.Errorf("no available VFIO group with exactly one GPU")
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
