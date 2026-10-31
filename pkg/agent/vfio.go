package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

var (
	vfioPCIRoot    = "/sys/bus/pci/devices"
	vfioPCIAddress = regexp.MustCompile(`^[0-9a-f]{4}:[0-9a-f]{2}:[01][0-9a-f]\.[0-7]$`)
)

type vfioInventory struct {
	model   types.GpuType
	devices []string
	pciRoot string
}

func vfioInventoryFromEnv() *vfioInventory {
	model, devices := os.Getenv(types.MicroVMGPUTypeEnv), os.Getenv(types.MicroVMVFIODevicesEnv)
	if strings.TrimSpace(model) == "" && strings.TrimSpace(devices) == "" {
		return nil
	}
	return &vfioInventory{
		model:   types.NormalizeGPUType(model),
		devices: strings.Split(devices, ","),
		pciRoot: vfioPCIRoot,
	}
}

func (v *vfioInventory) preflight() ([]gpuDevice, []check) {
	devices, err := v.detect()
	return devices, []check{{
		Name:     "vfio-gpus",
		Ok:       err == nil,
		Message:  capacityCheckMessage(err == nil, fmt.Sprintf("%d GPU groups reserved for microVMs", len(devices)), err),
		Severity: severity(err == nil),
	}}
}

// VFIO owns the devices, so their model is provisioned explicitly instead of
// querying the host NVIDIA driver. Capacity comes from validated PCI groups.
func (v *vfioInventory) detect() ([]gpuDevice, error) {
	model := v.model
	if !types.KnownGPUType(model) || model == types.GPU_ANY {
		return nil, fmt.Errorf("%s requires a concrete supported GPU type", types.MicroVMGPUTypeEnv)
	}
	type group struct {
		path      string
		functions []string
		gpu       string
	}
	groups := make(map[string]*group)
	seen := make(map[string]bool)
	for _, value := range v.devices {
		device := strings.ToLower(strings.TrimSpace(value))
		if !vfioPCIAddress.MatchString(device) || seen[device] {
			return nil, fmt.Errorf("invalid or duplicate VFIO PCI address %q", device)
		}
		seen[device] = true
		path := filepath.Join(v.pciRoot, device)
		driver, err := filepath.EvalSymlinks(filepath.Join(path, "driver"))
		if err != nil || filepath.Base(driver) != "vfio-pci" {
			return nil, fmt.Errorf("VFIO device %s must be bound to vfio-pci", device)
		}
		groupPath, err := filepath.EvalSymlinks(filepath.Join(path, "iommu_group"))
		if err != nil {
			return nil, fmt.Errorf("VFIO device %s requires an IOMMU group: %w", device, err)
		}
		id := filepath.Base(groupPath)
		if n, err := strconv.ParseUint(id, 10, 32); err != nil || strconv.FormatUint(n, 10) != id {
			return nil, fmt.Errorf("invalid IOMMU group %q", id)
		}
		class, err := os.ReadFile(filepath.Join(path, "class"))
		if err != nil {
			return nil, fmt.Errorf("read VFIO device %s class: %w", device, err)
		}
		pciClass, err := strconv.ParseUint(strings.TrimSpace(string(class)), 0, 24)
		if err != nil {
			return nil, fmt.Errorf("invalid VFIO device %s class: %w", device, err)
		}
		if groups[id] == nil {
			groups[id] = &group{path: groupPath}
		}
		g := groups[id]
		g.functions = append(g.functions, device)
		if pciClass>>16 == 3 {
			if g.gpu != "" {
				return nil, fmt.Errorf("VFIO group %s contains more than one GPU", id)
			}
			g.gpu = device
		}
	}
	devices := make([]gpuDevice, 0, len(groups))
	for id, g := range groups {
		members, err := os.ReadDir(filepath.Join(g.path, "devices"))
		if err != nil {
			return nil, fmt.Errorf("read VFIO group %s: %w", id, err)
		}
		if g.gpu == "" || len(members) != len(g.functions) {
			return nil, fmt.Errorf("VFIO group %s requires exactly one GPU and all its functions", id)
		}
		for _, member := range members {
			if !slices.Contains(g.functions, member.Name()) {
				return nil, fmt.Errorf("VFIO group %s contains unauthorized device %s", id, member.Name())
			}
		}
		slices.Sort(g.functions)
		devices = append(devices, gpuDevice{ID: g.gpu, BusID: g.gpu, Name: model.String(), VFIOFunctions: g.functions})
	}
	slices.SortFunc(devices, func(a, b gpuDevice) int { return strings.Compare(a.ID, b.ID) })
	return devices, nil
}

func (v *vfioInventory) workerOptions(slot *pb.AgentWorkerSlot) (workerGPUConfig, error) {
	config := workerGPUConfig{env: map[string]string{
		types.MicroVMVFIODevicesEnv:   "",
		types.MicroVMGPUTypeEnv:       "",
		types.NvidiaVisibleDevicesEnv: "void",
		types.WorkerGPUDevicesEnv:     "",
	}}
	if slot.GpuCount == 0 {
		return config, nil
	}
	if v == nil {
		return workerGPUConfig{}, fmt.Errorf("microvm GPU workers require configured VFIO inventory")
	}
	devices, err := v.detect()
	if err != nil {
		return workerGPUConfig{}, err
	}
	selected, _, checks := selectGPUDevices(types.AgentJoinOptions{GPUIDs: slot.GpuAssignment}, devices)
	for _, check := range checks {
		if !check.Ok {
			return workerGPUConfig{}, fmt.Errorf("invalid VFIO assignment: %s", check.Message)
		}
	}
	if len(selected) != int(slot.GpuCount) || slot.GetPoolConfig().GetGpuVirtualized() {
		return workerGPUConfig{}, fmt.Errorf("VFIO worker requires %d assigned physical GPUs", slot.GpuCount)
	}
	if types.NormalizeGPUType(slot.Gpu) != v.model {
		return workerGPUConfig{}, fmt.Errorf("VFIO inventory does not match worker GPU type %q", slot.Gpu)
	}
	var functions []string
	for _, device := range selected {
		functions = append(functions, device.VFIOFunctions...)
	}
	config.env[types.MicroVMVFIODevicesEnv] = strings.Join(functions, ",")
	config.env[types.MicroVMGPUTypeEnv] = v.model.String()
	config.env[types.WorkerGPUDevicesEnv] = slot.GpuAssignment
	return config, nil
}
