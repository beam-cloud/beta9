package agent

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

type gpuDevice struct {
	ID            string
	UUID          string
	Name          string
	BusID         string
	VFIOFunctions []string
}

type agentCapacity struct {
	CPUCount                  uint32
	CPUMillicores             int64
	MemoryMB                  uint64
	GPUs                      []string
	GPUIDs                    []string
	GPUCount                  uint32
	NetworkSlotPoolSize       uint32
	ContainerStartConcurrency uint32
}

func resolveAgentCapacity(opts types.AgentJoinOptions, preflight preflightResult) (agentCapacity, []check, bool) {
	checks := append([]check{}, preflight.checks...)
	schedulable := preflight.schedulable

	capacity := agentCapacity{
		CPUCount:                  uint32(systemCPUCount()),
		CPUMillicores:             int64(systemCPUCount()) * 1000,
		MemoryMB:                  systemMemoryMB(),
		GPUs:                      types.NormalizeGPUStrings(preflight.gpus),
		GPUIDs:                    gpuDeviceIDs(preflight.gpuDevices),
		GPUCount:                  uint32(len(preflight.gpus)),
		NetworkSlotPoolSize:       uint32(opts.NetworkSlots),
		ContainerStartConcurrency: uint32(opts.ContainerStartConcurrency),
	}

	if opts.MaxCPU != "" {
		millicores, err := parseCPUMillicores(opts.MaxCPU)
		ok := err == nil && millicores <= int64(systemCPUCount())*1000
		checks = append(checks, check{
			Name:     "capacity.max_cpu",
			Ok:       ok,
			Message:  capacityCheckMessage(ok, fmt.Sprintf("using %dm CPU", millicores), err),
			Severity: severity(ok),
		})
		if ok {
			capacity.CPUMillicores = millicores
			capacity.CPUCount = uint32((millicores + 999) / 1000)
		} else {
			schedulable = false
		}
	}

	if opts.MaxMemory != "" {
		memoryMB, err := parseMemoryMB(opts.MaxMemory)
		ok := err == nil && memoryMB <= systemMemoryMB()
		checks = append(checks, check{
			Name:     "capacity.max_memory",
			Ok:       ok,
			Message:  capacityCheckMessage(ok, fmt.Sprintf("using %d MB memory", memoryMB), err),
			Severity: severity(ok),
		})
		if ok {
			capacity.MemoryMB = memoryMB
		} else {
			schedulable = false
		}
	}

	selectedDevices, explicitGPUSelection, gpuChecks := selectGPUDevices(opts, preflight.gpuDevices)
	checks = append(checks, gpuChecks...)
	for _, gpuCheck := range gpuChecks {
		if !gpuCheck.Ok {
			schedulable = false
		}
	}
	if explicitGPUSelection {
		capacity.GPUs = make([]string, 0, len(selectedDevices))
		capacity.GPUIDs = make([]string, 0, len(selectedDevices))
		for _, device := range selectedDevices {
			capacity.GPUs = append(capacity.GPUs, string(types.NormalizeGPUType(device.Name)))
			capacity.GPUIDs = append(capacity.GPUIDs, device.ID)
		}
		capacity.GPUCount = uint32(len(selectedDevices))
	}

	return capacity, checks, schedulable
}

// SystemCPUCount and SystemMemoryMB expose detected host capacity for
// callers (e.g. the vast compat shim) that derive per-GPU capacity shares.
func SystemCPUCount() int { return systemCPUCount() }

func SystemMemoryMB() uint64 { return systemMemoryMB() }

func systemCPUCount() int {
	count := runtime.NumCPU()
	if count <= 0 {
		return 1
	}
	return count
}

func parseCPUMillicores(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("max cpu is empty")
	}
	cores, err := strconv.ParseFloat(value, 64)
	if err != nil || cores <= 0 {
		return 0, fmt.Errorf("max cpu must be a positive number of cores")
	}
	return int64(math.Round(cores * 1000)), nil
}

func parseMemoryMB(value string) (uint64, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return 0, fmt.Errorf("max memory is empty")
	}

	units := []struct {
		suffix string
		scale  float64
	}{
		{"tib", 1024 * 1024},
		{"tb", 1000 * 1000},
		{"gib", 1024},
		{"gb", 1000},
		{"gi", 1024},
		{"g", 1000},
		{"mib", 1},
		{"mb", 1},
		{"mi", 1},
		{"m", 1},
	}
	scale := float64(1)
	for _, unit := range units {
		if strings.HasSuffix(value, unit.suffix) {
			value = strings.TrimSpace(strings.TrimSuffix(value, unit.suffix))
			scale = unit.scale
			break
		}
	}

	amount, err := strconv.ParseFloat(value, 64)
	if err != nil || amount <= 0 {
		return 0, fmt.Errorf("max memory must be a positive size")
	}
	return uint64(math.Round(amount * scale)), nil
}

func selectGPUDevices(opts types.AgentJoinOptions, devices []gpuDevice) ([]gpuDevice, bool, []check) {
	gpuIDs := splitCSV(opts.GPUIDs)
	if len(gpuIDs) > 0 && opts.MaxGPUs > 0 {
		return nil, true, []check{{
			Name:     "capacity.gpu_selection",
			Ok:       false,
			Message:  "--gpu-ids and --max-gpus cannot both be set",
			Severity: severity(false),
		}}
	}

	if len(gpuIDs) > 0 {
		selected := make([]gpuDevice, 0, len(gpuIDs))
		seen := make(map[string]bool, len(gpuIDs))
		for _, id := range gpuIDs {
			device, ok := findGPUDevice(devices, id)
			if !ok || seen[device.ID] {
				return nil, true, []check{{
					Name:     "capacity.gpu_ids",
					Ok:       false,
					Message:  fmt.Sprintf("GPU id %q was not detected or was selected more than once", id),
					Severity: severity(false),
				}}
			}
			seen[device.ID] = true
			device.ID = id
			selected = append(selected, device)
		}
		return selected, true, []check{{
			Name:     "capacity.gpu_ids",
			Ok:       true,
			Message:  fmt.Sprintf("using GPU ids %s", strings.Join(gpuIDs, ",")),
			Severity: severity(true),
		}}
	}

	if opts.MaxGPUs > 0 {
		if int(opts.MaxGPUs) > len(devices) {
			return nil, true, []check{{
				Name:     "capacity.max_gpus",
				Ok:       false,
				Message:  fmt.Sprintf("requested %d GPUs, detected %d", opts.MaxGPUs, len(devices)),
				Severity: severity(false),
			}}
		}
		return append([]gpuDevice{}, devices[:int(opts.MaxGPUs)]...), true, []check{{
			Name:     "capacity.max_gpus",
			Ok:       true,
			Message:  fmt.Sprintf("using %d GPUs", opts.MaxGPUs),
			Severity: severity(true),
		}}
	}

	return nil, false, nil
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func findGPUDevice(devices []gpuDevice, id string) (gpuDevice, bool) {
	for _, device := range devices {
		if device.ID == id || device.UUID == id {
			return device, true
		}
	}
	return gpuDevice{}, false
}

func gpuDeviceIDs(devices []gpuDevice) []string {
	ids := make([]string, 0, len(devices))
	for _, device := range devices {
		if device.ID != "" {
			ids = append(ids, device.ID)
		}
	}
	return ids
}

func capacityCheckMessage(ok bool, success string, err error) string {
	if ok {
		return success
	}
	if err != nil {
		return err.Error()
	}
	return "requested capacity exceeds detected machine capacity"
}

var (
	agentVFIOPCIRoot    = "/sys/bus/pci/devices"
	agentVFIOPCIAddress = regexp.MustCompile(`^[0-9a-f]{4}:[0-9a-f]{2}:[01][0-9a-f]\.[0-7]$`)
)

func vfioGPUConfigured() bool {
	_, devices := os.LookupEnv(types.MicroVMVFIODevicesEnv)
	_, model := os.LookupEnv(types.MicroVMGPUTypeEnv)
	return devices || model
}

// VFIO owns the devices, so their model is provisioned explicitly instead of
// querying the host NVIDIA driver. Capacity comes from validated PCI groups.
func detectVFIOGPUDevices() ([]gpuDevice, error) {
	model := types.NormalizeGPUType(os.Getenv(types.MicroVMGPUTypeEnv))
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
	for _, value := range strings.Split(strings.ToLower(os.Getenv(types.MicroVMVFIODevicesEnv)), ",") {
		device := strings.TrimSpace(value)
		if !agentVFIOPCIAddress.MatchString(device) || seen[device] {
			return nil, fmt.Errorf("invalid or duplicate VFIO PCI address %q", device)
		}
		seen[device] = true
		path := filepath.Join(agentVFIOPCIRoot, device)
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

var queryNvidiaGPUDevices = func() ([]byte, error) {
	return exec.Command("nvidia-smi", "--query-gpu=index,uuid,name,pci.domain,pci.bus_id", "--format=csv,noheader,nounits").Output()
}

func detectNvidiaGPUDevices() []gpuDevice {
	out, err := queryNvidiaGPUDevices()
	if err != nil {
		return nil
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	devices := make([]gpuDevice, 0, len(lines))
	for _, line := range lines {
		fields := strings.SplitN(line, ",", 5)
		if len(fields) != 5 {
			continue
		}
		id := strings.TrimSpace(fields[0])
		uuid := strings.TrimSpace(fields[1])
		name := string(types.NormalizeGPUType(fields[2]))
		busID, ok := nvidiaSMISystemBusID(fields[3], fields[4])
		if id == "" || name == "" {
			continue
		}
		if ok {
			healthy, err := nvidiaProcGPUInfoHealthy(busID)
			if err != nil || !healthy {
				continue
			}
		}
		devices = append(devices, gpuDevice{ID: id, UUID: uuid, Name: name, BusID: busID})
	}
	return devices
}

func nvidiaSMISystemBusID(domain, smiBusID string) (string, bool) {
	domain = strings.TrimSpace(domain)
	if len(domain) >= 2 && strings.EqualFold(domain[:2], "0x") {
		domain = domain[2:]
	}
	value, err := strconv.ParseUint(domain, 16, 16)
	if err != nil {
		return "", false
	}
	domain = fmt.Sprintf("%04x", value)

	parts := strings.Split(strings.TrimSpace(smiBusID), ":")
	if len(parts) != 3 {
		return "", false
	}
	return strings.ToLower(strings.Join([]string{domain, parts[1], parts[2]}, ":")), true
}
