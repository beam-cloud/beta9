package agent

import (
	"fmt"
	"math"
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

type gpuDevice struct {
	ID   string
	UUID string
	Name string
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
		for _, id := range gpuIDs {
			device, ok := findGPUDevice(devices, id)
			if !ok {
				return nil, true, []check{{
					Name:     "capacity.gpu_ids",
					Ok:       false,
					Message:  fmt.Sprintf("GPU id %q was not detected", id),
					Severity: severity(false),
				}}
			}
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

func capacityCheckMessage(ok bool, success string, err error) string {
	if ok {
		return success
	}
	if err != nil {
		return err.Error()
	}
	return "requested capacity exceeds detected machine capacity"
}

func detectNvidiaGPUDevices() []gpuDevice {
	cmd := exec.Command("nvidia-smi", "--query-gpu=index,uuid,name", "--format=csv,noheader")
	out, err := cmd.Output()
	if err != nil {
		return nil
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	devices := make([]gpuDevice, 0, len(lines))
	for _, line := range lines {
		fields := strings.SplitN(line, ",", 3)
		if len(fields) != 3 {
			continue
		}
		id := strings.TrimSpace(fields[0])
		uuid := strings.TrimSpace(fields[1])
		name := string(types.NormalizeGPUType(fields[2]))
		if id == "" || name == "" {
			continue
		}
		devices = append(devices, gpuDevice{ID: id, UUID: uuid, Name: name})
	}
	return devices
}
