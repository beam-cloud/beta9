package worker

import (
	"errors"
	"fmt"
	"time"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/process"
)

func (w *Worker) collectAndSendContainerMetrics(request *types.ContainerRequest, spec *specs.Spec, pidChan <-chan int, done chan bool) {
	containerPid := <-pidChan
	if containerPid == 0 {
		return
	}

	ticker := time.NewTicker(w.config.Monitoring.ContainerMetricsInterval)
	defer ticker.Stop()

	monitor := NewProcessMonitor(containerPid, spec.Linux.Resources.Devices)

	for {
		select {
		case <-done:
			return

		case <-ticker.C:
			stats, err := monitor.GetStatistics()
			if err != nil {
				return
			}

			w.eventRepo.PushContainerResourceMetricsEvent(
				w.workerId,
				request,
				types.EventContainerMetricsData{
					CPUUsed:            stats.CPU,
					CPUTotal:           uint64(request.Cpu),
					CPUPercent:         float32((float64(stats.CPU) * 100 / float64(request.Cpu))),
					MemoryRSS:          stats.Memory.RSS,
					MemoryVMS:          stats.Memory.VMS,
					MemorySwap:         stats.Memory.Swap,
					MemoryTotal:        uint64(request.Memory * 1024 * 1024),
					DiskReadBytes:      stats.IO.ReadBytes,
					DiskWriteBytes:     stats.IO.WriteBytes,
					NetworkBytesRecv:   stats.NetIO.BytesRecv,
					NetworkBytesSent:   stats.NetIO.BytesSent,
					NetworkPacketsRecv: stats.NetIO.PacketsRecv,
					NetworkPacketsSent: stats.NetIO.PacketsSent,
					GPUMemoryUsed:      stats.GPU.MemoryUsed,
					GPUMemoryTotal:     stats.GPU.MemoryTotal,
					GPUType:            request.Gpu,
				},
			)
		}
	}
}

type ProcessStats struct {
	CPU    uint64 // in millicores
	Memory process.MemoryInfoStat
	IO     process.IOCountersStat
	NetIO  net.IOCountersStat
	GPU    GPUInfoStat
}

type GPUInfoStat struct {
	MemoryUsed  uint64
	MemoryTotal uint64
}

type ProcessMonitor struct {
	pid       int32
	devices   []specs.LinuxDeviceCgroup
	lastIO    process.IOCountersStat
	lastNetIO net.IOCountersStat
	nvidiaSmi NvidiaSMIClientInterface
}

func NewProcessMonitor(pid int, devices []specs.LinuxDeviceCgroup) *ProcessMonitor {
	return &ProcessMonitor{pid: int32(pid), devices: devices, nvidiaSmi: NewNvidiaSMIClient()}
}

func (m *ProcessMonitor) GetStatistics() (*ProcessStats, error) {
	processes, err := m.findProcesses()
	if err != nil {
		return nil, err
	}

	gpuStat := m.fetchGPUMemory()

	netIOStat, err := m.fetchNetworkIO()
	if err != nil {
		return nil, err
	}

	ioStat, err := m.fetchIO(processes)
	if err != nil {
		return nil, err
	}

	millicores := m.fetchCPU(processes)
	memoryStat := m.fetchMemory(processes)

	return &ProcessStats{
		CPU:    uint64(millicores),
		Memory: *memoryStat,
		IO:     *ioStat,
		NetIO:  *netIOStat,
		GPU:    *gpuStat,
	}, nil
}

func (m *ProcessMonitor) fetchGPUMemory() *GPUInfoStat {
	stat := &GPUInfoStat{}
	for _, device := range m.devices {
		if device.Minor == nil || !device.Allow {
			continue
		}
		stats, err := m.nvidiaSmi.GetGpuMemoryUsage(int(*device.Minor))
		if err == nil {
			stat.MemoryUsed += uint64(stats.UsedCapacity)
			stat.MemoryTotal += uint64(stats.TotalCapacity)
		}
	}
	return stat
}

// fetchNetworkIO gets network IO for all NICs globally, regardless of process.
// Avoid using this data for external purposes.
// TODO: Look into per-process network IO
func (m *ProcessMonitor) fetchNetworkIO() (*net.IOCountersStat, error) {
	// net.IOCountersByFile() does exist, but the proc files for all PIDs have the same
	// data as in /proc/net/dev which is where net.IOCounters() gets its data from on linux.
	counters, err := net.IOCounters(false)
	if err != nil {
		return nil, err
	}

	if len(counters) != 1 {
		return nil, errors.New("failed to get network io counter")
	}
	currentNetIO := counters[0]

	deltaIO := net.IOCountersStat{
		BytesSent:   currentNetIO.BytesSent - m.lastNetIO.BytesSent,
		BytesRecv:   currentNetIO.BytesRecv - m.lastNetIO.BytesRecv,
		PacketsSent: currentNetIO.PacketsSent - m.lastNetIO.PacketsSent,
		PacketsRecv: currentNetIO.PacketsRecv - m.lastNetIO.PacketsRecv,
	}

	m.lastNetIO = currentNetIO

	return &deltaIO, nil
}

func (m *ProcessMonitor) fetchIO(proceses []*process.Process) (*process.IOCountersStat, error) {
	var currentIO = process.IOCountersStat{}
	for _, p := range proceses {
		pio, err := p.IOCounters()
		if err != nil {
			continue
		}
		currentIO.ReadCount += pio.ReadCount
		currentIO.WriteCount += pio.WriteCount
		currentIO.ReadBytes += pio.ReadBytes
		currentIO.WriteBytes += pio.WriteBytes
		currentIO.DiskReadBytes += pio.DiskReadBytes
		currentIO.DiskWriteBytes += pio.DiskWriteBytes
	}

	deltaIO := process.IOCountersStat{
		ReadCount:      currentIO.ReadCount - m.lastIO.ReadCount,
		WriteCount:     currentIO.WriteCount - m.lastIO.WriteCount,
		ReadBytes:      currentIO.ReadBytes - m.lastIO.ReadBytes,
		WriteBytes:     currentIO.WriteBytes - m.lastIO.WriteBytes,
		DiskReadBytes:  currentIO.DiskReadBytes - m.lastIO.DiskReadBytes,
		DiskWriteBytes: currentIO.DiskWriteBytes - m.lastIO.DiskWriteBytes,
	}

	m.lastIO = currentIO

	return &deltaIO, nil
}

func (m *ProcessMonitor) fetchCPU(processes []*process.Process) float64 {
	millicores := 0.0
	for _, p := range processes {
		cpuPercent, err := p.CPUPercent()
		if err != nil {
			return 0
		}
		millicores += (cpuPercent / 100.0) * 1000.0
	}
	return millicores
}

func (m *ProcessMonitor) fetchMemory(processes []*process.Process) *process.MemoryInfoStat {
	currentMemory := process.MemoryInfoStat{}
	for _, p := range processes {
		memory, err := p.MemoryInfo()
		if err != nil {
			continue
		}

		currentMemory.RSS += memory.RSS
		currentMemory.VMS += memory.VMS
		currentMemory.Swap += memory.Swap
	}

	return &currentMemory
}

func (m *ProcessMonitor) findProcesses() ([]*process.Process, error) {
	processes, err := process.Processes()
	if err != nil {
		return nil, err
	}

	for _, p := range processes {
		if p.Pid == m.pid {
			return m.findChildProcesses(p), nil
		}
	}

	return nil, fmt.Errorf("failed to find processes for pid %v", m.pid)
}

func (m *ProcessMonitor) findChildProcesses(p *process.Process) []*process.Process {
	children, err := p.Children()
	if err != nil {
		// An error will occur when there are no children (pgrep -P <pid>)
		return nil
	}

	processes := []*process.Process{}
	processes = append(processes, children...)

	for _, child := range children {
		grandChildren := m.findChildProcesses(child)
		if grandChildren == nil {
			continue
		}
		processes = append(processes, grandChildren...)
	}

	return processes
}
