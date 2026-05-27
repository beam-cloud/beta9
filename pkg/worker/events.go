package worker

import (
	"context"
	"fmt"
	"time"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	gopsutilnet "github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/process"
)

func (w *Worker) collectAndSendContainerMetrics(ctx context.Context, request *types.ContainerRequest, spec *specs.Spec, containerPid int) {
	ticker := time.NewTicker(w.config.Monitoring.ContainerMetricsInterval)
	defer ticker.Stop()

	monitor := NewProcessMonitor(containerPid, spec.Linux.Resources.Devices, w.containerGPUManager.GetContainerGPUDevices(request.ContainerId))
	monitor.Prime()
	lastCollectedAt := time.Now()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			now := time.Now()
			sampleInterval := now.Sub(lastCollectedAt)
			lastCollectedAt = now

			stats, err := monitor.GetStatistics()
			if err != nil {
				return
			}

			w.eventRepo.PushContainerResourceMetricsEvent(
				w.workerId,
				request,
				types.EventContainerMetricsData{
					SampleIntervalMs:   sampleInterval.Milliseconds(),
					CPUUsed:            stats.CPU,
					CPUTotal:           uint64(request.Cpu),
					CPUPercent:         cpuPercent(stats.CPU, request.Cpu),
					MemoryRSS:          stats.Memory.RSS,
					MemoryVMS:          stats.Memory.VMS,
					MemorySwap:         stats.Memory.Swap,
					MemoryTotal:        uint64(request.Memory * 1024 * 1024),
					DiskReadBytes:      stats.IO.DiskReadBytes,
					DiskWriteBytes:     stats.IO.DiskWriteBytes,
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
	NetIO  gopsutilnet.IOCountersStat
	GPU    GPUInfoStat
}

type GPUInfoStat struct {
	MemoryUsed  uint64
	MemoryTotal uint64
}

type ProcessMonitor struct {
	pid           int32
	devices       []specs.LinuxDeviceCgroup
	lastIOByPID   map[int32]process.IOCountersStat
	lastNetIO     gopsutilnet.IOCountersStat
	hasLastNetIO  bool
	gpuInfoClient GPUInfoClient
	gpuDeviceIds  []int
}

func NewProcessMonitor(pid int, devices []specs.LinuxDeviceCgroup, gpuDeviceIds []int) *ProcessMonitor {
	return &ProcessMonitor{
		pid:           int32(pid),
		devices:       devices,
		lastIOByPID:   map[int32]process.IOCountersStat{},
		gpuInfoClient: &NvidiaInfoClient{},
		gpuDeviceIds:  gpuDeviceIds,
	}
}

func (m *ProcessMonitor) Prime() {
	if processes, err := m.findProcesses(); err == nil {
		_, _ = m.fetchIO(processes)
	}
	_, _ = m.fetchNetworkIO()
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

	for _, device := range m.gpuDeviceIds {
		stats, err := m.gpuInfoClient.GetGPUMemoryUsage(device)
		if err == nil {
			stat.MemoryUsed += uint64(stats.UsedCapacity)
			stat.MemoryTotal += uint64(stats.TotalCapacity)
		}
	}
	return stat
}

func (m *ProcessMonitor) fetchNetworkIO() (*gopsutilnet.IOCountersStat, error) {
	currentNetIO, err := networkCountersForPID(m.pid)
	if err != nil {
		return nil, err
	}

	if !m.hasLastNetIO {
		m.lastNetIO = currentNetIO
		m.hasLastNetIO = true
		return &gopsutilnet.IOCountersStat{}, nil
	}

	deltaIO := networkIODelta(currentNetIO, m.lastNetIO)

	m.lastNetIO = currentNetIO

	return &deltaIO, nil
}

func networkCountersForPID(pid int32) (gopsutilnet.IOCountersStat, error) {
	counters, err := gopsutilnet.IOCountersByFile(true, fmt.Sprintf("/proc/%d/net/dev", pid))
	if err != nil {
		return gopsutilnet.IOCountersStat{}, err
	}
	return aggregateNetworkCounters(counters), nil
}

func aggregateNetworkCounters(counters []gopsutilnet.IOCountersStat) gopsutilnet.IOCountersStat {
	total := gopsutilnet.IOCountersStat{}
	for _, counter := range counters {
		if counter.Name == "lo" {
			continue
		}
		total.BytesSent += counter.BytesSent
		total.BytesRecv += counter.BytesRecv
		total.PacketsSent += counter.PacketsSent
		total.PacketsRecv += counter.PacketsRecv
		total.Errin += counter.Errin
		total.Errout += counter.Errout
		total.Dropin += counter.Dropin
		total.Dropout += counter.Dropout
		total.Fifoin += counter.Fifoin
		total.Fifoout += counter.Fifoout
	}
	return total
}

func (m *ProcessMonitor) fetchIO(processes []*process.Process) (*process.IOCountersStat, error) {
	deltaIO := process.IOCountersStat{}
	currentPIDs := map[int32]struct{}{}
	for _, p := range processes {
		pio, err := p.IOCounters()
		if err != nil {
			continue
		}
		currentPIDs[p.Pid] = struct{}{}
		if last, ok := m.lastIOByPID[p.Pid]; ok {
			addProcessIOCounters(&deltaIO, processIODelta(*pio, last))
		}
		m.lastIOByPID[p.Pid] = *pio
	}

	for pid := range m.lastIOByPID {
		if _, ok := currentPIDs[pid]; !ok {
			delete(m.lastIOByPID, pid)
		}
	}

	return &deltaIO, nil
}

func (m *ProcessMonitor) fetchCPU(processes []*process.Process) float64 {
	millicores := 0.0
	for _, p := range processes {
		cpuPercent, err := p.CPUPercent()
		if err != nil {
			continue
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
			return m.findProcessTree(p), nil
		}
	}

	return nil, fmt.Errorf("failed to find processes for pid %v", m.pid)
}

func (m *ProcessMonitor) findProcessTree(p *process.Process) []*process.Process {
	processes := []*process.Process{p}

	children, err := p.Children()
	if err != nil {
		// An error will occur when there are no children (pgrep -P <pid>)
		return processes
	}

	for _, child := range children {
		childProcesses := m.findProcessTree(child)
		if childProcesses == nil {
			continue
		}
		processes = append(processes, childProcesses...)
	}

	return processes
}

func cpuPercent(cpuUsedMillicores uint64, cpuTotalMillicores int64) float32 {
	if cpuTotalMillicores <= 0 {
		return 0
	}
	return float32(float64(cpuUsedMillicores) * 100 / float64(cpuTotalMillicores))
}

func addProcessIOCounters(total *process.IOCountersStat, delta process.IOCountersStat) {
	total.ReadCount += delta.ReadCount
	total.WriteCount += delta.WriteCount
	total.ReadBytes += delta.ReadBytes
	total.WriteBytes += delta.WriteBytes
	total.DiskReadBytes += delta.DiskReadBytes
	total.DiskWriteBytes += delta.DiskWriteBytes
}

func processIODelta(current process.IOCountersStat, previous process.IOCountersStat) process.IOCountersStat {
	return process.IOCountersStat{
		ReadCount:      counterDelta(current.ReadCount, previous.ReadCount),
		WriteCount:     counterDelta(current.WriteCount, previous.WriteCount),
		ReadBytes:      counterDelta(current.ReadBytes, previous.ReadBytes),
		WriteBytes:     counterDelta(current.WriteBytes, previous.WriteBytes),
		DiskReadBytes:  counterDelta(current.DiskReadBytes, previous.DiskReadBytes),
		DiskWriteBytes: counterDelta(current.DiskWriteBytes, previous.DiskWriteBytes),
	}
}

func networkIODelta(current gopsutilnet.IOCountersStat, previous gopsutilnet.IOCountersStat) gopsutilnet.IOCountersStat {
	return gopsutilnet.IOCountersStat{
		BytesSent:   counterDelta(current.BytesSent, previous.BytesSent),
		BytesRecv:   counterDelta(current.BytesRecv, previous.BytesRecv),
		PacketsSent: counterDelta(current.PacketsSent, previous.PacketsSent),
		PacketsRecv: counterDelta(current.PacketsRecv, previous.PacketsRecv),
		Errin:       counterDelta(current.Errin, previous.Errin),
		Errout:      counterDelta(current.Errout, previous.Errout),
		Dropin:      counterDelta(current.Dropin, previous.Dropin),
		Dropout:     counterDelta(current.Dropout, previous.Dropout),
		Fifoin:      counterDelta(current.Fifoin, previous.Fifoin),
		Fifoout:     counterDelta(current.Fifoout, previous.Fifoout),
	}
}

func counterDelta(current uint64, previous uint64) uint64 {
	if current < previous {
		return 0
	}
	return current - previous
}
