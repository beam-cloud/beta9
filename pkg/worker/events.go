package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	runtime "github.com/beam-cloud/beta9/pkg/runtime"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	gopsutilnet "github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/process"
)

const (
	cgroupV2Root      = "/sys/fs/cgroup"
	cgroupCPUStatFile = "cpu.stat"
)

func (w *Worker) collectAndSendContainerMetrics(ctx context.Context, request *types.ContainerRequest, spec *specs.Spec, containerPid int) {
	ticker := time.NewTicker(w.config.Monitoring.ContainerMetricsInterval)
	defer ticker.Stop()

	monitor := NewProcessMonitor(containerPid, spec.Linux.Resources.Devices, w.gpuManagerForRequest(request).GetContainerGPUDevices(request.ContainerId))
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
	pid          int32
	devices      []specs.LinuxDeviceCgroup
	lastIOByPID  map[int32]process.IOCountersStat
	lastNetIO    gopsutilnet.IOCountersStat
	hasLastNetIO bool
	// cpuStatPath is the container cgroup's cpu.stat, "" when unavailable.
	cpuStatPath      string
	lastCgroupCPU    float64
	hasLastCgroupCPU bool
	lastCPUByPID     map[int32]float64
	hasCPUBaseline   bool
	lastCPUSampledAt time.Time
	gpuInfoClient    GPUInfoClient
	gpuDeviceIds     []int
}

func NewProcessMonitor(pid int, devices []specs.LinuxDeviceCgroup, gpuDeviceIds []int) *ProcessMonitor {
	return &ProcessMonitor{
		pid:           int32(pid),
		devices:       devices,
		lastIOByPID:   map[int32]process.IOCountersStat{},
		cpuStatPath:   cgroupCPUStatPath(pid),
		lastCPUByPID:  map[int32]float64{},
		gpuInfoClient: &NvidiaInfoClient{},
		gpuDeviceIds:  gpuDeviceIds,
	}
}

func (m *ProcessMonitor) Prime() {
	if processes, err := m.findProcesses(); err == nil {
		_, _ = m.fetchIO(processes)
		_ = m.fetchCPU(processes)
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

// fetchCPU returns the millicores the container consumed since the previous
// sample, so bursts and idle periods show up instead of being smoothed into a
// lifetime average.
//
// The container cgroup's cpu.stat is the authoritative counter: it is
// monotonic and still charges processes that exited between two samples.
// Without it (cgroup v1, or a cgroup the worker cannot see) CPU time is
// tracked per PID, so a child exiting only loses what it burned after the
// previous sample rather than zeroing the whole interval for the survivors.
func (m *ProcessMonitor) fetchCPU(processes []*process.Process) float64 {
	return m.fetchCPUAt(processCPUSamples(processes), time.Now())
}

func (m *ProcessMonitor) fetchCPUAt(samples []processCPUSample, now time.Time) float64 {
	// The per-PID baseline is kept even while the cgroup counter is in use so
	// a cgroup that becomes unreadable (it is removed as the container exits)
	// falls back without a gap.
	treeDelta := m.processTreeCPUDelta(samples)
	if seconds, ok := m.readCgroupCPUSeconds(); ok {
		return m.intervalMillicores(m.cgroupCPUDelta(seconds), now)
	}
	return m.intervalMillicores(treeDelta, now)
}

// processCPUSample is one process's cumulative CPU time at a sample.
type processCPUSample struct {
	pid     int32
	seconds float64
}

func processCPUSamples(processes []*process.Process) []processCPUSample {
	samples := make([]processCPUSample, 0, len(processes))
	for _, p := range processes {
		times, err := p.Times()
		if err != nil {
			continue
		}
		samples = append(samples, processCPUSample{pid: p.Pid, seconds: times.User + times.System})
	}
	return samples
}

// processTreeCPUDelta returns the CPU seconds the tree consumed since the
// previous call: survivors contribute their increase, processes first seen
// now contribute their whole lifetime (they started since the last sample),
// and processes that exited contribute nothing further. The first call only
// records the baseline.
func (m *ProcessMonitor) processTreeCPUDelta(samples []processCPUSample) float64 {
	current := make(map[int32]float64, len(samples))
	delta := 0.0
	for _, sample := range samples {
		current[sample.pid] = sample.seconds
		if !m.hasCPUBaseline {
			continue
		}
		last, seen := m.lastCPUByPID[sample.pid]
		switch {
		case !seen:
			delta += sample.seconds
		case sample.seconds > last:
			delta += sample.seconds - last
		}
	}
	m.lastCPUByPID = current
	m.hasCPUBaseline = true
	return delta
}

// cgroupCPUDelta returns the increase in the cgroup's cumulative CPU time since
// the previous reading. The first reading only records the baseline.
func (m *ProcessMonitor) cgroupCPUDelta(seconds float64) float64 {
	previous, had := m.lastCgroupCPU, m.hasLastCgroupCPU
	m.lastCgroupCPU, m.hasLastCgroupCPU = seconds, true
	if !had || seconds < previous {
		return 0
	}
	return seconds - previous
}

// intervalMillicores converts CPU seconds consumed since the previous sample
// into millicores over that interval. The first call only records the sample
// time and reports 0.
func (m *ProcessMonitor) intervalMillicores(cpuSeconds float64, now time.Time) float64 {
	previousAt := m.lastCPUSampledAt
	m.lastCPUSampledAt = now

	elapsed := now.Sub(previousAt).Seconds()
	if previousAt.IsZero() || elapsed <= 0 || cpuSeconds <= 0 {
		return 0
	}
	return cpuSeconds / elapsed * 1000
}

// readCgroupCPUSeconds reads the container cgroup's cumulative CPU time. A
// failed read drops the cgroup baseline so the next successful read starts
// over instead of spanning the gap.
func (m *ProcessMonitor) readCgroupCPUSeconds() (float64, bool) {
	if m.cpuStatPath == "" {
		return 0, false
	}
	data, err := os.ReadFile(m.cpuStatPath)
	if err != nil {
		m.hasLastCgroupCPU = false
		return 0, false
	}
	seconds, err := parseCgroupCPUUsage(data)
	if err != nil {
		m.hasLastCgroupCPU = false
		return 0, false
	}
	return seconds, true
}

// parseCgroupCPUUsage extracts usage_usec from a cgroup v2 cpu.stat file as
// seconds.
func parseCgroupCPUUsage(data []byte) (float64, error) {
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != "usage_usec" {
			continue
		}
		usec, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse cpu.stat usage_usec %q: %w", fields[1], err)
		}
		return float64(usec) / 1e6, nil
	}
	return 0, errors.New("cpu.stat has no usage_usec")
}

// cgroupCPUStatPath resolves the cpu.stat file of the cgroup pid runs in, the
// same cgroup the OOM watcher reads memory.events from. It is "" when the
// cgroup cannot be resolved or is not a cgroup v2 hierarchy the worker can
// read.
func cgroupCPUStatPath(pid int) string {
	if pid <= 0 {
		return ""
	}
	cgroupPath, err := runtime.GetCgroupPathFromPID(pid)
	if err != nil {
		return ""
	}
	path := filepath.Join(cgroupV2Root, cgroupPath, cgroupCPUStatFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	if _, err := parseCgroupCPUUsage(data); err != nil {
		return ""
	}
	return path
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
