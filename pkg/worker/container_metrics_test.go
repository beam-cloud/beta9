package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	gopsutilnet "github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/process"
	"github.com/stretchr/testify/require"
)

func TestProcessTreeIncludesRootProcess(t *testing.T) {
	root, err := process.NewProcess(int32(os.Getpid()))
	require.NoError(t, err)

	monitor := NewProcessMonitor(os.Getpid(), nil, nil)
	processes := monitor.findProcessTree(root)

	require.NotEmpty(t, processes)
	require.Equal(t, int32(os.Getpid()), processes[0].Pid)
}

func TestCgroupCPUMillicoresUsesIntervalDelta(t *testing.T) {
	start := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	// A container that has been alive for an hour but only used 10 CPU seconds
	// in total has a lifetime average of ~3 millicores; that must not leak
	// into the interval measurement.
	monitor := NewProcessMonitor(0, nil, nil)
	for i, tt := range []struct {
		cpuSeconds float64
		want       float64
	}{
		{cpuSeconds: 10, want: 0},
		{cpuSeconds: 11.5, want: 500},
		{cpuSeconds: 11.5, want: 0},
		{cpuSeconds: 14.5, want: 1000},
	} {
		now := start.Add(time.Duration(i*3) * time.Second)
		require.InDelta(t, tt.want, monitor.intervalMillicores(monitor.cgroupCPUDelta(tt.cpuSeconds), now), 0.01, "sample %d", i)
	}
}

// A child exiting between samples must not zero the interval for the
// processes that survived it, and a child that started since the previous
// sample is charged in full.
func TestProcessTreeCPUDeltaSurvivesChildExit(t *testing.T) {
	monitor := NewProcessMonitor(0, nil, nil)

	// Baseline: nothing is reported for the first sample.
	require.Zero(t, monitor.processTreeCPUDelta([]processCPUSample{{pid: 1, seconds: 10}, {pid: 2, seconds: 6}}))

	// pid 2 exited (its 6s no longer appear), pid 1 used 1.5s more, pid 3 is
	// new and has used 0.5s since it started.
	delta := monitor.processTreeCPUDelta([]processCPUSample{{pid: 1, seconds: 11.5}, {pid: 3, seconds: 0.5}})
	require.InDelta(t, 2.0, delta, 1e-9)

	// Steady state: only the increments count.
	delta = monitor.processTreeCPUDelta([]processCPUSample{{pid: 1, seconds: 11.5}, {pid: 3, seconds: 1.5}})
	require.InDelta(t, 1.0, delta, 1e-9)
}

func TestParseCgroupCPUUsage(t *testing.T) {
	seconds, err := parseCgroupCPUUsage([]byte("usage_usec 2500000\nuser_usec 2000000\nsystem_usec 500000\nnr_periods 0\n"))
	require.NoError(t, err)
	require.InDelta(t, 2.5, seconds, 1e-9)

	_, err = parseCgroupCPUUsage([]byte("user_usec 2000000\n"))
	require.Error(t, err)
}

// The cgroup counter is authoritative when readable; when the file disappears
// the monitor falls back to the per-PID delta for that sample and rebaselines
// the cgroup counter once it is readable again.
func TestFetchCPUPrefersCgroupAndFallsBackToProcessTree(t *testing.T) {
	statPath := filepath.Join(t.TempDir(), "cpu.stat")
	writeUsage := func(usec uint64) {
		require.NoError(t, os.WriteFile(statPath, []byte(fmt.Sprintf("usage_usec %d\n", usec)), 0o644))
	}
	start := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	monitor := NewProcessMonitor(0, nil, nil)
	monitor.cpuStatPath = statPath

	sample := func(i int, usec uint64, tree []processCPUSample) float64 {
		if usec > 0 {
			writeUsage(usec)
		} else {
			_ = os.Remove(statPath)
		}
		return monitor.fetchCPUAt(tree, start.Add(time.Duration(i)*time.Second))
	}

	require.Zero(t, sample(0, 1_000_000, []processCPUSample{{pid: 1, seconds: 0.8}}))
	// cgroup charges 0.5s (includes an exited child); the tree only saw 0.2s.
	require.InDelta(t, 500, sample(1, 1_500_000, []processCPUSample{{pid: 1, seconds: 1.0}}), 0.01)
	// cgroup gone: the per-PID delta covers this interval.
	require.InDelta(t, 300, sample(2, 0, []processCPUSample{{pid: 1, seconds: 1.3}}), 0.01)
	// cgroup back: rebaselined, so the gap is not attributed to this interval.
	require.Zero(t, sample(3, 2_200_000, []processCPUSample{{pid: 1, seconds: 1.6}}))
	require.InDelta(t, 100, sample(4, 2_300_000, []processCPUSample{{pid: 1, seconds: 1.7}}), 0.01)
}

func TestProcessIODeltaDoesNotUnderflow(t *testing.T) {
	delta := processIODelta(
		process.IOCountersStat{
			ReadCount:      8,
			WriteCount:     20,
			ReadBytes:      50,
			WriteBytes:     200,
			DiskReadBytes:  900,
			DiskWriteBytes: 25,
		},
		process.IOCountersStat{
			ReadCount:      10,
			WriteCount:     5,
			ReadBytes:      60,
			WriteBytes:     150,
			DiskReadBytes:  1000,
			DiskWriteBytes: 10,
		},
	)

	require.Equal(t, uint64(0), delta.ReadCount)
	require.Equal(t, uint64(15), delta.WriteCount)
	require.Equal(t, uint64(0), delta.ReadBytes)
	require.Equal(t, uint64(50), delta.WriteBytes)
	require.Equal(t, uint64(0), delta.DiskReadBytes)
	require.Equal(t, uint64(15), delta.DiskWriteBytes)
}

func TestNetworkCountersExcludeLoopback(t *testing.T) {
	total := aggregateNetworkCounters([]gopsutilnet.IOCountersStat{
		{
			Name:        "lo",
			BytesRecv:   1000,
			BytesSent:   1000,
			PacketsRecv: 10,
			PacketsSent: 10,
		},
		{
			Name:        "eth0",
			BytesRecv:   500,
			BytesSent:   700,
			PacketsRecv: 5,
			PacketsSent: 7,
		},
		{
			Name:        "eth1",
			BytesRecv:   30,
			BytesSent:   40,
			PacketsRecv: 3,
			PacketsSent: 4,
		},
	})

	require.Equal(t, uint64(530), total.BytesRecv)
	require.Equal(t, uint64(740), total.BytesSent)
	require.Equal(t, uint64(8), total.PacketsRecv)
	require.Equal(t, uint64(11), total.PacketsSent)
}

func TestNetworkIODeltaDoesNotUnderflow(t *testing.T) {
	delta := networkIODelta(
		gopsutilnet.IOCountersStat{
			BytesRecv:   10,
			BytesSent:   80,
			PacketsRecv: 5,
			PacketsSent: 12,
		},
		gopsutilnet.IOCountersStat{
			BytesRecv:   20,
			BytesSent:   50,
			PacketsRecv: 8,
			PacketsSent: 2,
		},
	)

	require.Equal(t, uint64(0), delta.BytesRecv)
	require.Equal(t, uint64(30), delta.BytesSent)
	require.Equal(t, uint64(0), delta.PacketsRecv)
	require.Equal(t, uint64(10), delta.PacketsSent)
}
