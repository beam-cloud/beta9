package worker

import (
	"os"
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

func TestCPUMillicoresUsesIntervalDelta(t *testing.T) {
	start := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	// A process that has been alive for an hour but only used 10 CPU seconds
	// in total has a lifetime average of ~3 millicores; that must not leak
	// into the interval measurement.
	tests := []struct {
		name         string
		cpuSeconds   []float64
		want         []float64
		intervalSecs float64
	}{
		{
			name:         "half a core over three seconds",
			cpuSeconds:   []float64{10, 11.5, 11.5, 14.5},
			intervalSecs: 3,
			want:         []float64{0, 500, 0, 1000},
		},
		{
			name:         "child exit drops cumulative time",
			cpuSeconds:   []float64{10, 4, 4.3},
			intervalSecs: 3,
			want:         []float64{0, 0, 100},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitor := NewProcessMonitor(0, nil, nil)
			for i, cpuSeconds := range tt.cpuSeconds {
				now := start.Add(time.Duration(float64(i)*tt.intervalSecs) * time.Second)
				require.InDelta(t, tt.want[i], monitor.cpuMillicores(cpuSeconds, now), 0.01, "sample %d", i)
			}
		})
	}
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
