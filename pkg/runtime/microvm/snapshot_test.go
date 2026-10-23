package microvm

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// This is the queue and sparse RAM layout captured from the failed burst.
func TestSnapshotBlockQueueIntegrity(t *testing.T) {
	dir := t.TempDir()
	vhost, gpa := true, uint64(0)
	avail, used := uint64(73537536), uint64(73537856)
	const memorySize = int64(1476395008)
	section := func(value any) *snapshotSection {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		return &snapshotSection{SnapshotData: snapshotData{State: string(data)}}
	}
	config := snapshotConfig{Disks: []snapshotDisk{{ID: "_disk0", VhostUser: &vhost, NumQueues: 1, QueueSize: 128}}}
	state := snapshotSection{Snapshots: map[string]*snapshotSection{
		"memory-manager": section(snapshotMemory{MemoryRanges: snapshotMemoryRanges{
			Data: []snapshotMemoryRange{{GPA: &gpa, Length: uint64(memorySize)}},
		}}),
		"device-manager": {Snapshots: map[string]*snapshotSection{
			"_virtio-pci-_disk0": section(snapshotTransport{Queues: []snapshotQueue{
				{Ready: true, Size: 128, Avail: &avail, Used: &used},
			}}),
		}},
	}}
	for name, value := range map[string]any{"config.json": config, "state.json": state} {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0600))
	}
	file, err := os.Create(filepath.Join(dir, "memory-ranges"))
	require.NoError(t, err)
	defer file.Close()
	for _, test := range []struct {
		name            string
		available, used uint16
		size            int64
		wantError       string
	}{
		{"observed_pending", 733, 720, memorySize, "13 outstanding requests"},
		{"drained", 733, 733, memorySize, ""},
		{"wrapped_pending", 3, 65526, memorySize, "13 outstanding requests"},
		{"truncated_ram", 733, 733, memorySize - 1, "length"},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, file.Truncate(test.size))
			var index [2]byte
			for address, value := range map[uint64]uint16{avail: test.available, used: test.used} {
				binary.LittleEndian.PutUint16(index[:], value)
				_, err := file.WriteAt(index[:], int64(address+2))
				require.NoError(t, err)
			}
			err := ValidateSnapshotBlockQueues(dir)
			if test.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantError)
			}
		})
	}
}

func TestSnapshotMemoryUsesMetadataOffsets(t *testing.T) {
	for _, first := range []uint64{8192, math.MaxUint64 - 4095} {
		second := uint64(4096)
		memory := snapshotMemory{MemoryRanges: snapshotMemoryRanges{Data: []snapshotMemoryRange{
			{GPA: &first, Length: 2048}, {GPA: &second, Length: 2048},
		}}}
		file, err := os.CreateTemp(t.TempDir(), "ram")
		require.NoError(t, err)
		t.Cleanup(func() { file.Close() })
		require.NoError(t, file.Truncate(4096))
		_, err = file.WriteAt([]byte{0x34, 0x12}, 8)
		require.NoError(t, err)
		_, err = file.WriteAt([]byte{0x78, 0x56}, 2062)
		require.NoError(t, err)
		require.NoError(t, memory.bind(file))
		for address, want := range map[uint64]uint16{first + 6: 0x1234, second + 12: 0x5678} {
			got, err := memory.readIndex(address)
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
		_, err = memory.readIndex(100)
		require.Error(t, err)
	}
}
