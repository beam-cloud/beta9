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

// The memory manager state of a 16 MiB guest whose RAM is a linked file, as
// the hypervisor writes it: no saved ranges, and RAM on both sides of the
// 32-bit hole.
const linkedRAMState = `{"memory_ranges":{"data":[]},"guest_ram_mappings":[` +
	`{"slot":1,"gpa":4294967296,"size":12582912,"zone_id":"ram","virtio_mem":false,"file_offset":4194304},` +
	`{"slot":0,"gpa":0,"size":4194304,"zone_id":"ram","virtio_mem":false,"file_offset":0}],` +
	`"arch_mem_regions":[{"base":4294967296,"size":18446744073709551615,"r_type":"Ram"}],"boot_ram":16777216}`

func writeSnapshotState(t *testing.T, dir, memoryManager string, queues ...snapshotQueue) {
	t.Helper()
	encode := func(value any) json.RawMessage {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		return data
	}
	section := func(state string, sections map[string]json.RawMessage) json.RawMessage {
		return encode(map[string]any{"snapshots": sections, "snapshot_data": map[string]string{"state": state}})
	}
	state := section(`{"clock":"<&>"}`, map[string]json.RawMessage{
		"memory-manager": section(memoryManager, nil),
		"device-manager": section("{}", map[string]json.RawMessage{
			"_virtio-pci-_disk0": section(string(encode(snapshotTransport{Queues: queues})), nil),
		}),
	})
	require.NoError(t, os.WriteFile(filepath.Join(dir, "state.json"), state, 0o600))
	vhost := true
	config := encode(snapshotConfig{Disks: []snapshotDisk{{ID: "_disk0", VhostUser: &vhost, NumQueues: 1, QueueSize: 128}}})
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.json"), config, 0o600))
}

func TestListSnapshotRAM(t *testing.T) {
	const size, page = 16 << 20, 2 << 20
	dir := t.TempDir()
	// A block queue above the hole: its rings are 5 MiB into the RAM file.
	avail, used := uint64(4294967296+1<<20), uint64(4294967296+1<<20+4096)
	writeSnapshotState(t, dir, linkedRAMState, snapshotQueue{Ready: true, Size: 128, Avail: &avail, Used: &used})
	// The image keeps a run on both sides of the hole and the seventh page:
	// the rings are 3 MiB into it.
	kept := []RAMRun{{Offset: page, Length: 2 * page}, {Offset: 6 * page, Length: page}}
	image, err := os.Create(filepath.Join(dir, SnapshotMemoryFile))
	require.NoError(t, err)
	defer image.Close()
	require.NoError(t, image.Truncate(3*page))
	for offset, index := range map[int64]uint16{3 << 20: 733, 3<<20 + 4096: 720} {
		var value [2]byte
		binary.LittleEndian.PutUint16(value[:], index)
		_, err := image.WriteAt(value[:], offset+2)
		require.NoError(t, err)
	}
	require.ErrorContains(t, ValidateSnapshotBlockQueues(dir), "no saved memory ranges")

	before, err := os.ReadFile(filepath.Join(dir, "state.json"))
	require.NoError(t, err)
	require.ErrorContains(t, ListSnapshotRAM(dir, size-4096, kept), "its RAM file has")
	for name, runs := range map[string][]RAMRun{
		"out of order": {kept[1], kept[0]},
		"overlapping":  {kept[0], {Offset: 2 * page, Length: page}},
		"past the end": {{Offset: 7 * page, Length: 2 * page}},
		"empty":        {{Offset: page}},
		"before it":    {{Offset: -page, Length: page}},
	} {
		require.ErrorContains(t, ListSnapshotRAM(dir, size, runs), "not in order within its file", name)
	}
	after, err := os.ReadFile(filepath.Join(dir, "state.json"))
	require.NoError(t, err)
	require.Equal(t, string(before), string(after))

	require.NoError(t, ListSnapshotRAM(dir, size, kept))
	require.ErrorContains(t, ValidateSnapshotBlockQueues(dir), "13 outstanding requests", "guest addresses are found where the image holds them")
	require.ErrorContains(t, ListSnapshotRAM(dir, size, kept), "already lists 3 saved memory ranges")
	_, err = os.Stat(filepath.Join(dir, "state.json.listed"))
	require.ErrorIs(t, err, os.ErrNotExist)

	// The hypervisor reads the list back as it would its own, and every
	// other value as it wrote it.
	var state snapshotSection
	require.NoError(t, readSnapshotJSON(dir, "state.json", &state))
	require.Equal(t, `{"clock":"<&>"}`, state.SnapshotData.State)
	var fields map[string]json.RawMessage
	require.NoError(t, state.decode("memory-manager", &fields))
	require.JSONEq(t, `{"data":[{"gpa":2097152,"length":2097152},{"gpa":4294967296,"length":2097152},{"gpa":4303355904,"length":2097152}]}`, string(fields["memory_ranges"]))
	require.Equal(t, `[{"base":4294967296,"size":18446744073709551615,"r_type":"Ram"}]`, string(fields["arch_mem_regions"]))
	var written map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(linkedRAMState), &written))
	require.Equal(t, string(written["guest_ram_mappings"]), string(fields["guest_ram_mappings"]))
	require.NoError(t, state.Snapshots["device-manager"].decode("_virtio-pci-_disk0", &snapshotTransport{}))
}

func TestSnapshotRAMThatIsNotKeptHeldZeroes(t *testing.T) {
	const size, page = 16 << 20, 2 << 20
	for name, test := range map[string]struct {
		avail, used uint64
		want        string
	}{
		"rings in a page without data": {4294967296 + 5<<20, 4294967296 + 5<<20 + 4096, ""},
		"one ring in a kept page":      {4294967296 + 5<<20, page + 4096, "outstanding requests (available=0 used=720)"},
		"rings outside guest RAM":      {size, size + 4096, "outside guest memory"},
		"rings at the end of the RAM":  {4294967296 + 12<<20 - 2, 4096, "outside guest memory"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeSnapshotState(t, dir, linkedRAMState, snapshotQueue{Ready: true, Size: 128, Avail: &test.avail, Used: &test.used})
			image := make([]byte, page)
			binary.LittleEndian.PutUint16(image[4096+2:], 720)
			require.NoError(t, os.WriteFile(filepath.Join(dir, SnapshotMemoryFile), image, 0o600))
			require.NoError(t, ListSnapshotRAM(dir, size, []RAMRun{{Offset: page, Length: page}}))
			err := ValidateSnapshotBlockQueues(dir)
			if test.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.want)
			}
		})
	}
}

func TestListSnapshotRAMRejectsOtherLayouts(t *testing.T) {
	for name, test := range map[string]struct{ state, want string }{
		"hypervisor saved it": {`{"memory_ranges":{"data":[{"gpa":0,"length":4096}]},"guest_ram_mappings":[]}`, "already lists 1 saved memory ranges"},
		"gap in the file":     {`{"memory_ranges":{"data":[]},"guest_ram_mappings":[{"gpa":0,"size":4096,"file_offset":4096}]}`, "not one file in order"},
		"overlap in the file": {`{"memory_ranges":{"data":[]},"guest_ram_mappings":[{"gpa":0,"size":4096,"file_offset":0},{"gpa":8192,"size":4096,"file_offset":0}]}`, "not one file in order"},
		"hotplugged memory":   {`{"memory_ranges":{"data":[]},"guest_ram_mappings":[{"gpa":0,"size":4096,"file_offset":0,"virtio_mem":true}]}`, "not one file in order"},
		"no RAM":              {`{"memory_ranges":{"data":[]},"guest_ram_mappings":[]}`, "has 0 bytes of guest RAM"},
		"no mappings":         {`{"memory_ranges":{"data":[]}}`, "unexpected end of JSON input"},
		"not a state":         {`[]`, "cannot unmarshal"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeSnapshotState(t, dir, test.state)
			before, err := os.ReadFile(filepath.Join(dir, "state.json"))
			require.NoError(t, err)
			require.ErrorContains(t, ListSnapshotRAM(dir, 4096, []RAMRun{{Length: 4096}}), test.want)
			after, err := os.ReadFile(filepath.Join(dir, "state.json"))
			require.NoError(t, err)
			require.Equal(t, string(before), string(after), "a snapshot that cannot be completed is left as the hypervisor wrote it")
		})
	}
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "state.json"), []byte(`{"snapshots":{}}`), 0o600))
	require.ErrorContains(t, ListSnapshotRAM(dir, 4096, nil), `no "memory-manager"`)
	require.ErrorIs(t, ListSnapshotRAM(t.TempDir(), 4096, nil), os.ErrNotExist)
}
