package microvm

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
)

type snapshotDisk struct {
	ID        string `json:"id"`
	VhostUser *bool  `json:"vhost_user"`
	NumQueues uint64 `json:"num_queues"`
	QueueSize uint64 `json:"queue_size"`
}

type snapshotConfig struct {
	Disks []snapshotDisk `json:"disks"`
}

func (c snapshotConfig) blockDisks() ([]snapshotDisk, error) {
	if len(c.Disks) == 0 {
		return nil, fmt.Errorf("snapshot has no disks")
	}
	var disks []snapshotDisk
	seen := make(map[string]bool)
	for _, disk := range c.Disks {
		if disk.VhostUser == nil {
			return nil, fmt.Errorf("snapshot disk %q is missing vhost_user", disk.ID)
		}
		if !*disk.VhostUser {
			continue
		}
		if disk.ID == "" || seen[disk.ID] || disk.NumQueues == 0 || !validQueueSize(disk.QueueSize) {
			return nil, fmt.Errorf("invalid snapshot disk %q queue config", disk.ID)
		}
		seen[disk.ID] = true
		disks = append(disks, disk)
	}
	return disks, nil
}

type snapshotQueue struct {
	Ready bool    `json:"ready"`
	Size  uint64  `json:"size"`
	Avail *uint64 `json:"avail_ring"`
	Used  *uint64 `json:"used_ring"`
}

type snapshotTransport struct {
	Queues []snapshotQueue `json:"queues"`
}

type snapshotSection struct {
	Snapshots    map[string]*snapshotSection `json:"snapshots"`
	SnapshotData snapshotData                `json:"snapshot_data"`
}

type snapshotData struct {
	State string `json:"state"`
}

func (s *snapshotSection) decode(name string, target any) error {
	if s == nil || s.Snapshots[name] == nil {
		return fmt.Errorf("snapshot is missing %q", name)
	}
	return json.Unmarshal([]byte(s.Snapshots[name].SnapshotData.State), target)
}

type snapshotMemoryRange struct {
	GPA    *uint64 `json:"gpa"`
	Length uint64  `json:"length"`
	offset uint64
}

type snapshotMemoryRanges struct {
	Data []snapshotMemoryRange `json:"data"`
}

type snapshotMemory struct {
	MemoryRanges snapshotMemoryRanges `json:"memory_ranges"`
	RAM          []snapshotRAMMapping `json:"guest_ram_mappings"`
	file         *os.File
}

// SnapshotMemoryFile holds a snapshot's guest RAM: the ranges state.json
// lists, one after another.
const SnapshotMemoryFile = "memory-ranges"

type snapshotRAMMapping struct {
	GPA        uint64 `json:"gpa"`
	Size       uint64 `json:"size"`
	VirtioMem  bool   `json:"virtio_mem"`
	FileOffset uint64 `json:"file_offset"`
}

// RAMRun is a run of the bytes of a guest's RAM file.
type RAMRun struct {
	Offset, Length int64
}

// ListSnapshotRAM completes a snapshot whose guest RAM the hypervisor left
// out because it is a linked file: state.json then lists no saved ranges. It
// lists the runs kept of a RAM file of size bytes, in the file's order, so
// that a file named SnapshotMemoryFile that holds them one after another
// restores like one the hypervisor wrote. The rest of the guest's RAM is
// restored as the zeroes new RAM holds.
func ListSnapshotRAM(dir string, size int64, kept []RAMRun) error {
	path := filepath.Join(dir, "state.json")
	state, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	state, err = editJSON(state, []string{"snapshots", "memory-manager", "snapshot_data", "state"}, func(encoded []byte) ([]byte, error) {
		var manager string
		if err := json.Unmarshal(encoded, &manager); err != nil {
			return nil, err
		}
		// Only the list changes: the other fields hold numbers that do not
		// survive a float64.
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(manager), &fields); err != nil {
			return nil, err
		}
		var saved snapshotMemoryRanges
		if err := json.Unmarshal(fields["memory_ranges"], &saved); err != nil {
			return nil, err
		}
		if len(saved.Data) != 0 {
			return nil, fmt.Errorf("snapshot already lists %d saved memory ranges", len(saved.Data))
		}
		var mappings []snapshotRAMMapping
		if err := json.Unmarshal(fields["guest_ram_mappings"], &mappings); err != nil {
			return nil, err
		}
		sort.Slice(mappings, func(i, j int) bool { return mappings[i].FileOffset < mappings[j].FileOffset })
		var end uint64
		for _, mapping := range mappings {
			if mapping.VirtioMem || mapping.Size == 0 || mapping.FileOffset != end || mapping.Size > math.MaxInt64-end {
				return nil, fmt.Errorf("snapshot guest RAM is not one file in order")
			}
			end += mapping.Size
		}
		if size <= 0 || end != uint64(size) {
			return nil, fmt.Errorf("snapshot has %d bytes of guest RAM, its RAM file has %d", end, size)
		}
		// A run is listed by guest address: once for each mapping it is in.
		var at uint64
		mapping := mappings[0]
		for _, run := range kept {
			if run.Length <= 0 || run.Offset < int64(at) || run.Length > size-run.Offset {
				return nil, fmt.Errorf("kept runs of guest RAM are not in order within its file")
			}
			at = uint64(run.Offset)
			for end := at + uint64(run.Length); at < end; {
				for mapping.FileOffset+mapping.Size <= at {
					mappings = mappings[1:]
					mapping = mappings[0]
				}
				gpa := mapping.GPA + at - mapping.FileOffset
				length := min(end, mapping.FileOffset+mapping.Size) - at
				saved.Data = append(saved.Data, snapshotMemoryRange{GPA: &gpa, Length: length})
				at += length
			}
		}
		if fields["memory_ranges"], err = json.Marshal(saved); err != nil {
			return nil, err
		}
		edited, err := json.Marshal(fields)
		if err != nil {
			return nil, err
		}
		return json.Marshal(string(edited))
	})
	if err != nil {
		return fmt.Errorf("list snapshot RAM: %w", err)
	}
	// Renamed into place: a restore must never read half a state file.
	staged := path + ".listed"
	if err := os.WriteFile(staged, state, 0o600); err != nil {
		return err
	}
	return os.Rename(staged, path)
}

// editJSON replaces the value at path inside a JSON object, leaving every
// other value as it was written.
func editJSON(data []byte, path []string, edit func([]byte) ([]byte, error)) ([]byte, error) {
	if len(path) == 0 {
		return edit(data)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	if _, ok := object[path[0]]; !ok {
		return nil, fmt.Errorf("snapshot state has no %q", path[0])
	}
	value, err := editJSON(object[path[0]], path[1:], edit)
	if err != nil {
		return nil, err
	}
	object[path[0]] = value
	return json.Marshal(object)
}

func readSnapshotJSON(dir, name string, target any) error {
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func validQueueSize(size uint64) bool {
	return size > 0 && size <= 32768 && size&(size-1) == 0
}

// ValidateSnapshotBlockQueues rejects pending vhost-user block requests. These
// requests cannot be recovered by a fresh backend. Read four bytes per queue,
// using the native RAM file's metadata order rather than guest-address order.
func ValidateSnapshotBlockQueues(dir string) error {
	var config snapshotConfig
	if err := readSnapshotJSON(dir, "config.json", &config); err != nil {
		return err
	}
	disks, err := config.blockDisks()
	if err != nil || len(disks) == 0 {
		return err
	}
	var state snapshotSection
	if err := readSnapshotJSON(dir, "state.json", &state); err != nil {
		return err
	}
	var memory snapshotMemory
	if err := state.decode("memory-manager", &memory); err != nil {
		return err
	}
	file, err := os.Open(filepath.Join(dir, SnapshotMemoryFile))
	if err != nil {
		return err
	}
	defer file.Close()
	if err := memory.bind(file); err != nil {
		return err
	}
	for _, disk := range disks {
		var transport snapshotTransport
		if err := state.Snapshots["device-manager"].decode("_virtio-pci-"+disk.ID, &transport); err != nil {
			return err
		}
		if uint64(len(transport.Queues)) != disk.NumQueues {
			return fmt.Errorf("snapshot disk %q queue count differs from config", disk.ID)
		}
		for i, queue := range transport.Queues {
			if err := queue.validate(&memory, disk.QueueSize); err != nil {
				return fmt.Errorf("snapshot disk %q queue %d: %w", disk.ID, i, err)
			}
		}
	}
	return nil
}

func (m *snapshotMemory) bind(file *os.File) error {
	regions := m.MemoryRanges.Data
	var total uint64
	for i := range regions {
		r := &regions[i]
		if r.GPA == nil || r.Length == 0 || *r.GPA > math.MaxUint64-r.Length || r.Length > math.MaxInt64-total {
			return fmt.Errorf("invalid snapshot memory range")
		}
		r.offset = total
		total += r.Length
	}
	if len(regions) == 0 {
		return fmt.Errorf("snapshot has no saved memory ranges")
	}
	sort.Slice(regions, func(i, j int) bool { return *regions[i].GPA < *regions[j].GPA })
	for i := 1; i < len(regions); i++ {
		if *regions[i].GPA < *regions[i-1].GPA+regions[i-1].Length {
			return fmt.Errorf("snapshot memory ranges overlap")
		}
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != int64(total) {
		return fmt.Errorf("snapshot memory file length does not match saved ranges")
	}
	m.file = file
	return nil
}

// readIndex reads the index of a ring: the two bytes after its flags.
func (m *snapshotMemory) readIndex(ring uint64) (uint16, error) {
	if ring <= math.MaxUint64-4 {
		index := ring + 2
		for _, r := range m.MemoryRanges.Data {
			if index >= *r.GPA && index+2 <= *r.GPA+r.Length {
				var value [2]byte
				_, err := m.file.ReadAt(value[:], int64(r.offset+(index-*r.GPA)))
				return binary.LittleEndian.Uint16(value[:]), err
			}
		}
		// RAM that was not saved held zeroes.
		for _, ram := range m.RAM {
			if index >= ram.GPA && index-ram.GPA < ram.Size {
				return 0, nil
			}
		}
	}
	return 0, fmt.Errorf("snapshot disk index is outside guest memory")
}

func (q snapshotQueue) validate(m *snapshotMemory, capacity uint64) error {
	if !q.Ready || !validQueueSize(q.Size) || q.Size > capacity {
		return fmt.Errorf("block queue is not ready or has invalid size")
	}
	if q.Avail == nil || q.Used == nil || *q.Avail%2 != 0 || *q.Used%4 != 0 || *q.Avail == *q.Used {
		return fmt.Errorf("missing or invalid block queue rings")
	}
	available, err := m.readIndex(*q.Avail)
	if err != nil {
		return err
	}
	used, err := m.readIndex(*q.Used)
	if err != nil {
		return err
	}
	if pending := uint16(available - used); pending != 0 {
		return fmt.Errorf("%d outstanding requests (available=%d used=%d)", pending, available, used)
	}
	return nil
}
