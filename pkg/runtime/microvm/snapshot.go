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
	file         *os.File
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
	file, err := os.Open(filepath.Join(dir, "memory-ranges"))
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

func (m *snapshotMemory) readIndex(ring uint64) (uint16, error) {
	if ring <= math.MaxUint64-4 {
		for _, r := range m.MemoryRanges.Data {
			if ring >= *r.GPA && ring+4 <= *r.GPA+r.Length {
				var value [2]byte
				_, err := m.file.ReadAt(value[:], int64(r.offset+(ring-*r.GPA)+2))
				return binary.LittleEndian.Uint16(value[:]), err
			}
		}
	}
	return 0, fmt.Errorf("snapshot disk index is outside saved memory")
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
