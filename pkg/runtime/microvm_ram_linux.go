//go:build linux

package runtime

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

const (
	microVMHugePage = 2 << 20
	// One populate call holds the worker's mmap lock for reading; a short
	// range keeps the worker's own mmap and munmap calls from waiting long.
	microVMRAMChunk = 32 << 20
	// Surplus huge page allocation does not get faster with more threads.
	microVMRAMPopulators = 8
	// How often, and how many times in all, a chunk asks for its pages.
	microVMRAMPopulateWait     = 25 * time.Millisecond
	microVMRAMPopulateAttempts = 20
	// A spare is kept of each of this many sizes: those the last guests had.
	microVMRAMSpareSizes = 4
	// A spare is prepared only while an eighth of host memory would remain
	// available, and every spare is given back once a sixteenth is left.
	microVMRAMSpareFloor    = 8
	microVMRAMPressureFloor = 16

	microVMMeminfoPath           = "/proc/meminfo"
	microVMSurplusHugePagesPath  = "/proc/sys/vm/nr_overcommit_hugepages"
	microVMRAMPressureInterval   = time.Second
	microVMCgroupHugePagesMax    = "hugetlb.2MB.max"
	microVMCgroupHugePagesRsvMax = "hugetlb.2MB.rsvd.max"
)

// microVMRAMPool supplies guest RAM for VFIO guests as zeroed 2 MiB pages.
//
// VFIO pins all guest RAM before the guest runs. On ordinary shared memory
// the kernel faults, zeroes and pins 4 KiB pages on one thread. Huge pages
// that already exist pin in milliseconds, and allocating them is what takes
// time, so the pool keeps spares for each NUMA node: one of each size the
// last GPU guests had.
// A guest of another size waits for the pages that the spare it takes lacks.
// Pages are on the node of the guest's GPU where the node has them to give.
//
// Huge pages cannot be reclaimed, and the scheduler does not know about
// spares; watch returns them to the host when its memory runs short.
type microVMRAMPool struct {
	dir     *os.File // where RAM files are linked; nil when they cannot be
	meminfo string

	mu      sync.Mutex
	spares  map[int][]*microVMRAM // by NUMA node; -1 when the GPU has none
	filling map[int]bool
	sizes   []int64 // those the last guests had, the latest first
	closed  bool
	done    chan struct{}
}

// microVMRAM is a file of 2 MiB pages. The hypervisor leaves the contents of
// a linked file out of its snapshots, where it would write every page, used
// or not; save writes the used ones instead. An unlinked file is the
// fallback: its pages go with its last descriptor, but its snapshots are
// written in full.
type microVMRAM struct {
	file *os.File
	size int64
	dir  *os.File // set when the file is linked there, as file.Name()
}

func newMicroVMRAMPool(dir *os.File, meminfo string) *microVMRAMPool {
	p := &microVMRAMPool{
		dir:     dir,
		meminfo: meminfo,
		spares:  map[int][]*microVMRAM{},
		filling: map[int]bool{},
		done:    make(chan struct{}),
	}
	go p.watch()
	return p
}

// allowSurplusHugePages lets huge pages be taken from free memory on demand,
// up to all of it. Zero is the kernel default; an operator's own limit is
// left alone. Tenants of a VFIO host run only inside VMs, whose cgroups get
// no huge pages.
func allowSurplusHugePages(path, meminfo string) error {
	current, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(current)) != "0" {
		return nil
	}
	total, _, err := hostMemory(meminfo)
	if err != nil {
		return err
	}
	return os.WriteFile(path, []byte(strconv.FormatInt(total/microVMHugePage, 10)), 0o644)
}

// take returns size bytes of zeroed huge pages on node, and how many of them
// a spare had ready. The caller closes it.
func (p *microVMRAMPool) take(node int, size int64) (*microVMRAM, int64, error) {
	if size <= 0 || size%microVMHugePage != 0 {
		return nil, 0, fmt.Errorf("microvm RAM must be a positive multiple of %d MiB", microVMHugePage>>20)
	}
	p.mu.Lock()
	sizes := append([]int64{size}, slices.DeleteFunc(p.sizes, func(recent int64) bool { return recent == size })...)
	p.sizes = sizes[:min(len(sizes), microVMRAMSpareSizes)]
	ram := p.spare(node, size)
	p.mu.Unlock()
	if ram == nil {
		var err error
		if ram, err = newMicroVMRAM(p.dir); err != nil {
			return nil, 0, err
		}
	}
	if ram.size >= size {
		ram.trim(size)
		return ram, size, nil
	}
	// A guest's RAM comes before a spare. Where its pages would leave the
	// host as short as watch gives the spares back for, it has theirs.
	if total, available, err := hostMemory(p.meminfo); err == nil && available-(size-ram.size) < total/microVMRAMPressureFloor {
		p.release()
	}
	ready := ram.size
	if err := ram.resize(size, node, false); err != nil {
		ram.close()
		return nil, 0, err
	}
	return ram, ready, nil
}

// spare takes from node's spares the one a guest of size waits least for:
// the smallest that holds it, or else the largest. The caller holds p.mu.
func (p *microVMRAMPool) spare(node int, size int64) *microVMRAM {
	spares := p.spares[node]
	best := -1
	for i, ram := range spares {
		if best < 0 {
			best = i
			continue
		}
		if chosen := spares[best].size; chosen >= size {
			if ram.size >= size && ram.size < chosen {
				best = i
			}
		} else if ram.size > chosen {
			best = i
		}
	}
	if best < 0 {
		return nil
	}
	ram := spares[best]
	p.spares[node] = slices.Delete(spares, best, best+1)
	return ram
}

// refill gives node, in the background, a spare of each size the last guests
// had, and gives back those of other sizes.
func (p *microVMRAMPool) refill(node int) {
	p.mu.Lock()
	if p.closed || p.filling[node] {
		p.mu.Unlock()
		return
	}
	missing := slices.Clone(p.sizes)
	var kept, stale []*microVMRAM
	for _, ram := range p.spares[node] {
		if i := slices.Index(missing, ram.size); i >= 0 {
			missing = slices.Delete(missing, i, i+1)
			kept = append(kept, ram)
		} else {
			stale = append(stale, ram)
		}
	}
	if len(missing) == 0 && len(stale) == 0 {
		p.mu.Unlock()
		return
	}
	p.spares[node] = kept
	p.filling[node] = true
	p.mu.Unlock()

	go func() {
		for _, ram := range stale {
			ram.close()
		}
		for _, size := range missing {
			start := time.Now()
			ram, err := p.prepare(node, size)
			p.mu.Lock()
			keep := err == nil && !p.closed
			if keep {
				p.spares[node] = append(p.spares[node], ram)
			}
			held := p.held()
			p.mu.Unlock()
			if !keep && ram != nil {
				ram.close()
			}
			if err != nil {
				log.Warn().Err(err).Int("numa_node", node).Int64("bytes", size).Msg("no spare microvm RAM prepared")
				continue
			}
			log.Info().Int("numa_node", node).Int64("bytes", size).Int64("held_bytes", held).Dur("took", time.Since(start)).Msg("spare microvm RAM prepared")
		}
		p.mu.Lock()
		delete(p.filling, node)
		p.mu.Unlock()
	}()
}

// prepare returns a spare of size bytes on node, if the host can spare them:
// a guest far larger than the rest must not leave them without one.
func (p *microVMRAMPool) prepare(node int, size int64) (*microVMRAM, error) {
	total, available, err := hostMemory(p.meminfo)
	if err != nil {
		return nil, err
	}
	if available-size < total/microVMRAMSpareFloor {
		return nil, fmt.Errorf("host has %d MiB available of %d MiB", available>>20, total>>20)
	}
	ram, err := newMicroVMRAM(p.dir)
	if err != nil {
		return nil, err
	}
	return ram, ram.resize(size, node, true)
}

// watch gives every spare back when host memory runs short.
func (p *microVMRAMPool) watch() {
	ticker := time.NewTicker(microVMRAMPressureInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			p.relieve()
		}
	}
}

func (p *microVMRAMPool) relieve() {
	p.mu.Lock()
	held := p.held()
	p.mu.Unlock()
	if held == 0 {
		return
	}
	total, available, err := hostMemory(p.meminfo)
	if err != nil || available >= total/microVMRAMPressureFloor {
		return
	}
	log.Warn().Int64("available_mib", available>>20).Int64("held_bytes", held).Msg("host memory is short; releasing spare microvm RAM")
	p.release()
}

// held is the RAM the spares of every node hold, which no guest was given
// and the scheduler does not count. The caller holds p.mu.
func (p *microVMRAMPool) held() (bytes int64) {
	for _, spares := range p.spares {
		for _, ram := range spares {
			bytes += ram.size
		}
	}
	return bytes
}

func (p *microVMRAMPool) release() {
	p.mu.Lock()
	spares := p.spares
	p.spares = map[int][]*microVMRAM{}
	p.mu.Unlock()
	for _, spares := range spares {
		for _, ram := range spares {
			ram.close()
		}
	}
}

func (p *microVMRAMPool) close() {
	p.mu.Lock()
	closed := p.closed
	p.closed = true
	p.mu.Unlock()
	if !closed {
		close(p.done)
	}
	p.release()
}

// mountMicroVMRAM returns the root of a hugetlbfs of 2 MiB pages that is
// attached nowhere. Its files are linked, yet the mount ends with the
// descriptors of the worker and its hypervisors: nothing can leak a guest's
// RAM by leaving a file behind.
func mountMicroVMRAM() (*os.File, error) {
	fs, err := unix.Fsopen("hugetlbfs", unix.FSOPEN_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("open hugetlbfs: %w", err)
	}
	defer unix.Close(fs)
	for _, option := range [][2]string{{"pagesize", "2M"}, {"mode", "0700"}} {
		if err := unix.FsconfigSetString(fs, option[0], option[1]); err != nil {
			return nil, fmt.Errorf("set hugetlbfs %s: %w", option[0], err)
		}
	}
	if err := unix.FsconfigCreate(fs); err != nil {
		return nil, fmt.Errorf("create hugetlbfs: %w", err)
	}
	root, err := unix.Fsmount(fs, unix.FSMOUNT_CLOEXEC, unix.MOUNT_ATTR_NOSUID|unix.MOUNT_ATTR_NODEV|unix.MOUNT_ATTR_NOEXEC)
	if err != nil {
		return nil, fmt.Errorf("mount hugetlbfs: %w", err)
	}
	return os.NewFile(uintptr(root), "beam-microvm-ram"), nil
}

var microVMRAMFiles atomic.Uint64

// newMicroVMRAM creates an empty RAM file, linked in dir when there is one.
func newMicroVMRAM(dir *os.File) (*microVMRAM, error) {
	if dir == nil {
		fd, err := unix.MemfdCreate("beam-microvm-ram", unix.MFD_CLOEXEC|unix.MFD_HUGETLB|unix.MFD_HUGE_2MB)
		if err != nil {
			return nil, fmt.Errorf("create huge page memfd: %w", err)
		}
		return &microVMRAM{file: os.NewFile(uintptr(fd), "beam-microvm-ram")}, nil
	}
	name := "ram-" + strconv.FormatUint(microVMRAMFiles.Add(1), 10)
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create huge page file: %w", err)
	}
	return &microVMRAM{file: os.NewFile(uintptr(fd), name), dir: dir}, nil
}

// close frees the pages, once the hypervisor that mapped them has exited.
func (r *microVMRAM) close() {
	if r.dir != nil {
		_ = unix.Unlinkat(int(r.dir.Fd()), r.file.Name(), 0)
	}
	_ = r.file.Close()
}

// trim makes the file's first size bytes the guest's RAM, which are all the
// hypervisor maps, and frees the rest behind it, so a guest given a spare
// larger than its RAM does not wait for the pages it has no use for.
func (r *microVMRAM) trim(size int64) {
	if r.size == size {
		return
	}
	r.size = size
	go func() { _ = r.file.Truncate(size) }()
}

// resize leaves the file with exactly size bytes of zeroed pages, the new
// ones on node. Truncating frees the pages past size.
func (r *microVMRAM) resize(size int64, node int, insist bool) error {
	populated := r.size
	if size == populated {
		return nil
	}
	if err := r.file.Truncate(size); err != nil {
		return fmt.Errorf("resize huge page file: %w", err)
	}
	r.size = size
	if size < populated {
		return nil
	}
	return populateHugePages(int(r.file.Fd()), populated, size-populated, node, insist)
}

// populateHugePages allocates and zeroes a range of a hugetlb file. Without
// MAP_NORESERVE, mmap would allocate every page itself: on one thread, while
// holding the worker's mmap lock for writing.
func populateHugePages(fd int, offset, size int64, node int, insist bool) error {
	region, err := unix.Mmap(fd, offset, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_NORESERVE)
	if err != nil {
		return fmt.Errorf("map huge pages: %w", err)
	}
	defer unix.Munmap(region)
	elsewhere, err := populateNear(node, insist, func(mode int) error {
		mask := make([]uint64, node/64+1)
		mask[node/64] = 1 << (node % 64)
		if _, _, errno := unix.Syscall6(unix.SYS_MBIND, uintptr(unsafe.Pointer(&region[0])), uintptr(len(region)),
			uintptr(mode), uintptr(unsafe.Pointer(&mask[0])), uintptr(len(mask)*64+1), 0); errno != 0 {
			return fmt.Errorf("place huge pages on NUMA node %d: %w", node, errno)
		}
		return nil
	}, func() error {
		return populateRAM(region, func(chunk []byte) error {
			return unix.Madvise(chunk, unix.MADV_POPULATE_WRITE)
		})
	})
	if err != nil {
		return fmt.Errorf("allocate %d MiB of huge pages: %w", size>>20, err)
	}
	if elsewhere {
		log.Warn().Int("numa_node", node).Int64("bytes", size).Msg("NUMA node has no huge pages to give; microvm RAM is in part on another")
	}
	return nil
}

// populateNear allocates RAM on the NUMA node of a guest's GPU, and what the
// node does not have, on another. It reports whether a node that was asked
// to give all of it could not.
//
// A node can have memory and no huge page for minutes on end. Linux 6.8
// cannot move the large folios XFS fills the file cache with, so it cannot
// make a huge page of the free memory between them, and it reclaims nothing
// for an allocation while that much memory is free.
//
// A spare can wait, so its node is asked for all of it first (insist). A
// guest that waits for its RAM has what the node has free, and the rest from
// wherever it is free.
func populateNear(node int, insist bool, policy func(mode int) error, populate func() error) (elsewhere bool, err error) {
	if node < 0 {
		return false, populate()
	}
	if insist {
		// The policy the hypervisor applies with host_numa_node.
		if err := policy(unix.MPOL_BIND); err != nil {
			return false, err
		}
		if err := populate(); !errors.Is(err, unix.EFAULT) {
			return false, err
		}
	}
	if err := policy(unix.MPOL_PREFERRED); err != nil {
		return false, err
	}
	return insist, populate()
}

// populateRAM allocates the pages of region, a chunk at a time. EFAULT is a
// page the kernel did not find. On a host whose free memory has gone to its
// file cache that is not for want of memory: reclaiming and compacting it
// lags the threads that ask, and a chunk that is asked for again is given.
func populateRAM(region []byte, populate func(chunk []byte) error) error {
	return eachRAMChunk(int64(len(region)), func(start, end int64) error {
		for attempt := 1; ; attempt++ {
			err := populate(region[start:end])
			if !errors.Is(err, unix.EFAULT) || attempt == microVMRAMPopulateAttempts {
				return err
			}
			time.Sleep(microVMRAMPopulateWait)
		}
	})
}

// eachRAMChunk covers [0, size) in chunks, a few at a time, until one fails.
func eachRAMChunk(size int64, work func(start, end int64) error) error {
	var next atomic.Int64
	var failed atomic.Bool
	var workers errgroup.Group
	for range microVMRAMPopulators {
		workers.Go(func() error {
			for !failed.Load() {
				start := next.Add(microVMRAMChunk) - microVMRAMChunk
				if start >= size {
					return nil
				}
				if err := work(start, min(start+microVMRAMChunk, size)); err != nil {
					failed.Store(true)
					return err
				}
			}
			return nil
		})
	}
	return workers.Wait()
}

// save writes the RAM of a stopped guest to path: the runs of 2 MiB pages
// that hold data, one after another. It returns them. Guest memory is
// clustered: an idle 8 GiB guest keeps 0.6 GiB in a few dozen runs, and 4 KiB
// pages would keep a tenth less in eighty times as many.
//
// The file has no holes, where the pages without data could have been left.
// An archiver copies a file with holes 512 bytes at a time, and one without
// in records as large as it is told to.
func (r *microVMRAM) save(path string) (kept []microvm.RAMRun, err error) {
	ram, err := unix.Mmap(int(r.file.Fd()), 0, int(r.size), unix.PROT_READ, unix.MAP_SHARED|unix.MAP_NORESERVE)
	if err != nil {
		return nil, fmt.Errorf("map guest RAM: %w", err)
	}
	defer unix.Munmap(ram)
	image, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := image.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			_ = os.Remove(path)
		}
	}()

	// Where a run is written depends on those before it: find them all.
	found := make([][]microvm.RAMRun, (r.size+microVMRAMChunk-1)/microVMRAMChunk)
	_ = eachRAMChunk(r.size, func(start, end int64) error {
		for page := start; page < end; page += microVMHugePage {
			if holdsData(ram[page : page+microVMHugePage]) {
				found[start/microVMRAMChunk] = extendRAMRuns(found[start/microVMRAMChunk], microvm.RAMRun{Offset: page, Length: microVMHugePage})
			}
		}
		return nil
	})
	var ends []int64 // of each run, in the image
	var size int64
	for _, runs := range found {
		for _, run := range runs {
			kept = extendRAMRuns(kept, run)
			size += run.Length
			ends = append(ends[:len(kept)-1], size)
		}
	}
	if err := image.Truncate(size); err != nil {
		return nil, err
	}
	err = eachRAMChunk(size, func(start, end int64) error {
		for i := sort.Search(len(ends), func(i int) bool { return ends[i] > start }); start < end; i++ {
			n := min(end, ends[i]) - start
			from := kept[i].Offset + kept[i].Length - (ends[i] - start)
			if _, err := image.WriteAt(ram[from:from+n], start); err != nil {
				return err
			}
			start += n
		}
		return nil
	})
	return kept, err
}

// extendRAMRuns adds a run that starts at or after the end of the last.
func extendRAMRuns(runs []microvm.RAMRun, run microvm.RAMRun) []microvm.RAMRun {
	if n := len(runs); n > 0 && runs[n-1].Offset+runs[n-1].Length == run.Offset {
		runs[n-1].Length += run.Length
		return runs
	}
	return append(runs, run)
}

var microVMZeroes [4096]byte

func holdsData(page []byte) bool {
	for len(page) > 0 {
		n := min(len(page), len(microVMZeroes))
		if !bytes.Equal(page[:n], microVMZeroes[:n]) {
			return true
		}
		page = page[n:]
	}
	return false
}

// hostMemory reads MemTotal and MemAvailable, in bytes.
func hostMemory(path string) (total, available int64, err error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	values := map[string]int64{}
	for scanner := bufio.NewScanner(file); scanner.Scan(); {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 3 && fields[2] == "kB" {
			if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
				values[fields[0]] = kb << 10
			}
		}
	}
	total, available = values["MemTotal:"], values["MemAvailable:"]
	if total <= 0 || available <= 0 {
		return 0, 0, fmt.Errorf("%s reports no MemTotal or MemAvailable", path)
	}
	return total, available, nil
}
