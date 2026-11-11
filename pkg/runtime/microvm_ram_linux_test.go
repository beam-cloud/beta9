//go:build linux

package runtime

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func meminfoFile(t *testing.T, totalKB, availableKB int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "meminfo")
	require.NoError(t, os.WriteFile(path, fmt.Appendf(nil, "MemTotal:       %d kB\nMemFree:  1 kB\nMemAvailable:   %d kB\nHugePages_Total:       0\n", totalKB, availableKB), 0o600))
	return path
}

func TestMicroVMHostMemory(t *testing.T) {
	total, available, err := hostMemory(meminfoFile(t, 1056595332, 1042060148))
	require.NoError(t, err)
	require.Equal(t, int64(1056595332)<<10, total)
	require.Equal(t, int64(1042060148)<<10, available)

	empty := filepath.Join(t.TempDir(), "meminfo")
	require.NoError(t, os.WriteFile(empty, []byte("MemTotal: 1 kB\n"), 0o600))
	_, _, err = hostMemory(empty)
	require.ErrorContains(t, err, "MemAvailable")
}

func TestMicroVMAllowSurplusHugePages(t *testing.T) {
	meminfo := meminfoFile(t, 64<<20, 32<<20) // 64 GiB
	limit := filepath.Join(t.TempDir(), "nr_overcommit_hugepages")
	require.ErrorIs(t, allowSurplusHugePages(limit, meminfo), os.ErrNotExist)

	require.NoError(t, os.WriteFile(limit, []byte("0\n"), 0o600))
	require.NoError(t, allowSurplusHugePages(limit, meminfo))
	data, err := os.ReadFile(limit)
	require.NoError(t, err)
	require.Equal(t, "32768", string(data), "all of host memory, in 2 MiB pages")

	require.NoError(t, os.WriteFile(limit, []byte("512\n"), 0o600))
	require.NoError(t, allowSurplusHugePages(limit, meminfo))
	data, err = os.ReadFile(limit)
	require.NoError(t, err)
	require.Equal(t, "512\n", string(data), "an operator's limit is kept")
}

// hugePagesAvailable skips hosts that cannot allocate 2 MiB pages on demand.
func hugePagesAvailable(t *testing.T) {
	t.Helper()
	ram, err := newMicroVMRAM(nil)
	if err == nil {
		err = ram.resize(microVMHugePage, -1, true)
		ram.close()
	}
	if err != nil {
		t.Skipf("host has no 2 MiB huge pages: %v", err)
	}
}

// eachRAMDir runs test with unlinked RAM files and, where the host lets a
// test mount a filesystem, with linked ones.
func eachRAMDir(t *testing.T, test func(t *testing.T, dir *os.File)) {
	hugePagesAvailable(t)
	t.Run("unlinked", func(t *testing.T) { test(t, nil) })
	t.Run("linked", func(t *testing.T) {
		dir, err := mountMicroVMRAM()
		if err != nil {
			t.Skipf("cannot mount hugetlbfs: %v", err)
		}
		t.Cleanup(func() { _ = dir.Close() })
		test(t, dir)
	})
}

func hugePagesInUse(t *testing.T) int64 {
	t.Helper()
	data, err := os.ReadFile("/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages")
	require.NoError(t, err)
	free, err := os.ReadFile("/sys/kernel/mm/hugepages/hugepages-2048kB/free_hugepages")
	require.NoError(t, err)
	var total, unused int64
	_, err = fmt.Sscan(strings.TrimSpace(string(data)), &total)
	require.NoError(t, err)
	_, err = fmt.Sscan(strings.TrimSpace(string(free)), &unused)
	require.NoError(t, err)
	return total - unused
}

func TestMicroVMRAMPool(t *testing.T) {
	eachRAMDir(t, testMicroVMRAMPool)
}

func testMicroVMRAMPool(t *testing.T, dir *os.File) {
	const size = 8 * microVMHugePage
	baseline := hugePagesInUse(t)
	pool := newMicroVMRAMPool(dir, meminfoFile(t, 64<<20, 32<<20))
	t.Cleanup(pool.close)
	spares := func() []int64 {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		sizes := []int64{}
		for _, ram := range pool.spares[-1] {
			sizes = append(sizes, ram.size/microVMHugePage)
		}
		slices.Sort(sizes)
		return sizes
	}
	refill := func() {
		pool.refill(-1)
		require.Eventually(t, func() bool {
			pool.mu.Lock()
			defer pool.mu.Unlock()
			return !pool.filling[-1]
		}, 10*time.Second, time.Millisecond)
	}
	inUse := func(pages int64, why string) {
		require.Eventually(t, func() bool { return hugePagesInUse(t) == baseline+pages }, 10*time.Second, time.Millisecond, why)
	}
	guest := func(size, ready int64, why string) {
		ram, had, err := pool.take(-1, size)
		require.NoError(t, err)
		require.EqualValues(t, ready, had, why)
		require.EqualValues(t, size, ram.size)
		ram.close()
	}

	refill()
	require.Empty(t, spares(), "no guest has said what size a spare should be")

	ram, ready, err := pool.take(-1, size)
	require.NoError(t, err)
	require.Zero(t, ready, "nothing was prepared before the first guest")
	info, err := ram.file.Stat()
	require.NoError(t, err)
	require.EqualValues(t, size, info.Size())
	require.EqualValues(t, size, ram.size)
	var stat unix.Stat_t
	require.NoError(t, unix.Fstat(int(ram.file.Fd()), &stat))
	require.Equal(t, dir != nil, stat.Nlink == 1, "the hypervisor leaves only a linked file out of its snapshots")
	require.Equal(t, baseline+8, hugePagesInUse(t), "every page exists before the hypervisor maps the file")
	page := make([]byte, 4096)
	_, err = ram.file.ReadAt(page, size-int64(len(page)))
	require.NoError(t, err)
	require.Equal(t, make([]byte, len(page)), page)
	ram.close()
	require.Equal(t, baseline, hugePagesInUse(t))

	refill()
	require.Equal(t, []int64{8}, spares())

	ram, ready, err = pool.take(-1, size/4)
	require.NoError(t, err)
	require.EqualValues(t, size/4, ready)
	require.EqualValues(t, size/4, ram.size, "the guest's RAM is the head of a larger spare")
	inUse(2, "whose tail is given back behind the guest")
	_, err = ram.file.ReadAt(page, size/4-int64(len(page)))
	require.NoError(t, err)
	ram.close()

	refill()
	require.Equal(t, []int64{2, 8}, spares(), "a spare of each size the last guests had")
	inUse(10, "")
	guest(size, size, "a guest takes the spare of its size")
	require.Equal(t, []int64{2}, spares())

	refill()
	guest(size/2, size/2, "a guest of a new size takes the smallest spare that holds it")
	require.Equal(t, []int64{2}, spares())
	guest(size, size/4, "or the largest there is, and waits for the rest")
	require.Empty(t, spares())

	refill()
	require.Equal(t, []int64{2, 4, 8}, spares())
	recent := []int64{}
	for i := range int64(microVMRAMSpareSizes) {
		recent = append(recent, 9+i)
		ram, ready, err = pool.take(0, (9+i)*microVMHugePage)
		require.NoError(t, err)
		require.Zero(t, ready, "a spare is of use to the guests of its NUMA node")
		ram.close()
	}
	require.Equal(t, []int64{2, 4, 8}, spares())
	refill()
	require.Equal(t, recent, spares(), "a spare of a size no recent guest had is given back")

	_, _, err = pool.take(-1, microVMHugePage+4096)
	require.ErrorContains(t, err, "multiple of 2 MiB")
	pool.close()
	inUse(0, "the pool's pages go with it")
}

func TestMicroVMRAMPoolYieldsToHostMemory(t *testing.T) {
	eachRAMDir(t, testMicroVMRAMPoolYieldsToHostMemory)
}

func testMicroVMRAMPoolYieldsToHostMemory(t *testing.T, dir *os.File) {
	const size = 4 * microVMHugePage
	baseline := hugePagesInUse(t)
	meminfo := meminfoFile(t, 64<<20, 32<<20)
	available := func(kb int64) {
		require.NoError(t, os.WriteFile(meminfo, fmt.Appendf(nil, "MemTotal: 67108864 kB\nMemAvailable: %d kB\n", kb), 0o600))
	}
	pool := newMicroVMRAMPool(dir, meminfo)
	t.Cleanup(pool.close)
	held := func() []int64 {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		sizes := []int64{}
		for _, ram := range pool.spares[-1] {
			sizes = append(sizes, ram.size/size)
		}
		slices.Sort(sizes)
		return sizes
	}
	refill := func() {
		pool.refill(-1)
		require.Eventually(t, func() bool {
			pool.mu.Lock()
			defer pool.mu.Unlock()
			return len(pool.filling) == 0
		}, 10*time.Second, time.Millisecond)
	}
	guest := func(size int64) {
		ram, _, err := pool.take(-1, size)
		require.NoError(t, err, "a guest's own RAM is not subject to the spare floor")
		ram.close()
	}

	guest(size)
	refill()
	require.Equal(t, []int64{1}, held())
	pool.relieve()
	require.Equal(t, []int64{1}, held(), "half of host memory is available")

	available(4194303)
	pool.relieve()
	require.Empty(t, held(), "less than a sixteenth of host memory is available")
	require.Equal(t, baseline, hugePagesInUse(t))

	available(8388607)
	refill()
	require.Empty(t, held(), "a spare must leave an eighth of host memory available")
	require.Equal(t, baseline, hugePagesInUse(t))

	// An eighth, and room for the smaller of two guests.
	available(8388608 + 3*size/2048)
	guest(2 * size)
	refill()
	require.Equal(t, []int64{1}, held(), "a guest the host has no spare for does not leave the others without one")

	available(32 << 20)
	guest(3 * size)
	refill()
	require.Equal(t, []int64{1, 2, 3}, held())
	// A sixteenth, and room for less than the guest lacks.
	available(4194304 + size/2048)
	guest(2 * size)
	require.Equal(t, []int64{1, 3}, held(), "a guest that finds its RAM ready needs no more of the host")
	guest(5 * size)
	require.Empty(t, held(), "a guest whose RAM would leave the host short has the spares' first")

	pool.close()
	pool.refill(-1)
	require.Empty(t, held())
	require.Eventually(t, func() bool { return hugePagesInUse(t) == baseline }, 10*time.Second, time.Millisecond)
}

func TestPopulateNearTakesFromAnotherNodeWhatItsOwnDoesNotHave(t *testing.T) {
	const asked = -1 // among the policies set: the pages were asked for
	bind, prefer := unix.MPOL_BIND, unix.MPOL_PREFERRED
	for name, tc := range map[string]struct {
		node      int
		insist    bool
		refused   int   // times the pages are refused
		with      error // what they are refused with
		noPolicy  error
		calls     []int
		elsewhere bool
		err       error
	}{
		"GPU on no node":                     {node: -1, insist: true, calls: []int{asked}},
		"spare on its node":                  {insist: true, calls: []int{bind, asked}},
		"spare on a node with no huge pages": {insist: true, refused: 1, with: unix.EFAULT, calls: []int{bind, asked, prefer, asked}, elsewhere: true},
		"spare on a host with no huge pages": {insist: true, refused: 2, with: unix.EFAULT, calls: []int{bind, asked, prefer, asked}, elsewhere: true, err: unix.EFAULT},
		"spare on a host with no memory":     {insist: true, refused: 1, with: unix.ENOMEM, calls: []int{bind, asked}, err: unix.ENOMEM},
		"guest that waits":                   {node: 1, calls: []int{prefer, asked}},
		"guest on a host with no huge pages": {node: 1, refused: 1, with: unix.EFAULT, calls: []int{prefer, asked}, err: unix.EFAULT},
		"node that takes no policy":          {insist: true, noPolicy: unix.EINVAL, calls: []int{bind}, err: unix.EINVAL},
	} {
		t.Run(name, func(t *testing.T) {
			var calls []int
			refused := tc.refused
			elsewhere, err := populateNear(tc.node, tc.insist, func(mode int) error {
				calls = append(calls, mode)
				return tc.noPolicy
			}, func() error {
				calls = append(calls, asked)
				if refused > 0 {
					refused--
					return tc.with
				}
				return nil
			})
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.calls, calls)
			require.Equal(t, tc.elsewhere, elsewhere)
		})
	}
}

func TestPopulateRAMAsksAgainForPagesTheKernelDidNotFind(t *testing.T) {
	region := make([]byte, 3*microVMRAMChunk+microVMHugePage)
	var mu sync.Mutex
	populate := func(answer func(chunk, attempt int) error) (map[int]int, error) {
		asked := map[int]int{}
		err := populateRAM(region, func(chunk []byte) error {
			mu.Lock()
			defer mu.Unlock()
			index := (cap(region) - cap(chunk)) / microVMRAMChunk
			asked[index]++
			if want := min(microVMRAMChunk, len(region)-index*microVMRAMChunk); len(chunk) != want {
				t.Errorf("chunk %d has %d bytes, want %d", index, len(chunk), want)
			}
			return answer(index, asked[index])
		})
		return asked, err
	}

	asked, err := populate(func(chunk, attempt int) error {
		if chunk == 1 && attempt <= 2 {
			return unix.EFAULT
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 1, 1: 3, 2: 1, 3: 1}, asked)

	asked, err = populate(func(chunk, _ int) error {
		if chunk == 2 {
			return unix.EFAULT
		}
		return nil
	})
	require.ErrorIs(t, err, unix.EFAULT)
	require.Equal(t, microVMRAMPopulateAttempts, asked[2], "a node with no memory left is not asked for ever")

	asked, err = populate(func(chunk, _ int) error {
		if chunk == 0 {
			return unix.ENOMEM
		}
		return nil
	})
	require.ErrorIs(t, err, unix.ENOMEM)
	require.Equal(t, 1, asked[0], "only a page the kernel did not find is asked for again")
}

func TestEachRAMChunkStopsAtAFailure(t *testing.T) {
	var chunks atomic.Int32
	err := eachRAMChunk(1000*microVMRAMChunk, func(_, _ int64) error {
		chunks.Add(1)
		return unix.EIO
	})
	require.ErrorIs(t, err, unix.EIO)
	require.LessOrEqual(t, chunks.Load(), int32(microVMRAMPopulators), "a worker takes no chunk after one has failed")
}

func TestMicroVMRAMMountEndsWithItsDescriptors(t *testing.T) {
	hugePagesAvailable(t)
	dir, err := mountMicroVMRAM()
	if err != nil {
		t.Skipf("cannot mount hugetlbfs: %v", err)
	}
	baseline := hugePagesInUse(t)
	ram, err := newMicroVMRAM(dir)
	require.NoError(t, err)
	require.NoError(t, ram.resize(2*microVMHugePage, -1, true))
	// What a worker that is killed leaves: a linked file nobody will remove.
	require.NoError(t, ram.file.Close())
	require.Equal(t, baseline+2, hugePagesInUse(t), "a linked file keeps its pages")
	require.NoError(t, dir.Close())
	require.Equal(t, baseline, hugePagesInUse(t))
}

func TestMicroVMRAMSave(t *testing.T) {
	eachRAMDir(t, func(t *testing.T, dir *os.File) {
		// Three pages more than a worker takes at a time.
		const pages = microVMRAMChunk/microVMHugePage + 3
		baseline := hugePagesInUse(t)
		ram, err := newMicroVMRAM(dir)
		require.NoError(t, err)
		require.NoError(t, ram.resize(pages*microVMHugePage, -1, true))
		guest, err := unix.Mmap(int(ram.file.Fd()), 0, int(ram.size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_NORESERVE)
		require.NoError(t, err)
		// A page with one byte at its end, a run of three that two workers
		// find, and the last page.
		runs := []microvm.RAMRun{
			{Offset: microVMHugePage, Length: microVMHugePage},
			{Offset: microVMRAMChunk - 2*microVMHugePage, Length: 3 * microVMHugePage},
			{Offset: (pages - 1) * microVMHugePage, Length: microVMHugePage},
		}
		guest[2*microVMHugePage-1] = 1
		_, err = rand.Read(guest[runs[1].Offset : runs[1].Offset+runs[1].Length])
		require.NoError(t, err)
		guest[len(guest)-microVMHugePage] = 2
		var want []byte
		for _, run := range runs {
			want = append(want, guest[run.Offset:run.Offset+run.Length]...)
		}

		image := filepath.Join(t.TempDir(), "memory-ranges")
		kept, err := ram.save(image)
		require.NoError(t, err)
		require.Equal(t, runs, kept, "only pages that hold data are kept")
		got, err := os.ReadFile(image)
		require.NoError(t, err)
		require.True(t, string(want) == string(got), "the image holds the runs one after another")
		file, err := os.Open(image)
		require.NoError(t, err)
		defer file.Close()
		hole, err := unix.Seek(int(file.Fd()), 0, unix.SEEK_HOLE)
		require.NoError(t, err)
		require.EqualValues(t, len(want), hole, "the image has no holes")

		_, err = ram.save(image)
		require.ErrorIs(t, err, os.ErrExist)
		got, err = os.ReadFile(image)
		require.NoError(t, err)
		require.True(t, string(want) == string(got), "a refused save leaves the image alone")

		_, err = ram.save(filepath.Join(image, "under-a-file"))
		require.Error(t, err)

		clear(guest)
		empty := filepath.Join(t.TempDir(), "memory-ranges")
		kept, err = ram.save(empty)
		require.NoError(t, err)
		require.Empty(t, kept)
		info, err := os.Stat(empty)
		require.NoError(t, err)
		require.Zero(t, info.Size())

		require.NoError(t, unix.Munmap(guest))
		ram.close()
		require.Equal(t, baseline, hugePagesInUse(t))
	})
}

func TestMicroVMCheckpointSavesRAMTheHypervisorLeftOut(t *testing.T) {
	hugePagesAvailable(t)
	const size = 4 * microVMHugePage
	// What the hypervisor writes for a guest whose RAM is a linked file.
	state := fmt.Sprintf(`{"snapshots":{"memory-manager":{"snapshots":{},"snapshot_data":{"state":%q}}}}`,
		`{"memory_ranges":{"data":[]},"guest_ram_mappings":[`+
			`{"gpa":0,"size":4194304,"virtio_mem":false,"file_offset":0},{"gpa":4294967296,"size":4194304,"virtio_mem":false,"file_offset":4194304}]}`)
	snapshot := func() string {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "state.json"), []byte(state), 0o600))
		return dir
	}
	ram, err := newMicroVMRAM(nil)
	require.NoError(t, err)
	t.Cleanup(ram.close)
	require.NoError(t, ram.resize(size, -1, true))
	guest, err := unix.Mmap(int(ram.file.Fd()), 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_NORESERVE)
	require.NoError(t, err)
	// One run of two pages, at guest addresses that are 4 GiB apart.
	copy(guest[microVMHugePage:], "below the hole")
	copy(guest[2*microVMHugePage:], "above the hole")
	require.NoError(t, unix.Munmap(guest))
	inst := &microVMInstance{id: "vm", ram: ram}

	dir := snapshot()
	saved, err := inst.saveRAM(dir)
	require.NoError(t, err)
	require.EqualValues(t, 2*microVMHugePage, saved)
	image, err := os.ReadFile(filepath.Join(dir, microvm.SnapshotMemoryFile))
	require.NoError(t, err)
	require.Len(t, image, 2*microVMHugePage)
	require.Equal(t, "below the hole", string(image[:14]))
	require.Equal(t, "above the hole", string(image[microVMHugePage:][:14]))
	listed, err := os.ReadFile(filepath.Join(dir, "state.json"))
	require.NoError(t, err)
	require.Contains(t, string(listed), `{\"data\":[{\"gpa\":2097152,\"length\":2097152},{\"gpa\":4294967296,\"length\":2097152}]}`)

	// A snapshot the hypervisor wrote the RAM of is complete.
	saved, err = inst.saveRAM(dir)
	require.NoError(t, err)
	require.Zero(t, saved)
	again, err := os.ReadFile(filepath.Join(dir, "state.json"))
	require.NoError(t, err)
	require.Equal(t, string(listed), string(again))

	inst.ram = nil
	_, err = inst.saveRAM(snapshot())
	require.ErrorContains(t, err, "saved no guest RAM")

	inst.ram = &microVMRAM{file: ram.file, size: size / 2}
	_, err = inst.saveRAM(snapshot())
	require.ErrorContains(t, err, "its RAM file has 4194304")
}

// BenchmarkMicroVMRAM reports how fast guest RAM is zeroed on this host:
// go test -run '^$' -bench MicroVMRAM -benchtime 1x ./pkg/runtime
func BenchmarkMicroVMRAM(b *testing.B) {
	const size = 16 << 30
	for b.Loop() {
		ram, err := newMicroVMRAM(nil)
		if err == nil {
			err = ram.resize(size, 0, true)
		}
		if err != nil {
			b.Skipf("host cannot provide %d GiB of huge pages on node 0: %v", size>>30, err)
		}
		b.SetBytes(size)
		ram.close()
	}
}
