//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"golang.org/x/sys/unix"
)

// linux/fs.h: _IOWR('X', 119, int) and _IOWR('X', 120, int).
const (
	ioctlFIFREEZE = 0xC0045877
	ioctlFITHAW   = 0xC0045878
)

var errFreezeAbandoned = errors.New("the host stopped waiting for the freeze")

// freezer freezes filesystems for the host, which snapshots a disk while the
// guest is not writing to it.
//
// FIFREEZE holds new writes back and then waits for the ones under way, and
// the kernel cannot give up on that wait. A write under way may itself be
// waiting for a loop device whose file is on the freezing filesystem, where
// the device's own writes are now held back: the freeze never finishes, and
// nothing writes to the disk again. So the filesystems on loop devices are
// frozen first, which leaves those devices nothing to write, and are thawed
// after the last frozen disk.
//
// The host is answered within wait. A freeze the kernel has not finished by
// then fails, and is thawed whenever it does finish; until then every other
// freeze fails at once. A freeze the host never thaws (it gave up waiting) is
// thawed after limit, so the host cannot leave writes hanging for good.
type freezer struct {
	wait  time.Duration
	limit time.Duration
	// loops opens the loop-backed filesystems to freeze ahead of a disk.
	loops func(disk string) []*os.File
	ioctl func(dir *os.File, request uint) error

	// One freeze at a time: they share the loop-backed filesystems.
	turn sync.Mutex

	mu      sync.Mutex
	frozen  map[string]*frozenFS // disks, by the mount path the host names
	stacked []*os.File           // loop-backed filesystems, in the order frozen
	active  *freezing            // the freeze that has the turn
}

type frozenFS struct {
	dir   *os.File
	timer *time.Timer
}

// freezing is a freeze the kernel has yet to finish.
type freezing struct {
	path  string
	began time.Time
	// The host no longer waits: whatever this freezes is thawed again.
	abandoned bool
	// Closed once err is final.
	done chan struct{}
	err  error
}

func newFreezer() *freezer {
	return &freezer{
		wait:  microvm.FreezeTimeout,
		limit: microvm.FreezeLimit,
		loops: loopFilesystems,
		ioctl: func(dir *os.File, request uint) error {
			return unix.IoctlSetInt(int(dir.Fd()), request, 0)
		},
		frozen: map[string]*frozenFS{},
	}
}

func (f *freezer) freeze(path string) error {
	f.mu.Lock()
	_, frozen := f.frozen[path]
	var stuck *freezing
	if f.active != nil && f.active.abandoned {
		stuck = f.active
	}
	f.mu.Unlock()
	switch {
	case frozen:
		return nil
	case stuck != nil:
		return fmt.Errorf("the kernel has been freezing %s for %s", stuck.path, time.Since(stuck.began).Round(time.Second))
	}

	attempt := &freezing{path: path, began: time.Now(), done: make(chan struct{})}
	go f.run(attempt)
	select {
	case <-attempt.done:
		return attempt.err
	case <-time.After(f.wait):
	}
	if !f.abandon(attempt) {
		return attempt.err
	}
	f.report("freeze of %s is not finished after %s", path, f.wait)
	return fmt.Errorf("the kernel did not freeze %s within %s", path, f.wait)
}

// abandon stops waiting for an attempt, unless it has just finished.
func (f *freezer) abandon(attempt *freezing) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-attempt.done:
		return false
	default:
		attempt.abandoned = true
		return true
	}
}

// report logs without waiting: the journal that takes the line may itself be
// waiting to write to a frozen filesystem.
func (f *freezer) report(format string, args ...any) {
	go logf(format, args...)
}

// run freezes the loop-backed filesystems and then the disk at attempt.path.
func (f *freezer) run(attempt *freezing) {
	f.turn.Lock()
	defer f.turn.Unlock()

	f.mu.Lock()
	if _, frozen := f.frozen[attempt.path]; frozen || attempt.abandoned {
		// Frozen by the request that had the turn, or given up on meanwhile.
		if !frozen {
			attempt.err = errFreezeAbandoned
		}
		close(attempt.done)
		f.mu.Unlock()
		return
	}
	f.active = attempt
	f.mu.Unlock()

	var dir *os.File
	err := f.freezeStacked(attempt)
	if err == nil {
		dir, err = f.freezeFS(attempt.path)
	}

	f.mu.Lock()
	abandoned := attempt.abandoned
	switch {
	case err != nil:
	case abandoned:
		_ = f.thawFS(dir)
		err = errFreezeAbandoned
	default:
		entry := &frozenFS{dir: dir}
		entry.timer = time.AfterFunc(f.limit, func() { f.expire(attempt.path, entry) })
		f.frozen[attempt.path] = entry
	}
	f.active = nil
	f.thawStacked()
	attempt.err = err
	close(attempt.done)
	f.mu.Unlock()
	if abandoned {
		f.report("freeze of %s, which the host gave up on, ended after %s", attempt.path, time.Since(attempt.began).Round(time.Second))
	}
}

// freezeStacked freezes the loop-backed filesystems ahead of the disk, for as
// long as the host waits. One that cannot be frozen, or is frozen already, is
// left as it is.
func (f *freezer) freezeStacked(attempt *freezing) error {
	dirs := f.loops(attempt.path)
	for i, dir := range dirs {
		err := f.ioctl(dir, ioctlFIFREEZE)
		if err != nil {
			dir.Close()
			if !errors.Is(err, unix.EBUSY) && !errors.Is(err, unix.EOPNOTSUPP) {
				f.report("freeze %s ahead of %s: %v", dir.Name(), attempt.path, err)
			}
		}
		f.mu.Lock()
		if err == nil {
			f.stacked = append(f.stacked, dir)
		}
		abandoned := attempt.abandoned
		f.mu.Unlock()
		if abandoned {
			for _, rest := range dirs[i+1:] {
				rest.Close()
			}
			return errFreezeAbandoned
		}
	}
	return nil
}

func (f *freezer) freezeFS(path string) (*os.File, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if err := f.ioctl(dir, ioctlFIFREEZE); err != nil {
		dir.Close()
		return nil, err
	}
	return dir, nil
}

func (f *freezer) thawFS(dir *os.File) error {
	err := f.ioctl(dir, ioctlFITHAW)
	dir.Close()
	return err
}

func (f *freezer) thaw(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if attempt := f.active; attempt != nil && attempt.path == path {
		// The host thaws a freeze it has given up on.
		attempt.abandoned = true
	}
	return f.release(path)
}

// expire thaws a disk the host has left frozen for the limit.
func (f *freezer) expire(path string, entry *frozenFS) {
	f.mu.Lock()
	if f.frozen[path] != entry {
		f.mu.Unlock()
		return
	}
	err := f.release(path)
	f.mu.Unlock()
	if err == nil {
		f.report("thawed %s: no thaw request within %s", path, f.limit)
	}
}

func (f *freezer) thawAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active != nil {
		f.active.abandoned = true
	}
	for path := range f.frozen {
		_ = f.release(path)
	}
}

// release thaws a frozen disk and, after the last one, the loop-backed
// filesystems. The caller holds mu.
func (f *freezer) release(path string) error {
	entry, ok := f.frozen[path]
	if !ok {
		return nil
	}
	entry.timer.Stop()
	delete(f.frozen, path)
	err := f.thawFS(entry.dir)
	f.thawStacked()
	return err
}

// thawStacked thaws the loop-backed filesystems, the first frozen last, once
// no disk is frozen. It leaves them while the kernel is still freezing: their
// writes are what that freeze could never finish behind. The caller holds mu.
func (f *freezer) thawStacked() {
	if len(f.frozen) != 0 || f.active != nil {
		return
	}
	for _, dir := range slices.Backward(f.stacked) {
		if err := f.thawFS(dir); err != nil {
			f.report("thaw %s: %v", dir.Name(), err)
		}
	}
	f.stacked = nil
}

var procDir = "/proc"

// loopFilesystems opens each filesystem mounted from a writable loop device,
// or from a device on top of one, apart from the one at disk. The latest
// mount comes first: a loop-backed filesystem can hold the file of another
// loop device, whose filesystem was then mounted after it and has to freeze
// before it. Every mount namespace counts, as a container has its own.
func loopFilesystems(disk string) []*os.File {
	devices := loopDevices()
	if len(devices) == 0 {
		return nil
	}
	seen := map[string]bool{}
	var st unix.Stat_t
	if err := unix.Stat(disk, &st); err == nil {
		seen[fmt.Sprintf("%d:%d", unix.Major(uint64(st.Dev)), unix.Minor(uint64(st.Dev)))] = true
	}
	var dirs []*os.File
	for _, table := range mountTables() {
		data, err := os.ReadFile(table.path)
		if err != nil {
			continue
		}
		for _, mount := range mountsOf(string(data), devices) {
			if seen[mount.device] {
				continue
			}
			if dir := openMount(filepath.Join(table.root, mount.point), mount.id); dir != nil {
				seen[mount.device] = true
				dirs = append(dirs, dir)
			}
		}
	}
	slices.Reverse(dirs)
	return dirs
}

// loopDevices lists the writable loop devices that have a file, with their
// partitions and the devices on top of them, each by its number and by its
// path: a filesystem with device numbers of its own has only the path in its
// mount entry.
func loopDevices() map[string]bool {
	devices := map[string]bool{}
	entries, _ := os.ReadDir(sysBlockDir)
	for _, entry := range entries {
		dir := filepath.Join(sysBlockDir, entry.Name())
		if !strings.HasPrefix(entry.Name(), "loop") {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, "loop", "backing_file")); err != nil {
			continue
		}
		if readOnly, _ := os.ReadFile(filepath.Join(dir, "ro")); strings.TrimSpace(string(readOnly)) == "1" {
			continue
		}
		addBlockDevice(devices, dir)
	}
	return devices
}

// addBlockDevice adds the device at a sysfs directory, its partitions, and
// the devices that hold any of them: a device-mapper or RAID volume.
func addBlockDevice(devices map[string]bool, dir string) {
	number, err := os.ReadFile(filepath.Join(dir, "dev"))
	path := "/dev/" + filepath.Base(dir)
	if err != nil || devices[path] {
		return
	}
	devices[path] = true
	devices[strings.TrimSpace(string(number))] = true
	holders, _ := os.ReadDir(filepath.Join(dir, "holders"))
	for _, holder := range holders {
		addBlockDevice(devices, filepath.Join(sysBlockDir, holder.Name()))
	}
	partitions, _ := os.ReadDir(dir)
	for _, partition := range partitions {
		if strings.HasPrefix(partition.Name(), filepath.Base(dir)) {
			addBlockDevice(devices, filepath.Join(dir, partition.Name()))
		}
	}
}

type mountEntry struct {
	id     uint64
	device string // major:minor
	point  string
}

// mountsOf picks the mounts of the given devices out of a mountinfo table:
//
//	36 35 7:0 / /mnt/image rw,relatime shared:1 - ext4 /dev/loop0 rw
func mountsOf(table string, devices map[string]bool) []mountEntry {
	var mounts []mountEntry
	for line := range strings.Lines(table) {
		target, source, ok := strings.Cut(line, " - ")
		targets, sources := strings.Fields(target), strings.Fields(source)
		if !ok || len(targets) < 5 || len(sources) < 2 {
			continue
		}
		if !devices[targets[2]] && !devices[sources[1]] {
			continue
		}
		id, err := strconv.ParseUint(targets[0], 10, 64)
		if err != nil {
			continue
		}
		mounts = append(mounts, mountEntry{id: id, device: targets[2], point: unescapeMountPath(targets[4])})
	}
	return mounts
}

// unescapeMountPath decodes the octal escapes mountinfo uses for spaces etc.
func unescapeMountPath(path string) string {
	var b strings.Builder
	for i := 0; i < len(path); i++ {
		if path[i] == '\\' && i+3 < len(path) {
			if c, err := strconv.ParseUint(path[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(c))
				i += 3
				continue
			}
		}
		b.WriteByte(path[i])
	}
	return b.String()
}

type mountTable struct {
	root string // reaches the namespace's root from here; "" in this namespace
	path string
}

// mountTables lists the mount table of each mount namespace once, starting
// with this one.
func mountTables() []mountTable {
	tables := []mountTable{{path: filepath.Join(procDir, "self", "mountinfo")}}
	seen := map[string]bool{}
	if namespace, err := os.Readlink(filepath.Join(procDir, "self", "ns", "mnt")); err == nil {
		seen[namespace] = true
	}
	entries, _ := os.ReadDir(procDir)
	for _, entry := range entries {
		pid := entry.Name()
		if _, err := strconv.Atoi(pid); err != nil {
			continue
		}
		namespace, err := os.Readlink(filepath.Join(procDir, pid, "ns", "mnt"))
		if err != nil || seen[namespace] {
			continue
		}
		seen[namespace] = true
		tables = append(tables, mountTable{root: filepath.Join(procDir, pid, "root"), path: filepath.Join(procDir, pid, "mountinfo")})
	}
	return tables
}

// openMount opens a mount point for FIFREEZE, unless another mount covers it
// or opening it could have an effect.
func openMount(path string, id uint64) *os.File {
	var stat unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, path, unix.AT_NO_AUTOMOUNT, unix.STATX_TYPE|unix.STATX_MNT_ID, &stat); err != nil {
		return nil
	}
	if stat.Mask&unix.STATX_MNT_ID == 0 || stat.Mnt_id != id {
		return nil
	}
	if kind := stat.Mode & unix.S_IFMT; kind != unix.S_IFDIR && kind != unix.S_IFREG {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return nil
	}
	return dir
}
