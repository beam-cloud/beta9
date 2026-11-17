package worker

import (
	"archive/tar"
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"golang.org/x/sys/unix"
)

// A cache export is a tar a guest wrote: nothing in it is trusted. It may add
// regular files below the layer's directories, and the directories that hold
// them, and nothing else. A file never gets a set-id bit, and no name leaves
// the layer's filesystem (os.Root).

type cacheMergeStats struct {
	files   int   // files added or replaced
	bytes   int64 // their size
	touched int   // files the export only used: their time is brought forward
	refused int   // entries the layer does not take
	full    bool  // the layer had no room for the rest
}

const cacheMergeSuffix = ".beam-partial"

type cacheMerge struct {
	root    *os.Root
	allowed []string
	// owners are the owners of the directories the export has, by name.
	owners map[string]int
	free   int64
	stats  cacheMergeStats
}

// mergeCacheExport adds the entries of the tar at export to the filesystem
// at dir, which holds the layer's directories dirs at the paths they have in
// a guest. It stops adding once less than minFree would be left.
func mergeCacheExport(export, dir string, dirs []string, minFree int64) (cacheMergeStats, error) {
	file, err := os.OpenFile(export, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return cacheMergeStats{}, err
	}
	defer file.Close()
	root, err := os.OpenRoot(dir)
	if err != nil {
		return cacheMergeStats{}, err
	}
	defer root.Close()

	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil {
		return cacheMergeStats{}, err
	}
	m := &cacheMerge{root: root, owners: map[string]int{}, free: int64(fs.Bavail)*int64(fs.Bsize) - minFree}
	for _, allowed := range dirs {
		if name, ok := cacheEntryName(allowed); ok {
			m.allowed = append(m.allowed, name)
		}
	}

	reader := tar.NewReader(bufio.NewReaderSize(file, 1<<20))
	for !m.stats.full {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			// What a truncated export had before its end is whole.
			return m.stats, fmt.Errorf("read cache export: %w", err)
		}
		if err := m.add(header, reader); err != nil {
			return m.stats, fmt.Errorf("add %q: %w", header.Name, err)
		}
	}
	// Sealing the volume freezes this filesystem, which writes it out.
	return m.stats, nil
}

// cacheEntryName is a tar entry's name as a path below the layer's root.
func cacheEntryName(name string) (string, bool) {
	if name == "" || strings.ContainsRune(name, 0) {
		return "", false
	}
	for _, part := range strings.Split(strings.Trim(name, "/"), "/") {
		if part == ".." {
			return "", false
		}
	}
	clean := strings.TrimPrefix(path.Clean("/"+name), "/")
	return clean, clean != "" && !strings.HasSuffix(clean, cacheMergeSuffix)
}

// within reports whether name is below one of the layer's directories, or,
// with above, is one of them or a directory that leads to one.
func (m *cacheMerge) within(name string, above bool) bool {
	for _, allowed := range m.allowed {
		if strings.HasPrefix(name, allowed+"/") {
			return true
		}
		if above && (name == allowed || strings.HasPrefix(allowed, name+"/")) {
			return true
		}
	}
	return false
}

func (m *cacheMerge) add(header *tar.Header, content io.Reader) error {
	name, ok := cacheEntryName(header.Name)
	if !ok || header.Uid < 0 || header.Gid < 0 {
		m.stats.refused++
		return nil
	}
	switch header.Typeflag {
	case tar.TypeDir:
		if !m.within(name, true) {
			m.stats.refused++
			return nil
		}
		return m.addDir(name, header)
	case tar.TypeReg:
		if !m.within(name, false) {
			m.stats.refused++
			return nil
		}
		if _, touched := header.PAXRecords[microvm.CacheExportTouched]; touched {
			// The guest used the file and left its content as it was.
			if info, err := m.root.Lstat(name); err == nil && info.Mode().IsRegular() {
				if err := m.root.Chtimes(name, header.ModTime, header.ModTime); err != nil {
					return err
				}
				m.stats.touched++
			}
			return nil
		}
		return m.addFile(name, header, content)
	default:
		m.stats.refused++
		return nil
	}
}

// addDir makes a directory the layer does not have. The first export to
// bring a directory decides what it is: the image's own directories show
// through the layer, and a directory only the layer has is what every VM
// after this one gets.
//
// So a directory must belong to the owner of the directory it is in, unless
// that one is root's. A job that made its cache directory as root would
// otherwise leave it to jobs that cannot write there.
func (m *cacheMerge) addDir(name string, header *tar.Header) error {
	m.owners[name] = header.Uid
	if info, err := m.root.Lstat(name); err == nil {
		if !info.IsDir() {
			m.stats.refused++
		}
		return nil
	}
	if parent, known := m.owners[path.Dir(name)]; known && parent != 0 && parent != header.Uid {
		delete(m.owners, name)
		m.stats.refused++
		return nil
	}
	if err := m.root.Mkdir(name, 0o700); err != nil {
		return m.skipped(err)
	}
	if err := m.root.Lchown(name, header.Uid, header.Gid); err != nil {
		return err
	}
	return m.root.Chmod(name, os.FileMode(header.Mode)&os.ModePerm)
}

func (m *cacheMerge) addFile(name string, header *tar.Header, content io.Reader) error {
	// A filesystem block is the least a file takes.
	need := (header.Size + 4095) &^ 4095
	if need > m.free {
		m.stats.full = true
		return nil
	}
	if info, err := m.root.Lstat(name); err == nil && !info.Mode().IsRegular() {
		m.stats.refused++
		return nil
	}
	partial := name + cacheMergeSuffix
	_ = m.root.Remove(partial)
	file, err := m.root.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		// A file whose directory the layer did not take goes with it.
		return m.skipped(err)
	}
	written, err := io.Copy(file, io.LimitReader(content, header.Size))
	if err == nil && written != header.Size {
		err = io.ErrUnexpectedEOF
	}
	if err == nil {
		err = file.Chown(header.Uid, header.Gid)
	}
	if err == nil {
		err = file.Chmod(os.FileMode(header.Mode) & os.ModePerm)
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = m.root.Chtimes(partial, header.ModTime, header.ModTime)
	}
	if err == nil {
		err = m.root.Rename(partial, name)
	}
	if err != nil {
		_ = m.root.Remove(partial)
		return m.skipped(err)
	}
	m.free -= need
	m.stats.files++
	m.stats.bytes += header.Size
	return nil
}

// skipped counts an entry the filesystem would not take and goes on with the
// next, or stops the merge when the filesystem is full. Any other error ends
// the merge.
func (m *cacheMerge) skipped(err error) error {
	switch {
	case errors.Is(err, syscall.ENOSPC), errors.Is(err, syscall.EDQUOT):
		m.stats.full = true
	case errors.Is(err, os.ErrNotExist), errors.Is(err, os.ErrExist), errors.Is(err, syscall.ENOTDIR),
		errors.Is(err, syscall.EISDIR), errors.Is(err, syscall.ENAMETOOLONG), errors.Is(err, syscall.ELOOP):
		m.stats.refused++
	default:
		return err
	}
	return nil
}
