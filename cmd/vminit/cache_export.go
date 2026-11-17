//go:build linux

package main

import (
	"archive/tar"
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"golang.org/x/sys/unix"
)

// overlayMetacopy marks a file of the root disk that has the attributes of a
// file of the image and none of its data (microvm.Spec.RootMetacopy).
const overlayMetacopy = "trusted.overlay.metacopy"

// exportCache writes what the workload added to the VM's cache directories
// into the file the host collects once the VM is gone. It runs as the guest
// shuts down, after the workload and before the exit is reported, so nothing
// is still writing them. A VM without an export does nothing here.
func exportCache() {
	vm, err := readVMSpec()
	if err != nil || vm.CacheExport == nil || len(vm.CacheExport.Paths) == 0 || vm.CacheExport.File == "" {
		return
	}
	start := time.Now()
	upper := filepath.Join("/", microvm.DiskMount, microvm.DiskOverlayUpper)
	stats, err := writeCacheExport(vm.CacheExport.File, "/", upper, vm.CacheExport.Paths)
	if err != nil {
		logf("cache export: %v", err)
		return
	}
	logf("cache export: %d new files (%d bytes), %d used, in %s", stats.files, stats.bytes, stats.touched, time.Since(start).Round(time.Millisecond))
}

type cacheExportStats struct {
	files, touched int
	bytes          int64
}

// writeCacheExport writes file: a tar of the regular files that upper, the
// writable layer of root, has under dirs. A file the workload wrote is there
// with its content. One it only used is there as a header, which carries the
// time of its use (microvm.CacheExportTouched). Nothing else is exported:
// no link, device or deletion.
//
// The file has its name only once it is complete, so a guest that is cut
// short leaves nothing the host would take for an export. Nor does a
// workload that wrote no file: the host has nothing to add.
func writeCacheExport(file, root, upper string, dirs []string) (stats cacheExportStats, err error) {
	partial := file + ".partial"
	out, err := os.OpenFile(partial, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return stats, err
	}
	defer func() {
		if err != nil {
			out.Close()
			os.Remove(partial)
		}
	}()
	buffered := bufio.NewWriterSize(out, 1<<20)
	tw := tar.NewWriter(buffered)
	written := map[string]bool{}
	for _, dir := range dirs {
		if err := writeCacheDir(tw, root, upper, dir, written, &stats); err != nil {
			return stats, err
		}
	}
	if err := tw.Close(); err != nil {
		return stats, err
	}
	if err := buffered.Flush(); err != nil {
		return stats, err
	}
	if err := out.Close(); err != nil {
		return stats, err
	}
	if stats.files == 0 {
		return stats, os.Remove(partial)
	}
	return stats, os.Rename(partial, file)
}

func writeCacheDir(tw *tar.Writer, root, upper, dir string, written map[string]bool, stats *cacheExportStats) error {
	dir = strings.Trim(filepath.Clean("/"+dir), "/")
	if dir == "" {
		return nil
	}
	top := filepath.Join(upper, dir)
	if info, err := os.Lstat(top); err != nil || !info.IsDir() {
		// The workload wrote nothing there.
		return nil
	}
	// The directories above a cache directory, as the workload saw them: the
	// host makes the ones its disk does not have yet.
	parts := strings.Split(dir, "/")
	for i := 1; i < len(parts); i++ {
		name := strings.Join(parts[:i], "/")
		if written[name] {
			continue
		}
		info, err := os.Lstat(filepath.Join(root, name))
		if err != nil || !info.IsDir() {
			return nil
		}
		if err := writeCacheHeader(tw, name+"/", info, nil); err != nil {
			return err
		}
		written[name] = true
	}
	return filepath.WalkDir(top, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(upper, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		switch {
		case info.IsDir():
			if written[name] {
				return nil
			}
			written[name] = true
			return writeCacheHeader(tw, name+"/", info, nil)
		case !info.Mode().IsRegular():
			return nil
		}
		if _, err := getXattr(path, overlayMetacopy); err == nil {
			stats.touched++
			return writeCacheHeader(tw, name, info, map[string]string{microvm.CacheExportTouched: "1"})
		} else if !errors.Is(err, unix.ENODATA) && !errors.Is(err, unix.ENOTSUP) {
			return err
		}
		if err := writeCacheHeader(tw, name, info, nil); err != nil {
			return err
		}
		stats.files++
		stats.bytes += info.Size()
		if info.Size() == 0 {
			return nil
		}
		source, err := os.Open(path)
		if err != nil {
			return err
		}
		defer source.Close()
		n, err := io.CopyN(tw, source, info.Size())
		if err == io.EOF {
			_, err = io.CopyN(tw, zeroReader{}, info.Size()-n)
		}
		return err
	})
}

func writeCacheHeader(tw *tar.Writer, name string, info os.FileInfo, records map[string]string) error {
	hdr, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return err
	}
	hdr.Name = name
	hdr.Format = tar.FormatPAX
	hdr.Uname, hdr.Gname = "", ""
	hdr.PAXRecords = records
	if records != nil {
		hdr.Size = 0
	}
	return tw.WriteHeader(hdr)
}
