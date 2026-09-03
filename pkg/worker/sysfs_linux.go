//go:build linux

package worker

import (
	"errors"
	"os"

	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

const sysfsPath = "/sys"

// ensureWritableSysfs remounts /sys read-write when the container runtime
// mounted it read-only. The FUSE mounts the worker creates (cachefs,
// workspace volumes) tune their read-ahead window through
// /sys/class/bdi/<dev>/read_ahead_kb; FUSE INIT can only lower that window
// from the kernel's 128 KiB default, never raise it, so without a writable
// sysfs every FUSE read is capped at 128 KiB and cached reads run several
// times slower than the disk cache can serve them. The remount only changes
// this mount namespace and needs the CAP_SYS_ADMIN the worker already holds
// for mounting; failure is logged and otherwise ignored.
func ensureWritableSysfs() {
	var st unix.Statfs_t
	if err := unix.Statfs(sysfsPath, &st); err != nil || st.Flags&unix.ST_RDONLY == 0 {
		return
	}
	flags := uintptr(unix.MS_REMOUNT | unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC)
	if err := unix.Mount("", sysfsPath, "", flags, ""); err != nil {
		if !errors.Is(err, os.ErrPermission) && !errors.Is(err, unix.EPERM) {
			log.Warn().Err(err).Msg("could not remount /sys read-write; FUSE read-ahead tuning unavailable")
		}
		return
	}
	log.Info().Msg("remounted /sys read-write for FUSE read-ahead tuning")
}
