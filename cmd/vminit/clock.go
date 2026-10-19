//go:build linux

package main

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func restoreHostClock(nanos int64, settime func(int32, *unix.Timespec) error) error {
	if nanos <= 0 {
		return fmt.Errorf("restore guest clock: missing host time")
	}
	ts := unix.NsecToTimespec(nanos)
	if err := settime(unix.CLOCK_REALTIME, &ts); err != nil {
		return fmt.Errorf("restore guest clock: %w", err)
	}
	return nil
}
