//go:build linux

package main

import (
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestRestoreHostClock(t *testing.T) {
	// Neither the host clock nor monotonic timers may be changed by this test.
	now := time.Date(2026, 10, 18, 18, 0, 0, 123456789, time.UTC).UnixNano()
	var restored int64
	err := restoreHostClock(now, func(clock int32, ts *unix.Timespec) error {
		if clock != unix.CLOCK_REALTIME {
			t.Fatalf("changed clock %d instead of wall time", clock)
		}
		restored = ts.Nano()
		return nil
	})
	if err != nil || restored != now {
		t.Fatalf("restored %d: %v; want %d", restored, err, now)
	}
	denied := unix.EPERM
	if err := restoreHostClock(now, func(int32, *unix.Timespec) error { return denied }); !errors.Is(err, denied) {
		t.Fatalf("clock failure was lost: %v", err)
	}
	for _, invalid := range []int64{0, -1} {
		if err := restoreHostClock(invalid, func(int32, *unix.Timespec) error {
			t.Fatal("invalid host time reached the clock")
			return nil
		}); err == nil {
			t.Fatal("accepted missing host time")
		}
	}
}
