//go:build linux

package worker

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"golang.org/x/sys/unix"
)

func TestLinuxSecureOpenRegularRejectsHeldFIFOAndClosesLeafExactlyOnce(t *testing.T) {
	root := t.TempDir()
	fifo := filepath.Join(root, "journal.json")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}

	var leafCloses atomic.Int64
	ops := linuxStateVolumeSecurePathOps{closeLeafFD: func(fd int) error {
		leafCloses.Add(1)
		return unix.Close(fd)
	}}
	stop := make(chan struct{})
	churnErr := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				churnErr <- nil
				return
			default:
			}
			file, err := os.Open("/dev/null")
			if err != nil {
				churnErr <- err
				return
			}
			if err := file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				churnErr <- err
				return
			}
		}
	}()

	const attempts = 256
	for i := 0; i < attempts; i++ {
		if file, err := ops.OpenRegular(fifo); err == nil || file != nil {
			t.Fatalf("held FIFO was accepted as a regular journal: file=%v err=%v", file, err)
		}
	}
	close(stop)
	if err := <-churnErr; err != nil {
		t.Fatalf("concurrent descriptor churn observed an unrelated close: %v", err)
	}
	if got := leafCloses.Load(); got != attempts {
		t.Fatalf("rejected leaf descriptors closed %d times, want exactly %d", got, attempts)
	}
}
