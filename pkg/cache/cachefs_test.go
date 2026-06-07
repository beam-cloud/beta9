package cache

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestWriteReadAheadKBTimeout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "read_ahead_kb")
	if err := unix.Mkfifo(path, 0600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	defer os.Remove(path)

	start := time.Now()
	err := writeReadAheadKB(path, 128, 25*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("writeReadAheadKB took %s, want bounded timeout", elapsed)
	}
}
