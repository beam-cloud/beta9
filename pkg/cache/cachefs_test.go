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

func TestSpliceSafeMaxWriteFor(t *testing.T) {
	const page = 4096
	cases := []struct{ pipeMax, want int }{
		{1 << 20, (1 << 20) - 2*page}, // stock fs.pipe-max-size: 1 MiB reads must shrink
		{4 << 20, (4 << 20) - 2*page},
		{2 * page, 0},
		{0, 0},
		{(1 << 20) + 100, (1 << 20) - 2*page},
	}
	for _, c := range cases {
		if got := spliceSafeMaxWriteFor(c.pipeMax, page); got != c.want {
			t.Fatalf("spliceSafeMaxWriteFor(%d) = %d, want %d", c.pipeMax, got, c.want)
		}
		// header (16) + payload + one page must fit in the pipe
		if c.want > 0 && 16+c.want+page > c.pipeMax {
			t.Fatalf("pipeMax %d: %d does not leave room for header and extra page", c.pipeMax, c.want)
		}
	}
}
