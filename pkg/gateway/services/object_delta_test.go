package gatewayservices

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeZip(t *testing.T, path string, entries map[string]string, method uint16) *os.File {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	w := zip.NewWriter(f)
	for name, content := range entries {
		hdr := &zip.FileHeader{Name: name, Method: method}
		hdr.SetMode(0o755)
		fw, err := w.CreateHeader(hdr)
		require.NoError(t, err)
		_, err = fw.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	return f
}

func readZip(t *testing.T, data []byte) map[string]string {
	t.Helper()
	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	out := map[string]string{}
	for _, f := range r.File {
		rc, err := f.Open()
		require.NoError(t, err)
		b, err := io.ReadAll(rc)
		require.NoError(t, err)
		rc.Close()
		out[f.Name] = string(b)
	}
	return out
}

func TestMergeZipArchivesReplacesAddsAndRemoves(t *testing.T) {
	dir := t.TempDir()
	base := writeZip(t, filepath.Join(dir, "base.zip"), map[string]string{
		"app/a.py":    "a v1",
		"app/b.py":    "b v1",
		"app/gone.py": "gone",
		"README":      "readme",
	}, zip.Store)
	delta := writeZip(t, filepath.Join(dir, "delta.zip"), map[string]string{
		"app/a.py":   "a v2",
		"app/new.py": "new",
	}, zip.Deflate)

	var out bytes.Buffer
	size, err := mergeZipArchives(base, delta, []string{"app/gone.py"}, &out)
	require.NoError(t, err)
	require.Equal(t, int64(out.Len()), size)

	got := readZip(t, out.Bytes())
	require.Equal(t, map[string]string{
		"app/a.py":   "a v2",
		"app/b.py":   "b v1",
		"app/new.py": "new",
		"README":     "readme",
	}, got)

	// Modes and compression methods survive the raw copy.
	r, err := zip.NewReader(bytes.NewReader(out.Bytes()), int64(out.Len()))
	require.NoError(t, err)
	for _, f := range r.File {
		require.Equal(t, os.FileMode(0o755), f.Mode().Perm(), f.Name)
		if f.Name == "app/a.py" || f.Name == "app/new.py" {
			require.Equal(t, zip.Deflate, f.Method, f.Name)
		} else {
			require.Equal(t, zip.Store, f.Method, f.Name)
		}
	}
}

func TestMergeZipArchivesWithPythonZipfile(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	script := `
import sys, zipfile
base, delta = sys.argv[1], sys.argv[2]
with zipfile.ZipFile(base, "w") as z:
    z.writestr("f0000.bin", b"\x00" * 100000)
    z.writestr("f0001.bin", b"\x01" * 100000)
with zipfile.ZipFile(delta, "w") as z:
    z.writestr("f0000.bin", b"\x02" * 100000)
`
	cmd := exec.Command(python, "-c", script, filepath.Join(dir, "base.zip"), filepath.Join(dir, "delta.zip"))
	outb, err := cmd.CombinedOutput()
	require.NoError(t, err, string(outb))

	base, err := os.Open(filepath.Join(dir, "base.zip"))
	require.NoError(t, err)
	defer base.Close()
	delta, err := os.Open(filepath.Join(dir, "delta.zip"))
	require.NoError(t, err)
	defer delta.Close()

	var out bytes.Buffer
	_, err = mergeZipArchives(base, delta, nil, &out)
	require.NoError(t, err)
	got := readZip(t, out.Bytes())
	require.Len(t, got, 2)
	require.Equal(t, bytes.Repeat([]byte{2}, 100000), []byte(got["f0000.bin"]))
	require.Equal(t, bytes.Repeat([]byte{1}, 100000), []byte(got["f0001.bin"]))

	// And Python can read what Go merged.
	mergedPath := filepath.Join(dir, "merged.zip")
	require.NoError(t, os.WriteFile(mergedPath, out.Bytes(), 0o644))
	check := exec.Command(python, "-c", `
import sys, zipfile
z = zipfile.ZipFile(sys.argv[1]); assert z.testzip() is None
assert z.read("f0000.bin") == b"\x02" * 100000
print("ok")`, mergedPath)
	outb, err = check.CombinedOutput()
	require.NoError(t, err, string(outb))
	require.Contains(t, string(outb), "ok")
}
