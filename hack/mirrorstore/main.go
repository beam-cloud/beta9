// mirrorstore writes and checks mirror generations in a local directory with
// the Go reference implementation, for testing other writers and readers of
// the format (docs/mirror/format.md) against it. Objects are files named by
// their SHA-256.
//
//	mirrorstore write TREE STORE          store TREE as a generation; prints its manifest digest
//	mirrorstore verify STORE DIGEST TREE  check that the generation holds exactly TREE
//	mirrorstore show STORE DIGEST         list the generation
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/beam-cloud/beta9/pkg/abstractions/vm/mirror"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "mirrorstore:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	switch {
	case len(args) == 3 && args[0] == "write":
		return write(args[1], args[2])
	case len(args) == 4 && args[0] == "verify":
		return verify(args[1], args[2], args[3])
	case len(args) == 3 && args[0] == "show":
		return show(args[1], args[2])
	}
	return fmt.Errorf("usage: mirrorstore write TREE STORE | verify STORE DIGEST TREE | show STORE DIGEST")
}

type localEntry struct {
	path string
	info fs.FileInfo
}

// walk lists what a generation can hold of tree: directories, regular files
// and symlinks, parents first.
func walk(tree string) ([]localEntry, error) {
	var entries []localEntry
	err := filepath.Walk(tree, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(tree, path)
		if err != nil {
			return err
		}
		if mode := info.Mode(); mode.IsDir() || mode.IsRegular() || mode&fs.ModeSymlink != 0 {
			entries = append(entries, localEntry{filepath.ToSlash(relative), info})
		}
		return nil
	})
	return entries, err
}

func write(tree, store string) error {
	if err := os.MkdirAll(store, 0o755); err != nil {
		return err
	}
	put := func(digest mirror.Digest, data []byte) error {
		return os.WriteFile(filepath.Join(store, digest.String()), data, 0o644)
	}
	writer, err := mirror.NewWriter(mirror.DefaultWriterOptions(), func(object mirror.Object, data []byte) error {
		return put(object.Digest, data)
	})
	if err != nil {
		return err
	}
	entries, err := walk(tree)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		mode, mtime := uint32(entry.info.Mode().Perm()), entry.info.ModTime().UnixNano()
		full := filepath.Join(tree, filepath.FromSlash(entry.path))
		switch {
		case entry.path == ".":
			writer.SetRoot(mode, mtime)
		case entry.info.IsDir():
			err = writer.AddDir(entry.path, mode, mtime)
		case entry.info.Mode().IsRegular():
			var file *os.File
			if file, err = os.Open(full); err == nil {
				err = writer.AddFile(entry.path, mode, mtime, file)
				file.Close()
			}
		default:
			var target string
			if target, err = os.Readlink(full); err == nil {
				err = writer.AddSymlink(entry.path, target, mtime)
			}
		}
		if err != nil {
			return fmt.Errorf("%s: %w", entry.path, err)
		}
	}
	stored, manifest, err := writer.Finish([]byte(`{"client":"mirrorstore"}`))
	if err != nil {
		return err
	}
	if err := put(manifest.Digest(), stored); err != nil {
		return err
	}
	fmt.Println(manifest.Digest())
	return nil
}

type dirSource string

func (d dirSource) ReadObject(_ context.Context, object mirror.Object, offset int64, dest []byte) error {
	file, err := os.Open(filepath.Join(string(d), object.Digest.String()))
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if uint64(info.Size()) != object.Size {
		return fmt.Errorf("object %s is %d bytes, the manifest says %d", object.Digest, info.Size(), object.Size)
	}
	_, err = file.ReadAt(dest, offset)
	return err
}

func open(store, digest string) (*mirror.Manifest, *mirror.FS, error) {
	stored, err := os.ReadFile(filepath.Join(store, digest))
	if err != nil {
		return nil, nil, err
	}
	manifest, err := mirror.Parse(stored)
	if err != nil {
		return nil, nil, err
	}
	if manifest.Digest().String() != digest {
		return nil, nil, fmt.Errorf("manifest is %s, not %s", manifest.Digest(), digest)
	}
	// Every object must be the bytes its name says.
	for i := range manifest.Objects() {
		object := manifest.Object(i)
		data, err := os.ReadFile(filepath.Join(store, object.Digest.String()))
		if err != nil {
			return nil, nil, err
		}
		if mirror.DigestOf(data) != object.Digest || uint64(len(data)) != object.Size {
			return nil, nil, fmt.Errorf("object %s does not match its name or size", object.Digest)
		}
	}
	fsys, err := mirror.NewFS(manifest, dirSource(store), mirror.FSOptions{})
	return manifest, fsys, err
}

func verify(store, digest, tree string) error {
	manifest, fsys, err := open(store, digest)
	if err != nil {
		return err
	}
	local, err := walk(tree)
	if err != nil {
		return err
	}
	want := map[string]fs.FileInfo{}
	for _, entry := range local {
		want[entry.path] = entry.info
	}
	root := manifest.Entry(0)
	if info := want["."]; uint32(info.Mode().Perm()) != root.Mode || info.ModTime().UnixNano() != root.MtimeNs {
		return fmt.Errorf("root: mode or mtime differs")
	}
	delete(want, ".")
	var problems []string
	err = manifest.Walk(func(path string, index uint32, entry mirror.Entry) error {
		info, ok := want[path]
		if !ok {
			problems = append(problems, path+": not in the tree")
			return nil
		}
		delete(want, path)
		full := filepath.Join(tree, filepath.FromSlash(path))
		switch {
		case info.IsDir() != (entry.Kind == mirror.KindDir), info.Mode().IsRegular() != (entry.Kind == mirror.KindFile):
			problems = append(problems, path+": kind differs")
		case info.ModTime().UnixNano() != entry.MtimeNs:
			problems = append(problems, fmt.Sprintf("%s: mtime %d, want %d", path, entry.MtimeNs, info.ModTime().UnixNano()))
		case entry.Kind == mirror.KindSymlink:
			if target, err := os.Readlink(full); err != nil || target != string(manifest.Target(index)) {
				problems = append(problems, path+": symlink target differs")
			}
		case uint32(info.Mode().Perm()) != entry.Mode:
			problems = append(problems, fmt.Sprintf("%s: mode %o, want %o", path, entry.Mode, info.Mode().Perm()))
		case entry.Kind == mirror.KindFile:
			expected, err := os.ReadFile(full)
			if err != nil {
				return err
			}
			actual := make([]byte, entry.Size)
			n, err := fsys.ReadFile(context.Background(), index, 0, actual)
			if err != nil && err != io.EOF {
				return fmt.Errorf("%s: %w", path, err)
			}
			if !bytes.Equal(expected, actual[:n]) {
				problems = append(problems, path+": content differs")
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for path := range want {
		problems = append(problems, path+": missing from the generation")
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		for _, problem := range problems[:min(len(problems), 50)] {
			fmt.Fprintln(os.Stderr, problem)
		}
		return fmt.Errorf("%d differences", len(problems))
	}
	fmt.Printf("ok: %d entries, %d objects, %d bytes\n", manifest.Entries(), manifest.Objects(), manifest.TotalSize())
	return nil
}

func show(store, digest string) error {
	manifest, _, err := open(store, digest)
	if err != nil {
		return err
	}
	fmt.Printf("entries %d, objects %d, extents %d, bytes %d, manifest %d stored / %d decoded\nmeta %s\n",
		manifest.Entries(), manifest.Objects(), manifest.Extents(), manifest.TotalSize(), manifest.StoredSize(), manifest.IndexBytes(), manifest.Meta())
	return manifest.Walk(func(path string, index uint32, entry mirror.Entry) error {
		kind := map[uint8]string{mirror.KindDir: "d", mirror.KindFile: "-", mirror.KindSymlink: "l"}[entry.Kind]
		line := fmt.Sprintf("%s %04o %12d %s", kind, entry.Mode, entry.Size, path)
		if entry.Kind == mirror.KindSymlink {
			line += " -> " + string(manifest.Target(index))
		}
		fmt.Println(line)
		return nil
	})
}
