package runtime

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirectMetadataLayerMatchesCanvasOverridesWithoutFollowingImage(t *testing.T) {
	image, owned, outside := t.TempDir(), t.TempDir(), t.TempDir()
	metadata := filepath.Join(owned, "metadata")
	for _, name := range []string{".beam", "dev", "workspace", "volumes"} {
		if e := os.Symlink(outside, filepath.Join(image, name)); e != nil {
			t.Fatal(e)
		}
	}
	if e := os.WriteFile(filepath.Join(image, "proc"), []byte("file"), 0644); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"run", "tmp"} {
		if e := os.Mkdir(filepath.Join(image, name), 0711); e != nil {
			t.Fatal(e)
		}
	}
	config := []byte("actual per-VM OCI spec")
	if e := stageDirectImageMetadata(image, metadata, config); e != nil {
		t.Fatal(e)
	}
	expected := map[string]bool{"config.json": true, "dev": true, "proc": true, "sys": true, "workspace": true, "volumes": true}
	entries, e := os.ReadDir(metadata)
	if e != nil || len(entries) != len(expected) {
		t.Fatalf("metadata entries %v %v", entries, e)
	}
	for _, entry := range entries {
		if !expected[entry.Name()] {
			t.Fatal("extra bootstrap or bind data", entry.Name())
		}
		if entry.Name() != "config.json" {
			s, e := os.Lstat(filepath.Join(metadata, entry.Name()))
			if e != nil || !s.IsDir() || s.Mode().Perm() != 0755 {
				t.Fatal("override mode/type")
			}
		}
	}
	data, e := os.ReadFile(filepath.Join(metadata, "config.json"))
	if e != nil || string(data) != string(config) {
		t.Fatal("wrong spec")
	}
	s, e := os.Stat(filepath.Join(metadata, "config.json"))
	if e != nil || s.Mode().Perm() != 0644 {
		t.Fatal("wrong config mode")
	}
	entries, e = os.ReadDir(outside)
	if e != nil || len(entries) != 0 {
		t.Fatal("image symlink followed")
	}
	for _, name := range []string{"dev", "workspace", "volumes"} {
		s, e := os.Lstat(filepath.Join(image, name))
		if e != nil || s.Mode()&os.ModeSymlink == 0 {
			t.Fatal("image mutated")
		}
	}
}
func TestDirectMetadataNeverOverwritesExistingGeneratedSpec(t *testing.T) {
	image, owned := t.TempDir(), t.TempDir()
	metadata := filepath.Join(owned, "metadata")
	if e := stageDirectImageMetadata(image, metadata, []byte("initial")); e != nil {
		t.Fatal(e)
	}
	if e := stageDirectImageMetadata(image, metadata, []byte("second")); !os.IsExist(e) {
		t.Fatal("reused existing metadata tree")
	}
	data, e := os.ReadFile(filepath.Join(metadata, "config.json"))
	if e != nil || string(data) != "initial" {
		t.Fatal("fresh-only metadata guard failed to protect the first spec")
	}
}

func TestDirectMetadataRefusesPreexistingSymlink(t *testing.T) {
	owned, outside := t.TempDir(), t.TempDir()
	metadata := filepath.Join(owned, "metadata")
	if e := os.Symlink(outside, metadata); e != nil {
		t.Fatal(e)
	}
	if e := stageDirectImageMetadata(t.TempDir(), metadata, []byte("spec")); !os.IsExist(e) {
		t.Fatal("symlink metadata destination accepted")
	}
	entries, e := os.ReadDir(outside)
	if e != nil || len(entries) != 0 {
		t.Fatal("metadata destination symlink followed")
	}
}
