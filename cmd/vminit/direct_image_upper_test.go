package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirectUpperChangesOnlyReservedPlumbing(t *testing.T) {
	upper, outside := t.TempDir(), t.TempDir()
	if e := os.WriteFile(filepath.Join(upper, "config.json"), []byte("application config"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(outside, "keep"), []byte("safe"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink(outside, filepath.Join(upper, ".beam")); e != nil {
		t.Fatal(e)
	}
	if e := prepareDirectImageUpper(upper); e != nil {
		t.Fatal(e)
	}
	entries, e := os.ReadDir(upper)
	if e != nil || len(entries) != 2 {
		t.Fatalf("upper entries %v %v", entries, e)
	}
	s, e := os.Lstat(filepath.Join(upper, ".beam"))
	if e != nil || !s.IsDir() || s.Mode().Perm() != 0755 {
		t.Fatal("reserved path is not a real directory")
	}
	data, e := os.ReadFile(filepath.Join(upper, "config.json"))
	if e != nil || string(data) != "application config" {
		t.Fatal("application upper config changed")
	}
	entries, e = os.ReadDir(outside)
	if e != nil || len(entries) != 1 || entries[0].Name() != "keep" {
		t.Fatal("reserved symlink was followed")
	}
}
func TestDirectFreshUpperHasNoGeneratedSnapshotEntries(t *testing.T) {
	upper := t.TempDir()
	if e := prepareDirectImageUpper(upper); e != nil {
		t.Fatal(e)
	}
	entries, e := os.ReadDir(upper)
	if e != nil || len(entries) != 1 || entries[0].Name() != ".beam" {
		t.Fatalf("generated entries leaked into upper: %v %v", entries, e)
	}
	for _, name := range []string{"config.json", "workspace", "volumes", "dev", "proc", "sys", "run", "tmp"} {
		if _, e := os.Lstat(filepath.Join(upper, name)); !os.IsNotExist(e) {
			t.Fatal("generated entry present in snapshot delta", name)
		}
	}
}
