package common

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractObjectFile(t *testing.T) {
	objectPath := filepath.Join(t.TempDir(), "object.zip")
	archive, err := os.Create(objectPath)
	if err != nil {
		t.Fatal(err)
	}

	zipWriter := zip.NewWriter(archive)
	fileWriter, err := zipWriter.Create("nested/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fileWriter.Write([]byte("contents")); err != nil {
		t.Fatal(err)
	}
	if err := zipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	destPath := filepath.Join(t.TempDir(), "extracted")
	if err := ExtractObjectFile(t.Context(), objectPath, destPath); err != nil {
		t.Fatal(err)
	}

	contents, err := os.ReadFile(filepath.Join(destPath, "nested", "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "contents" {
		t.Fatalf("unexpected contents: %q", contents)
	}
}

func TestExtractObjectFileRejectsPathTraversal(t *testing.T) {
	objectPath := filepath.Join(t.TempDir(), "object.zip")
	archive, err := os.Create(objectPath)
	if err != nil {
		t.Fatal(err)
	}

	zipWriter := zip.NewWriter(archive)
	if _, err := zipWriter.Create("../escape.txt"); err != nil {
		t.Fatal(err)
	}
	if err := zipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	rootPath := t.TempDir()
	destPath := filepath.Join(rootPath, "extracted")
	if err := ExtractObjectFile(t.Context(), objectPath, destPath); err == nil {
		t.Fatal("expected path traversal error")
	}
	if _, err := os.Stat(filepath.Join(rootPath, "escape.txt")); !os.IsNotExist(err) {
		t.Fatalf("escape file was created: %v", err)
	}
}
