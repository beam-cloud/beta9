//go:build ignore

// Build the worker with a pinned dependency patch in a private module copy.
// The repository go.mod, go.sum and shared Go module cache stay unchanged.
package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	if err := prepare(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func prepare() error {
	if len(os.Args) != 2 || !strings.HasSuffix(os.Args[1], ".mod") {
		return fmt.Errorf("usage: go run hack/worker-build-modfile.go <worker.mod>")
	}
	data, err := exec.Command("go", "list", "-m", "-json", "github.com/beam-cloud/clip").Output()
	if err != nil {
		return err
	}
	var module struct{ Version, Dir string }
	if err := json.Unmarshal(data, &module); err != nil {
		return err
	}
	if module.Version != "v0.0.0-20261014192325-93ffd805f3ff" || module.Dir == "" {
		return fmt.Errorf("review the image-FUSE patch for clip version %q", module.Version)
	}
	output, err := filepath.Abs(os.Args[1])
	if err != nil {
		return err
	}
	patch, err := filepath.Abs("docker/patches/clip/no-open-rpc.patch")
	if err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(output), "clip-worker-module-")
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(tmp)
		}
	}()
	err = filepath.WalkDir(module.Dir, func(source string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(module.Dir, source)
		if err != nil {
			return err
		}
		target := filepath.Join(tmp, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unexpected module entry %q", rel)
		}
		data, err := os.ReadFile(source)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode().Perm()|0200)
	})
	if err != nil {
		return err
	}
	for _, args := range [][]string{{"apply", "--check", patch}, {"apply", patch}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = tmp
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("apply image-FUSE patch: %w: %s", err, out)
		}
	}
	for source, target := range map[string]string{"go.mod": output, "go.sum": strings.TrimSuffix(output, ".mod") + ".sum"} {
		data, err := os.ReadFile(source)
		if err != nil {
			return err
		}
		if err := os.WriteFile(target, data, 0644); err != nil {
			return err
		}
	}
	cmd := exec.Command("go", "mod", "edit", "-modfile", output, "-replace", "github.com/beam-cloud/clip="+tmp)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("prepare worker modfile: %w: %s", err, out)
	}
	keep = true
	fmt.Println("Prepared private worker dependency for clip", module.Version)
	return nil
}
