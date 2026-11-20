//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"golang.org/x/sys/unix"
)

const (
	testGPURelease = "6.18.64"
	testGPUVersion = "580.126.18"
)

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// testGPUDriver lays out what a worker has beside its kernel.
func testGPUDriver(t *testing.T) string {
	t.Helper()
	driver := t.TempDir()
	modules := filepath.Join(driver, microvm.GPUDriverModules, testGPURelease)
	writeTestFile(t, filepath.Join(modules, "modules.dep"), "video/nvidia-uvm.ko: video/nvidia.ko\nvideo/nvidia.ko:\n")
	writeTestFile(t, filepath.Join(modules, "video/nvidia.ko"), "module")
	writeTestFile(t, filepath.Join(driver, microvm.GPUDriverFirmware, "nvidia", testGPUVersion, "gsp_ga10x.bin"), "firmware")
	return driver
}

// testGPURoot is an image whose /lib is the usual link, with the libraries
// of an NVIDIA driver in libraries.
func testGPURoot(t *testing.T, lib, libraries, version string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "usr/lib"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(lib, filepath.Join(root, "lib")); err != nil {
		t.Fatal(err)
	}
	if version != "" {
		writeTestFile(t, filepath.Join(root, libraries, "libnvidia-ml.so."+version), "library")
		writeTestFile(t, filepath.Join(root, libraries, "libc.so.6"), "library")
	}
	return root
}

func TestGPUDriverGoesOnlyToARootThatUsesIt(t *testing.T) {
	driver := testGPUDriver(t)
	const debian = "usr/lib/x86_64-linux-gnu"

	for _, libraries := range []string{debian, "usr/lib64", "usr/local/nvidia/lib64"} {
		root := testGPURoot(t, "usr/lib", libraries, testGPUVersion)
		want := []gpuDriverBind{
			{filepath.Join(driver, "modules", testGPURelease), filepath.Join(root, "usr/lib/modules", testGPURelease)},
			{filepath.Join(driver, "firmware"), filepath.Join(root, "usr/lib/firmware", testGPURelease)},
		}
		// Directories an earlier boot left empty are the root's to give.
		for _, state := range []string{"no directories for the kernel", "empty ones left by an earlier boot"} {
			got, err := gpuDriverBinds(root, driver, testGPURelease)
			if err != nil || !slices.Equal(got, want) {
				t.Fatalf("root with libraries in %s and %s: %v, %v", libraries, state, got, err)
			}
			for _, bind := range want {
				if err := os.MkdirAll(bind.target, 0o755); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	// A root with a driver of its own loads it as it always has.
	for name, file := range map[string]string{
		"modules":  filepath.Join("usr/lib/modules", testGPURelease, "modules.dep"),
		"firmware": filepath.Join("usr/lib/firmware", testGPURelease, "nvidia/595.99.02/gsp_ga10x.bin"),
	} {
		root := testGPURoot(t, "usr/lib", debian, testGPUVersion)
		writeTestFile(t, filepath.Join(root, file), "")
		if got, err := gpuDriverBinds(root, driver, testGPURelease); got != nil || err != nil {
			t.Fatalf("root with its own %s for the kernel: %v, %v", name, got, err)
		}
	}

	// Modules of one version do not work with the libraries of another, and
	// a root whose libraries are elsewhere may load modules from elsewhere.
	for name, version := range map[string]string{"another driver's libraries": "595.99.02", "no NVIDIA libraries": ""} {
		got, err := gpuDriverBinds(testGPURoot(t, "usr/lib", debian, version), driver, testGPURelease)
		if got != nil || err == nil || !strings.Contains(err.Error(), "NVIDIA "+testGPUVersion) {
			t.Fatalf("root with %s: %v, %v", name, got, err)
		}
	}
	// 580.126.18 is not 580.126.181, nor a file that only mentions it.
	root := testGPURoot(t, "usr/lib", debian, testGPUVersion+"1")
	writeTestFile(t, filepath.Join(root, debian, "libnvidia-ml.so."+testGPUVersion+".bak"), "")
	if got, err := gpuDriverBinds(root, driver, testGPURelease); got != nil || err == nil {
		t.Fatalf("root with libraries named like the driver's: %v, %v", got, err)
	}

	got, err := gpuDriverBinds(testGPURoot(t, "usr/lib", debian, testGPUVersion), driver, "6.18.70")
	if got != nil || err == nil || !strings.Contains(err.Error(), "no modules for kernel 6.18.70") {
		t.Fatalf("kernel the host has no driver for: %v, %v", got, err)
	}
	if got, err := gpuDriverBinds(testGPURoot(t, t.TempDir(), debian, testGPUVersion), driver, testGPURelease); got != nil || err == nil {
		t.Fatalf("lib that leaves the root: %v, %v", got, err)
	}
	if took, err := installGPUDriver(testGPURoot(t, "usr/lib", debian, testGPUVersion), &microvm.Spec{}); took || err != nil {
		t.Fatalf("guest the host offers no driver: %v, %v", took, err)
	}
}

func TestGPUDriverStaysTheHostsFiles(t *testing.T) {
	driver := testGPUDriver(t)
	root := testGPURoot(t, "usr/lib", "usr/lib/x86_64-linux-gnu", testGPUVersion)
	modules := filepath.Join(root, "lib/modules", testGPURelease)
	firmware := filepath.Join(root, "lib/firmware", testGPURelease)
	took, err := bindGPUDriver(root, driver, testGPURelease)
	if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) {
		t.Skip("requires privileged mounts")
	}
	t.Cleanup(func() {
		_ = unix.Unmount(modules, unix.MNT_DETACH)
		_ = unix.Unmount(firmware, unix.MNT_DETACH)
	})
	if err != nil || !took {
		t.Fatalf("took the driver: %v, %v", took, err)
	}

	// Where modprobe and the kernel's firmware loader look.
	for path, want := range map[string]string{
		filepath.Join(modules, "video/nvidia.ko"):                          "module",
		filepath.Join(firmware, "nvidia", testGPUVersion, "gsp_ga10x.bin"): "firmware",
	} {
		if data, err := os.ReadFile(path); err != nil || string(data) != want {
			t.Fatalf("%s: %q, %v", path, data, err)
		}
	}
	for _, dir := range []string{modules, firmware} {
		if err := os.WriteFile(filepath.Join(dir, "written"), nil, 0o644); !errors.Is(err, unix.EROFS) {
			t.Fatalf("%s can be changed: %v", dir, err)
		}
	}
	if _, err := os.Stat(filepath.Join(driver, microvm.GPUDriverModules, testGPURelease, "written")); !os.IsNotExist(err) {
		t.Fatalf("the host's files were changed: %v", err)
	}
	// The root has what it was given, and is not given it twice.
	if took, err := bindGPUDriver(root, driver, testGPURelease); took || err != nil {
		t.Fatalf("root that has the driver: %v, %v", took, err)
	}
}

func TestModuleFilesFollowTheModulesTheyNeed(t *testing.T) {
	dir := t.TempDir()
	// As depmod writes it for the driver.
	writeTestFile(t, filepath.Join(dir, "modules.dep"), `video/nvidia-modeset.ko: video/nvidia.ko
video/nvidia-peermem.ko:
video/nvidia-drm.ko: video/nvidia-modeset.ko video/nvidia.ko
video/nvidia-uvm.ko: video/nvidia.ko
video/nvidia.ko:
/opt/extra/outside.ko: video/nvidia.ko
`)
	in := func(names ...string) []string {
		for i, name := range names {
			names[i] = filepath.Join(dir, "video", name)
		}
		return names
	}
	for _, test := range []struct {
		names []string
		want  []string
	}{
		{gpuDriverModules, in("nvidia.ko", "nvidia-uvm.ko")},
		{[]string{"nvidia_uvm", "nvidia"}, in("nvidia.ko", "nvidia-uvm.ko")},
		{[]string{"nvidia_drm"}, in("nvidia.ko", "nvidia-modeset.ko", "nvidia-drm.ko")},
		{[]string{"nvidia_peermem"}, in("nvidia-peermem.ko")},
		{[]string{"outside"}, append(in("nvidia.ko"), "/opt/extra/outside.ko")},
	} {
		got, err := moduleFiles(dir, test.names...)
		if err != nil || !slices.Equal(got, test.want) {
			t.Fatalf("%v: %v, %v", test.names, got, err)
		}
	}
	if got, err := moduleFiles(dir, "nvidia", "nouveau"); got != nil || err == nil || !strings.Contains(err.Error(), "no module nouveau") {
		t.Fatalf("module the kernel has none of: %v, %v", got, err)
	}
	if got, err := moduleFiles(t.TempDir(), "nvidia"); got != nil || !os.IsNotExist(err) {
		t.Fatalf("directory without an index: %v, %v", got, err)
	}
}

func TestGPUDriverLoadsItsModulesInOrder(t *testing.T) {
	driver := testGPUDriver(t)
	modules := filepath.Join(driver, microvm.GPUDriverModules, testGPURelease)
	var loaded []string
	d := &gpuDriver{modules: modules, load: func(file string) error {
		loaded = append(loaded, strings.TrimPrefix(file, modules+"/"))
		return nil
	}}
	if err := d.loadModules(); err != nil || !slices.Equal(loaded, []string{"video/nvidia.ko", "video/nvidia-uvm.ko"}) {
		t.Fatalf("loaded %v, %v", loaded, err)
	}

	// Nothing that needs a module is loaded without it.
	loaded = nil
	d.load = func(file string) error {
		loaded = append(loaded, filepath.Base(file))
		return unix.ENOEXEC
	}
	if err := d.loadModules(); !errors.Is(err, unix.ENOEXEC) || !strings.Contains(err.Error(), "load nvidia.ko") || len(loaded) != 1 {
		t.Fatalf("module built for another kernel: loaded %v, %v", loaded, err)
	}
}

func TestGPUDriverWaitsForItsModules(t *testing.T) {
	// A guest that did not take the host's driver has nothing to wait for
	// and nothing to name.
	var none *gpuDriver
	none.await(time.Hour)
	none.nameDevices()

	d := &gpuDriver{loaded: make(chan struct{})}
	begin := time.Now()
	d.await(20 * time.Millisecond)
	if waited := time.Since(begin); waited < 20*time.Millisecond {
		t.Fatalf("gave up on the modules after %s", waited)
	}
	time.AfterFunc(20*time.Millisecond, func() { close(d.loaded) })
	begin = time.Now()
	d.await(time.Minute)
	if waited := time.Since(begin); waited < 20*time.Millisecond || waited > 30*time.Second {
		t.Fatalf("waited %s for modules that took 20ms", waited)
	}
}

func TestGPUDriverNamesItsDevices(t *testing.T) {
	proc, dev := t.TempDir(), t.TempDir()
	made := map[string][2]uint32{}
	d := &gpuDriver{proc: proc, dev: dev, mknod: func(path string, major, minor uint32) error {
		made[strings.TrimPrefix(path, dev+"/")] = [2]uint32{major, minor}
		return nil
	}}
	if err := d.makeDevices(); err == nil || len(made) != 0 {
		t.Fatalf("without a driver: made %v, %v", made, err)
	}

	writeTestFile(t, filepath.Join(proc, "devices"), "Character devices:\n  1 mem\n195 nvidia\n195 nvidiactl\n236 nvidia-uvm\n\nBlock devices:\n  7 loop\n")
	control := map[string][2]uint32{"nvidiactl": {195, 255}, "nvidia-uvm": {236, 0}, "nvidia-uvm-tools": {236, 1}}
	// A GPU the driver did not take is said, and does not stop the rest.
	if err := d.makeDevices(); err == nil || !strings.Contains(err.Error(), "bound no GPU") || fmt.Sprint(made) != fmt.Sprint(control) {
		t.Fatalf("driver without a GPU: made %v, %v", made, err)
	}

	writeTestFile(t, filepath.Join(proc, "driver/nvidia/gpus/0000:00:06.0/information"), "Model: \t\t NVIDIA GeForce RTX 4090\nDevice Minor: \t 3\n")
	clear(made)
	control["nvidia3"] = [2]uint32{195, 3}
	if err := d.makeDevices(); err != nil || fmt.Sprint(made) != fmt.Sprint(control) {
		t.Fatalf("made %v, %v", made, err)
	}

	// One name that cannot be made is said, and the others are made.
	clear(made)
	d.mknod = func(path string, major, minor uint32) error {
		if filepath.Base(path) == "nvidia-uvm" {
			return unix.EROFS
		}
		made[filepath.Base(path)] = [2]uint32{major, minor}
		return nil
	}
	if err := d.makeDevices(); !errors.Is(err, unix.EROFS) || !strings.Contains(err.Error(), "name nvidia-uvm:") || len(made) != 3 {
		t.Fatalf("made %v, %v", made, err)
	}
}

func TestMakeCharDevice(t *testing.T) {
	dir := t.TempDir()
	old := unix.Umask(0o077)
	defer unix.Umask(old)
	null := filepath.Join(dir, "null")
	err := makeCharDevice(null, 1, 3)
	if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) {
		t.Skipf("cannot make a device node: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err := unix.Stat(null, &stat); err != nil || stat.Mode != unix.S_IFCHR|0o666 || stat.Rdev != unix.Mkdev(1, 3) {
		t.Fatalf("mode %#o, device %#x, %v", stat.Mode, stat.Rdev, err)
	}
	// A name the image's own tools made first is theirs.
	if err := os.Chmod(null, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := makeCharDevice(null, 1, 5); err != nil {
		t.Fatal(err)
	}
	if err := unix.Stat(null, &stat); err != nil || stat.Mode != unix.S_IFCHR|0o600 || stat.Rdev != unix.Mkdev(1, 3) {
		t.Fatalf("an existing name was changed: mode %#o, device %#x, %v", stat.Mode, stat.Rdev, err)
	}
}
