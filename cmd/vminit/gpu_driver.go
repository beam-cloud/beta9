//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"golang.org/x/sys/unix"
)

// A guest that is given a GPU and boots the host's kernel is offered the
// NVIDIA driver the host has for that kernel (microvm.Spec.GPUDriver). An
// image then needs no kernel modules, which load only into the kernel they
// were built for, and nothing to load them with: init does. It still brings
// the driver's libraries, and they must be of the modules' version, so a
// root without those is left as it was before there was a driver to offer:
// it has another driver, which it loads itself, or none.

// gpuDriverModules are what a program needs loaded to use the GPU. The
// display modules are left for the image to load, as they are in an image
// with a driver of its own.
var gpuDriverModules = []string{"nvidia", "nvidia_uvm"}

// gpuDriverLoadLimit is how long the host's rescan waits for the modules,
// which is well within how long the host waits for the rescan.
const gpuDriverLoadLimit = 15 * time.Second

// gpuLibraryDirs are where a root is looked through for the driver's
// libraries: where NVIDIA's installer and the distributions' packages put
// them.
var gpuLibraryDirs = []string{
	"/usr/lib/x86_64-linux-gnu",
	"/usr/lib/aarch64-linux-gnu",
	"/usr/lib64",
	"/usr/lib",
	"/usr/local/nvidia/lib64",
	"/usr/local/lib",
}

// installGPUDriver gives a root the host's NVIDIA driver and reports whether
// the root took it. A root takes it that has no modules for the running
// kernel and has the driver's libraries. The modules go where modprobe looks
// for them and the firmware where the kernel looks for this release's, which
// is not where a driver installs its own. Both stay the host's files,
// read-only: the host gives a restored guest the same ones.
func installGPUDriver(root string, vm *microvm.Spec) (bool, error) {
	if !vm.GPUDriver {
		return false, nil
	}
	release, err := kernelRelease()
	if err != nil {
		return false, err
	}
	return bindGPUDriver(root, microvm.GPUDriverDir, release)
}

// gpuDriverBind is a directory of the host's driver and where a root takes it.
type gpuDriverBind struct{ source, target string }

// gpuDriverBinds is what a root takes of the host's driver for a kernel
// release; nothing where the root has modules or firmware of its own for
// that release. A root that could take the driver and has no use for it is
// an error, which says what it lacks.
func gpuDriverBinds(root, driver, release string) ([]gpuDriverBind, error) {
	binds := []gpuDriverBind{
		{filepath.Join(driver, microvm.GPUDriverModules, release), filepath.Join("/lib/modules", release)},
		{filepath.Join(driver, microvm.GPUDriverFirmware), filepath.Join("/lib/firmware", release)},
	}
	for i, bind := range binds {
		// The lookup is also what has virtio-fs mount the host's directory.
		if info, err := os.Stat(bind.source); err != nil || !info.IsDir() {
			return nil, fmt.Errorf("the host's driver has no %s for kernel %s", filepath.Base(filepath.Dir(bind.target)), release)
		}
		target, err := unusedRootDir(root, bind.target)
		if err != nil || target == "" {
			return nil, err
		}
		binds[i].target = target
	}
	// The firmware is named for the driver that asks for it.
	entries, err := os.ReadDir(filepath.Join(driver, microvm.GPUDriverFirmware, "nvidia"))
	if err != nil {
		return nil, err
	}
	versions := make([]string, len(entries))
	for i, entry := range entries {
		versions[i] = entry.Name()
	}
	if !slices.ContainsFunc(versions, func(version string) bool { return hasGPULibraries(root, version) }) {
		return nil, fmt.Errorf("the host's driver is NVIDIA %s, and the root has no libraries of that version in %s",
			strings.Join(versions, ", "), strings.Join(gpuLibraryDirs, ", "))
	}
	return binds, nil
}

func bindGPUDriver(root, driver, release string) (bool, error) {
	binds, err := gpuDriverBinds(root, driver, release)
	if err != nil || binds == nil {
		return false, err
	}
	for i, bind := range binds {
		if err := bindReadOnly(bind.source, bind.target); err != nil {
			for _, bound := range binds[:i] {
				_ = unix.Unmount(bound.target, unix.MNT_DETACH)
			}
			return false, err
		}
	}
	return true, nil
}

// hasGPULibraries reports whether a root has a library of an NVIDIA driver
// version. Every library of a driver is named for its version, and none works
// with the modules of another.
func hasGPULibraries(root, version string) bool {
	for _, dir := range gpuLibraryDirs {
		// A link that leaves the root names no directory of the root's.
		dir, err := canonicalMountTarget(root, dir)
		if err != nil {
			continue
		}
		libraries, err := os.Open(dir)
		if err != nil {
			continue
		}
		names, _ := libraries.Readdirnames(-1)
		libraries.Close()
		if slices.ContainsFunc(names, func(name string) bool { return strings.HasSuffix(name, ".so."+version) }) {
			return true
		}
	}
	return false
}

func bindReadOnly(source, target string) error {
	if err := os.MkdirAll(target, 0o755); err != nil {
		return err
	}
	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind %s: %w", target, err)
	}
	if err := unix.Mount("", target, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY|unix.MS_NOSUID|unix.MS_NODEV, ""); err != nil {
		_ = unix.Unmount(target, unix.MNT_DETACH)
		return fmt.Errorf("remount %s read-only: %w", target, err)
	}
	return nil
}

// gpuDriver is the host's NVIDIA driver in a guest whose root took it.
type gpuDriver struct {
	modules string // the running kernel's, where the root has them
	proc    string // procfs
	dev     string // devtmpfs

	load  func(file string) error
	mknod func(path string, major, minor uint32) error

	loaded chan struct{} // closed once the modules are loaded, or cannot be
}

// startGPUDriver loads the driver's modules. They are loaded before the GPU
// is there, so the driver takes it as the bus finds it.
func startGPUDriver() *gpuDriver {
	d := &gpuDriver{proc: "/proc", dev: "/dev", load: loadModule, mknod: makeCharDevice, loaded: make(chan struct{})}
	go func() {
		defer close(d.loaded)
		begin := time.Now()
		release, err := kernelRelease()
		if err == nil {
			d.modules = filepath.Join("/lib/modules", release)
			err = d.loadModules()
		}
		if err != nil {
			logf("gpu driver: %v", err)
			return
		}
		// What the image's libraries must match.
		version, _ := os.ReadFile("/sys/module/nvidia/version")
		logf("gpu driver: loaded NVIDIA %s for kernel %s in %s", strings.TrimSpace(string(version)), release, time.Since(begin).Round(time.Millisecond))
	}()
	return d
}

// await returns once the modules are loaded. A guest that did not take the
// host's driver has none to wait for.
func (d *gpuDriver) await(limit time.Duration) {
	if d == nil {
		return
	}
	select {
	case <-d.loaded:
	case <-time.After(limit):
		logf("gpu driver: not loaded after %s", limit)
	}
}

func (d *gpuDriver) loadModules() error {
	files, err := moduleFiles(d.modules, gpuDriverModules...)
	if err != nil {
		return err
	}
	for _, file := range files {
		if err := d.load(file); err != nil {
			return fmt.Errorf("load %s: %w", filepath.Base(file), err)
		}
	}
	return nil
}

// moduleFiles lists the files of the named modules and of the modules they
// need, each after those it needs, as modules.dep in dir has them.
func moduleFiles(dir string, names ...string) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(dir, "modules.dep"))
	if err != nil {
		return nil, err
	}
	needs, files := map[string][]string{}, map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		file, deps, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		file = strings.TrimSpace(file)
		needs[file] = strings.Fields(deps)
		// nvidia-uvm.ko is the module nvidia_uvm.
		name, _, _ := strings.Cut(filepath.Base(file), ".")
		files[strings.ReplaceAll(name, "-", "_")] = file
	}
	var order []string
	listed := map[string]bool{}
	var list func(file string)
	list = func(file string) {
		if listed[file] {
			return
		}
		listed[file] = true
		for _, dep := range needs[file] {
			list(dep)
		}
		if !filepath.IsAbs(file) {
			file = filepath.Join(dir, file)
		}
		order = append(order, file)
	}
	for _, name := range names {
		file, ok := files[name]
		if !ok {
			return nil, fmt.Errorf("%s has no module %s", dir, name)
		}
		list(file)
	}
	return order, nil
}

// loadModule loads a module file as insmod does, without parameters. A
// module that is loaded already stays as it is.
func loadModule(file string) error {
	module, err := os.Open(file)
	if err != nil {
		return err
	}
	defer module.Close()
	for {
		err = unix.FinitModule(int(module.Fd()), "", 0)
		if err != unix.EINTR {
			break
		}
	}
	if errors.Is(err, unix.EEXIST) {
		return nil
	}
	return err
}

// nameDevices gives the driver's devices the names its libraries open them
// by. Nothing else does in a guest without udev rules for them: the
// libraries run nvidia-modprobe for it, which an image may not have.
func (d *gpuDriver) nameDevices() {
	if d == nil {
		return
	}
	if err := d.makeDevices(); err != nil {
		logf("gpu driver: %v", err)
	}
}

func (d *gpuDriver) makeDevices() error {
	devices := filepath.Join(d.proc, "devices")
	major, err := nvidiaMajor(devices)
	if err != nil {
		return err
	}
	type device struct {
		name         string
		major, minor uint32
	}
	nodes := []device{{"nvidiactl", major, 255}}
	var errs []error
	gpus, _ := filepath.Glob(filepath.Join(d.proc, "driver/nvidia/gpus/*/information"))
	if len(gpus) == 0 {
		errs = append(errs, errors.New("the driver has bound no GPU"))
	}
	for _, gpu := range gpus {
		minor, err := nvidiaMinor(gpu)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", filepath.Base(filepath.Dir(gpu)), err))
			continue
		}
		nodes = append(nodes, device{fmt.Sprintf("nvidia%d", minor), major, minor})
	}
	if uvm, err := charMajor(devices, "nvidia-uvm"); err != nil {
		errs = append(errs, err)
	} else {
		nodes = append(nodes, device{"nvidia-uvm", uvm, 0}, device{"nvidia-uvm-tools", uvm, 1})
	}
	for _, node := range nodes {
		if err := d.mknod(filepath.Join(d.dev, node.name), node.major, node.minor); err != nil {
			errs = append(errs, fmt.Errorf("name %s: %w", node.name, err))
		}
	}
	return errors.Join(errs...)
}

// makeCharDevice names a character device for every user to open, as
// nvidia-modprobe does. A name that is there already is left as it is.
func makeCharDevice(path string, major, minor uint32) error {
	err := unix.Mknod(path, unix.S_IFCHR|0o666, int(unix.Mkdev(major, minor)))
	if errors.Is(err, unix.EEXIST) {
		return nil
	}
	if err != nil {
		return err
	}
	// Whatever init's umask took away.
	return os.Chmod(path, 0o666)
}
