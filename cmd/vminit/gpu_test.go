//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// fakeGPU stands in for the driver, cuda-checkpoint and nvidia-smi: each
// process holds the GPU through a descriptor until it is checkpointed.
type fakeGPU struct {
	t      *testing.T
	proc   string
	uuid   string         // "" while no GPU is attached
	state  map[int]string // CUDA processes
	memory map[int]int    // MiB of GPU memory per CUDA process
	fail   map[string]error
	calls  []string
	rescan int
	free   int64

	driver atomic.Bool  // the driver has bound the GPU
	opens  atomic.Int32 // times the agent looked for it
	held   atomic.Int32 // whether the agent has it open
}

type fakeGPUHold struct{ f *fakeGPU }

func (h fakeGPUHold) Close() error {
	h.f.held.Add(-1)
	return nil
}

func newFakeGPU(t *testing.T) (*fakeGPU, *gpuCheckpointer) {
	f := &fakeGPU{t: t, proc: t.TempDir(), uuid: "GPU-old", state: map[int]string{}, memory: map[int]int{}, fail: map[string]error{}, free: 64 << 30}
	tool := filepath.Join(t.TempDir(), "cuda-checkpoint")
	if err := os.WriteFile(tool, nil, 0o755); err != nil {
		t.Fatal(err)
	}
	g := &gpuCheckpointer{
		proc: f.proc,
		tool: tool,
		run:  f.run,
		rescan: func() error {
			f.rescan++
			return nil
		},
		devices: func() func(string) bool {
			return func(fd string) bool {
				target, err := os.Readlink(fd)
				return err == nil && strings.HasPrefix(target, "/dev/nvidia")
			}
		},
		available: func() int64 { return f.free },
		open: func() ([]io.Closer, error) {
			f.opens.Add(1)
			if !f.driver.Load() {
				return nil, nil
			}
			f.held.Add(1)
			return []io.Closer{fakeGPUHold{f}}, nil
		},
	}
	f.driver.Store(true)
	return f, g
}

// heldSoon waits until the agent has the GPU open, or has not.
func (f *fakeGPU) heldSoon(want int32) {
	f.t.Helper()
	for deadline := time.Now().Add(5 * time.Second); f.held.Load() != want; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			f.t.Fatalf("the agent has the GPU open %d times; want %d", f.held.Load(), want)
		}
	}
}

// process adds a process that has the GPU open; cuda says whether it is a
// CUDA process.
func (f *fakeGPU) process(pid int, comm string, cuda bool) {
	f.t.Helper()
	dir := filepath.Join(f.proc, strconv.Itoa(pid), "fd")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.proc, strconv.Itoa(pid), "comm"), []byte(comm+"\n"), 0o644); err != nil {
		f.t.Fatal(err)
	}
	for fd, target := range map[string]string{"0": "/dev/null", "7": "/dev/nvidia0"} {
		if err := os.Symlink(target, filepath.Join(dir, fd)); err != nil {
			f.t.Fatal(err)
		}
	}
	if cuda {
		f.state[pid] = "running"
	}
}

func (f *fakeGPU) open(pid int, open bool) {
	f.t.Helper()
	fd := filepath.Join(f.proc, strconv.Itoa(pid), "fd", "7")
	_ = os.Remove(fd)
	if open {
		if err := os.Symlink("/dev/nvidia0", fd); err != nil {
			f.t.Fatal(err)
		}
	}
}

func (f *fakeGPU) run(_ context.Context, name string, args ...string) (string, error) {
	call := filepath.Base(name) + " " + strings.Join(args, " ")
	f.calls = append(f.calls, call)
	if err := f.fail[call]; err != nil {
		return "", err
	}
	if name == "nvidia-smi" {
		switch {
		case f.uuid == "":
			return "", errors.New("nvidia-smi: No devices were found")
		case slices.Contains(args, "--query-gpu=uuid"):
			return f.uuid + "\n", nil
		default:
			var rows []string
			for pid, mib := range f.memory {
				rows = append(rows, fmt.Sprintf("%d, %d", pid, mib))
			}
			return strings.Join(rows, "\n"), nil
		}
	}
	pid, _ := strconv.Atoi(args[len(args)-1])
	state, cuda := f.state[pid]
	if !cuda {
		return "", errors.New("cuda-checkpoint: not a CUDA process")
	}
	switch {
	case args[0] == "--get-state":
		return state, nil
	case args[1] == "lock" && state == "running":
		f.state[pid] = "locked"
	case args[1] == "checkpoint" && state == "locked":
		f.state[pid] = "checkpointed"
		f.open(pid, false)
	case args[1] == "restore" && state == "checkpointed":
		f.state[pid] = "locked"
		f.open(pid, true)
	case args[1] == "unlock" && state == "locked":
		f.state[pid] = "running"
	default:
		return "", fmt.Errorf("cuda-checkpoint: %s in state %s", args[1], state)
	}
	return "", nil
}

func (f *fakeGPU) cudaCalls() []string {
	var calls []string
	for _, call := range f.calls {
		if strings.HasPrefix(call, "cuda-checkpoint --action") {
			calls = append(calls, strings.TrimPrefix(call, "cuda-checkpoint --action "))
		}
	}
	return calls
}

func TestGPUSuspendAndResume(t *testing.T) {
	ctx := context.Background()

	t.Run("idle GPU", func(t *testing.T) {
		f, g := newFakeGPU(t)
		if err := os.MkdirAll(filepath.Join(f.proc, "12", "fd"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := g.suspend(ctx); err != nil {
			t.Fatal(err)
		}
		if err := g.resume(ctx); err != nil {
			t.Fatal(err)
		}
		if len(f.calls) != 0 || f.rescan != 1 {
			t.Fatalf("calls %v, %d rescans; want only a rescan", f.calls, f.rescan)
		}
	})

	t.Run("state moves to another GPU", func(t *testing.T) {
		f, g := newFakeGPU(t)
		f.process(40, "python3", true)
		f.process(41, "trainer", true)
		if err := g.suspend(ctx); err != nil {
			t.Fatal(err)
		}
		if left, _ := g.holders(); len(left) != 0 {
			t.Fatalf("processes %v still hold the GPU", left)
		}
		// One process exits while the VM is saved; the GPU is replaced.
		if err := os.RemoveAll(filepath.Join(f.proc, "41")); err != nil {
			t.Fatal(err)
		}
		f.uuid = "GPU-new"
		if err := g.resume(ctx); err != nil {
			t.Fatal(err)
		}
		want := []string{
			"lock --timeout 30000 --pid 40", "checkpoint --pid 40",
			"lock --timeout 30000 --pid 41", "checkpoint --pid 41",
			"restore --device-map GPU-old=GPU-new --pid 40", "unlock --pid 40",
		}
		if got := f.cudaCalls(); !slices.Equal(got, want) {
			t.Fatalf("cuda-checkpoint calls\n got %q\nwant %q", got, want)
		}
		if f.state[40] != "running" || f.rescan != 1 {
			t.Fatalf("state %q after %d rescans", f.state[40], f.rescan)
		}
		// Nothing is left to restore a second time.
		f.calls = nil
		if err := g.resume(ctx); err != nil || len(f.cudaCalls()) != 0 {
			t.Fatalf("second resume: %v, calls %v", err, f.cudaCalls())
		}
	})

	t.Run("same GPU needs no device map", func(t *testing.T) {
		f, g := newFakeGPU(t)
		f.process(40, "python3", true)
		if err := g.suspend(ctx); err != nil {
			t.Fatal(err)
		}
		if err := g.resume(ctx); err != nil {
			t.Fatal(err)
		}
		if got := f.cudaCalls(); !slices.Equal(got[2:], []string{"restore --pid 40", "unlock --pid 40"}) {
			t.Fatalf("cuda-checkpoint calls %q", got)
		}
	})

	t.Run("resume waits for the driver", func(t *testing.T) {
		f, g := newFakeGPU(t)
		f.process(40, "python3", true)
		if err := g.suspend(ctx); err != nil {
			t.Fatal(err)
		}
		f.uuid = ""
		g.rescan = func() error {
			f.rescan++
			f.calls = nil
			return nil
		}
		probes := 0
		run := g.run
		g.run = func(ctx context.Context, name string, args ...string) (string, error) {
			if name == "nvidia-smi" {
				if probes++; probes == 3 {
					f.uuid = "GPU-old"
				}
			}
			return run(ctx, name, args...)
		}
		if err := g.resume(ctx); err != nil {
			t.Fatal(err)
		}
		if probes != 3 || f.state[40] != "running" {
			t.Fatalf("%d probes, state %q", probes, f.state[40])
		}
	})

	t.Run("image without nvidia-smi", func(t *testing.T) {
		f, g := newFakeGPU(t)
		f.process(40, "python3", true)
		missing := fmt.Errorf("nvidia-smi: %w", exec.ErrNotFound)
		f.fail["nvidia-smi --query-gpu=uuid --format=csv,noheader"] = missing
		f.fail["nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader,nounits"] = missing
		if err := g.suspend(ctx); err != nil {
			t.Fatal(err)
		}
		if err := g.resume(ctx); err != nil || f.state[40] != "running" {
			t.Fatalf("resume: %v, state %q", err, f.state[40])
		}
	})
}

func TestGPUSuspendChangesNothingOnFailure(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		prepare func(*fakeGPU, *gpuCheckpointer)
		want    string
	}{
		"process that is not CUDA": {
			prepare: func(f *fakeGPU, _ *gpuCheckpointer) { f.process(90, "nvtop", false) },
			want:    "nvtop (pid 90) has the GPU open and cannot be suspended",
		},
		"too little guest memory": {
			prepare: func(f *fakeGPU, _ *gpuCheckpointer) {
				f.memory[40], f.free = 20000, 16<<30
			},
			want: "saving GPU memory needs 20256 MiB of free guest memory and 16384 MiB is free",
		},
		"checkpoint fails": {
			prepare: func(f *fakeGPU, _ *gpuCheckpointer) {
				f.process(50, "second", true)
				f.fail["cuda-checkpoint --action checkpoint --pid 50"] = errors.New("out of memory")
			},
			want: "save GPU state of second (pid 50): out of memory",
		},
		"device kept open": {
			prepare: func(f *fakeGPU, g *gpuCheckpointer) {
				run := g.run
				g.run = func(ctx context.Context, name string, args ...string) (string, error) {
					out, err := run(ctx, name, args...)
					if slices.Contains(args, "checkpoint") {
						f.open(40, true)
					}
					return out, err
				}
			},
			want: "the GPU is still open in python3 (pid 40)",
		},
		"no cuda-checkpoint": {
			prepare: func(_ *fakeGPU, g *gpuCheckpointer) { g.tool = filepath.Join(g.proc, "missing") },
			want:    "the GPU is in use by python3 (pid 40) and cuda-checkpoint is not installed on this host",
		},
	} {
		t.Run(name, func(t *testing.T) {
			f, g := newFakeGPU(t)
			f.process(40, "python3", true)
			tc.prepare(f, g)
			err := g.suspend(ctx)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("suspend: %v; want %q", err, tc.want)
			}
			for pid, state := range f.state {
				if state != "running" {
					t.Fatalf("pid %d was left %s", pid, state)
				}
			}
			if len(g.suspended) != 0 {
				t.Fatalf("remembered %v after a failed suspend", g.suspended)
			}
			if holders, _ := g.holders(); !slices.Contains(holders, 40) {
				t.Fatalf("pid 40 lost the GPU: holders %v", holders)
			}
		})
	}
}

// A process whose state could not be given back is still without it. It is
// asked for again, and until it has its state a resume fails.
func TestGPURestoreIsAskedForAgain(t *testing.T) {
	ctx := context.Background()

	t.Run("after a suspend that was refused", func(t *testing.T) {
		f, g := newFakeGPU(t)
		f.process(40, "python3", true)
		f.process(50, "second", true)
		f.fail["cuda-checkpoint --action checkpoint --pid 50"] = errors.New("out of memory")
		f.fail["cuda-checkpoint --action restore --pid 40"] = errors.New("the GPU is busy")
		err := g.suspend(ctx)
		for _, want := range []string{"save GPU state of second (pid 50): out of memory", "restore GPU state of python3 (pid 40): the GPU is busy"} {
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("suspend: %v; want %q", err, want)
			}
		}
		if f.state[40] != "checkpointed" || f.state[50] != "running" || !slices.Equal(g.suspended, []int{40}) {
			t.Fatalf("states %v, remembered %v", f.state, g.suspended)
		}
		// The host answers a refusal with a resume.
		if err := g.resume(ctx); err == nil || !strings.Contains(err.Error(), "restore GPU state of python3 (pid 40): the GPU is busy") {
			t.Fatalf("resume: %v", err)
		}
		if !slices.Equal(g.suspended, []int{40}) {
			t.Fatalf("remembered %v", g.suspended)
		}
		delete(f.fail, "cuda-checkpoint --action restore --pid 40")
		if err := g.resume(ctx); err != nil || f.state[40] != "running" || len(g.suspended) != 0 {
			t.Fatalf("resume: %v, state %q, remembered %v", err, f.state[40], g.suspended)
		}
	})

	t.Run("from where the process was left", func(t *testing.T) {
		f, g := newFakeGPU(t)
		f.process(40, "python3", true)
		if err := g.suspend(ctx); err != nil {
			t.Fatal(err)
		}
		f.uuid = "GPU-new"
		f.fail["cuda-checkpoint --action unlock --pid 40"] = errors.New("interrupted")
		if err := g.resume(ctx); err == nil || !strings.Contains(err.Error(), "unlock python3 (pid 40): interrupted") {
			t.Fatalf("resume: %v", err)
		}
		if f.state[40] != "locked" || !slices.Equal(g.suspended, []int{40}) {
			t.Fatalf("state %q, remembered %v", f.state[40], g.suspended)
		}
		delete(f.fail, "cuda-checkpoint --action unlock --pid 40")
		f.calls = nil
		if err := g.resume(ctx); err != nil || f.state[40] != "running" || len(g.suspended) != 0 {
			t.Fatalf("resume: %v, state %q, remembered %v", err, f.state[40], g.suspended)
		}
		if got, want := f.cudaCalls(), []string{"restore --device-map GPU-old=GPU-new --pid 40", "unlock --pid 40"}; !slices.Equal(got, want) {
			t.Fatalf("cuda-checkpoint calls\n got %q\nwant %q", got, want)
		}
	})
}

// Where it is PID 1 the agent reaps whatever exits. A tool is its caller's
// to wait for.
func TestGPUToolIsNotReapedFromUnderItsCaller(t *testing.T) {
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-stop:
				return
			default:
				_, _, _ = reap(-1)
			}
		}
	}()
	defer func() {
		close(stop)
		<-stopped
	}()
	for range 200 {
		if out, err := runGPUTool(context.Background(), "/bin/sh", "-c", "echo state"); err != nil || out != "state" {
			t.Fatalf("tool: %q, %v", out, err)
		}
	}
}

func TestGPUKeptInitialised(t *testing.T) {
	ctx := context.Background()

	t.Run("once the driver has it", func(t *testing.T) {
		f, g := newFakeGPU(t)
		f.driver.Store(false)
		g.keep(0)
		if f.held.Load() != 0 {
			t.Fatal("opened a GPU the driver does not have")
		}
		done := make(chan struct{})
		go func() {
			g.keep(time.Minute)
			close(done)
		}()
		for f.opens.Load() < 3 {
			time.Sleep(time.Millisecond)
		}
		f.driver.Store(true)
		<-done
		g.keep(0) // it already is
		if f.held.Load() != 1 {
			t.Fatalf("open %d times", f.held.Load())
		}
	})

	t.Run("released for a suspend, taken back first by a resume", func(t *testing.T) {
		f, g := newFakeGPU(t)
		f.process(40, "python3", true)
		g.keep(0)
		var held []int32
		run := g.run
		g.run = func(ctx context.Context, name string, args ...string) (string, error) {
			held = append(held, f.held.Load())
			return run(ctx, name, args...)
		}
		if err := g.suspend(ctx); err != nil {
			t.Fatal(err)
		}
		if f.held.Load() != 0 || slices.Contains(held, 1) {
			t.Fatalf("open %d times after the suspend, %v during it", f.held.Load(), held)
		}
		held = nil
		if err := g.resume(ctx); err != nil {
			t.Fatal(err)
		}
		// nvidia-smi and each process find the GPU initialised.
		if len(held) == 0 || slices.Contains(held, 0) || f.held.Load() != 1 {
			t.Fatalf("open %v during the resume, %d times after it", held, f.held.Load())
		}
	})

	t.Run("idle GPU is taken back behind the host", func(t *testing.T) {
		f, g := newFakeGPU(t)
		g.keep(0)
		if err := g.suspend(ctx); err != nil || f.held.Load() != 0 {
			t.Fatalf("suspend: %v, open %d times", err, f.held.Load())
		}
		if err := g.resume(ctx); err != nil {
			t.Fatal(err)
		}
		f.heldSoon(1)
	})

	t.Run("taken back after a suspend that failed", func(t *testing.T) {
		f, g := newFakeGPU(t)
		f.process(90, "nvtop", false)
		g.keep(0)
		if err := g.suspend(ctx); err == nil {
			t.Fatal("suspended a process that is not CUDA")
		}
		f.heldSoon(1)
	})

	t.Run("not by a wait that a suspend overtook", func(t *testing.T) {
		f, g := newFakeGPU(t)
		f.driver.Store(false)
		done := make(chan struct{})
		go func() {
			g.keep(time.Minute)
			close(done)
		}()
		for f.opens.Load() == 0 {
			time.Sleep(time.Millisecond)
		}
		if err := g.suspend(ctx); err != nil {
			t.Fatal(err)
		}
		f.driver.Store(true) // the GPU the host is about to unplug
		<-done
		if f.held.Load() != 0 {
			t.Fatal("opened a GPU that was suspended")
		}
	})
}

func TestOpenNVIDIADevices(t *testing.T) {
	proc, dev := t.TempDir(), t.TempDir()
	if files, err := openNVIDIADevices(proc, dev); err != nil || len(files) != 0 {
		t.Fatalf("without a driver: %v, %d devices", err, len(files))
	}
	gpu := filepath.Join(proc, "driver/nvidia/gpus/0000:00:06.0")
	if err := os.MkdirAll(gpu, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(path, content string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(gpu, "information"), "Model: \t\t NVIDIA GeForce RTX 4090\nBus Location: \t 0000:00:06.0\n")
	write(filepath.Join(proc, "devices"), "Character devices:\n195 nvidiactl\n236 nvidia-uvm\n\nBlock devices:\n195 nvidia\n")
	if _, err := openNVIDIADevices(proc, dev); err == nil || !strings.Contains(err.Error(), "registered no device number") {
		t.Fatalf("driver without a device number: %v", err)
	}
	for _, name := range []string{"nvidia", "nvidia-frontend"} {
		write(filepath.Join(proc, "devices"), "Character devices:\n  1 mem\n195 "+name+"\n195 nvidiactl\n\nBlock devices:\n  7 loop\n")
		if major, err := nvidiaMajor(filepath.Join(proc, "devices")); err != nil || major != 195 {
			t.Fatalf("%s: major %d, %v", name, major, err)
		}
	}
	if _, err := openNVIDIADevices(proc, dev); err == nil || !strings.Contains(err.Error(), "open 0000:00:06.0: the driver does not say which device it is") {
		t.Fatalf("GPU without a device number: %v", err)
	}
	write(filepath.Join(gpu, "information"), "Model: \t\t NVIDIA GeForce RTX 4090\nDevice Minor: \t 3\nGPU Excluded:\t No\n")
	if minor, err := nvidiaMinor(filepath.Join(gpu, "information")); err != nil || minor != 3 {
		t.Fatalf("minor %d, %v", minor, err)
	}

	// /dev/null by number, as a GPU is opened: needs a filesystem that
	// allows devices and the right to make one.
	null, err := openCharDevice("/dev", 1, 3)
	if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) {
		t.Skipf("cannot make a device node: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer null.Close()
	if n, err := null.Write([]byte("discarded")); err != nil || n != 9 {
		t.Fatalf("write to the device: %d, %v", n, err)
	}
	if left, _ := filepath.Glob("/dev/.beam-vm-*"); len(left) != 0 {
		t.Fatalf("left %v behind", left)
	}
}

func TestNVIDIAMajors(t *testing.T) {
	devices := filepath.Join(t.TempDir(), "devices")
	content := "Character devices:\n  1 mem\n195 nvidia\n195 nvidiactl\n236 nvidia-uvm\n237 nvidia-caps\n\nBlock devices:\n195 nvidia-not-a-char-device\n  7 loop\n"
	if err := os.WriteFile(devices, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := nvidiaMajors(devices); !slices.Equal(got, []uint32{195, 236, 237}) {
		t.Fatalf("majors %v", got)
	}
	if got := nvidiaMajors(filepath.Join(t.TempDir(), "missing")); got != nil {
		t.Fatalf("majors %v without a driver", got)
	}
	// Without the driver nothing is an NVIDIA device, whatever it is called.
	if nvidiaDeviceMatcher(filepath.Join(t.TempDir(), "missing"))("/dev/null") {
		t.Fatal("matched /dev/null")
	}
}

func TestWaitUnplugged(t *testing.T) {
	root := t.TempDir()
	gpu, audio, disk := "0000:00:06.0", "0000:00:07.0", "0000:00:01.0"
	for _, device := range []string{gpu, audio, disk} {
		if err := os.Mkdir(filepath.Join(root, device), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// The audio function has no driver and goes at once; the GPU's driver
	// takes a moment to let go.
	_ = os.Remove(filepath.Join(root, audio))
	time.AfterFunc(30*time.Millisecond, func() { _ = os.Remove(filepath.Join(root, gpu)) })
	begin := time.Now()
	if err := waitUnplugged(ctx, root, []string{audio, gpu}); err != nil || time.Since(begin) < 30*time.Millisecond {
		t.Fatalf("returned after %s with %v, before the GPU was gone", time.Since(begin), err)
	}
	if err := waitUnplugged(ctx, root, nil); err != nil {
		t.Fatalf("nothing to wait for: %v", err)
	}

	held, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	err := waitUnplugged(held, root, []string{gpu, disk})
	if err == nil || !strings.Contains(err.Error(), disk+" still attached") || strings.Contains(err.Error(), gpu) {
		t.Fatalf("a held device must be named, and only it: %v", err)
	}
	for _, device := range []string{"", "../" + disk, "a/b"} {
		if err := waitUnplugged(ctx, root, []string{device}); err == nil || !strings.Contains(err.Error(), "invalid PCI address") {
			t.Fatalf("%q must be rejected, got %v", device, err)
		}
	}
}
