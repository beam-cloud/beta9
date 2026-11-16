package runtime

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/beam-cloud/beta9/pkg/types"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestWaitForGuestControl(t *testing.T) {
	for _, outcome := range []string{"ready", "exit", "cancel", "missing", "no_control"} {
		t.Run(outcome, func(t *testing.T) {
			ready, exited := make(chan struct{}), make(chan struct{})
			inst := &microVMInstance{ctrl: &microVMControl{ready: ready}, exitDone: exited}
			m := &MicroVM{vms: map[string]*microVMInstance{"guest": inst}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			switch outcome {
			case "ready":
				close(ready)
			case "exit":
				close(exited)
			case "cancel":
				cancel()
			case "missing":
				delete(m.vms, "guest")
			case "no_control":
				inst.ctrl = nil
			}
			err := m.WaitForGuestControl(ctx, "guest")
			if outcome == "ready" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// A guest waits for late devices only when told they are coming. It asks on
// every boot, and one rescan follows the first.
func TestBootAnnouncementIsAnsweredOnce(t *testing.T) {
	vsock := filepath.Join(t.TempDir(), "vsock.sock")
	ctrl := &microVMControl{booted: make(chan struct{})}
	require.NoError(t, ctrl.listenBoot(vsock))
	t.Cleanup(ctrl.close)
	path := fmt.Sprintf("%s_%d", vsock, microvm.BootPort)

	conn, err := net.Dial("unix", path)
	require.NoError(t, err)
	answer, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Len(t, answer, 1)
	require.NoError(t, conn.Close())
	select {
	case <-ctrl.booted:
	case <-time.After(5 * time.Second):
		t.Fatal("the host did not hear the guest's init start")
	}

	require.Eventually(t, func() bool {
		conn, err := net.Dial("unix", path)
		if err != nil {
			return true
		}
		answer, _ := io.ReadAll(conn)
		_ = conn.Close()
		return len(answer) == 0
	}, 5*time.Second, 5*time.Millisecond)
}

func TestWaitForDevices(t *testing.T) {
	for _, outcome := range []string{"attached", "pending", "exit", "missing"} {
		t.Run(outcome, func(t *testing.T) {
			devices, exited := make(chan struct{}), make(chan struct{})
			// The guest has reported in: it answers before it has its GPU.
			inst := &microVMInstance{ctrl: &microVMControl{ready: make(chan struct{})}, devices: devices, exitDone: exited}
			close(inst.ctrl.ready)
			m := &MicroVM{vms: map[string]*microVMInstance{"guest": inst}}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			require.NoError(t, m.WaitForGuestControl(ctx, "guest"))
			switch outcome {
			case "attached":
				time.AfterFunc(10*time.Millisecond, func() { close(devices) })
			case "exit":
				close(exited)
			case "missing":
				delete(m.vms, "guest")
			}
			err := m.WaitForDevices(ctx, "guest")
			if outcome == "attached" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// vfioTestGuest is a hypervisor and a guest for attach and detach: the
// hypervisor's API, and the guest's end of the control channel.
type vfioTestGuest struct {
	mu      sync.Mutex
	inst    *microVMInstance
	state   func() microVMState
	tree    []string // device ids vm.info reports
	calls   []string // API requests and guest commands, in order
	reject  string   // API request to fail
	refuses string   // guest command to refuse
}

func newVFIOTestGuest(t *testing.T, paths ...string) *vfioTestGuest {
	t.Helper()
	g := newBootingVFIOTestGuest(t, paths...)
	g.reportIn(t)
	return g
}

// newBootingVFIOTestGuest is a guest that has not reported in yet.
func newBootingVFIOTestGuest(t *testing.T, paths ...string) *vfioTestGuest {
	t.Helper()
	g := &vfioTestGuest{state: func() microVMState { return microVMRunning }}
	g.inst = snapshotTestInstance(t, func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		defer g.mu.Unlock()
		var body struct{ ID, Path string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		action := strings.TrimPrefix(r.URL.Path, "/api/v1/")
		if action == "vm.info" {
			tree := map[string]struct{}{"_disk0": {}}
			for _, id := range g.tree {
				tree[id] = struct{}{}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"state": g.state(), "device_tree": tree})
			return
		}
		call := action + " " + body.ID
		if body.Path != "" {
			call += "=" + filepath.Base(body.Path)
		}
		g.calls = append(g.calls, call)
		if call == g.reject {
			http.Error(w, "device busy", http.StatusInternalServerError)
			return
		}
		if action == "vm.add-device" {
			// The guest finds device vfioN in PCI slot 6+N.
			_ = json.NewEncoder(w).Encode(map[string]string{"id": body.ID, "bdf": "0000:00:0" + string(body.ID[4]+6) + ".0"})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	g.inst.vfio = &microVMVFIO{paths: paths}
	g.inst.exitDone = make(chan struct{})
	g.inst.ctrl = &microVMControl{ready: make(chan struct{}), booted: make(chan struct{}), pending: map[uint64]chan microvm.Message{}}
	return g
}

func (g *vfioTestGuest) reportIn(t *testing.T) {
	t.Helper()
	host, guest := net.Pipe()
	t.Cleanup(func() { _ = host.Close(); _ = guest.Close() })
	go g.inst.ctrl.serve(host)
	go func() {
		dec, enc := microvm.NewDecoder(guest), microvm.NewEncoder(guest)
		for {
			msg, err := dec.Decode()
			if err != nil {
				return
			}
			g.mu.Lock()
			g.calls = append(g.calls, strings.TrimSpace(msg.Type+" "+strings.Join(msg.Devices, ",")))
			ok := msg.Type != g.refuses
			g.mu.Unlock()
			_ = enc.Encode(microvm.Message{Type: microvm.MsgAck, ID: msg.ID, OK: ok, Error: "guest says no"})
		}
	}()
}

func (g *vfioTestGuest) require(t *testing.T, calls ...string) {
	t.Helper()
	g.mu.Lock()
	defer g.mu.Unlock()
	require.Equal(t, calls, g.calls)
	g.calls = nil
}

func TestMicroVMAttachVFIO(t *testing.T) {
	gpu, audio := "/sys/bus/pci/devices/0000:01:00.0", "/sys/bus/pci/devices/0000:01:00.1"
	addGPU, addAudio := "vm.add-device vfio0=0000:01:00.0", "vm.add-device vfio1=0000:01:00.1"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("a device added before the VM runs is not hot-plugged", func(t *testing.T) {
		g := newVFIOTestGuest(t, gpu, audio)
		polls := 0
		g.state = func() microVMState {
			if polls++; polls < 3 {
				return "Created"
			}
			return microVMRunning
		}
		require.NoError(t, g.inst.hotplugVFIO(ctx))
		require.GreaterOrEqual(t, polls, 3)
		g.require(t, addGPU, addAudio, microvm.MsgRescan)
		require.Equal(t, []microVMVFIOSlot{{bdf: "0000:00:06.0"}, {bdf: "0000:00:07.0"}}, g.inst.slots)
	})
	t.Run("a booting guest is not held up by its devices", func(t *testing.T) {
		g := newBootingVFIOTestGuest(t, gpu)
		attached := make(chan error, 1)
		go func() { attached <- g.inst.hotplugVFIO(ctx) }()
		waits := func(why string) {
			t.Helper()
			select {
			case err := <-attached:
				t.Fatalf("attached before the guest %s: %v", why, err)
			case <-time.After(50 * time.Millisecond):
			}
		}
		waits("had its boot devices")
		g.require(t)
		close(g.inst.ctrl.booted) // its init has started
		waits("could look for the device")
		g.require(t, addGPU)
		g.reportIn(t)
		require.NoError(t, <-attached)
		g.require(t, microvm.MsgRescan)
	})
	t.Run("a restored guest takes its GPU state back", func(t *testing.T) {
		g := newVFIOTestGuest(t, gpu, audio)
		g.inst.restored = true
		require.NoError(t, g.inst.hotplugVFIO(ctx))
		g.require(t, addGPU, addAudio, microvm.MsgGPUResume)
	})
	t.Run("hypervisor rejects", func(t *testing.T) {
		g := newVFIOTestGuest(t, gpu, audio)
		g.reject = addAudio
		err := g.inst.hotplugVFIO(ctx)
		require.ErrorContains(t, err, "0000:01:00.1")
		require.ErrorContains(t, err, "device busy")
		g.require(t, addGPU, addAudio) // the guest is not told about a failed attach
	})
	for command, restored := range map[string]bool{microvm.MsgRescan: false, microvm.MsgGPUResume: true} {
		t.Run("guest refuses "+command, func(t *testing.T) {
			g := newVFIOTestGuest(t, gpu)
			g.inst.restored, g.refuses = restored, command
			require.ErrorContains(t, g.inst.hotplugVFIO(ctx), "guest says no")
		})
	}
	t.Run("cancelled before boot", func(t *testing.T) {
		g := newVFIOTestGuest(t, gpu)
		g.state = func() microVMState { return "Created" }
		ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, g.inst.hotplugVFIO(ctx), context.DeadlineExceeded)
		g.require(t)
	})
	t.Run("hypervisor exits before boot", func(t *testing.T) {
		g := newVFIOTestGuest(t, gpu)
		g.state = func() microVMState { return "Created" }
		close(g.inst.exitDone)
		require.ErrorContains(t, g.inst.hotplugVFIO(ctx), "exited")
	})
}

func TestMicroVMDetachVFIO(t *testing.T) {
	gpu, audio := "/sys/bus/pci/devices/0000:01:00.0", "/sys/bus/pci/devices/0000:01:00.1"
	addGPU, addAudio := "vm.add-device vfio0=0000:01:00.0", "vm.add-device vfio1=0000:01:00.1"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attached := func(t *testing.T) *vfioTestGuest {
		g := newVFIOTestGuest(t, gpu, audio)
		require.NoError(t, g.inst.attachVFIO(ctx, false, nil))
		g.require(t, addGPU, addAudio, microvm.MsgRescan)
		return g
	}

	t.Run("the guest saves its GPU state, then its devices go", func(t *testing.T) {
		g := attached(t)
		require.NoError(t, g.inst.detachVFIO(ctx))
		g.require(t, microvm.MsgGPUSuspend, "vm.remove-device vfio1", "vm.remove-device vfio0",
			microvm.MsgUnplugged+" 0000:00:07.0,0000:00:06.0")
		require.NoError(t, g.inst.vfioDetached(ctx))

		// The same guest, or one restored from its snapshot, gets them back.
		require.NoError(t, g.inst.attachVFIO(ctx, true, nil))
		g.require(t, addGPU, addAudio, microvm.MsgGPUResume)
	})
	t.Run("a guest that cannot save its GPU state keeps its devices", func(t *testing.T) {
		g := attached(t)
		g.refuses = microvm.MsgGPUSuspend
		require.ErrorContains(t, g.inst.detachVFIO(ctx), "guest says no")
		g.require(t, microvm.MsgGPUSuspend)
		g.refuses = ""
		require.NoError(t, g.inst.attachVFIO(ctx, true, nil))
		g.require(t, microvm.MsgGPUResume) // nothing to add; what it had suspended resumes
	})
	t.Run("a device the guest holds on to cannot be given back", func(t *testing.T) {
		g := attached(t)
		g.refuses = microvm.MsgUnplugged
		err := g.inst.detachVFIO(ctx)
		require.ErrorContains(t, err, "did not release its VFIO devices")
		require.ErrorContains(t, err, "guest says no")
		g.require(t, microvm.MsgGPUSuspend, "vm.remove-device vfio1", "vm.remove-device vfio0",
			microvm.MsgUnplugged+" 0000:00:07.0,0000:00:06.0")
		// The guest may eject it at any moment, so it is neither attached
		// nor free to attach again.
		require.ErrorContains(t, g.inst.attachVFIO(ctx, true, nil), "still releasing")
		g.require(t)
	})
	t.Run("hypervisor rejects", func(t *testing.T) {
		g := attached(t)
		g.reject = "vm.remove-device vfio0"
		err := g.inst.detachVFIO(ctx)
		require.ErrorContains(t, err, "unplug 0000:01:00.0")
		require.ErrorContains(t, err, "device busy")
		require.ErrorContains(t, g.inst.attachVFIO(ctx, true, nil), "0000:01:00.1: the guest is still releasing it")
	})
	t.Run("a snapshot is not taken around a device the hypervisor still has", func(t *testing.T) {
		g := attached(t)
		g.tree = []string{"vfio0"}
		require.ErrorContains(t, g.inst.vfioDetached(ctx), "0000:01:00.0 is still attached")
	})
}

func TestMicroVMAttachVFIONeedsGuestAddress(t *testing.T) {
	inst := snapshotTestInstance(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"id":"vfio0"}`)) })
	inst.vfio = &microVMVFIO{paths: []string{"/sys/bus/pci/devices/0000:01:00.0"}}
	inst.ctrl = &microVMControl{ready: make(chan struct{})}
	require.ErrorContains(t, inst.attachVFIO(context.Background(), false, nil), "did not say where the guest finds it",
		"a device that cannot be named to the guest cannot be detached")
}

func TestMicroVMCPUWeightAppliesWhileQuotaIsDeferred(t *testing.T) {
	path := t.TempDir()
	shares := uint64(102)
	require.NoError(t, applyMicroVMCPU(path, &specs.LinuxCPU{Shares: &shares}))
	assertCPUFile(t, path, "cpu.weight", "4")
	_, err := os.Stat(filepath.Join(path, "cpu.max"))
	require.True(t, os.IsNotExist(err), "startup must still defer the quota")
	inst := &microVMInstance{id: "weight-test", cgroup: path}
	m := &MicroVM{vms: map[string]*microVMInstance{inst.id: inst}}
	shares, quota := uint64(1024), int64(10000)
	require.NoError(t, m.UpdateResources(context.Background(), inst.id, &specs.LinuxResources{CPU: &specs.LinuxCPU{Shares: &shares, Quota: &quota}}))
	assertCPUFile(t, path, "cpu.weight", "39")
	assertCPUFile(t, path, "cpu.max", "10000 100000")
}

func TestMicroVMCPUWeightBoundsAndFailures(t *testing.T) {
	for _, test := range []struct {
		shares uint64
		weight string
	}{{1, "1"}, {2, "1"}, {262144, "10000"}, {^uint64(0), "10000"}} {
		path := t.TempDir()
		require.NoError(t, applyMicroVMCPU(path, &specs.LinuxCPU{Shares: &test.shares}))
		assertCPUFile(t, path, "cpu.weight", test.weight)
	}
	shares := uint64(102)
	require.ErrorContains(t, applyMicroVMCPU(filepath.Join(t.TempDir(), "missing"), &specs.LinuxCPU{Shares: &shares}), "set cpu.weight")
}

func TestMicroVMRealCPUWeight(t *testing.T) {
	if os.Getenv("BETA9_MICROVM_REAL_CGROUP") != "1" {
		t.Skip("requires a writable cgroup v2 mount")
	}
	name := fmt.Sprintf("beam-weight-test-%d", os.Getpid())
	path := filepath.Join(microVMSysfsCgroupRoot, name)
	require.NoError(t, os.Mkdir(path, 0755))
	t.Cleanup(func() { require.NoError(t, os.Remove(path)) })
	shares := uint64(102)
	inst := &microVMInstance{id: name}
	spec := &specs.Spec{Linux: &specs.Linux{CgroupsPath: name, Resources: &specs.LinuxResources{CPU: &specs.LinuxCPU{Shares: &shares}}}}
	require.NoError(t, (&MicroVM{}).setupCgroup(inst, spec, 1<<30))
	assertCPUFile(t, path, "cpu.weight", "4\n")
	assertCPUFile(t, path, "cpu.max", "max 100000\n")
}

func snapshotTestInstance(t *testing.T, serve func(http.ResponseWriter, *http.Request)) *microVMInstance {
	t.Helper()
	inst := &microVMInstance{id: "snapshot-test", stateDir: t.TempDir()}
	listener, err := net.Listen("unix", filepath.Join(inst.stateDir, microVMAPISocket))
	require.NoError(t, err)
	server := &http.Server{Handler: http.HandlerFunc(serve)}
	go server.Serve(listener)
	t.Cleanup(func() { _ = server.Close() })
	return inst
}

type snapshotActions struct {
	mu     sync.Mutex
	values []string
}

func (a *snapshotActions) record(action string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.values = append(a.values, action)
}

func (a *snapshotActions) serve(w http.ResponseWriter, r *http.Request) {
	a.record(r.URL.Path)
	w.WriteHeader(http.StatusNoContent)
}

func (a *snapshotActions) require(t *testing.T, want ...string) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	require.Equal(t, want, a.values)
}

func resumableSnapshotTestInstance(t *testing.T) (*microVMInstance, func(bool)) {
	t.Helper()
	var actions snapshotActions
	inst := snapshotTestInstance(t, actions.serve)
	return inst, func(mustStop bool) {
		t.Helper()
		require.False(t, mustStop)
		require.False(t, inst.isPaused())
		actions.require(t, "/api/v1/vm.pause", "/api/v1/vm.resume")
	}
}

func TestMicroVMSnapshotSealsDisksBeforeCapture(t *testing.T) {
	var actions snapshotActions
	inst := snapshotTestInstance(t, actions.serve)
	mustStop, err := snapshotMicroVM(context.Background(), inst, t.TempDir(), func(context.Context) error {
		actions.record("seal")
		return nil
	})
	require.NoError(t, err)
	require.True(t, mustStop)
	require.True(t, inst.isPaused())
	actions.require(t, "/api/v1/vm.pause", "seal", "/api/v1/vm.snapshot")
}

// The hypervisor is asked whether the devices are gone only of a paused
// guest, and before anything is done with it: a guest that runs may be
// ejecting one.
func TestMicroVMSnapshotFindsDevicesGoneOncePaused(t *testing.T) {
	for name, tc := range map[string]struct {
		tree  []string
		calls []string
		err   string
	}{
		"gone":           {calls: []string{"/api/v1/vm.pause", "/api/v1/vm.info", "seal", "/api/v1/vm.snapshot"}},
		"still attached": {tree: []string{"vfio0"}, calls: []string{"/api/v1/vm.pause", "/api/v1/vm.info", "/api/v1/vm.resume"}, err: "0000:01:00.0 is still attached to the paused guest"},
	} {
		t.Run(name, func(t *testing.T) {
			var actions snapshotActions
			inst := snapshotTestInstance(t, func(w http.ResponseWriter, r *http.Request) {
				actions.record(r.URL.Path)
				if r.URL.Path != "/api/v1/vm.info" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				tree := map[string]struct{}{}
				for _, id := range tc.tree {
					tree[id] = struct{}{}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"state": microVMPaused, "device_tree": tree})
			})
			inst.vfio = &microVMVFIO{paths: []string{"/sys/bus/pci/devices/0000:01:00.0"}}
			mustStop, err := snapshotMicroVM(context.Background(), inst, t.TempDir(), inst.onceDetached(func(context.Context) error {
				actions.record("seal")
				return nil
			}))
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.err)
			}
			require.Equal(t, tc.err == "", mustStop)
			actions.require(t, tc.calls...)
		})
	}
}

func TestMicroVMSnapshotOtherErrorsAreNotSafeToRetry(t *testing.T) {
	var actions snapshotActions
	inst := snapshotTestInstance(t, func(w http.ResponseWriter, r *http.Request) {
		actions.record(r.URL.Path)
		if r.URL.Path == "/api/v1/vm.snapshot" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	captured, err := snapshotMicroVM(context.Background(), inst, t.TempDir(), nil)
	require.Error(t, err)
	require.True(t, captured)
	require.True(t, inst.isPaused())
	actions.require(t, "/api/v1/vm.pause", "/api/v1/vm.snapshot")
}

func TestMicroVMSnapshotCPUPreparationFailureResumesBeforeCapture(t *testing.T) {
	inst, assertResumed := resumableSnapshotTestInstance(t)
	inst.cgroup = t.TempDir() // Missing cgroup controls make preparation fail before capture.
	mustStop, err := snapshotMicroVM(context.Background(), inst, t.TempDir(), nil)
	require.ErrorContains(t, err, "read snapshot cpu.max")
	assertResumed(mustStop)
}

func TestMicroVMSnapshotCPUPreparationFailureStopsIfResumeFails(t *testing.T) {
	inst := snapshotTestInstance(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/vm.resume" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/api/v1/vm.snapshot" {
			t.Error("snapshot attempted after CPU preparation failed")
		}
		w.WriteHeader(http.StatusNoContent)
	})
	inst.cgroup = t.TempDir()
	mustStop, err := snapshotMicroVM(context.Background(), inst, t.TempDir(), nil)
	require.ErrorContains(t, err, "resume uncaptured vm")
	require.True(t, mustStop)
	require.True(t, inst.isPaused())
}

func TestMicroVMSnapshotCPUQuotaOnlyLiftedDuringPausedCapture(t *testing.T) {
	for _, failCapture := range []bool{false, true} {
		t.Run(fmt.Sprint(failCapture), func(t *testing.T) {
			original := "10000 100000\n"
			cgroup := newCPUFixture(t, original, "4\n")
			cpuMax, weightPath := filepath.Join(cgroup, "cpu.max"), filepath.Join(cgroup, "cpu.weight")
			var observed string
			inst := snapshotTestInstance(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v1/vm.snapshot" {
					weight, weightErr := os.ReadFile(weightPath)
					if weightErr != nil || string(weight) != "100" {
						t.Errorf("capture weight: %q, %v", weight, weightErr)
					}
					value, err := os.ReadFile(cpuMax)
					if err != nil {
						t.Error(err)
					}
					observed = string(value)
					if failCapture {
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
				}
				w.WriteHeader(http.StatusNoContent)
			})
			inst.cgroup = cgroup
			mustStop, err := snapshotMicroVM(context.Background(), inst, t.TempDir(), func(context.Context) error {
				value, err := os.ReadFile(cpuMax)
				require.NoError(t, err)
				require.Equal(t, original, string(value), "disk publication keeps the tenant quota")
				return nil
			})
			require.Equal(t, failCapture, err != nil)
			require.True(t, mustStop)
			require.True(t, inst.isPaused())
			require.Equal(t, "max 100000", observed)
			restored, err := os.ReadFile(cpuMax)
			require.NoError(t, err)
			require.Equal(t, original, string(restored))
			restoredWeight, err := os.ReadFile(weightPath)
			require.NoError(t, err)
			require.Equal(t, "4\n", string(restoredWeight))
		})
	}
}

func TestMicroVMSnapshotCPUQuotaRejectsRunningGuest(t *testing.T) {
	inst := &microVMInstance{cgroup: t.TempDir()}
	require.ErrorContains(t, inst.withSnapshotCPU(func() error { t.Fatal("must not capture"); return nil }), "paused vCPUs")
}

func TestMicroVMSnapshotWeightRestoredWithUnlimitedQuota(t *testing.T) {
	for _, weight := range []string{"4", "157"} {
		inst := &microVMInstance{cgroup: newCPUFixture(t, "max 100000", weight), paused: true}
		weightPath := filepath.Join(inst.cgroup, "cpu.weight")
		want := "100"
		if weight == "157" {
			want = weight
		}
		captureErr := errors.New("capture failed")
		err := inst.withSnapshotCPU(func() error {
			actual, err := os.ReadFile(weightPath)
			require.NoError(t, err)
			require.Equal(t, want, string(actual))
			return captureErr
		})
		require.ErrorIs(t, err, captureErr)
		actual, err := os.ReadFile(weightPath)
		require.NoError(t, err)
		require.Equal(t, weight, string(actual))
	}
}

func TestMicroVMSnapshotCPUQuotaSerializesResourceUpdate(t *testing.T) {
	inst := &microVMInstance{id: "quota-test", cgroup: newCPUFixture(t, "10000 100000", "4"), paused: true}
	cpuMax := filepath.Join(inst.cgroup, "cpu.max")
	m := &MicroVM{vms: map[string]*microVMInstance{inst.id: inst}}
	quota, period := int64(50000), uint64(100000)
	started, updated := make(chan struct{}), make(chan error, 1)
	require.NoError(t, inst.withSnapshotCPU(func() error {
		go func() {
			close(started)
			updated <- m.UpdateResources(context.Background(), inst.id, &specs.LinuxResources{CPU: &specs.LinuxCPU{Quota: &quota, Period: &period}})
		}()
		<-started
		select {
		case err := <-updated:
			t.Fatalf("resource update raced capture: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		return nil
	}))
	select {
	case err := <-updated:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("resource update did not finish after capture")
	}
	value, err := os.ReadFile(cpuMax)
	require.NoError(t, err)
	require.Equal(t, "50000 100000", string(value), "restoring the previous quota must not overwrite a newer update")
}

func TestMicroVMSnapshotSealFailureResumesBeforeAnyCapture(t *testing.T) {
	inst, assertResumed := resumableSnapshotTestInstance(t)
	sealErr := errors.New("disk publication failed")
	captured, err := snapshotMicroVM(context.Background(), inst, t.TempDir(), func(context.Context) error { return sealErr })
	require.ErrorIs(t, err, sealErr)
	assertResumed(captured)
}

func TestMicroVMSnapshotLostPauseResponseResolvesActualState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var actions snapshotActions
	inst := snapshotTestInstance(t, func(w http.ResponseWriter, r *http.Request) {
		actions.record(r.URL.Path)
		switch r.URL.Path {
		case "/api/v1/vm.pause":
			cancel() // CH paused, but the client never receives the response.
			// Returning now could race cancellation with an implicit HTTP200.
			// Wait for the client transport to close before finishing the handler.
			<-r.Context().Done()
		case "/api/v1/vm.info":
			_ = json.NewEncoder(w).Encode(struct {
				State microVMState `json:"state"`
			}{State: microVMPaused})
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})
	mustStop, err := snapshotMicroVM(ctx, inst, t.TempDir(), nil)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, mustStop)
	require.False(t, inst.isPaused(), "resolve and resume the actual paused VM despite caller cancellation")
	actions.require(t, "/api/v1/vm.pause", "/api/v1/vm.info", "/api/v1/vm.resume")
}

func TestMicroVMRestoreCannotUsePreviousConnectionAcknowledgement(t *testing.T) {
	host, guest := net.Pipe()
	t.Cleanup(func() { _ = host.Close(); _ = guest.Close() })
	require.NoError(t, guest.SetDeadline(time.Now().Add(5*time.Second)))
	ctrl := &microVMControl{
		ready: make(chan struct{}), pending: map[uint64]chan microvm.Message{},
		network: &microvm.Network{}, networkApplied: make(chan error, 1),
	}
	go ctrl.serve(host)
	encoder, decoder := microvm.NewEncoder(guest), microvm.NewDecoder(guest)
	require.NoError(t, encoder.Encode(microvm.Message{Type: microvm.MsgStarted}))
	msg, err := decoder.Decode()
	require.NoError(t, err)
	previous := ctrl.networkApplied
	current := ctrl.configureRestore(&microvm.Network{}, []string{microvm.DiskMount})
	require.NoError(t, encoder.Encode(microvm.Message{Type: microvm.MsgAck, ID: msg.ID, OK: true}))
	select {
	case err := <-previous:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("previous connection did not finish")
	}
	// Even a delayed duplicate Started from that connection cannot initialize
	// the next restore or satisfy its readiness channel.
	ctrl.pushNetwork(host, 1)
	select {
	case <-current:
		t.Fatal("a previous guest connection satisfied the new restore")
	default:
	}
}

func TestMicroVMRestoredGuestThawsBeforeNetworkWrites(t *testing.T) {
	for _, failThaw := range []bool{false, true} {
		t.Run(fmt.Sprint(failThaw), func(t *testing.T) {
			host, guest := net.Pipe()
			t.Cleanup(func() { _ = host.Close(); _ = guest.Close() })
			ctrl := &microVMControl{ready: make(chan struct{}), networkApplied: make(chan error, 1), pending: map[uint64]chan microvm.Message{}}
			ctrl.networkApplied <- errors.New("stale restore result")
			ctrl.configureRestore(&microvm.Network{}, []string{microvm.DiskMount, "/data"})
			go ctrl.serve(host)
			result := make(chan []microvm.Message, 1)
			go func() {
				enc, dec := microvm.NewEncoder(guest), microvm.NewDecoder(guest)
				if err := enc.Encode(microvm.Message{Type: microvm.MsgStarted}); err != nil {
					return
				}
				var messages []microvm.Message
				count := 3
				if failThaw {
					count = 2
				}
				for range count {
					msg, err := dec.Decode()
					if err != nil {
						return
					}
					messages = append(messages, msg)
					ack := microvm.Message{Type: microvm.MsgAck, ID: msg.ID, OK: !(failThaw && len(messages) == 1)}
					if !ack.OK {
						ack.Error = "thaw rejected"
					}
					if err := enc.Encode(ack); err != nil {
						return
					}
				}
				result <- messages
			}()
			select {
			case err := <-ctrl.networkApplied:
				if failThaw {
					require.ErrorContains(t, err, "thaw rejected")
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("restore control did not complete")
			}
			messages := <-result
			require.Equal(t, microvm.MsgThaw, messages[0].Type)
			require.Equal(t, microvm.DiskMount, messages[0].Text)
			require.Equal(t, microvm.MsgThaw, messages[1].Type)
			require.Equal(t, "/data", messages[1].Text)
			if !failThaw {
				require.Equal(t, microvm.MsgNetwork, messages[2].Type)
			}
		})
	}
}

func TestMicroVMPreparationFailureIsRetryableStartupFailure(t *testing.T) {
	bundle := t.TempDir()
	data, err := json.Marshal(&specs.Spec{
		Root: &specs.Root{Path: t.TempDir()}, Linux: &specs.Linux{},
		Process: &specs.Process{Args: []string{"/bin/true"}, Cwd: "/"},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(bundle, "config.json"), data, 0600))
	vm := &MicroVM{cfg: Config{MicroVMStateRoot: t.TempDir()}, vms: map[string]*microVMInstance{}}
	// The missing network namespace fails before any guest process starts.
	code, err := vm.Run(context.Background(), "cold", bundle, nil)
	require.ErrorContains(t, err, "network namespace")
	require.Equal(t, int(types.ContainerExitCodeStartupFailed), code)
	code, err = vm.Restore(context.Background(), "restore", &RestoreOpts{BundlePath: bundle, ImagePath: t.TempDir()})
	require.ErrorContains(t, err, "network namespace")
	require.Equal(t, int(types.ContainerExitCodeStartupFailed), code)
}

func TestGuestFilesystemConnectRetriesBeforeSendingRequest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vsock.sock")
	listener, err := net.Listen("unix", path)
	require.NoError(t, err)
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for attempt := 0; attempt < 2; attempt++ {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			line, err := bufio.NewReader(conn).ReadString('\n')
			if err == nil && attempt == 1 && line == "CONNECT 1025\n" {
				_, _ = conn.Write([]byte("OK 1025\n"))
			}
			conn.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := dialGuestVsock(ctx, path, microvm.FSPort)
	require.NoError(t, err)
	conn.Close()
	<-done
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = dialGuestVsock(ctx, path+"-missing", microvm.FSPort)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestMicroVMOOMEventsDoNotBlockControlAcknowledgements(t *testing.T) {
	host, guest := net.Pipe()
	defer host.Close()
	defer guest.Close()
	require.NoError(t, guest.SetDeadline(time.Now().Add(2*time.Second)))
	ack := make(chan microvm.Message, 1)
	ctrl := &microVMControl{
		ready: make(chan struct{}), pending: map[uint64]chan microvm.Message{1: ack},
		applicationOOM: make(chan Event, 16), eventDone: make(chan struct{}),
	}
	go ctrl.serve(host)
	encoder := microvm.NewEncoder(guest)
	// No event consumer: excess OOM notifications must never stall the
	// protocol used for network updates, filesystem freezing and shutdown.
	for i := 0; i < 20; i++ {
		require.NoError(t, encoder.Encode(microvm.Message{Type: microvm.MsgApplicationOOM, OOM: &microvm.ApplicationOOM{Kills: 1}}))
	}
	require.NoError(t, encoder.Encode(microvm.Message{Type: microvm.MsgAck, ID: 1, OK: true}))
	select {
	case msg := <-ack:
		require.True(t, msg.OK)
	case <-time.After(2 * time.Second):
		t.Fatal("OOM notifications blocked the control acknowledgement")
	}
}

func TestRestoredGuestReadinessWaitsForHostClockAcknowledgement(t *testing.T) {
	for _, accepted := range []bool{true, false} {
		host, guest := net.Pipe()
		ctrl := &microVMControl{
			ready: make(chan struct{}), pending: map[uint64]chan microvm.Message{},
			network: &microvm.Network{IPv4: "10.0.0.2"}, networkApplied: make(chan error, 1),
		}
		go ctrl.serve(host)
		require.NoError(t, guest.SetDeadline(time.Now().Add(2*time.Second)))
		encoder, decoder := microvm.NewEncoder(guest), microvm.NewDecoder(guest)
		before := time.Now().UnixNano()
		require.NoError(t, encoder.Encode(microvm.Message{Type: microvm.MsgStarted, Pid: 1}))
		msg, err := decoder.Decode()
		require.NoError(t, err)
		require.Equal(t, microvm.MsgNetwork, msg.Type)
		require.GreaterOrEqual(t, msg.TimeUnixNano, before)
		require.LessOrEqual(t, msg.TimeUnixNano, time.Now().UnixNano())
		select {
		case <-ctrl.networkApplied:
			t.Fatal("restore became ready before the guest applied the clock")
		default:
		}
		require.NoError(t, encoder.Encode(microvm.Message{Type: microvm.MsgAck, ID: msg.ID, OK: accepted, Error: "clock rejected"}))
		select {
		case err := <-ctrl.networkApplied:
			if accepted {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "clock rejected")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("restore acknowledgement was lost")
		}
		guest.Close()
		host.Close()
	}
}

// The canvas is image content: a symlink where the host writes must be
// replaced, never followed onto the host.
func TestPrepareCanvasDoesNotFollowImageSymlinks(t *testing.T) {
	outside := t.TempDir()
	canvas := t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(canvas, ".beam")))
	require.NoError(t, os.Symlink(outside, filepath.Join(canvas, "tmp")))
	require.NoError(t, os.WriteFile(filepath.Join(canvas, "proc"), []byte("not a dir"), 0o644))

	inst := &microVMInstance{canvas: canvas}
	for _, dir := range []string{"tmp", "proc"} {
		require.NoError(t, inst.canvasDir(dir))
	}
	require.NoError(t, removeCanvasEntry(filepath.Join(canvas, microvm.CanvasDir)))
	require.NoError(t, inst.canvasDir(microvm.CanvasDir))
	require.NoError(t, writeFileNoFollow(filepath.Join(canvas, microvm.SpecFile), []byte("{}"), 0o644))

	for _, rel := range []string{"tmp", "proc", ".beam"} {
		info, err := os.Lstat(filepath.Join(canvas, rel))
		require.NoError(t, err)
		require.True(t, info.IsDir(), "%s must be a real directory", rel)
	}
	entries, err := os.ReadDir(outside)
	require.NoError(t, err)
	require.Empty(t, entries, "nothing may land outside the canvas")

	// A symlink planted where a file goes is refused rather than written through.
	require.NoError(t, os.Remove(filepath.Join(canvas, microvm.SpecFile)))
	require.NoError(t, os.Symlink(filepath.Join(outside, "pwned"), filepath.Join(canvas, microvm.SpecFile)))
	require.Error(t, writeFileNoFollow(filepath.Join(canvas, microvm.SpecFile), []byte("{}"), 0o644))
	_, err = os.Stat(filepath.Join(outside, "pwned"))
	require.True(t, os.IsNotExist(err))
}

func TestRestoreCanvasKeepsCheckpointExecutable(t *testing.T) {
	checkpoint := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(checkpoint, checkpointInitFile), []byte("captured agent"), 0o755))
	current := filepath.Join(t.TempDir(), "init")
	require.NoError(t, os.WriteFile(current, []byte("upgraded agent"), 0o755))
	rt := &MicroVM{cfg: Config{MicroVMInitPath: current}}
	spec := &specs.Spec{}
	prepare := func(from string) (*microVMInstance, error) {
		inst := &microVMInstance{canvas: t.TempDir()}
		t.Cleanup(func() {
			for _, target := range inst.submounts {
				require.NoError(t, unix.Unmount(target, unix.MNT_DETACH))
			}
		})
		return inst, rt.prepareCanvas(inst, spec, microvm.Network{}, microVMDisk{}, nil, from)
	}
	inst, err := prepare(checkpoint)
	if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) {
		t.Skip("requires privileged bind mounts")
	}
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(inst.canvas, microvm.InitPath))
	require.NoError(t, err)
	require.Equal(t, "captured agent", string(data))
	inst, err = prepare("")
	require.NoError(t, err)
	data, err = os.ReadFile(filepath.Join(inst.canvas, microvm.InitPath))
	require.NoError(t, err)
	require.Equal(t, "upgraded agent", string(data))
	require.NoError(t, os.Remove(filepath.Join(checkpoint, checkpointInitFile)))
	_, err = prepare(checkpoint)
	require.Error(t, err, "never substitute another executable into captured memory")
}

func TestLinkLocalFromMAC(t *testing.T) {
	mac, _ := net.ParseMAC("de:f0:d8:b8:88:fb")
	require.Equal(t, "fe80::dcf0:d8ff:feb8:88fb", linkLocalFromMAC(mac).String())
	require.Nil(t, linkLocalFromMAC(net.HardwareAddr{1, 2}))
}

type treeEntry struct {
	hdr  tar.Header
	body string
}

var treeModTime = time.Unix(1700000000, 0)

// guestTar builds a tar the way a guest could, owned by the test's user so
// extraction works unprivileged.
func guestTar(t *testing.T, entries ...treeEntry) *bytes.Buffer {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, entry := range entries {
		hdr := entry.hdr
		hdr.Uid, hdr.Gid = os.Getuid(), os.Getgid()
		hdr.Size = int64(len(entry.body))
		hdr.ModTime = treeModTime
		if hdr.Mode == 0 {
			hdr.Mode = 0o755
		}
		require.NoError(t, tw.WriteHeader(&hdr))
		_, err := tw.Write([]byte(entry.body))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	return &buf
}

// The guest chooses every name in the export: an entry routed through an
// earlier symlink must fail instead of landing on the host.
func TestExtractTreeDoesNotResolveThroughSymlinks(t *testing.T) {
	cases := []struct {
		name    string
		entries func(outside string) []treeEntry
	}{
		{"file below a symlinked directory", func(outside string) []treeEntry {
			return []treeEntry{
				{hdr: tar.Header{Name: "a", Typeflag: tar.TypeSymlink, Linkname: outside}},
				{hdr: tar.Header{Name: "a/cron.d/x", Typeflag: tar.TypeReg, Mode: 0o644}, body: "* * * * * root pwn\n"},
			}
		}},
		{"directory below a symlinked directory", func(outside string) []treeEntry {
			return []treeEntry{
				{hdr: tar.Header{Name: "a", Typeflag: tar.TypeSymlink, Linkname: outside}},
				{hdr: tar.Header{Name: "a/sub/", Typeflag: tar.TypeDir}},
			}
		}},
		{"directory entry over a symlink", func(outside string) []treeEntry {
			return []treeEntry{
				{hdr: tar.Header{Name: "d", Typeflag: tar.TypeSymlink, Linkname: outside}},
				{hdr: tar.Header{Name: "d/", Typeflag: tar.TypeDir, Mode: 0o777}},
			}
		}},
		{"fifo below a symlinked directory", func(outside string) []treeEntry {
			return []treeEntry{
				{hdr: tar.Header{Name: "a", Typeflag: tar.TypeSymlink, Linkname: outside}},
				{hdr: tar.Header{Name: "a/fifo", Typeflag: tar.TypeFifo, Mode: 0o600}},
			}
		}},
		{"hard link through a symlinked directory", func(outside string) []treeEntry {
			return []treeEntry{
				{hdr: tar.Header{Name: "a", Typeflag: tar.TypeSymlink, Linkname: outside}},
				{hdr: tar.Header{Name: "stolen", Typeflag: tar.TypeLink, Linkname: "a/secret"}},
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outside := t.TempDir()
			require.NoError(t, os.Chmod(outside, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(outside, "secret"), []byte("host"), 0o600))
			dst := t.TempDir()

			require.Error(t, extractTree(guestTar(t, tc.entries(outside)...), dst))

			entries, err := os.ReadDir(outside)
			require.NoError(t, err)
			require.Len(t, entries, 1, "nothing may be created outside the export")
			info, err := os.Stat(outside)
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o700), info.Mode().Perm(), "the outside directory keeps its mode")
			data, err := os.ReadFile(filepath.Join(outside, "secret"))
			require.NoError(t, err)
			require.Equal(t, "host", string(data))
			_, err = os.Lstat(filepath.Join(dst, "stolen"))
			require.True(t, os.IsNotExist(err), "no hard link to a host file")
		})
	}
}

// A file entry where an earlier entry left a symlink replaces the symlink.
func TestExtractTreeReplacesASymlinkWithAFile(t *testing.T) {
	outside := t.TempDir()
	dst := t.TempDir()
	target := filepath.Join(outside, "target")

	require.NoError(t, extractTree(guestTar(t,
		treeEntry{hdr: tar.Header{Name: "f", Typeflag: tar.TypeSymlink, Linkname: target}},
		treeEntry{hdr: tar.Header{Name: "f", Typeflag: tar.TypeReg, Mode: 0o644}, body: "guest"},
	), dst))

	_, err := os.Lstat(target)
	require.True(t, os.IsNotExist(err), "the write must not follow the symlink")
	info, err := os.Lstat(filepath.Join(dst, "f"))
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())
}

func TestExtractTreeReplaysTheGuestTree(t *testing.T) {
	dst := t.TempDir()
	require.NoError(t, extractTree(guestTar(t,
		treeEntry{hdr: tar.Header{Name: "etc/", Typeflag: tar.TypeDir}},
		treeEntry{hdr: tar.Header{Name: "etc/app.conf", Typeflag: tar.TypeReg, Mode: 0o640}, body: "key=value\n"},
		treeEntry{hdr: tar.Header{Name: "etc/current", Typeflag: tar.TypeSymlink, Linkname: "/etc/app.conf"}},
		treeEntry{hdr: tar.Header{Name: "etc/copy", Typeflag: tar.TypeLink, Linkname: "etc/app.conf", Mode: 0o640}},
		treeEntry{hdr: tar.Header{Name: "run/pipe", Typeflag: tar.TypeFifo, Mode: 0o620}},
	), dst))

	data, err := os.ReadFile(filepath.Join(dst, "etc/app.conf"))
	require.NoError(t, err)
	require.Equal(t, "key=value\n", string(data))
	file, err := os.Lstat(filepath.Join(dst, "etc/app.conf"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), file.Mode().Perm())
	require.True(t, file.ModTime().Equal(treeModTime))

	link, err := os.Readlink(filepath.Join(dst, "etc/current"))
	require.NoError(t, err)
	require.Equal(t, "/etc/app.conf", link, "symlinks are kept as written")
	copied, err := os.Lstat(filepath.Join(dst, "etc/copy"))
	require.NoError(t, err)
	require.True(t, os.SameFile(file, copied))

	pipe, err := os.Lstat(filepath.Join(dst, "run/pipe"))
	require.NoError(t, err)
	require.True(t, pipe.Mode()&os.ModeNamedPipe != 0)
	require.Equal(t, os.FileMode(0o620), pipe.Mode().Perm())

	dir, err := os.Stat(filepath.Join(dst, "etc"))
	require.NoError(t, err)
	require.True(t, dir.ModTime().Equal(treeModTime), "directory times are restored after their children")
}

func TestGuestReset(t *testing.T) {
	// As Cloud Hypervisor writes them: indented, with a blank line between.
	event := func(source, name string) string {
		return fmt.Sprintf("{\n  \"timestamp\": {\n    \"secs\": 12,\n    \"nanos\": 345\n  },\n  \"source\": %q,\n  \"event\": %q,\n  \"properties\": null\n}\n\n", source, name)
	}
	booted := event("vm", "booting") + event("virtio-device", "activated") + event("vm", "booted")
	for name, test := range map[string]struct {
		events string
		reset  bool
	}{
		"a guest that rebooted":              {booted + event("vm", "rebooting") + event("vm", "rebooted"), true},
		"a guest that powered off":           {booted + event("vm", "shutdown"), false},
		"a guest that is still running":      {booted, false},
		"another source's event":             {booted + event("vmm", "rebooting"), false},
		"a hypervisor that died mid-event":   {booted + `{"source": "vm", "event": "reboo`, false},
		"a hypervisor that reported nothing": {"", false},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.reset, guestReset(strings.NewReader(test.events)))
		})
	}
}

func TestMicroVMStateWaitsForTerminalResult(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 0")
	require.NoError(t, cmd.Start())
	require.NoError(t, cmd.Wait())
	inst := &microVMInstance{hypervisor: cmd, exitDone: make(chan struct{})}
	rt := &MicroVM{vms: map[string]*microVMInstance{"vm": inst}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := rt.State(ctx, "vm")
	require.ErrorIs(t, err, context.DeadlineExceeded, "a reaped process is not yet a collected exit result")
	inst.finishExit(0, nil)
	state, err := rt.State(context.Background(), "vm")
	require.NoError(t, err)
	require.Equal(t, "stopped", state.Status)
	require.NotNil(t, state.ExitCode)
	require.Zero(t, *state.ExitCode)
}

func vfioTestHost(t *testing.T, groups map[string][]string) microVMVFIOHost {
	t.Helper()
	root := t.TempDir()
	host := microVMVFIOHost{pciRoot: filepath.Join(root, "pci"), devRoot: filepath.Join(root, "dev")}
	driver := filepath.Join(root, "drivers", "vfio-pci")
	require.NoError(t, os.MkdirAll(driver, 0o700))
	require.NoError(t, os.MkdirAll(host.devRoot, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(host.devRoot, "vfio"), nil, 0o600))
	for group, devices := range groups {
		path := filepath.Join(root, "groups", group)
		require.NoError(t, os.MkdirAll(filepath.Join(path, "devices"), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(host.devRoot, group), nil, 0o600))
		for _, device := range devices {
			pci := filepath.Join(host.pciRoot, device)
			require.NoError(t, os.MkdirAll(pci, 0o700))
			class := "0x040300\n"
			if strings.HasSuffix(device, ".0") {
				class = "0x030000\n"
			}
			require.NoError(t, os.WriteFile(filepath.Join(pci, "class"), []byte(class), 0o600))
			require.NoError(t, os.Symlink(driver, filepath.Join(pci, "driver")))
			require.NoError(t, os.Symlink(path, filepath.Join(pci, "iommu_group")))
			require.NoError(t, os.Symlink(pci, filepath.Join(path, "devices", device)))
			host.devices = append(host.devices, device)
		}
	}
	return host
}

func TestMicroVMVFIOValidation(t *testing.T) {
	require.True(t, (&MicroVM{}).Capabilities().CheckpointRestore)
	require.False(t, (&MicroVM{}).Capabilities().GPU)
	caps := (&MicroVM{cfg: Config{MicroVMVFIODevices: []string{"0000:01:00.0"}}}).Capabilities()
	require.True(t, caps.GPU)
	require.False(t, caps.CDI, "guest drivers do not use host CDI injection")
	require.True(t, caps.CheckpointRestore, "a GPU guest gives its devices up for the snapshot")
	for _, value := range []string{"../77", "01:00.0", "0000:01:20.0", "0000:01:00.8", "0000:01:00.0,path=/tmp/evil", "0000:01:00.0,0000:01:00.0"} {
		_, err := (microVMVFIOHost{devices: []string{"0000:01:00.0"}}).acquire(value)
		require.ErrorContains(t, err, "invalid or duplicate", value)
	}
	_, err := (microVMVFIOHost{}).acquire("")
	require.ErrorContains(t, err, "configured VFIO inventory", "GPU allocation must never fall back to a CPU guest")

	for _, test := range []struct {
		name, want string
		change     func(*testing.T, *microVMVFIOHost)
	}{
		{"unauthorized", "not authorized", func(t *testing.T, h *microVMVFIOHost) { h.devices = nil }},
		{"driver", "bound to vfio-pci", func(t *testing.T, h *microVMVFIOHost) {
			require.NoError(t, os.Remove(filepath.Join(h.pciRoot, "0000:01:00.0", "driver")))
		}},
		{"iommu", "requires an IOMMU group", func(t *testing.T, h *microVMVFIOHost) {
			require.NoError(t, os.Remove(filepath.Join(h.pciRoot, "0000:01:00.0", "iommu_group")))
		}},
		{"partial group", "complete", func(t *testing.T, h *microVMVFIOHost) {
			group, err := filepath.EvalSymlinks(filepath.Join(h.pciRoot, "0000:01:00.0", "iommu_group"))
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(group, "devices", "0000:01:00.1"), nil, 0o600))
		}},
		{"vfio access", "open VFIO vfio", func(t *testing.T, h *microVMVFIOHost) {
			require.NoError(t, os.Remove(filepath.Join(h.devRoot, "vfio")))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			host := vfioTestHost(t, map[string][]string{"77": {"0000:01:00.0"}})
			test.change(t, &host)
			_, err := host.acquire("0000:01:00.0")
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestMicroVMVFIOFailedClaimReleasesGroups(t *testing.T) {
	host := vfioTestHost(t, map[string][]string{"77": {"0000:01:00.0", "0000:01:00.1"}})
	devices := strings.Join(host.devices, ",")
	require.NoError(t, os.Remove(filepath.Join(host.devRoot, "77")))
	_, err := host.acquire(devices)
	require.ErrorContains(t, err, "open VFIO 77")
	require.NoError(t, os.WriteFile(filepath.Join(host.devRoot, "77"), nil, 0o600))
	claim, err := host.acquire(devices)
	require.NoError(t, err, "an unsuccessful launch must not strand any group reservations")
	claim.close()
}

func TestMicroVMVFIOOwnership(t *testing.T) {
	host := vfioTestHost(t, map[string][]string{"77": {"0000:01:00.0", "0000:01:00.1"}})
	devices := strings.Join(host.devices, ",")
	claim, err := host.acquire(devices)
	require.NoError(t, err)
	t.Cleanup(claim.close)
	_, err = host.acquire(devices)
	require.ErrorContains(t, err, "already in use")

	cmd := exec.Command("sleep", "30")
	cmd.ExtraFiles = claim.locks
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	claim.close()
	_, err = host.acquire(devices)
	require.ErrorContains(t, err, "already in use", "the VMM retains ownership after its parent closes the descriptors")
	require.NoError(t, cmd.Process.Kill())
	_ = cmd.Wait()
	claim, err = host.acquire(devices)
	require.NoError(t, err, "the GPU is reusable after the VMM exits")
	claim.close()
	claim.close()

	base := microVMHypervisorArgs("/state", "/kernel", "", 4, 8<<30, "", "02:00:00:00:00:01", microVMDisk{}, nil, nil, false)
	args := microVMHypervisorArgs("/state", "/kernel", "", 4, 8<<30, "", "02:00:00:00:00:01", microVMDisk{}, nil, nil, true)
	require.Contains(t, base, "--balloon")
	require.NotContains(t, args, "--balloon", "DMA-pinned RAM cannot be reclaimed")
	_, err = rewriteSnapshotConfig([]byte(`{"devices":[{"path":"/sys/bus/pci/devices/0000:01:00.0"}]}`), "/state", microvm.Network{}, microVMDisk{}, nil, snapshotHost{})
	require.ErrorContains(t, err, "VFIO", "a foreign snapshot cannot bypass device authorization")
}

func TestMicroVMVFIODefaultAllocation(t *testing.T) {
	host := vfioTestHost(t, map[string][]string{
		"77": {"0000:01:00.0", "0000:01:00.1"},
		"78": {"0000:25:00.0", "0000:25:00.1"},
	})
	first, err := host.acquire("")
	require.NoError(t, err)
	t.Cleanup(first.close)
	require.Equal(t, []string{filepath.Join(host.pciRoot, "0000:01:00.0"), filepath.Join(host.pciRoot, "0000:01:00.1")}, first.paths)
	second, err := host.acquire("")
	require.NoError(t, err)
	t.Cleanup(second.close)
	require.Equal(t, []string{filepath.Join(host.pciRoot, "0000:25:00.0"), filepath.Join(host.pciRoot, "0000:25:00.1")}, second.paths)
	_, err = host.acquire("")
	require.ErrorContains(t, err, "no available VFIO group", "exhausted GPU capacity must never become a CPU guest")
	first.close()
	reused, err := host.acquire("")
	require.NoError(t, err)
	t.Cleanup(reused.close)
	require.Equal(t, first.paths, reused.paths)
}

func TestMicroVMVFIOConcurrentAllocation(t *testing.T) {
	const count = 8
	groups := make(map[string][]string, count)
	for i := range count {
		groups[fmt.Sprint(i+10)] = []string{fmt.Sprintf("0000:%02x:00.0", i+1), fmt.Sprintf("0000:%02x:00.1", i+1)}
	}
	host := vfioTestHost(t, groups)
	type result struct {
		claim *microVMVFIO
		err   error
	}
	results := make(chan result, count)
	start := make(chan struct{})
	for range count {
		go func() {
			<-start
			claim, err := host.acquire("")
			results <- result{claim, err}
		}()
	}
	close(start)
	seen := map[string]bool{}
	for range count {
		select {
		case result := <-results:
			require.NoError(t, result.err)
			t.Cleanup(result.claim.close)
			require.Len(t, result.claim.paths, 2, "each guest receives one GPU and its companion function")
			key := strings.Join(result.claim.paths, ",")
			require.False(t, seen[key], "concurrent guests must not receive the same group")
			seen[key] = true
		case <-time.After(5 * time.Second):
			t.Fatal("GPU allocation blocked instead of using another free group")
		}
	}
	require.Len(t, seen, count)
	_, err := host.acquire("")
	require.ErrorContains(t, err, "no available VFIO group", "a ninth guest must not oversubscribe eight GPUs")
}

func TestMicroVMVFIOExplicitSelection(t *testing.T) {
	host := vfioTestHost(t, map[string][]string{
		"77": {"0000:01:00.0", "0000:01:00.1"},
		"78": {"0000:25:00.0", "0000:25:00.1"},
	})
	selected := "0000:25:00.0,0000:25:00.1"
	explicit, err := host.acquire(selected)
	require.NoError(t, err)
	t.Cleanup(explicit.close)
	_, err = host.acquire(selected)
	require.ErrorContains(t, err, "already in use", "an explicit selection must not silently change GPUs")
	automatic, err := host.acquire("")
	require.NoError(t, err)
	t.Cleanup(automatic.close)
	require.Equal(t, filepath.Join(host.pciRoot, "0000:01:00.0"), automatic.paths[0])
}

func TestMicroVMVFIOAliasesShareOwnership(t *testing.T) {
	host := vfioTestHost(t, map[string][]string{"77": {"0000:01:00.0"}})
	claim, err := host.acquire("")
	require.NoError(t, err)
	t.Cleanup(claim.close)
	alias := filepath.Join(t.TempDir(), "pci")
	require.NoError(t, os.Symlink(host.pciRoot, alias))
	other := microVMVFIOHost{devices: host.devices, pciRoot: alias, devRoot: host.devRoot}
	_, err = other.acquire("0000:01:00.0")
	require.ErrorContains(t, err, "already in use", "separate allocators must reserve the shared group inode")
	claim.close()
	reused, err := other.acquire("")
	require.NoError(t, err)
	t.Cleanup(reused.close)
}

func TestMicroVMVFIORejectsMultipleGPUs(t *testing.T) {
	for _, groups := range []map[string][]string{
		{"77": {"0000:01:00.0"}, "78": {"0000:25:00.0"}},
		{"77": {"0000:01:00.0", "0000:25:00.0"}},
		{"77": {"0000:01:00.1"}},
	} {
		host := vfioTestHost(t, groups)
		_, err := host.acquire(strings.Join(host.devices, ","))
		require.ErrorContains(t, err, "exactly one GPU")
	}
}

func TestMicroVMVFIOAutomaticallySelectsOnlySingleGPUGroup(t *testing.T) {
	host := vfioTestHost(t, map[string][]string{
		"77": {"0000:01:00.0", "0000:25:00.0"},
		"78": {"0000:41:00.1"},
		"79": {"0000:61:00.0", "0000:61:00.1"},
	})
	require.NoError(t, os.WriteFile(filepath.Join(host.pciRoot, "0000:61:00.0", "class"), []byte("0x030200\n"), 0o600))
	claim, err := host.acquire("")
	require.NoError(t, err)
	t.Cleanup(claim.close)
	require.Equal(t, []string{filepath.Join(host.pciRoot, "0000:61:00.0"), filepath.Join(host.pciRoot, "0000:61:00.1")}, claim.paths)
	_, err = host.acquire("")
	require.ErrorContains(t, err, "no available VFIO group", "coupled GPUs and non-GPU groups are ineligible")
}

func TestMicroVMVFIODoesNotSkipPreflightFailures(t *testing.T) {
	host := vfioTestHost(t, map[string][]string{"77": {"0000:01:00.0"}, "78": {"0000:25:00.0"}})
	require.NoError(t, os.Remove(filepath.Join(host.devRoot, "77")))
	_, err := host.acquire("")
	require.ErrorContains(t, err, "open VFIO 77", "only busy locks are skipped")
	require.NoError(t, os.WriteFile(filepath.Join(host.devRoot, "77"), nil, 0o600))
	claim, err := host.acquire("")
	require.NoError(t, err, "failed preflight must release its group")
	claim.close()
}

func TestMicroVMVFIORequestValidation(t *testing.T) {
	for _, test := range []struct {
		count, devices, want string
	}{
		{"1", "", "configured VFIO inventory"},
		{"2", "", "exactly one GPU"},
		{"-1", "", "exactly one GPU"},
		{"invalid", "", "exactly one GPU"},
		{"0", "0000:01:00.0", "requires a GPU request"},
	} {
		rt := &MicroVM{}
		inst := &microVMInstance{stateDir: filepath.Join(t.TempDir(), "state")}
		spec := &specs.Spec{Annotations: map[string]string{MicroVMGPUCountAnnotation: test.count, MicroVMVFIOAnnotation: test.devices}}
		_, _, _, err := rt.prepare(context.Background(), inst, spec, "")
		require.ErrorContains(t, err, test.want)
		require.Nil(t, inst.vfio)
		require.NoDirExists(t, inst.stateDir, "invalid requests must fail before VM preparation")
	}
}

func TestMicroVMVFIOCPURequestSkipsInventory(t *testing.T) {
	for _, count := range []string{"", "0"} {
		rt := &MicroVM{cfg: Config{MicroVMVFIODevices: []string{"0000:01:00.0"}}}
		inst := &microVMInstance{stateDir: filepath.Join(t.TempDir(), "state")}
		spec := &specs.Spec{Annotations: map[string]string{MicroVMGPUCountAnnotation: count}}
		_, _, _, err := rt.prepare(context.Background(), inst, spec, "checkpoint")
		require.ErrorContains(t, err, "no network namespace", "CPU preparation must not inspect host GPU inventory or reject memory restore")
		require.NotNil(t, inst.vfio)
		require.Empty(t, inst.vfio.paths)
		require.Empty(t, inst.vfio.locks)
	}
}

func imageKernelTestInstance(t *testing.T) (*MicroVM, *microVMInstance, string) {
	t.Helper()
	canvas := t.TempDir()
	image := filepath.Join(canvas, MicroVMImageKernelPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(image), 0o755))
	workerKernel := filepath.Join(t.TempDir(), "vmlinux")
	require.NoError(t, os.WriteFile(workerKernel, []byte("invalid worker kernel"), 0o600))
	inherited := func(name string) *os.File {
		file, err := os.Create(filepath.Join(t.TempDir(), name))
		require.NoError(t, err)
		t.Cleanup(func() { _ = file.Close() })
		return file
	}
	inst := &microVMInstance{canvas: canvas, stateDir: t.TempDir(), ram: &microVMRAM{file: inherited("ram")},
		vfio: &microVMVFIO{paths: []string{"gpu"}, locks: []*os.File{inherited("lock")}}}
	t.Cleanup(inst.releaseKernel)
	return &MicroVM{cfg: Config{MicroVMKernelPath: workerKernel}}, inst, image
}

func TestMicroVMImageKernelSelection(t *testing.T) {
	for _, mode := range []string{"GPU", "missing", "CPU"} {
		t.Run(mode, func(t *testing.T) {
			rt, inst, image := imageKernelTestInstance(t)
			content := []byte("kernel paired with image drivers")
			if mode != "missing" {
				require.NoError(t, os.WriteFile(image, content, 0o644))
			}
			if mode == "CPU" {
				inst.vfio = nil
			}
			kernel, err := rt.kernelPath(inst)
			require.NoError(t, err)
			if mode != "GPU" {
				require.Equal(t, rt.cfg.MicroVMKernelPath, kernel)
				require.Nil(t, inst.kernel)
				return
			}
			require.Equal(t, "/proc/self/fd/5", kernel, "the hypervisor inherits the kernel after the group lock and the guest RAM")
			require.Equal(t, []*os.File{inst.vfio.locks[0], inst.ram.file, inst.kernel}, inst.inherited())
			require.Equal(t, "/proc/self/fd/4", inst.inheritedPath(inst.ramFile()))

			replacement := filepath.Join(t.TempDir(), "vmlinux")
			require.NoError(t, os.WriteFile(replacement, []byte("another kernel"), 0o644))
			require.NoError(t, os.Rename(replacement, image))
			inherited, err := io.ReadAll(inst.kernel)
			require.NoError(t, err)
			require.Equal(t, content, inherited, "the VMM loads the file that was checked, not whatever the image path names later")

			inst.releaseKernel()
			require.Equal(t, []*os.File{inst.vfio.locks[0], inst.ram.file}, inst.inherited(), "the worker keeps the guest RAM, which a checkpoint saves")
			ram := inst.ram.file
			inst.releaseRAM()
			require.Equal(t, inst.vfio.locks, inst.inherited())
			require.ErrorIs(t, ram.Close(), os.ErrClosed, "the RAM goes with the VM")
			files, err := os.ReadDir(inst.stateDir)
			require.NoError(t, err)
			require.Empty(t, files, "booting must not copy the kernel")
		})
	}
}

func TestMicroVMImageKernelRejectsUnsafeFiles(t *testing.T) {
	for _, kind := range []string{"symlink", "ancestor symlink", "directory", "FIFO", "empty", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			rt, inst, image := imageKernelTestInstance(t)
			switch kind {
			case "symlink", "ancestor symlink":
				outside := t.TempDir()
				require.NoError(t, os.WriteFile(filepath.Join(outside, "vmlinux"), []byte("outside kernel"), 0o644))
				if kind == "ancestor symlink" {
					require.NoError(t, os.Remove(filepath.Dir(image)))
					require.NoError(t, os.Symlink(outside, filepath.Dir(image)))
				} else {
					require.NoError(t, os.Symlink(filepath.Join(outside, "vmlinux"), image))
				}
			case "directory":
				require.NoError(t, os.Mkdir(image, 0o755))
			case "FIFO":
				require.NoError(t, unix.Mkfifo(image, 0o600))
			case "empty", "oversized":
				file, err := os.Create(image)
				require.NoError(t, err)
				if kind == "oversized" {
					require.NoError(t, file.Truncate(microVMMaxKernelBytes+1))
				}
				require.NoError(t, file.Close())
			}
			result := make(chan error, 1)
			go func() { _, err := rt.kernelPath(inst); result <- err }()
			select {
			case err := <-result:
				require.ErrorContains(t, err, "image kernel")
			case <-time.After(5 * time.Second):
				t.Fatal("image kernel probe blocked")
			}
			require.Nil(t, inst.kernel, "the hypervisor must not inherit an unsafe kernel")
		})
	}
}

func TestMicroVMImageKernelRelativeRoot(t *testing.T) {
	rt, original, image := imageKernelTestInstance(t)
	require.NoError(t, os.WriteFile(image, []byte("image kernel"), 0o644))
	bundle := filepath.Dir(original.canvas)
	spec := &specs.Spec{Root: &specs.Root{Path: filepath.Base(original.canvas)}, Process: &specs.Process{Args: []string{"true"}}, Linux: &specs.Linux{}}
	data, err := json.Marshal(spec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(bundle, "config.json"), data, 0o644))
	rt.cfg.MicroVMStateRoot, rt.vms = t.TempDir(), make(map[string]*microVMInstance)
	inst, loaded, err := rt.newInstance("relative-root", bundle)
	require.NoError(t, err)
	require.Equal(t, spec.Root.Path, loaded.Root.Path, "OCI root paths remain relative to their bundle")
	require.Equal(t, original.canvas, inst.canvas)
	inst.vfio = original.vfio
	_, err = rt.kernelPath(inst)
	require.NoError(t, err)
	t.Cleanup(inst.releaseKernel)
	require.NotNil(t, inst.kernel)
	inherited, err := io.ReadAll(inst.kernel)
	require.NoError(t, err)
	require.Equal(t, "image kernel", string(inherited))
}

func assertCPUFile(t *testing.T, cgroup, name, want string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(cgroup, name))
	require.NoError(t, err)
	require.Equal(t, want, string(data))
}

func newCPUFixture(t *testing.T, quota, weight string) string {
	t.Helper()
	cgroup := t.TempDir()
	for name, value := range map[string]string{"cpu.max": quota, "cpu.weight": weight} {
		require.NoError(t, os.WriteFile(filepath.Join(cgroup, name), []byte(value), 0644))
	}
	return cgroup
}
