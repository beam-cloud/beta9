package disk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// recordingRunner is fakeRunner plus a log of every command it saw.
type recordingRunner struct {
	mu    sync.Mutex
	calls [][]string
}

func (r *recordingRunner) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	r.mu.Lock()
	r.calls = append(r.calls, append([]string{name}, args...))
	r.mu.Unlock()
	return fakeRunner(ctx, name, args...)
}

func (r *recordingRunner) sawBinary(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, call := range r.calls {
		if strings.Contains(call[0], name) {
			return true
		}
	}
	return false
}

func TestSealUsesFreezeHookForExportedVolume(t *testing.T) {
	volume, server := newTestVolume(t)
	server.writtenB.Store(4096)

	runner := &recordingRunner{}
	volume.manager.run = runner.run
	volume.state.Export = string(ExportVhostUser)
	volume.state.Mountpoint = ""

	var order []string
	volume.freeze = func(ctx context.Context) (func(), error) {
		order = append(order, "freeze")
		return func() { order = append(order, "thaw") }, nil
	}

	sealed, skipped, err := volume.Seal(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if skipped || len(sealed) != 1 {
		t.Fatalf("expected one sealed layer, got %v skipped=%v", sealed, skipped)
	}
	if strings.Join(order, ",") != "freeze,thaw" {
		t.Fatalf("freeze hook order = %v", order)
	}
	if server.pivots.Load() != 1 {
		t.Fatalf("expected one pivot, got %d", server.pivots.Load())
	}
	if runner.sawBinary("fsfreeze") {
		t.Fatal("exported volumes must not run the host fsfreeze")
	}
}

func TestSealExportedVolumeWithoutHookIsCrashConsistent(t *testing.T) {
	volume, server := newTestVolume(t)
	server.writtenB.Store(4096)
	runner := &recordingRunner{}
	volume.manager.run = runner.run
	volume.state.Export = string(ExportVhostUser)

	if _, _, err := volume.Seal(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if runner.sawBinary("fsfreeze") {
		t.Fatal("no mountpoint to freeze on the host")
	}
}

func TestSealFreezeHookFailureRollsBack(t *testing.T) {
	volume, server := newTestVolume(t)
	server.writtenB.Store(4096)
	volume.state.Export = string(ExportVhostUser)
	volume.freeze = func(ctx context.Context) (func(), error) {
		return nil, errors.New("guest unreachable")
	}
	originalHead := volume.state.HeadPath

	if _, _, err := volume.Seal(context.Background(), false); err == nil {
		t.Fatal("expected freeze failure")
	}
	if volume.state.HeadPath != originalHead || len(volume.state.Pending) != 0 {
		t.Fatalf("seal was not rolled back: %+v", volume.state)
	}
	if server.pivots.Load() != 0 {
		t.Fatal("must not pivot when the guest could not be frozen")
	}
}

func TestStartQSDExportArguments(t *testing.T) {
	// The daemon never runs; the recording runner captures its arguments and
	// materializes the pidfile and export socket so startQSD can return.
	dir, err := os.MkdirTemp("", "qsd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	runner := &recordingRunner{}
	manager := NewManager(Config{Root: filepath.Join(dir, "root"), Runner: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		out, err := runner.run(ctx, name, args...)
		if strings.Contains(name, "qemu-storage-daemon") {
			runtimeDir := filepath.Join(dir, "rt")
			_ = os.WriteFile(filepath.Join(runtimeDir, "qsd.pid"), []byte("1"), 0o600)
			for _, arg := range args {
				if strings.HasPrefix(arg, "type=vhost-user-blk") {
					_ = os.WriteFile(filepath.Join(runtimeDir, "vhost-user-blk.sock"), nil, 0o600)
				}
			}
			_ = os.WriteFile(filepath.Join(runtimeDir, "nbd.sock"), nil, 0o600)
		}
		return out, err
	}})

	proc, err := manager.startQSD(context.Background(), filepath.Join(dir, "rt"), "/head.qcow2", "fmt-3", false, ExportVhostUser)
	if err != nil {
		t.Fatal(err)
	}
	if proc.exportSocket == "" || proc.nbdSocket != "" {
		t.Fatalf("vhost-user export must set exportSocket only: %+v", proc)
	}
	joined := strings.Join(runner.calls[len(runner.calls)-1], " ")
	if !strings.Contains(joined, "type=vhost-user-blk,id=vol,node-name=fmt-3,addr.type=unix,addr.path="+proc.exportSocket+",writable=on") {
		t.Fatalf("unexpected export arguments: %s", joined)
	}
	if strings.Contains(joined, "--nbd-server") {
		t.Fatal("vhost-user export must not start an NBD server")
	}

	proc, err = manager.startQSD(context.Background(), filepath.Join(dir, "rt"), "/head.qcow2", "fmt-0", true, ExportNBD)
	if err != nil {
		t.Fatal(err)
	}
	if proc.nbdSocket == "" || proc.exportSocket != "" {
		t.Fatalf("nbd export must set nbdSocket only: %+v", proc)
	}
	joined = strings.Join(runner.calls[len(runner.calls)-1], " ")
	if !strings.Contains(joined, "--nbd-server") || !strings.Contains(joined, "type=nbd,id=vol,node-name=fmt-0,name=vol,writable=off") {
		t.Fatalf("unexpected nbd arguments: %s", joined)
	}
}

// A container's final cleanup detaches by key after its final sync already
// released the volume; if a successor re-attached the same key in between,
// that late detach must not take the successor's volume down.
func TestDetachOwnedLeavesSuccessorVolumeAlone(t *testing.T) {
	manager := NewManager(Config{Root: t.TempDir(), Runner: fakeRunner})
	dir := filepath.Join(manager.root, "vol")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	successor := &Volume{manager: manager, dir: dir, owner: "container-b",
		state: &volumeState{Key: "vol", Attached: true, Export: string(ExportVhostUser)}}
	manager.volumes["vol"] = successor

	if err := manager.DetachOwned(context.Background(), "vol", "container-a"); err != nil {
		t.Fatalf("stale owner detach must be a no-op, got %v", err)
	}
	if got, ok := manager.Volume("vol"); !ok || got != successor || !successor.state.Attached {
		t.Fatal("predecessor's late detach removed the successor's volume")
	}

	if err := manager.DetachOwned(context.Background(), "vol", "container-b"); err != nil {
		t.Fatalf("owner detach: %v", err)
	}
	if _, ok := manager.Volume("vol"); ok || successor.state.Attached {
		t.Fatal("owner detach must release the volume")
	}
	if err := manager.DetachOwned(context.Background(), "missing", "container-b"); err != nil {
		t.Fatalf("unknown key must be a no-op, got %v", err)
	}
}

func TestAttachRejectsUnknownExportAndNBDWithoutMountpoint(t *testing.T) {
	manager := NewManager(Config{Root: t.TempDir(), Runner: fakeRunner})
	_, err := manager.Attach(context.Background(), AttachSpec{Key: "k", VirtualSizeBytes: 1, Export: ExportMode("bogus")}, nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported export mode") {
		t.Fatalf("expected unsupported export error, got %v", err)
	}
	_, err = manager.Attach(context.Background(), AttachSpec{Key: "k", VirtualSizeBytes: 1}, nil)
	if err == nil || !strings.Contains(err.Error(), "requires a mountpoint") {
		t.Fatalf("nbd attach without mountpoint must fail, got %v", err)
	}
}
