package disk

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

// fakeQMP is a scriptable QMP server on a unix socket.
type fakeQMP struct {
	listener   net.Listener
	writtenB   atomic.Int64
	failPivots atomic.Bool
	nodes      atomic.Value // []string
	pivots     atomic.Int64
}

func newFakeQMP(t *testing.T, socketPath string) *fakeQMP {
	t.Helper()
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeQMP{listener: listener}
	server.nodes.Store([]string{})
	go server.serve()
	t.Cleanup(func() { listener.Close() })
	return server
}

func (f *fakeQMP) serve() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

func (f *fakeQMP) handle(conn net.Conn) {
	defer conn.Close()
	fmt.Fprintf(conn, `{"QMP":{"version":{}}}`)
	dec := json.NewDecoder(conn)
	for {
		var request struct {
			Execute   string          `json:"execute"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := dec.Decode(&request); err != nil {
			return
		}
		switch request.Execute {
		case types.QMPCommandCapabilities, types.QMPCommandQuit, types.QMPCommandBlockdevAdd, types.QMPCommandBlockdevDel:
			fmt.Fprintf(conn, `{"return":{}}`)
		case types.QMPCommandQueryBlockstats:
			fmt.Fprintf(conn, `{"return":[{"node-name":"file-fmt-0","stats":{"wr_highest_offset":%d}}]}`, f.writtenB.Load())
		case types.QMPCommandQueryNamedBlockNodes:
			// Listed nodes are reported as wired into a backing chain, which
			// is what pivotCommitted uses to recognize a committed pivot.
			nodes := f.nodes.Load().([]string)
			entries := make([]string, 0, len(nodes))
			for _, node := range nodes {
				entries = append(entries, fmt.Sprintf(`{"node-name":%q,"file":"","backing_file_depth":1}`, node))
			}
			fmt.Fprintf(conn, `{"return":[%s]}`, strings.Join(entries, ","))
		case types.QMPCommandTransaction:
			if f.failPivots.Load() {
				fmt.Fprintf(conn, `{"error":{"class":"GenericError","desc":"injected pivot failure"}}`)
				continue
			}
			f.pivots.Add(1)
			fmt.Fprintf(conn, `{"return":{}}`)
		default:
			fmt.Fprintf(conn, `{"error":{"class":"CommandNotFound","desc":%q}}`, request.Execute)
		}
	}
}

// fakeRunner satisfies qemu-img/fsfreeze/mount calls without external tools:
// "create" materializes the target file, everything else succeeds.
func fakeRunner(_ context.Context, name string, args ...string) ([]byte, error) {
	if strings.Contains(name, "qemu-img") && len(args) > 0 && args[0] == "create" {
		return nil, os.WriteFile(args[len(args)-2], []byte("qcow2-stub"), 0o600)
	}
	return nil, nil
}

func newTestVolume(t *testing.T) (*Volume, *fakeQMP) {
	t.Helper()
	dir := t.TempDir()
	layersDir := filepath.Join(dir, layersSubdir)
	if err := os.MkdirAll(layersDir, 0o700); err != nil {
		t.Fatal(err)
	}
	headPath := filepath.Join(layersDir, "head-000000.qcow2")
	if err := os.WriteFile(headPath, []byte("qcow2-stub"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Unix socket paths are limited to ~104 bytes on macOS; t.TempDir names
	// can exceed that, so the socket lives in its own short-lived directory.
	socketDir, err := os.MkdirTemp("", "qmp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(socketDir) })
	qmpSocket := filepath.Join(socketDir, "qmp.sock")
	server := newFakeQMP(t, qmpSocket)

	manager := NewManager(Config{Root: filepath.Join(dir, "root"), Runner: fakeRunner})
	state := &volumeState{
		Key:              "test",
		VirtualSizeBytes: 1 << 30,
		Mountpoint:       filepath.Join(dir, "mnt"),
		Attached:         true,
		Formatted:        true,
		HeadPath:         headPath,
		QMPSocket:        qmpSocket,
	}
	volume := &Volume{manager: manager, dir: dir, state: state, fmtNode: "fmt-0", freshHead: true}
	if err := saveVolumeState(dir, state); err != nil {
		t.Fatal(err)
	}
	return volume, server
}

func TestSealSkipsUnchangedVolume(t *testing.T) {
	volume, server := newTestVolume(t)
	server.writtenB.Store(0)

	sealed, skipped, err := volume.Seal(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if !skipped || sealed != nil {
		t.Fatalf("expected skip, got sealed=%v skipped=%v", sealed, skipped)
	}
	if server.pivots.Load() != 0 {
		t.Fatal("skip must not pivot")
	}
}

func TestSealPivotsAndPublishes(t *testing.T) {
	volume, server := newTestVolume(t)
	server.writtenB.Store(4096)
	originalHead := volume.state.HeadPath

	sealed, skipped, err := volume.Seal(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if skipped || len(sealed) != 1 {
		t.Fatalf("expected one sealed layer, got %v skipped=%v", sealed, skipped)
	}
	if sealed[0].Path != originalHead {
		t.Fatalf("sealed path %s, expected old head %s", sealed[0].Path, originalHead)
	}
	if volume.state.HeadPath == originalHead {
		t.Fatal("head was not replaced")
	}
	if volume.fmtNode != "fmt-1" {
		t.Fatalf("fmt node is %s", volume.fmtNode)
	}

	// A forced second seal without publishing stacks a second pending layer.
	sealed, _, err = volume.Seal(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != 2 {
		t.Fatalf("expected both pending layers, got %d", len(sealed))
	}

	// Publishing must go oldest first.
	if err := volume.MarkPublished(sealed[1].Path, "snap-b"); err == nil {
		t.Fatal("publishing the newer layer first must fail")
	}
	if err := volume.MarkPublished(sealed[0].Path, "snap-a"); err != nil {
		t.Fatal(err)
	}
	if err := volume.MarkPublished(sealed[1].Path, "snap-b"); err != nil {
		t.Fatal(err)
	}
	if len(volume.state.Pending) != 0 || len(volume.state.Chain) != 2 {
		t.Fatalf("unexpected state after publish: %+v", volume.state)
	}

	// State survives reload.
	reloaded, err := loadVolumeState(volume.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded.Chain) != 2 || reloaded.Chain[0].SnapshotID != "snap-a" {
		t.Fatalf("reloaded state mismatch: %+v", reloaded)
	}
}

func TestSealRollsBackFailedPivot(t *testing.T) {
	volume, server := newTestVolume(t)
	server.writtenB.Store(4096)
	server.failPivots.Store(true)
	originalHead := volume.state.HeadPath

	_, _, err := volume.Seal(context.Background(), false)
	if err == nil {
		t.Fatal("expected pivot failure")
	}
	if volume.state.HeadPath != originalHead {
		t.Fatal("head must be restored after a failed pivot")
	}
	if len(volume.state.Pending) != 0 {
		t.Fatal("pending must be empty after rollback")
	}
	if volume.state.PivotCount != 0 || volume.fmtNode != "fmt-0" {
		t.Fatalf("pivot bookkeeping leaked: count=%d node=%s", volume.state.PivotCount, volume.fmtNode)
	}
}

func TestSealTreatsCommittedLostReplyAsSuccess(t *testing.T) {
	volume, server := newTestVolume(t)
	server.writtenB.Store(4096)
	server.failPivots.Store(true)
	// The daemon reports the new node exists, so the "failure" was a lost
	// reply and the pivot must be treated as committed.
	server.nodes.Store([]string{"fmt-0", "fmt-1"})

	sealed, _, err := volume.Seal(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != 1 || volume.fmtNode != "fmt-1" {
		t.Fatalf("committed pivot not recognized: sealed=%v node=%s", sealed, volume.fmtNode)
	}
}

func TestReusableState(t *testing.T) {
	dir := t.TempDir()
	touch := func(name string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	head := touch("head.qcow2")
	layerA := touch("a.qcow2")

	published := &volumeState{
		Chain:    []stateLayer{{SnapshotID: "snap-a", Path: layerA}},
		HeadPath: head,
	}

	cases := []struct {
		name  string
		state *volumeState
		spec  AttachSpec
		want  bool
	}{
		{"no local state", nil, AttachSpec{}, false},
		{"read-only never reuses", published, AttachSpec{ReadOnly: true, Chain: []ChainLayer{{SnapshotID: "snap-a"}}}, false},
		{"local has requested latest", published, AttachSpec{Chain: []ChainLayer{{SnapshotID: "snap-a"}}}, true},
		{"remote is ahead", published, AttachSpec{Chain: []ChainLayer{{SnapshotID: "snap-a"}, {SnapshotID: "snap-b"}}}, false},
		{"remote empty but local published", published, AttachSpec{}, false},
		{"never published local head", &volumeState{HeadPath: head}, AttachSpec{}, true},
		{"missing layer file", &volumeState{Chain: []stateLayer{{SnapshotID: "snap-a", Path: filepath.Join(dir, "gone.qcow2")}}, HeadPath: head}, AttachSpec{Chain: []ChainLayer{{SnapshotID: "snap-a"}}}, false},
	}
	for _, tc := range cases {
		if got := reusableState(tc.state, tc.spec); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestRecoverCleansUpCrashedVolume(t *testing.T) {
	root := t.TempDir()
	manager := NewManager(Config{Root: root, Runner: fakeRunner})

	dir := manager.volumeDir("crashed")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	state := &volumeState{
		Key:      "crashed",
		Attached: true,
		QSDPid:   1 << 30, // certainly not a live qemu-storage-daemon
		HeadPath: headLayerPath(filepath.Join(dir, layersSubdir), 0),
	}
	if err := saveVolumeState(dir, state); err != nil {
		t.Fatal(err)
	}

	if err := manager.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered, err := loadVolumeState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Attached {
		t.Fatal("crashed volume must be marked detached")
	}
	if _, ok := manager.Volume("crashed"); ok {
		t.Fatal("crashed volume must not be adopted")
	}
}
