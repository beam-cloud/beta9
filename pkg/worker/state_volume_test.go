package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
)

const stateVolumeTestRecoveryProofToken = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"

func shortStateVolumeTestRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "beta9-sv-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func TestStateVolumePathsRejectCrossVolumeAndSymlinkOverlap(t *testing.T) {
	root := t.TempDir()
	backing := filepath.Join(root, "backing")
	mount := filepath.Join(root, "mount")
	if err := os.MkdirAll(backing, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(backing, mount); err != nil {
		t.Fatal(err)
	}
	if err := validateStateVolumePathPair(backing, filepath.Join(mount, "nested")); err == nil {
		t.Fatal("expected symlink-resolved overlap rejection")
	}

	specs := []StateVolumeSpec{
		{ID: "a", Name: "a", ContainerMountPath: "/a", BackingDir: filepath.Join(root, "state", "a"), MountPath: filepath.Join(root, "mnt", "a")},
		{ID: "b", Name: "b", ContainerMountPath: "/b", BackingDir: filepath.Join(root, "state", "b"), MountPath: filepath.Join(root, "mnt", "a", "nested")},
	}
	if err := validateStateVolumeGroupPaths(specs); err == nil {
		t.Fatal("expected cross-volume mount overlap rejection")
	}
}

func TestStateVolumeReadOnlyQSDGraphIsReadOnlyAtEveryLayer(t *testing.T) {
	runtime := &stateVolumeRuntime{
		spec:       StateVolumeSpec{ReadOnly: true, ActiveLayerPath: "/cache/layer.qcow2"},
		exportName: "export", fileNode: "file", activeNode: "active", rootNode: "root",
	}
	args, err := BuildStateVolumeQSDArgs("/runtime/qmp.sock", "/runtime/qsd.pid", "/runtime/nbd.sock", []*stateVolumeRuntime{runtime})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, " ")
	if strings.Count(joined, `"read-only":true`) != 3 || !strings.Contains(joined, "writable=off") {
		t.Fatalf("read-only export graph is not immutable: %s", joined)
	}
}

type fakeStateVolumeNBDKernel struct{}

func (fakeStateVolumeNBDKernel) ValidateDevice(string, string) error { return nil }
func (fakeStateVolumeNBDKernel) WaitConnected(context.Context, string, int64) error {
	return nil
}
func (fakeStateVolumeNBDKernel) VerifyMounted(string, string, string, bool) error { return nil }
func (fakeStateVolumeNBDKernel) WaitUnmounted(context.Context, string, string) error {
	return nil
}
func (fakeStateVolumeNBDKernel) WaitDisconnected(context.Context, string) error { return nil }

type rejectingStateVolumeNBDKernel struct {
	fakeStateVolumeNBDKernel
	err   error
	calls int
}

func (k *rejectingStateVolumeNBDKernel) ValidateDevice(string, string) error {
	k.calls++
	return k.err
}

func TestStateVolumeNBDKernelRejectsNonBlockAndReboundMounts(t *testing.T) {
	root := t.TempDir()
	sysDevice := filepath.Join(root, "sys", "nbd0")
	device := filepath.Join(root, "dev", "nbd0")
	mountInfo := filepath.Join(root, "mountinfo")
	if err := os.MkdirAll(sysDevice, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(device), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sysDevice, "dev"), []byte("43:0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(device, nil, 0600); err != nil {
		t.Fatal(err)
	}
	kernel := linuxStateVolumeNBDKernel{MountInfoPath: mountInfo}
	if err := kernel.ValidateDevice(sysDevice, device); err == nil || !strings.Contains(err.Error(), "block-special") {
		t.Fatalf("regular file was accepted as an NBD block device: %v", err)
	}

	valid := fmt.Sprintf("36 25 43:0 / %s ro,noatime - ext4 %s ro,noload\n", filepath.Join(root, "mount"), device)
	if err := os.WriteFile(mountInfo, []byte(valid), 0600); err != nil {
		t.Fatal(err)
	}
	if err := kernel.VerifyMounted(sysDevice, device, filepath.Join(root, "mount"), true); err != nil {
		t.Fatalf("exact immutable ext4 mount was rejected: %v", err)
	}
	rebound := fmt.Sprintf("36 25 43:1 / %s ro,noatime - ext4 %s ro,noload\n", filepath.Join(root, "mount"), device)
	if err := os.WriteFile(mountInfo, []byte(rebound), 0600); err != nil {
		t.Fatal(err)
	}
	if err := kernel.VerifyMounted(sysDevice, device, filepath.Join(root, "mount"), true); err == nil {
		t.Fatal("mount with a rebound kernel major:minor identity was accepted")
	}
	unsafe := fmt.Sprintf("36 25 43:0 / %s ro,noatime - ext4 %s ro\n", filepath.Join(root, "mount"), device)
	if err := os.WriteFile(mountInfo, []byte(unsafe), 0600); err != nil {
		t.Fatal(err)
	}
	if err := kernel.VerifyMounted(sysDevice, device, filepath.Join(root, "mount"), true); err == nil {
		t.Fatal("read-only ext4 mount without noload/norecovery was accepted")
	}
}

func TestStateVolumeNBDKernelRequiresExactConnectedSizeAndClearedDetach(t *testing.T) {
	root := t.TempDir()
	sysDevice := filepath.Join(root, "nbd0")
	if err := os.MkdirAll(sysDevice, 0700); err != nil {
		t.Fatal(err)
	}
	write := func(name, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(sysDevice, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("pid", "123\n")
	write("size", "2048\n")
	kernel := linuxStateVolumeNBDKernel{}
	if err := kernel.WaitConnected(context.Background(), sysDevice, 1<<20); err != nil {
		t.Fatalf("exact connected NBD was rejected: %v", err)
	}
	if err := kernel.WaitConnected(context.Background(), sysDevice, 2<<20); err == nil {
		t.Fatal("connected NBD with the wrong virtual size was accepted")
	}
	write("pid", "0\n")
	write("size", "0\n")
	if err := kernel.WaitDisconnected(context.Background(), sysDevice); err != nil {
		t.Fatalf("cleared NBD detach was rejected: %v", err)
	}
}

func TestStateVolumeJournalPathValidationAuthenticatesNBDKernelIdentity(t *testing.T) {
	allocator, _, devRoot, _ := setupTestNBD(t, 1)
	rejection := &rejectingStateVolumeNBDKernel{err: errors.New("not a block-special NBD device")}
	allocator.Kernel = rejection
	root := t.TempDir()
	runtimeDir := filepath.Join(root, "runtime", stateVolumeToken("container-", "container"))
	backingDir := filepath.Join(root, "graph")
	mountPath := filepath.Join(root, "mount")
	for _, path := range []string{runtimeDir, backingDir, mountPath} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	activeLayer := filepath.Join(backingDir, "active.qcow2")
	if err := os.WriteFile(activeLayer, nil, 0600); err != nil {
		t.Fatal(err)
	}
	manager := &StateVolumeManager{StateRoot: root, RuntimeRoot: filepath.Join(root, "runtime"), NBD: allocator}
	journal := StateVolumeJournal{
		ContainerID: "container", QMPSocket: filepath.Join(runtimeDir, "qmp.sock"), NBDSocket: filepath.Join(runtimeDir, "nbd.sock"),
		Volumes: []StateVolumeJournalVolume{{
			ID: "volume", BackingDir: backingDir, MountPath: mountPath, ActiveLayerPath: activeLayer,
			DevicePath: filepath.Join(devRoot, "nbd0"),
		}},
	}
	if err := manager.validateJournalPaths(journal); err == nil || !strings.Contains(err.Error(), "block-special") {
		t.Fatalf("journal-selected forged NBD identity was accepted: %v", err)
	}
	if rejection.calls != 1 {
		t.Fatalf("kernel identity validator was called %d times, want 1", rejection.calls)
	}
}

func TestStateVolumeReconcileScopesForeignOwnerAndRefusesSameWorkerLiveCrossEpochAdoption(t *testing.T) {
	allocator, sysRoot, devRoot, mountInfo := setupTestNBD(t, 1)
	root := t.TempDir()
	mountPath := filepath.Join(root, "mount")
	manager := &StateVolumeManager{
		WorkerID: "worker-a", WorkerInstanceID: "instance-a", StorageNodeID: "node-a",
		RuntimeRoot: filepath.Join(root, "runtime"), Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		NBD: allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: &fakeStateVolumeQMP{}}, Launcher: &inheritedFDLauncher{},
	}
	if _, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
		ID: "root", Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "backing"), MountPath: mountPath, SizeBytes: 1024, Format: true,
		AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}}); err != nil {
		t.Fatal(err)
	}
	journal, err := manager.Journals.Load("container")
	if err != nil {
		t.Fatal(err)
	}
	journal.QSDPID, journal.QSDExecutable, journal.QSDStartTime = 99, "/usr/bin/qemu-storage-daemon", 123
	if err := manager.Journals.Save(journal); err != nil {
		t.Fatal(err)
	}
	realDevice := filepath.Join(sysRoot, "nbd0")
	if err := os.WriteFile(filepath.Join(realDevice, "pid"), []byte("99\n"), 0600); err != nil {
		t.Fatal(err)
	}
	device := filepath.Join(devRoot, "nbd0")
	line := fmt.Sprintf("36 25 0:32 / %s rw,noatime - ext4 %s rw\n", mountPath, device)
	if err := os.WriteFile(mountInfo, []byte(line), 0600); err != nil {
		t.Fatal(err)
	}
	replacement := &StateVolumeManager{
		WorkerID: "worker-b", WorkerInstanceID: "instance-b", StorageNodeID: "node-a",
		RuntimeRoot: manager.RuntimeRoot, StateRoot: manager.StateRoot, Journals: manager.Journals,
		NBD: allocator, Connector: manager.Connector, Images: manager.Images, Mounts: manager.Mounts,
		QMPDialer: manager.QMPDialer, Launcher: manager.Launcher,
		ProcessIdentity: func(pid int) (string, uint64, error) {
			return "/usr/bin/qemu-storage-daemon", 123, nil
		},
	}
	err = replacement.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("foreign live worker journal blocked an innocent worker: %v", err)
	}
	if _, _, ok := replacement.ExistingGroup("container"); ok {
		t.Fatal("foreign live journal became an innocent worker's group")
	}
	replacement.WorkerID = "worker-a"
	err = replacement.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "refuse to adopt live state volume QSD owned by worker epoch") {
		t.Fatalf("same worker ID replacement adopted the prior live process epoch: %v", err)
	}
}

func setupTestNBD(t *testing.T, count int) (*StateVolumeNBDAllocator, string, string, string) {
	t.Helper()
	root := t.TempDir()
	sysRoot := filepath.Join(root, "sys", "block")
	realRoot := filepath.Join(root, "sys", "devices")
	devRoot := filepath.Join(root, "dev")
	lockRoot := filepath.Join(root, "locks")
	if err := os.MkdirAll(sysRoot, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(realRoot, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(devRoot, 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		realDevice := filepath.Join(realRoot, fmt.Sprintf("nbd%d", i))
		if err := os.MkdirAll(realDevice, 0700); err != nil {
			t.Fatal(err)
		}
		// Linux /sys/block entries are symlinks, not directory DirEntries.
		if err := os.Symlink(realDevice, filepath.Join(sysRoot, fmt.Sprintf("nbd%d", i))); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(devRoot, fmt.Sprintf("nbd%d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	mountInfo := filepath.Join(root, "mountinfo")
	if err := os.WriteFile(mountInfo, nil, 0600); err != nil {
		t.Fatal(err)
	}
	return &StateVolumeNBDAllocator{SysBlockRoot: sysRoot, DevRoot: devRoot, LockRoot: lockRoot, MountInfoPath: mountInfo, Kernel: fakeStateVolumeNBDKernel{}}, sysRoot, devRoot, mountInfo
}

func TestStateVolumeNBDAllocatorHandlesSymlinksLocksAndMounts(t *testing.T) {
	allocator, sysRoot, devRoot, mountInfo := setupTestNBD(t, 1)
	lease, err := allocator.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if lease.Index != 0 {
		t.Fatalf("got NBD index %d", lease.Index)
	}

	other := &StateVolumeNBDAllocator{SysBlockRoot: sysRoot, DevRoot: devRoot, LockRoot: allocator.LockRoot, MountInfoPath: mountInfo, Kernel: allocator.Kernel}
	if _, err := other.Acquire(); !errors.Is(err, ErrStateVolumeNBDUnavailable) {
		t.Fatalf("cross-allocator lock did not exclude contender: %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}

	device := filepath.Join(devRoot, "nbd0")
	line := fmt.Sprintf("36 25 0:32 / /mnt rw,relatime - ext4 %s rw\n", device)
	if err := os.WriteFile(mountInfo, []byte(line), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Acquire(); !errors.Is(err, ErrStateVolumeNBDUnavailable) {
		t.Fatalf("mounted NBD without pid was allocated: %v", err)
	}
}

func TestStateVolumeJournalAtomicRoundTrip(t *testing.T) {
	root := t.TempDir()
	store := StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")}
	journal := StateVolumeJournal{
		ContainerID: "container", QMPSocket: filepath.Join(root, "qmp.sock"), NBDSocket: filepath.Join(root, "nbd.sock"), QSDPID: 42, Phase: "running",
		Volumes: []StateVolumeJournalVolume{{
			ID: "root", ExportName: "export-root", DevicePath: "/dev/nbd0",
			BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"), SizeBytes: 1024,
			RootNode: "root-node", FileNode: "file-node", ActiveNode: "active-node", ActiveLayerPath: filepath.Join(root, "backing", "base.qcow2"), Depth: 1,
		}},
	}
	if err := store.Save(journal); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load("container")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version != stateVolumeJournalVersion || loaded.QSDPID != 42 || loaded.Volumes[0].ID != "root" {
		t.Fatalf("unexpected journal: %+v", loaded)
	}
	loaded.Version++
	data, _ := json.Marshal(loaded)
	path, _ := store.journalPath("container")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load("container"); err == nil {
		t.Fatal("expected journal version rejection")
	}
}

func TestStateVolumeJournalListBoundsAndQuarantinesMalformedPayloads(t *testing.T) {
	root := t.TempDir()
	store := StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")}
	valid := func(containerID string) StateVolumeJournal {
		return StateVolumeJournal{
			ContainerID: containerID, QMPSocket: filepath.Join(root, containerID, "qmp.sock"), NBDSocket: filepath.Join(root, containerID, "nbd.sock"),
			QSDPID: 42, Phase: "running", Volumes: []StateVolumeJournalVolume{{
				ID: "root", ExportName: "export-root", DevicePath: "/dev/nbd0", BackingDir: filepath.Join(root, containerID, "backing"),
				MountPath: filepath.Join(root, containerID, "mount"), SizeBytes: 1024, RootNode: "root-node", FileNode: "file-node",
				ActiveNode: "active-node", ActiveLayerPath: filepath.Join(root, containerID, "backing", "base.qcow2"), Depth: 1,
			}},
		}
	}
	oversized := valid("oversized")
	if err := store.Save(oversized); err != nil {
		t.Fatal(err)
	}
	oversizedPath, _ := store.journalPath(oversized.ContainerID)
	if err := os.WriteFile(oversizedPath, make([]byte, stateVolumeJournalMaxBytes+1), 0600); err != nil {
		t.Fatal(err)
	}
	trailing := valid("trailing")
	if err := store.Save(trailing); err != nil {
		t.Fatal(err)
	}
	trailingPath, _ := store.journalPath(trailing.ContainerID)
	data, err := os.ReadFile(trailingPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(trailingPath, append(data, []byte("{}")...), 0600); err != nil {
		t.Fatal(err)
	}
	journals, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(journals) != 0 {
		t.Fatalf("malformed journals reached reconciliation: %+v", journals)
	}
	quarantined, err := os.ReadDir(filepath.Join(store.RootDir, "quarantine"))
	if err != nil {
		t.Fatal(err)
	}
	if len(quarantined) != 2 {
		t.Fatalf("malformed journals were not quarantined as inert data: %v", quarantined)
	}
	tooMany := valid("too-many")
	tooMany.Version = stateVolumeJournalVersion
	tooMany.Volumes = make([]StateVolumeJournalVolume, stateVolumeJournalMaxVolumes+1)
	if err := validateStateVolumeJournal(tooMany); err == nil || !strings.Contains(err.Error(), "member count") {
		t.Fatalf("unbounded journal member array was accepted: %v", err)
	}
}

func TestStateVolumeJournalNeverPersistsAttachmentCredential(t *testing.T) {
	const attachmentToken = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	root := t.TempDir()
	manager := &StateVolumeManager{}
	group := &stateVolumeGroup{
		containerID: "container",
		qmpSocket:   filepath.Join(root, "qmp.sock"),
		nbdSocket:   filepath.Join(root, "nbd.sock"),
		volumes: []*stateVolumeRuntime{{
			spec: StateVolumeSpec{
				ID: "root", Name: "root", Root: true, ContainerMountPath: "/",
				BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"),
				SizeBytes: 1024, ActiveLayerPath: filepath.Join(root, "backing", "base.qcow2"),
				AttachmentToken: attachmentToken, FencingToken: 7,
			},
			exportName: "export-root", rootNode: "root-node", fileNode: "file-node", activeNode: "active-node",
		}},
	}
	journal := manager.groupJournal(group, "running", "")
	encoded, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(attachmentToken)) || bytes.Contains(encoded, []byte("attachment_token")) {
		t.Fatalf("state-volume journal leaked an attachment credential: %s", encoded)
	}
	rehydrated := stateVolumeGroupFromJournal(journal)
	if got := rehydrated.volumes[0].spec.AttachmentToken; got != "" {
		t.Fatalf("journal rehydrated attachment authority %q", got)
	}
}

func TestStateVolumeQMPHandshakeProbeAndAtomicTransaction(t *testing.T) {
	root, err := os.MkdirTemp("", "svqmp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	socket := filepath.Join(root, "qmp.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	received := make(chan map[string]any, 1)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		enc := json.NewEncoder(conn)
		dec := json.NewDecoder(conn)
		if err := enc.Encode(map[string]any{"QMP": map[string]any{"version": map[string]any{}, "capabilities": []any{}}}); err != nil {
			serverErr <- err
			return
		}
		for {
			var request map[string]any
			if err := dec.Decode(&request); err != nil {
				serverErr <- err
				return
			}
			id := request["id"]
			switch request["execute"] {
			case "qmp_capabilities":
				_ = enc.Encode(map[string]any{"return": map[string]any{}, "id": id})
			case "query-commands":
				_ = enc.Encode(map[string]any{"return": []map[string]string{
					{"name": "transaction"}, {"name": "blockdev-snapshot-sync"}, {"name": "query-named-block-nodes"},
					{"name": "query-blockstats"}, {"name": "query-block-exports"}, {"name": "block-stream"},
					{"name": "query-block-jobs"}, {"name": "job-finalize"}, {"name": "job-dismiss"},
					{"name": "block-job-cancel"},
				}, "id": id})
			case "query-blockstats":
				_ = enc.Encode(map[string]any{"return": []any{
					map[string]any{"node-name": "root-a", "parent": map[string]any{"node-name": "active-a"}},
					map[string]any{"node-name": "active-a"},
				}, "id": id})
			case "query-named-block-nodes":
				_ = enc.Encode(map[string]any{"return": []any{
					map[string]any{"node-name": "root-a", "drv": "raw", "file": "active-a"},
					map[string]any{"node-name": "active-a", "drv": "qcow2", "file": filepath.Join(root, "active-a.qcow2")},
				}, "id": id})
			case "query-block-exports":
				_ = enc.Encode(map[string]any{"return": []any{
					map[string]any{"id": "export-a", "node-name": "root-a", "shutting-down": false},
				}, "id": id})
			case "transaction":
				received <- request
				_ = enc.Encode(map[string]any{"event": "BLOCK_JOB_COMPLETED"})
				_ = enc.Encode(map[string]any{"return": map[string]any{}, "id": id})
				serverErr <- nil
				return
			}
		}
	}()

	client, err := (UnixStateVolumeQMPDialer{}).Dial(context.Background(), socket)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.ProbeSnapshotSupport(context.Background()); err != nil {
		t.Fatal(err)
	}
	graph, err := client.QuerySnapshotGraph(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if graph.Nodes["root-a"].ChildNode != "active-a" || graph.Exports["export-a"].NodeName != "root-a" {
		t.Fatalf("unexpected QMP snapshot graph: %+v", graph)
	}
	actions := []StateVolumeSnapshotAction{
		{CurrentNode: "active-b", NewNode: "next-b", NewPath: filepath.Join(root, "b.qcow2"), Mode: "existing"},
		{CurrentNode: "active-a", NewNode: "next-a", NewPath: filepath.Join(root, "a.qcow2"), Mode: "existing"},
	}
	if err := client.TransactionSnapshot(context.Background(), actions); err != nil {
		t.Fatal(err)
	}
	request := <-received
	arguments := request["arguments"].(map[string]any)
	wireActions := arguments["actions"].([]any)
	if len(wireActions) != 2 {
		t.Fatalf("transaction had %d actions", len(wireActions))
	}
	firstData := wireActions[0].(map[string]any)["data"].(map[string]any)
	if firstData["node-name"] != "active-a" || firstData["mode"] != "existing" {
		t.Fatalf("unexpected first action: %#v", firstData)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestStateVolumeQMPLostTransactionReplyIsIndeterminate(t *testing.T) {
	root, err := os.MkdirTemp("", "svqmp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	socket := filepath.Join(root, "qmp.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, _ := listener.Accept()
		enc := json.NewEncoder(conn)
		dec := json.NewDecoder(conn)
		_ = enc.Encode(map[string]any{"QMP": map[string]any{"version": map[string]any{}}})
		var request map[string]any
		_ = dec.Decode(&request)
		_ = enc.Encode(map[string]any{"return": map[string]any{}, "id": request["id"]})
		_ = dec.Decode(&request)
		_ = conn.Close()
	}()
	client, err := (UnixStateVolumeQMPDialer{}).Dial(context.Background(), socket)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	err = client.TransactionSnapshot(context.Background(), []StateVolumeSnapshotAction{{CurrentNode: "old", NewNode: "new", NewPath: filepath.Join(root, "new.qcow2")}})
	if !errors.Is(err, ErrStateVolumePivotIndeterminate) {
		t.Fatalf("lost reply was not indeterminate: %v", err)
	}
}

type qmpWriteFaultConn struct {
	bytes.Buffer
	full bool
}

func (c *qmpWriteFaultConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *qmpWriteFaultConn) Write(p []byte) (int, error) {
	n := len(p)
	if !c.full {
		n /= 2
		if n == 0 {
			n = 1
		}
	}
	_, _ = c.Buffer.Write(p[:n])
	return n, errors.New("injected QMP write failure")
}
func (*qmpWriteFaultConn) Close() error                     { return nil }
func (*qmpWriteFaultConn) LocalAddr() net.Addr              { return qmpTestAddr("local") }
func (*qmpWriteFaultConn) RemoteAddr() net.Addr             { return qmpTestAddr("remote") }
func (*qmpWriteFaultConn) SetDeadline(time.Time) error      { return nil }
func (*qmpWriteFaultConn) SetReadDeadline(time.Time) error  { return nil }
func (*qmpWriteFaultConn) SetWriteDeadline(time.Time) error { return nil }

type qmpTestAddr string

func (a qmpTestAddr) Network() string { return "test" }
func (a qmpTestAddr) String() string  { return string(a) }

func TestStateVolumeQMPTransactionWriteFailuresAreIndeterminate(t *testing.T) {
	for _, full := range []bool{false, true} {
		name := "partial"
		if full {
			name = "full-delivery"
		}
		t.Run(name, func(t *testing.T) {
			conn := &qmpWriteFaultConn{full: full}
			client := &stateVolumeQMPClient{conn: conn, enc: json.NewEncoder(conn), dec: json.NewDecoder(conn)}
			err := client.TransactionSnapshot(context.Background(), []StateVolumeSnapshotAction{{
				CurrentNode: "old", NewNode: "new", NewPath: filepath.Join(t.TempDir(), "next.qcow2"),
			}})
			if !errors.Is(err, ErrStateVolumePivotIndeterminate) {
				t.Fatalf("%s transaction write was not indeterminate: %v", name, err)
			}
			if !strings.Contains(conn.String(), `"execute":"transaction"`) {
				t.Fatalf("%s fault did not begin transaction transmission: %q", name, conn.String())
			}
		})
	}
}

type memoryBlockCAS struct {
	mu       sync.Mutex
	objects  map[string][]byte
	getCount int
	trailing bool
}

type recordingBlockContentCache struct {
	mu         sync.Mutex
	data       []byte
	repairData []byte
	stores     []cache.S3ContentSource
}

func (c *recordingBlockContentCache) ReadContentInto(_ context.Context, _ string, offset int64, destination []byte, _ cache.ClientOptions) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset < 0 || offset >= int64(len(c.data)) {
		return 0, errors.New("cache miss")
	}
	n := copy(destination, c.data[offset:])
	return int64(n), nil
}

func (c *recordingBlockContentCache) StoreContentFromS3Source(source cache.S3ContentSource, _ cache.StoreContentOptions) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stores = append(c.stores, source)
	c.data = append([]byte(nil), c.repairData...)
	digest := sha256.Sum256(c.data)
	return hex.EncodeToString(digest[:]), nil
}

func TestWorkspaceBlockV1CASCacheFirstAndRepairsSameSizeCorruption(t *testing.T) {
	originData := []byte("authenticated block object")
	digestBytes := sha256.Sum256(originData)
	digest := hex.EncodeToString(digestBytes[:])
	originReads := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			http.Error(writer, "unsupported", http.StatusMethodNotAllowed)
			return
		}
		originReads++
		_, _ = writer.Write(originData)
	}))
	defer server.Close()
	bucket, access, secret, region, endpoint := "bucket", "access", "secret", "us-east-1", server.URL
	storageClient, err := clients.NewWorkspaceStorageClient(context.Background(), "workspace", &types.WorkspaceStorage{
		BucketName: &bucket, AccessKey: &access, SecretKey: &secret, Region: &region, EndpointUrl: &endpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	contentCache := &recordingBlockContentCache{data: append([]byte(nil), originData...), repairData: originData}
	cas := &workspaceBlockV1CAS{client: storageClient, cache: contentCache}

	reader, err := cas.Get(context.Background(), digest, int64(len(originData)))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, reader)
	_ = reader.Close()
	if originReads != 0 {
		t.Fatalf("valid cache hit read origin %d times", originReads)
	}

	contentCache.mu.Lock()
	contentCache.data = bytes.Repeat([]byte{'x'}, len(originData))
	contentCache.mu.Unlock()
	reader, err = cas.Get(context.Background(), digest, int64(len(originData)))
	if err != nil {
		t.Fatal(err)
	}
	restored, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || !bytes.Equal(restored, originData) {
		t.Fatalf("origin fallback returned wrong bytes: %q err=%v", restored, err)
	}
	contentCache.mu.Lock()
	stores := append([]cache.S3ContentSource(nil), contentCache.stores...)
	contentCache.mu.Unlock()
	key, _ := stateBlockObjectKey(digest)
	if originReads != 1 || len(stores) != 1 || stores[0].Path != key || stores[0].CachePath != digest {
		t.Fatalf("corrupt cache was not repaired from the exact origin: reads=%d stores=%+v", originReads, stores)
	}

	reader, err = cas.Get(context.Background(), digest, int64(len(originData)))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, reader)
	_ = reader.Close()
	if originReads != 1 {
		t.Fatalf("second restore was not cache-only: origin reads=%d", originReads)
	}
}

func TestWorkspaceBlockV1CASRejectsSuccessResponseWithCorruptStoredBytes(t *testing.T) {
	data := []byte("authenticated upload")
	digestBytes := sha256.Sum256(data)
	digest := hex.EncodeToString(digestBytes[:])
	uploaded := false
	putCalls, getCalls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodHead:
			if !uploaded {
				http.NotFound(writer, request)
				return
			}
			writer.Header().Set("Content-Length", fmt.Sprint(len(data)))
			writer.WriteHeader(http.StatusOK)
		case http.MethodPut:
			_, _ = io.Copy(io.Discard, request.Body)
			_ = request.Body.Close()
			putCalls++
			uploaded = true
			writer.Header().Set("ETag", `"accepted-but-corrupt"`)
			writer.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getCalls++
			corrupt := bytes.Repeat([]byte{'z'}, len(data))
			writer.Header().Set("Content-Length", fmt.Sprint(len(corrupt)))
			_, _ = writer.Write(corrupt)
		default:
			http.Error(writer, "unsupported", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	bucket, access, secret, region, endpoint := "bucket", "access", "secret", "us-east-1", server.URL
	storageClient, err := clients.NewWorkspaceStorageClient(context.Background(), "workspace", &types.WorkspaceStorage{
		BucketName: &bucket, AccessKey: &access, SecretKey: &secret, Region: &region, EndpointUrl: &endpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = (&workspaceBlockV1CAS{client: storageClient}).Put(context.Background(), digest, int64(len(data)), bytes.NewReader(data))
	if err == nil || !strings.Contains(err.Error(), "verify uploaded") {
		t.Fatalf("success-returning corrupt origin was accepted: %v", err)
	}
	if putCalls != 1 || getCalls != 1 {
		t.Fatalf("origin verification calls mismatch: put=%d get=%d", putCalls, getCalls)
	}
}

func TestStateBlockRequiredContentReportsCompleteAuthenticatedAncestry(t *testing.T) {
	volumeID := "9c065bb6-4f91-41e5-a413-68041bd1d16f"
	parentID := "66f6a823-240c-4a8e-a2cb-4d03ca75604a"
	childID := "6e92cbd5-6268-4798-8625-e4ef51f65c18"
	parent := testBlockManifest(volumeID, parentID, "", 1)
	child := testBlockManifest(volumeID, childID, parentID, 2)
	parent.Generation = 1
	child.Generation = 2
	for manifest, value := range map[*BlockV1Manifest]byte{&parent: 'p', &child: 'c'} {
		digest := sha256.Sum256([]byte{value})
		manifest.Chunks = []BlockV1Chunk{{Index: 0, OffsetBytes: 0, SizeBytes: 1, Digest: hex.EncodeToString(digest[:])}}
	}
	cas := &memoryBlockCAS{objects: make(map[string][]byte)}
	records := make(map[string]*pb.VolumeGeneration)
	for _, manifest := range []BlockV1Manifest{parent, child} {
		data, digest, err := EncodeBlockV1ManifestCanonical(manifest)
		if err != nil {
			t.Fatal(err)
		}
		cas.objects[digest] = data
		key, _ := stateBlockObjectKey(digest)
		records[manifest.GenerationID] = &pb.VolumeGeneration{
			ExternalId: manifest.GenerationID, VolumeId: manifest.VolumeID, Generation: manifest.Generation,
			ParentGenerationId: manifest.ParentGenerationID, ManifestDigest: digest, ManifestKey: key,
			ManifestSizeBytes: int64(len(data)), LogicalSizeBytes: manifest.VirtualSizeBytes,
			ChunkCount: int64(len(manifest.Chunks)), StoredSizeBytes: 1, BucketName: "state-bucket",
		}
	}
	reporter := &cacheContentReporter{
		pending: make(map[reporterKey]map[string]types.CacheRequiredContentItem),
		recent:  make(map[reporterStubKey]struct{}), reported: make(map[string]struct{}),
	}
	worker := &Worker{
		backendRepoClient: &stateBlockCacheReportRepository{generations: records},
		cacheManager:      &WorkerCacheManager{reporter: reporter},
	}
	request := &types.ContainerRequest{
		Workspace: types.Workspace{ExternalId: "workspace"},
		Stub:      types.StubWithRelated{Stub: types.Stub{ExternalId: "stub"}},
	}
	reports, err := worker.stateBlockRequiredContentReports(
		context.Background(), request, []*pb.VolumeGeneration{records[childID]},
		map[string]BlockV1Manifest{childID: child}, cas,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || len(reports[0].items) != 4 || reports[0].scope != volumeID ||
		reports[0].revisionGeneration != 2 || reports[0].revisionID != childID || reports[0].kind != "" {
		t.Fatalf("complete parent chain was not reported: %+v", reports)
	}
	for _, report := range reports {
		for _, item := range report.items {
			key, _ := stateBlockObjectKey(item.Hash)
			if (item.Kind != types.CacheContentKindStateManifest && item.Kind != types.CacheContentKindStateChunk) ||
				item.Source != key || item.SourceBucket != "state-bucket" ||
				item.RoutingKey != item.Hash || item.ExpectedHash != item.Hash || item.VolumeID != volumeID || item.GenerationID == "" {
				t.Fatalf("invalid required-content item: %+v", item)
			}
		}
	}
	reporter.reportBatches("workspace", "stub", reports)
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.pending) != 0 || len(reporter.scoped) != 1 {
		t.Fatalf("state manifest/chunk revision was not queued as one scope: pending=%+v scoped=%+v", reporter.pending, reporter.scoped)
	}
}

type offlineStateSnapshotRepository struct {
	pb.BackendRepositoryServiceClient
	mu                 sync.Mutex
	operation          *pb.GetStateSnapshotResponse
	generations        map[string]*pb.VolumeGeneration
	commitRequests     []*pb.CommitStateSnapshotRequest
	failRequests       []*pb.FailStateSnapshotRequest
	releaseRequests    []*pb.ReleaseStateVolumeAttachmentsRequest
	recoveryProofToken string
}

func (r *offlineStateSnapshotRepository) expectedRecoveryProofToken() string {
	if r.recoveryProofToken != "" {
		return r.recoveryProofToken
	}
	return stateVolumeTestRecoveryProofToken
}

func (r *offlineStateSnapshotRepository) GetStateSnapshotRecoveryCredentials(_ context.Context, in *pb.GetStateSnapshotRecoveryCredentialsRequest, _ ...grpc.CallOption) (*pb.GetStateSnapshotRecoveryCredentialsResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.operation == nil || r.operation.Snapshot == nil || r.operation.Snapshot.ExternalId != in.StateSnapshotId {
		return &pb.GetStateSnapshotRecoveryCredentialsResponse{Ok: false, ErrorMsg: "snapshot unavailable"}, nil
	}
	if in.RecoveryProofToken != r.expectedRecoveryProofToken() {
		return &pb.GetStateSnapshotRecoveryCredentialsResponse{Ok: false, ErrorMsg: "recovery proof rejected"}, nil
	}
	snapshot := r.operation.Snapshot
	return &pb.GetStateSnapshotRecoveryCredentialsResponse{
		Ok: true, WorkspaceId: r.operation.WorkspaceId, WorkspaceName: "workspace-name",
		StubId: r.operation.StubId, StubName: "stub-name", StubType: "pod",
		ImageId: snapshot.ImageId, ImageDigest: snapshot.ImageDigest, RuntimeProfile: snapshot.RuntimeProfile,
		WorkspaceStorageId: 1, WorkspaceStorageExternalId: "storage",
		WorkspaceStorage: &pb.StateSnapshotWorkspaceStorageCredentials{
			Region: "us-east-1", BucketName: "bucket", AccessKey: "access", SecretKey: "secret",
		},
	}, nil
}

func (r *offlineStateSnapshotRepository) GetVolumeGeneration(_ context.Context, in *pb.GetVolumeGenerationRequest, _ ...grpc.CallOption) (*pb.GetVolumeGenerationResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	generation := r.generations[in.GenerationId]
	return &pb.GetVolumeGenerationResponse{Ok: generation != nil, Generation: generation}, nil
}

type stateBlockCacheReportRepository struct {
	pb.BackendRepositoryServiceClient
	generations map[string]*pb.VolumeGeneration
}

type failingRecentMetadataStore struct {
	cache.CacheMetadataStore
	mu  sync.Mutex
	err error
}

func (m *failingRecentMetadataStore) AddRecentStub(ctx context.Context, locality, workspaceID, stubID string, ttl time.Duration) error {
	m.mu.Lock()
	err := m.err
	m.mu.Unlock()
	if err != nil {
		return err
	}
	return m.CacheMetadataStore.AddRecentStub(ctx, locality, workspaceID, stubID, ttl)
}

func (r *stateBlockCacheReportRepository) GetVolumeGeneration(_ context.Context, in *pb.GetVolumeGenerationRequest, _ ...grpc.CallOption) (*pb.GetVolumeGenerationResponse, error) {
	generation := r.generations[in.GenerationId]
	return &pb.GetVolumeGenerationResponse{Ok: generation != nil, Generation: generation}, nil
}

func (r *offlineStateSnapshotRepository) ReleaseStateVolumeAttachments(_ context.Context, in *pb.ReleaseStateVolumeAttachmentsRequest, _ ...grpc.CallOption) (*pb.ReleaseStateVolumeAttachmentsResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.releaseRequests = append(r.releaseRequests, in)
	return &pb.ReleaseStateVolumeAttachmentsResponse{Ok: true}, nil
}

func (r *offlineStateSnapshotRepository) GetStateSnapshotByOperation(_ context.Context, _ *pb.GetStateSnapshotByOperationRequest, _ ...grpc.CallOption) (*pb.GetStateSnapshotResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.operation, nil
}

func (r *offlineStateSnapshotRepository) ArmStateSnapshot(_ context.Context, in *pb.ArmStateSnapshotRequest, _ ...grpc.CallOption) (*pb.StateSnapshotMutationResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.operation == nil || r.operation.Snapshot == nil || r.operation.Snapshot.ExternalId != in.StateSnapshotId {
		return &pb.StateSnapshotMutationResponse{Ok: false, ErrorMsg: "snapshot unavailable"}, nil
	}
	if in.RecoveryProofToken != r.expectedRecoveryProofToken() {
		return &pb.StateSnapshotMutationResponse{Ok: false, ErrorMsg: "recovery proof rejected"}, nil
	}
	armed := *r.operation.Snapshot
	armed.SourceWorkerId = in.WorkerId
	armed.StorageNodeId = in.StorageNodeId
	r.operation.Snapshot = &armed
	return &pb.StateSnapshotMutationResponse{Ok: true, Snapshot: &armed}, nil
}

func (r *offlineStateSnapshotRepository) ClaimStateSnapshotRecovery(_ context.Context, in *pb.ClaimStateSnapshotRecoveryRequest, _ ...grpc.CallOption) (*pb.StateSnapshotMutationResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.operation == nil || r.operation.Snapshot == nil || r.operation.Snapshot.ExternalId != in.StateSnapshotId {
		return &pb.StateSnapshotMutationResponse{Ok: false, ErrorMsg: "snapshot unavailable"}, nil
	}
	if in.RecoveryProofToken != r.expectedRecoveryProofToken() {
		return &pb.StateSnapshotMutationResponse{Ok: false, ErrorMsg: "recovery proof rejected"}, nil
	}
	claimed := *r.operation.Snapshot
	claimed.RecoveryWorkerId = in.WorkerId
	claimed.StorageNodeId = in.StorageNodeId
	claimed.RecoveryClaimGeneration = in.PreviousClaimGeneration + 1
	r.operation.Snapshot = &claimed
	return &pb.StateSnapshotMutationResponse{Ok: true, Snapshot: &claimed}, nil
}

func (r *offlineStateSnapshotRepository) FailStateSnapshot(_ context.Context, in *pb.FailStateSnapshotRequest, _ ...grpc.CallOption) (*pb.StateSnapshotMutationResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.operation == nil || r.operation.Snapshot == nil || r.operation.Snapshot.ExternalId != in.StateSnapshotId ||
		r.operation.Snapshot.RecoveryClaimGeneration != in.RecoveryClaimGeneration {
		return &pb.StateSnapshotMutationResponse{Ok: false, ErrorMsg: "snapshot failure claim mismatch"}, nil
	}
	r.failRequests = append(r.failRequests, in)
	failed := *r.operation.Snapshot
	failed.Status = string(types.StateSnapshotStatusFailed)
	failed.Reason = in.Reason
	r.operation.Snapshot = &failed
	return &pb.StateSnapshotMutationResponse{Ok: true, Snapshot: &failed}, nil
}

func (r *offlineStateSnapshotRepository) CommitStateSnapshot(_ context.Context, in *pb.CommitStateSnapshotRequest, _ ...grpc.CallOption) (*pb.CommitStateSnapshotResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commitRequests = append(r.commitRequests, in)
	committed := *in.Snapshot
	committed.Status = string(types.StateSnapshotStatusAvailable)
	r.operation = &pb.GetStateSnapshotResponse{
		Ok: true, Snapshot: &committed, WorkspaceId: r.operation.WorkspaceId,
		StubId: r.operation.StubId, PlannedMembers: r.operation.PlannedMembers,
	}
	return &pb.CommitStateSnapshotResponse{Ok: true, Snapshot: &committed}, nil
}

func TestStateSnapshotRecoveryRequiresExactJournalProofForClaimAndCredentials(t *testing.T) {
	snapshot := &pb.StateSnapshot{
		ExternalId: "snapshot", SourceContainerId: "container", OperationId: "operation",
		Status: string(types.StateSnapshotStatusPending),
	}
	repository := &offlineStateSnapshotRepository{
		recoveryProofToken: stateVolumeTestRecoveryProofToken,
		operation: &pb.GetStateSnapshotResponse{
			Ok: true, Snapshot: snapshot, WorkspaceId: "workspace", StubId: "stub",
		},
	}
	worker := &Worker{
		workerId: "replacement", workerInstanceId: "replacement-instance", machineID: "node",
		backendRepoClient: repository,
	}
	for _, proof := range []string{"", "dddddddd-dddd-4ddd-8ddd-dddddddddddd"} {
		expectedClaimError := "recovery proof rejected"
		if proof == "" {
			expectedClaimError = "identity is incomplete"
		}
		if _, err := worker.claimStateSnapshotRecovery(context.Background(), snapshot, "container", "operation", proof); err == nil || !strings.Contains(err.Error(), expectedClaimError) {
			t.Fatalf("recovery claim accepted proof %q: %v", proof, err)
		}
		envelope := StateVolumeRecoveryEnvelope{
			StateSnapshotID: "snapshot", RecoveryProofToken: proof, OperationID: "operation",
		}
		if _, err := worker.recoveryRequestFromEnvelope(context.Background(), "container", envelope); err == nil || !strings.Contains(err.Error(), "recovery proof rejected") {
			t.Fatalf("credential vend accepted proof %q: %v", proof, err)
		}
	}
	if _, err := worker.claimStateSnapshotRecovery(context.Background(), snapshot, "container", "operation", stateVolumeTestRecoveryProofToken); err != nil {
		t.Fatalf("exact journal proof was rejected for recovery claim: %v", err)
	}
	envelope := StateVolumeRecoveryEnvelope{
		StateSnapshotID: "snapshot", RecoveryProofToken: stateVolumeTestRecoveryProofToken, OperationID: "operation",
	}
	if _, err := worker.recoveryRequestFromEnvelope(context.Background(), "container", envelope); err != nil {
		t.Fatalf("exact journal proof was rejected for credential vend: %v", err)
	}
}

func (c *memoryBlockCAS) Put(_ context.Context, digest string, size int64, body io.Reader) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	actual := sha256.Sum256(data)
	if int64(len(data)) != size || hex.EncodeToString(actual[:]) != digest {
		return fmt.Errorf("invalid CAS put")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.objects == nil {
		c.objects = make(map[string][]byte)
	}
	c.objects[digest] = append([]byte(nil), data...)
	return nil
}

func (c *memoryBlockCAS) Get(_ context.Context, digest string, expectedSize int64) (io.ReadCloser, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.getCount++
	data, ok := c.objects[digest]
	if !ok {
		return nil, os.ErrNotExist
	}
	if int64(len(data)) != expectedSize {
		return nil, fmt.Errorf("unexpected object size %d, want %d", len(data), expectedSize)
	}
	data = append([]byte(nil), data...)
	if c.trailing {
		data = append(data, 1)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func TestBlockV1SparseUploadRestoreCacheAndCanonicalManifest(t *testing.T) {
	root := t.TempDir()
	layer := filepath.Join(root, "layer.qcow2")
	file, err := os.OpenFile(layer, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(3 * BlockV1ChunkSize); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("header"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("payload"), 2*BlockV1ChunkSize+123); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	cas := &memoryBlockCAS{}
	manifest, err := CreateBlockV1Manifest(context.Background(), layer, BlockV1Metadata{VolumeID: "root", GenerationID: "gen-1", Generation: 1, VirtualSizeBytes: 20 << 30, Depth: 1}, fakeStateVolumeImages{virtualSize: 20 << 30}, cas)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Chunks) != 2 {
		t.Fatalf("expected two non-zero chunks, got %d", len(manifest.Chunks))
	}
	encodedA, digestA, err := EncodeBlockV1ManifestCanonical(manifest)
	if err != nil {
		t.Fatal(err)
	}
	reversed := manifest
	reversed.Chunks = []BlockV1Chunk{manifest.Chunks[1], manifest.Chunks[0]}
	encodedB, digestB, err := EncodeBlockV1ManifestCanonical(reversed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encodedA, encodedB) || digestA != digestB {
		t.Fatal("canonical manifest changed with input chunk order")
	}
	if published, err := PublishBlockV1Manifest(context.Background(), manifest, cas); err != nil || published != digestA {
		t.Fatalf("publish manifest: %s, %v", published, err)
	}

	restored := filepath.Join(root, "restored.qcow2")
	if err := RestoreBlockV1Layer(context.Background(), restored, manifest, cas); err != nil {
		t.Fatal(err)
	}
	getsAfterRestore := cas.getCount
	if err := RestoreBlockV1Layer(context.Background(), restored, manifest, cas); err != nil {
		t.Fatal(err)
	}
	if cas.getCount != getsAfterRestore {
		t.Fatal("valid cached layer was downloaded again")
	}
	corrupt, err := os.OpenFile(restored, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = corrupt.WriteAt([]byte("broken"), 0)
	_ = corrupt.Close()
	if err := RestoreBlockV1Layer(context.Background(), restored, manifest, cas); err != nil {
		t.Fatalf("corrupt cache was not repaired atomically: %v", err)
	}
	data := make([]byte, 6)
	valid, _ := os.Open(restored)
	_, _ = valid.ReadAt(data, 0)
	_ = valid.Close()
	if string(data) != "header" {
		t.Fatalf("corrupt cache repair produced %q", data)
	}
}

func TestBlockV1RejectsTrailingCASDataAndForgedChains(t *testing.T) {
	root := t.TempDir()
	data := []byte("chunk")
	digestBytes := sha256.Sum256(data)
	digest := hex.EncodeToString(digestBytes[:])
	cas := &memoryBlockCAS{objects: map[string][]byte{digest: data}, trailing: true}
	manifest := testBlockManifest("volume-a", "child", "", 1)
	manifest.LayerFileSizeBytes = int64(len(data))
	manifest.Chunks = []BlockV1Chunk{{Index: 0, OffsetBytes: 0, SizeBytes: int64(len(data)), Digest: digest}}
	if err := RestoreBlockV1Layer(context.Background(), filepath.Join(root, "trailing.qcow2"), manifest, cas); err == nil {
		t.Fatal("accepted CAS object with trailing bytes")
	}

	resolver := mapManifestResolver{manifests: map[string]BlockV1Manifest{
		"parent": testBlockManifest("volume-a", "parent", "", 1),
		"child":  testBlockManifest("volume-a", "child", "parent", 3),
	}}
	if _, _, err := RestoreBlockV1ChainForVolume(context.Background(), "volume-a", "child", filepath.Join(root, "depth"), resolver, &memoryBlockCAS{}, fakeStateVolumeImages{}); err == nil || !strings.Contains(err.Error(), "does not follow") {
		t.Fatalf("forged depth was accepted: %v", err)
	}
	if _, _, err := RestoreBlockV1ChainForVolume(context.Background(), "volume-b", "parent", filepath.Join(root, "volume"), resolver, &memoryBlockCAS{}, fakeStateVolumeImages{}); err == nil || !strings.Contains(err.Error(), "belongs to volume") {
		t.Fatalf("forged volume was accepted: %v", err)
	}
	short := testBlockManifest("volume-a", "short", "", 1)
	short.LayerFileSizeBytes = 2 * BlockV1ChunkSize
	short.Chunks = []BlockV1Chunk{{
		Index: 0, OffsetBytes: 0, SizeBytes: BlockV1ChunkSize - 1, Digest: strings.Repeat("a", 64),
	}}
	if err := ValidateBlockV1Manifest(short); err == nil {
		t.Fatal("accepted a short block.v1 chunk that would silently zero-fill allocated bytes")
	}
}

func TestBlockV1ManifestRejectsResourceBoundsAndCheckedArithmeticBeforeMaterialization(t *testing.T) {
	valid := testBlockManifest("volume-a", "generation-1", "", 1)
	invalid := []struct {
		name     string
		manifest BlockV1Manifest
	}{
		{name: "virtual_size", manifest: func() BlockV1Manifest {
			m := valid
			m.VirtualSizeBytes = BlockV1MaxVirtualSizeBytes + 1
			return m
		}()},
		{name: "layer_size", manifest: func() BlockV1Manifest {
			m := valid
			m.LayerFileSizeBytes = BlockV1MaxLayerFileSizeBytes + 1
			return m
		}()},
		{name: "overflow_index", manifest: func() BlockV1Manifest {
			m := valid
			m.Chunks = []BlockV1Chunk{{Index: math.MaxInt64, OffsetBytes: 0, SizeBytes: 1, Digest: strings.Repeat("a", 64)}}
			return m
		}()},
		{name: "past_last_chunk", manifest: func() BlockV1Manifest {
			m := valid
			m.Chunks = []BlockV1Chunk{{Index: 1, OffsetBytes: BlockV1ChunkSize, SizeBytes: 1, Digest: strings.Repeat("a", 64)}}
			return m
		}()},
		{name: "chunk_count", manifest: func() BlockV1Manifest {
			m := valid
			m.Chunks = make([]BlockV1Chunk, BlockV1MaxChunks+1)
			return m
		}()},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateBlockV1Manifest(test.manifest); err == nil {
				t.Fatalf("accepted hostile block.v1 manifest: %+v", test.manifest)
			}
		})
	}

	if _, err := allBlockV1ChunkIndices(math.MaxInt64, BlockV1ChunkSize); err == nil {
		t.Fatal("untrusted layer size could allocate an unbounded index slice")
	}
	destination := filepath.Join(t.TempDir(), "must-not-exist.qcow2")
	if err := restoreBlockV1Layer(context.Background(), destination, valid, valid.VirtualSizeBytes+1, &memoryBlockCAS{}); err == nil || !strings.Contains(err.Error(), "requested size") {
		t.Fatalf("requested virtual size mismatch reached materialization: %v", err)
	}
	if _, err := os.Lstat(destination); !os.IsNotExist(err) {
		t.Fatalf("virtual-size mismatch created/truncated destination: %v", err)
	}
	cas := &memoryBlockCAS{}
	chainRoot := t.TempDir()
	if _, _, err := RestoreBlockV1ChainForVolume(context.Background(), valid.VolumeID, valid.GenerationID, chainRoot,
		mapManifestResolver{manifests: map[string]BlockV1Manifest{valid.GenerationID: valid}}, cas, fakeStateVolumeImages{}, valid.VirtualSizeBytes+1,
	); err == nil || !strings.Contains(err.Error(), "requested size") {
		t.Fatalf("scheduler virtual size was not bound before chain materialization: %v", err)
	}
	if cas.getCount != 0 {
		t.Fatalf("virtual-size mismatch read %d CAS objects before rejection", cas.getCount)
	}
}

func TestBlockV1ForkCloneRekeysPhysicalLineageAndAdvancesIndependently(t *testing.T) {
	root := t.TempDir()
	layerBytes := []byte("QFI:1:")
	layerHash := sha256.Sum256(layerBytes)
	layerDigest := hex.EncodeToString(layerHash[:])
	withLayer := func(manifest BlockV1Manifest) BlockV1Manifest {
		manifest.LayerFileSizeBytes = int64(len(layerBytes))
		manifest.Chunks = []BlockV1Chunk{{Index: 0, OffsetBytes: 0, SizeBytes: int64(len(layerBytes)), Digest: layerDigest}}
		return manifest
	}
	source := withLayer(testBlockManifest("volume-source", "source-7", "", 1))
	source.Generation = 7
	fork := withLayer(testBlockManifest("volume-fork", "fork-1", "", 2))
	fork.Generation = 1
	fork.CloneParentGenerationID = source.GenerationID
	sourceNext := withLayer(testBlockManifest("volume-source", "source-8", source.GenerationID, 2))
	sourceNext.Generation = 8
	forkNext := withLayer(testBlockManifest("volume-fork", "fork-2", fork.GenerationID, 3))
	forkNext.Generation = 2
	resolver := mapManifestResolver{manifests: map[string]BlockV1Manifest{
		source.GenerationID:     source,
		fork.GenerationID:       fork,
		sourceNext.GenerationID: sourceNext,
		forkNext.GenerationID:   forkNext,
	}}
	cas := &memoryBlockCAS{objects: map[string][]byte{layerDigest: layerBytes}}
	images := fakeStateVolumeImages{virtualSize: 1}

	forkPath, restoredFork, err := RestoreBlockV1ChainForVolume(
		context.Background(), fork.VolumeID, fork.GenerationID, filepath.Join(root, "fork"), resolver, cas, images,
	)
	if err != nil {
		t.Fatal(err)
	}
	if restoredFork.CloneParentGenerationID != source.GenerationID || restoredFork.VolumeID == source.VolumeID {
		t.Fatalf("fork did not preserve its one authenticated cross-volume edge: %+v", restoredFork)
	}
	forkInfo, err := images.Info(context.Background(), forkPath)
	if err != nil {
		t.Fatal(err)
	}
	sourcePath, _, err := RestoreBlockV1ChainForVolume(
		context.Background(), source.VolumeID, source.GenerationID, filepath.Join(root, "fork"), resolver, cas, images,
	)
	if err != nil {
		t.Fatal(err)
	}
	if forkInfo.BackingPath != sourcePath {
		t.Fatalf("fork graph does not physically point at exact clone parent: %+v", forkInfo)
	}

	sourceNextPath, _, err := RestoreBlockV1ChainForVolume(
		context.Background(), source.VolumeID, sourceNext.GenerationID, filepath.Join(root, "source-next"), resolver, cas, images,
	)
	if err != nil {
		t.Fatalf("advance source branch: %v", err)
	}
	forkNextPath, _, err := RestoreBlockV1ChainForVolume(
		context.Background(), fork.VolumeID, forkNext.GenerationID, filepath.Join(root, "fork-next"), resolver, cas, images,
	)
	if err != nil {
		t.Fatalf("advance fork branch: %v", err)
	}
	if sourceNextPath == forkNextPath {
		t.Fatal("independent source and fork branches resolved to the same graph path")
	}
}

func TestPendingBackingSurvivesCommittedPivotJournalRewrite(t *testing.T) {
	root := t.TempDir()
	journal := StateVolumeJournal{
		Version: stateVolumeJournalVersion, ContainerID: "container", OperationID: "operation", Phase: "pivot-intent",
		QMPSocket: filepath.Join(root, "runtime", "qmp.sock"), NBDSocket: filepath.Join(root, "runtime", "nbd.sock"),
		Volumes: []StateVolumeJournalVolume{{
			ID: "root", Name: "root", Root: true, ContainerMountPath: "/", ExportName: "export",
			DevicePath: "/dev/nbd0", BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"),
			SizeBytes: 1024, FileNode: "file", RootNode: "root", ActiveNode: "old", ActiveLayerPath: filepath.Join(root, "old.qcow2"),
			ActiveBackingPath: filepath.Join(root, "grandparent.qcow2"), Depth: 2, Generation: 1,
			PivotNode: "new", PivotLayerPath: filepath.Join(root, "new.qcow2"), PendingGenerationID: "generation-2",
			PendingGeneration: 2, PendingLayerPath: filepath.Join(root, "old.qcow2"),
			PendingBackingPath: filepath.Join(root, "grandparent.qcow2"), PendingParentGenerationID: "generation-1", PendingDepth: 2,
		}},
	}
	manager := &StateVolumeManager{
		Journals:        StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		ProcessIdentity: stateVolumeProcessIdentity,
	}
	if err := manager.defaults(); err != nil {
		t.Fatal(err)
	}
	group := stateVolumeGroupFromJournal(journal)
	graph := snapshotTestGraph("root", "new", "export", "old", "new")
	graph.Nodes["old"] = StateVolumeQMPNode{
		Name: "old", Driver: "qcow2", FilePath: journal.Volumes[0].ActiveLayerPath,
		BackingFilePath: journal.Volumes[0].ActiveBackingPath, BackingFileDepth: 1,
	}
	graph.Nodes["new"] = StateVolumeQMPNode{
		Name: "new", Driver: "qcow2", FilePath: journal.Volumes[0].PivotLayerPath,
		BackingFilePath: journal.Volumes[0].ActiveLayerPath, BackingFileDepth: 1,
	}
	group.qmp = &fakeStateVolumeQMP{graph: graph}
	if err := manager.reconcileJournalPivot(context.Background(), group, journal); err != nil {
		t.Fatal(err)
	}
	rewritten, err := manager.Journals.Load(journal.ContainerID)
	if err != nil {
		t.Fatal(err)
	}
	receipt := pendingReceiptFromJournal(rewritten)
	if receipt == nil || len(receipt.Generations) != 1 {
		t.Fatalf("pending receipt was lost after recovery: %+v", receipt)
	}
	if got, want := receipt.Generations[0].BackingPath, filepath.Join(root, "grandparent.qcow2"); got != want {
		t.Fatalf("double-crash recovery produced self-backing %q, want %q", got, want)
	}
}

func TestStateVolumePlanSnapshotCompletesLiveBlockStreamAtDepthThresholdAndHardCap(t *testing.T) {
	for _, test := range []struct {
		name              string
		depth             int
		finalizeReplyLost bool
	}{
		{name: "background_threshold", depth: StateVolumeCompactDepth},
		{name: "hard_cap", depth: StateVolumeMaxDepth},
		{name: "lost_finalize_reply", depth: StateVolumeCompactDepth, finalizeReplyLost: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			activePath := filepath.Join(root, "graphs", "active.qcow2")
			backingPath := filepath.Join(root, "graphs", "head.qcow2")
			mountPath := filepath.Join(root, "mounts", "root")
			for _, dir := range []string{filepath.Dir(activePath), mountPath, filepath.Join(root, "journals")} {
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			qmp := &fakeStateVolumeQMP{
				blockStreamAutoPending: true, blockStreamFinalizeFlatNode: "active-root",
				graph: StateVolumeQMPSnapshotGraph{
					Nodes: map[string]StateVolumeQMPNode{
						"root-root": {Name: "root-root", Driver: "raw", ChildNode: "active-root"},
						"active-root": {
							Name: "active-root", Driver: "qcow2", FilePath: activePath,
							BackingFilePath: backingPath, BackingFileDepth: test.depth - 1,
						},
					},
					Exports: map[string]StateVolumeQMPExport{
						"export-root": {ID: "export-root", NodeName: "root-root"},
					},
				},
			}
			if test.finalizeReplyLost {
				qmp.blockStreamFinalizeError = ErrStateVolumeCompactionIndeterminate
			}
			currentGenerationID := "11111111-1111-4111-8111-111111111111"
			volumeID := "22222222-2222-4222-8222-222222222222"
			group := &stateVolumeGroup{
				containerID: "container", runtimeDir: filepath.Join(root, "runtime", "container"),
				qmpSocket: filepath.Join(root, "runtime", "container", "qmp.sock"), nbdSocket: filepath.Join(root, "runtime", "container", "nbd.sock"),
				qmp: qmp, ready: true,
				volumes: []*stateVolumeRuntime{{
					exportName: "export-root", fileNode: "file-root", rootNode: "root-root", activeNode: "active-root",
					devicePath: "/dev/nbd0",
					spec: StateVolumeSpec{
						ID: volumeID, Name: "root", ContainerMountPath: "/", Root: true,
						Generation: int64(test.depth), CurrentGenerationID: currentGenerationID,
						BackingDir: filepath.Dir(activePath), MountPath: mountPath, SizeBytes: 1 << 30,
						ActiveLayerPath: activePath, ActiveBackingPath: backingPath,
						ParentGenerationID: currentGenerationID, Depth: test.depth,
					},
				}},
			}
			manager := &StateVolumeManager{
				RuntimeRoot: filepath.Join(root, "runtime"), StateRoot: root,
				Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
				groups:   map[string]*stateVolumeGroup{"container": group},
			}

			plan, err := manager.PlanSnapshot(context.Background(), "container", "operation")
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Generations) != 1 || plan.Generations[0].Depth != 1 ||
				plan.Generations[0].ParentGenerationID != "" || plan.Generations[0].CloneParentGenerationID != "" ||
				!plan.Generations[0].Compaction || plan.Generations[0].CompactionSourceGenerationID != currentGenerationID {
				t.Fatalf("compacted plan retained physical ancestry: %+v", plan.Generations)
			}
			if group.volumes[0].spec.ActiveBackingPath != "" || group.volumes[0].spec.Depth != 1 {
				t.Fatalf("active graph was not made parentless: %+v", group.volumes[0].spec)
			}
			if qmp.blockStreamStarts != 1 || qmp.blockStreamFinalizes != 1 || qmp.blockStreamDismisses != 1 {
				t.Fatalf("unexpected block-stream lifecycle starts=%d finalizes=%d dismisses=%d", qmp.blockStreamStarts, qmp.blockStreamFinalizes, qmp.blockStreamDismisses)
			}
			journal, err := manager.Journals.Load("container")
			if err != nil {
				t.Fatal(err)
			}
			if journal.Phase != "running" || journal.Volumes[0].Depth != 1 || journal.Volumes[0].ActiveBackingPath != "" || journal.Volumes[0].CompactionPhase != "" {
				t.Fatalf("compacted journal is not canonical: %+v", journal)
			}
		})
	}
}

func TestStateVolumeStuckBackgroundCompactionDoesNotHoldGroupLockAndStopCancelsIt(t *testing.T) {
	root := t.TempDir()
	activePath := filepath.Join(root, "graphs", "active.qcow2")
	backingPath := filepath.Join(root, "graphs", "head.qcow2")
	mountPath := filepath.Join(root, "mounts", "root")
	for _, dir := range []string{filepath.Dir(activePath), mountPath, filepath.Join(root, "journals")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	queryStarted := make(chan struct{})
	qmp := &fakeStateVolumeQMP{
		blockJobQueryStarted: queryStarted,
		graph: StateVolumeQMPSnapshotGraph{
			Nodes: map[string]StateVolumeQMPNode{
				"root-root": {Name: "root-root", Driver: "raw", ChildNode: "active-root"},
				"active-root": {
					Name: "active-root", Driver: "qcow2", FilePath: activePath,
					BackingFilePath: backingPath, BackingFileDepth: StateVolumeCompactDepth - 1,
				},
			},
			Exports: map[string]StateVolumeQMPExport{
				"export-root": {ID: "export-root", NodeName: "root-root"},
			},
		},
	}
	group := &stateVolumeGroup{
		containerID: "container", runtimeDir: filepath.Join(root, "runtime", "container"),
		qmpSocket: filepath.Join(root, "runtime", "container", "qmp.sock"), nbdSocket: filepath.Join(root, "runtime", "container", "nbd.sock"),
		qmp: qmp, ready: true,
		volumes: []*stateVolumeRuntime{{
			exportName: "export-root", fileNode: "file-root", rootNode: "root-root", activeNode: "active-root",
			devicePath: "/dev/nbd0",
			spec: StateVolumeSpec{
				ID: "22222222-2222-4222-8222-222222222222", Name: "root", ContainerMountPath: "/", Root: true,
				Generation: 16, CurrentGenerationID: "11111111-1111-4111-8111-111111111111",
				BackingDir: filepath.Dir(activePath), MountPath: mountPath, SizeBytes: 1 << 30,
				ActiveLayerPath: activePath, ActiveBackingPath: backingPath,
				ParentGenerationID: "11111111-1111-4111-8111-111111111111", Depth: StateVolumeCompactDepth,
			},
		}},
	}
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), StateRoot: root,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		groups:   map[string]*stateVolumeGroup{"container": group},
	}
	manager.scheduleStateVolumeCompactions(group)
	select {
	case <-queryStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("block-stream worker did not reach the injected stuck QMP query")
	}
	lockAcquired := make(chan struct{})
	go func() {
		group.mu.Lock()
		group.mu.Unlock()
		close(lockAcquired)
	}()
	select {
	case <-lockAcquired:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("background block-stream polling held group.mu")
	}
	cancelCtx, cancelCompaction := context.WithTimeout(context.Background(), 2*time.Second)
	if err := manager.cancelAndWaitStateVolumeCompactions(cancelCtx, group); err != nil {
		cancelCompaction()
		t.Fatal(err)
	}
	cancelCompaction()
	group.mu.Lock()
	compactionPhase := group.volumes[0].compactionPhase
	backingAfterCancel := group.volumes[0].spec.ActiveBackingPath
	group.mu.Unlock()
	qmp.mu.Lock()
	cancels := qmp.blockStreamCancels
	qmp.mu.Unlock()
	if compactionPhase != "" || backingAfterCancel != backingPath || cancels == 0 {
		t.Fatalf("compaction cancellation was not durably resolved: phase=%q backing=%q cancels=%d", compactionPhase, backingAfterCancel, cancels)
	}
	journal, err := manager.Journals.Load("container")
	if err != nil {
		t.Fatal(err)
	}
	if journal.Phase != "running" || journal.Volumes[0].CompactionPhase != "" {
		t.Fatalf("resolved compaction cancellation retained an ambiguous journal: %+v", journal)
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelStop()
	if err := manager.Stop(stopCtx, "container"); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := manager.ExistingGroup("container"); ok {
		t.Fatal("stopped compaction group remained schedulable")
	}
}

func TestStateVolumeIndeterminateCompactionCancellationBlocksTeardown(t *testing.T) {
	root := t.TempDir()
	activePath := filepath.Join(root, "graphs", "active.qcow2")
	backingPath := filepath.Join(root, "graphs", "head.qcow2")
	mountPath := filepath.Join(root, "mounts", "root")
	for _, dir := range []string{filepath.Dir(activePath), mountPath, filepath.Join(root, "journals")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	queryStarted := make(chan struct{})
	qmp := &fakeStateVolumeQMP{
		blockJobQueryStarted: queryStarted,
		graph: StateVolumeQMPSnapshotGraph{
			Nodes: map[string]StateVolumeQMPNode{
				"root-root": {Name: "root-root", Driver: "raw", ChildNode: "active-root"},
				"active-root": {
					Name: "active-root", Driver: "qcow2", FilePath: activePath,
					BackingFilePath: backingPath, BackingFileDepth: StateVolumeCompactDepth - 1,
				},
			},
			Exports: map[string]StateVolumeQMPExport{"export-root": {ID: "export-root", NodeName: "root-root"}},
		},
	}
	secure := &phaseFailingStateVolumeSecurePaths{
		stateVolumeSecurePathOps: newStateVolumeSecurePathOps(), phase: "running",
	}
	group := &stateVolumeGroup{
		containerID: "container", runtimeDir: filepath.Join(root, "runtime", "container"),
		qmpSocket: filepath.Join(root, "runtime", "container", "qmp.sock"), nbdSocket: filepath.Join(root, "runtime", "container", "nbd.sock"),
		qmp: qmp, ready: true,
		volumes: []*stateVolumeRuntime{{
			exportName: "export-root", fileNode: "file-root", rootNode: "root-root", activeNode: "active-root", devicePath: "/dev/nbd0",
			spec: StateVolumeSpec{
				ID: "22222222-2222-4222-8222-222222222222", Name: "root", ContainerMountPath: "/", Root: true,
				Generation: 16, CurrentGenerationID: "11111111-1111-4111-8111-111111111111",
				BackingDir: filepath.Dir(activePath), MountPath: mountPath, SizeBytes: 1 << 30,
				ActiveLayerPath: activePath, ActiveBackingPath: backingPath,
				ParentGenerationID: "11111111-1111-4111-8111-111111111111", Depth: StateVolumeCompactDepth,
			},
		}},
	}
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), StateRoot: root, SecurePaths: secure,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals"), SecurePaths: secure},
		groups:   map[string]*stateVolumeGroup{"container": group},
	}
	manager.scheduleStateVolumeCompactions(group)
	select {
	case <-queryStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("block-stream worker did not reach the injected QMP query")
	}
	qmp.mu.Lock()
	qmp.err = errors.New("injected QMP outage during cancellation")
	qmp.mu.Unlock()
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := manager.Stop(stopCtx, "container")
	cancel()
	if err == nil || !strings.Contains(err.Error(), "durable cancellation outcome") {
		t.Fatalf("indeterminate compaction cancellation allowed teardown: %v", err)
	}
	if _, _, ok := manager.ExistingGroup("container"); !ok {
		t.Fatal("indeterminate compaction cancellation removed the live group")
	}
	qmp.mu.Lock()
	quits := qmp.quits
	qmp.mu.Unlock()
	if quits != 0 {
		t.Fatalf("indeterminate compaction cancellation stopped QSD %d times", quits)
	}
	journal, loadErr := manager.Journals.Load("container")
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if journal.Phase != "compacting" || journal.Volumes[0].CompactionPhase != "cancel-indeterminate" {
		t.Fatalf("indeterminate compaction cancellation was not durable: %+v", journal)
	}

	qmp.mu.Lock()
	qmp.err = nil
	qmp.mu.Unlock()
	secure.mu.Lock()
	secure.remaining = 1
	secure.mu.Unlock()
	retryCtx, cancelRetry := context.WithTimeout(context.Background(), 2*time.Second)
	err = manager.Stop(retryCtx, "container")
	cancelRetry()
	if err == nil || !strings.Contains(err.Error(), "injected running journal write failure") {
		t.Fatalf("final compaction journal failure did not remain fenced: %v", err)
	}
	if _, _, ok := manager.ExistingGroup("container"); !ok {
		t.Fatal("failed final compaction journal write removed the live group")
	}
	group.mu.Lock()
	phaseAfterSaveFailure := group.volumes[0].compactionPhase
	group.mu.Unlock()
	if phaseAfterSaveFailure == "" {
		t.Fatal("failed final compaction journal write cleared the in-memory retry phase")
	}
	finalCtx, cancelFinal := context.WithTimeout(context.Background(), 2*time.Second)
	err = manager.Stop(finalCtx, "container")
	cancelFinal()
	if err != nil {
		t.Fatalf("exact teardown retry did not reconcile the restored compaction phase: %v", err)
	}
	if _, _, ok := manager.ExistingGroup("container"); ok {
		t.Fatal("successfully reconciled compaction group remained schedulable")
	}
	if _, err := manager.Journals.Load("container"); err == nil {
		t.Fatal("successfully reconciled teardown retained its compaction journal")
	}
	qmp.mu.Lock()
	cancels, quits := qmp.blockStreamCancels, qmp.quits
	qmp.mu.Unlock()
	if cancels < 2 || quits != 1 {
		t.Fatalf("second teardown skipped durable cancellation reconciliation: cancels=%d quits=%d", cancels, quits)
	}
}

func TestReadOnlyGenerationReusePersistsExactAncestryAcrossRecovery(t *testing.T) {
	volume := &stateVolumeRuntime{spec: StateVolumeSpec{
		ID: "read-only-volume", Name: "models", ContainerMountPath: "/models", ReadOnly: true,
		Generation: 7, CurrentGenerationID: "generation-7", ParentGenerationID: "generation-6",
		BackingDir: t.TempDir(), MountPath: filepath.Join(t.TempDir(), "mount"), ActiveLayerPath: filepath.Join(t.TempDir(), "generation-7.qcow2"),
		SizeBytes: 1024, Depth: 7,
	}, exportName: "export-models", fileNode: "file-models", activeNode: "active-models", rootNode: "root-models", devicePath: "/dev/nbd7"}
	runtimeDir := t.TempDir()
	group := &stateVolumeGroup{containerID: "container", runtimeDir: runtimeDir, qmpSocket: filepath.Join(runtimeDir, "qmp.sock"), nbdSocket: filepath.Join(runtimeDir, "nbd.sock"), volumes: []*stateVolumeRuntime{volume}}
	manager := &StateVolumeManager{groups: map[string]*stateVolumeGroup{"container": group}}
	plan, err := manager.PlanSnapshot(context.Background(), "container", "operation")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Generations) != 1 {
		t.Fatalf("read-only plan has %d members", len(plan.Generations))
	}
	planned := plan.Generations[0]
	if !planned.Reused || planned.GenerationID != volume.spec.CurrentGenerationID || planned.Generation != volume.spec.Generation ||
		planned.ParentGenerationID != volume.spec.ParentGenerationID || planned.Depth != volume.spec.Depth {
		t.Fatalf("read-only plan lost authoritative generation ancestry: %+v", planned)
	}
	group.pending = plan
	journal := manager.groupJournal(group, "pivoted", "operation")
	if err := validateStateVolumeJournal(journal); err != nil {
		t.Fatal(err)
	}
	if !journal.Volumes[0].PendingReused {
		t.Fatalf("journal did not explicitly encode read-only reuse: %+v", journal.Volumes[0])
	}
	recovered := pendingReceiptFromJournal(journal)
	if recovered == nil || len(recovered.Generations) != 1 {
		t.Fatalf("read-only recovery receipt is unavailable: %+v", recovered)
	}
	got := recovered.Generations[0]
	if !got.Reused || got.GenerationID != planned.GenerationID || got.Generation != planned.Generation ||
		got.ParentGenerationID != planned.ParentGenerationID || got.Depth != planned.Depth || got.LayerPath != "" {
		t.Fatalf("read-only recovery changed immutable generation: got=%+v want=%+v", got, planned)
	}
}

func TestRootAndReadOnlyCloneGenerationPlanPivotUploadStayExact(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 2)
	root := t.TempDir()
	images := fakeStateVolumeImages{}
	readOnlyRoot := filepath.Join(root, "block-cache")
	readOnlyLayer := filepath.Join(readOnlyRoot, "generation-7.qcow2")
	readOnlyBacking := filepath.Join(readOnlyRoot, "clone-source.qcow2")
	if err := images.Create(context.Background(), readOnlyLayer, 1024, readOnlyBacking); err != nil {
		t.Fatal(err)
	}
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		NBD: allocator, Connector: &fakeStateVolumeConnector{}, Images: images, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: &fakeStateVolumeQMP{}}, Launcher: &inheritedFDLauncher{},
	}
	_, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{
		{
			ID: "root", Name: "root", ContainerMountPath: "/", Root: true,
			BackingDir: filepath.Join(root, "root-graph"), MountPath: filepath.Join(root, "root-mount"),
			SizeBytes: 1024, Format: true, AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
		},
		{
			ID: "models", Name: "models", ContainerMountPath: "/models", ReadOnly: true,
			Generation: 7, CurrentGenerationID: "generation-7", CloneParentGenerationID: "source-generation-6",
			BackingDir: filepath.Join(root, "models-export"), MountPath: filepath.Join(root, "models-mount"),
			ReadOnlyLayerRoot: readOnlyRoot, ActiveLayerPath: readOnlyLayer, ActiveBackingPath: readOnlyBacking,
			SizeBytes: 1024, Depth: 7,
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := manager.PlanSnapshot(context.Background(), "container", "operation")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := manager.Pivot(context.Background(), "container", "operation")
	if err != nil {
		t.Fatal(err)
	}
	plannedMembers := make([]*pb.StateGeneration, 0, len(plan.Generations))
	for _, generation := range plan.Generations {
		plannedMembers = append(plannedMembers, &pb.StateGeneration{
			VolumeId: generation.VolumeID, GenerationId: generation.GenerationID, Generation: generation.Generation,
			ParentGenerationId: generation.ParentGenerationID, CloneParentGenerationId: generation.CloneParentGenerationID,
			Name: generation.Name, MountPath: generation.MountPath, ReadOnly: generation.ReadOnly, Root: generation.Root,
		})
	}
	if !stateVolumePlannedMembersMatchReceipt(plannedMembers, nil, receipt) {
		t.Fatalf("actual pivot changed the exact root+read-only escrow plan: plan=%+v receipt=%+v", plan, receipt)
	}
	uploaded, err := manager.UploadPending(context.Background(), "container", "operation", &memoryBlockCAS{})
	if err != nil {
		t.Fatal(err)
	}
	var reused *StateVolumeGenerationReceipt
	for index := range uploaded {
		if uploaded[index].Reused {
			reused = &uploaded[index]
		}
	}
	if reused == nil || reused.GenerationID != "generation-7" || reused.Generation != 7 ||
		reused.CloneParentGenerationID != "source-generation-6" || reused.ParentGenerationID != "" || reused.Depth != 7 ||
		reused.VirtualSizeBytes != 1024 || !reused.ReadOnly {
		t.Fatalf("read-only upload receipt lost authenticated clone ancestry: %+v", reused)
	}
	if err := manager.AcknowledgePending("container", "operation"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Stop(context.Background(), "container"); err != nil {
		t.Fatal(err)
	}
}

func TestUploadPendingRejectsImmutableQCOWCorruptionBeforeCAS(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		NBD: allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: &fakeStateVolumeQMP{}}, Launcher: &inheritedFDLauncher{},
	}
	if _, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
		ID: "root", Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"), SizeBytes: 1024, Format: true,
		AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}}); err != nil {
		t.Fatal(err)
	}
	receipt, err := manager.Pivot(context.Background(), "container", "operation")
	if err != nil {
		t.Fatal(err)
	}
	manager.Images = failingImmutableCheckImages{fakeStateVolumeImages: fakeStateVolumeImages{}, failPath: receipt.Generations[0].LayerPath}
	cas := &memoryBlockCAS{}
	if _, err := manager.UploadPending(context.Background(), "container", "operation", cas); err == nil || !strings.Contains(err.Error(), "refcount corruption") {
		t.Fatalf("corrupt immutable qcow2 was publishable: %v", err)
	}
	cas.mu.Lock()
	objects := len(cas.objects)
	cas.mu.Unlock()
	if objects != 0 {
		t.Fatalf("corrupt immutable qcow2 wrote %d CAS objects", objects)
	}
	if pendingID, pending := manager.PendingOperation("container"); !pending || pendingID != "operation" {
		t.Fatalf("failed upload lost recovery obligation: id=%q pending=%t", pendingID, pending)
	}
	group, _ := manager.group("container")
	group.mu.Lock()
	group.pending = nil
	group.mu.Unlock()
	if err := manager.Stop(context.Background(), "container"); err != nil {
		t.Fatal(err)
	}
}

func TestJournalPivotReconciliationRequiresExactRawWrapperAndExport(t *testing.T) {
	root := t.TempDir()
	journal := StateVolumeJournal{Volumes: []StateVolumeJournalVolume{
		{ID: "root-volume", RootNode: "root-a", ExportName: "export-a", ActiveNode: "old-a", PivotNode: "new-a", ActiveLayerPath: filepath.Join(root, "old-a.qcow2"), PivotLayerPath: filepath.Join(root, "new-a.qcow2")},
		{ID: "data-volume", RootNode: "root-b", ExportName: "export-b", ActiveNode: "old-b", PivotNode: "new-b", ActiveLayerPath: filepath.Join(root, "old-b.qcow2"), PivotLayerPath: filepath.Join(root, "new-b.qcow2")},
	}}
	committed := StateVolumeQMPSnapshotGraph{
		Nodes: map[string]StateVolumeQMPNode{
			"root-a": {Name: "root-a", ChildNode: "new-a", Driver: "raw"},
			"new-a":  {Name: "new-a", Driver: "qcow2", FilePath: filepath.Join(root, "new-a.qcow2"), BackingFilePath: filepath.Join(root, "old-a.qcow2"), BackingFileDepth: 1},
			"old-a":  {Name: "old-a", Driver: "qcow2", FilePath: filepath.Join(root, "old-a.qcow2")},
			"root-b": {Name: "root-b", ChildNode: "new-b", Driver: "raw"},
			"new-b":  {Name: "new-b", Driver: "qcow2", FilePath: filepath.Join(root, "new-b.qcow2"), BackingFilePath: filepath.Join(root, "old-b.qcow2"), BackingFileDepth: 1},
			"old-b":  {Name: "old-b", Driver: "qcow2", FilePath: filepath.Join(root, "old-b.qcow2")},
		},
		Exports: map[string]StateVolumeQMPExport{
			"export-a": {ID: "export-a", NodeName: "root-a"},
			"export-b": {ID: "export-b", NodeName: "root-b"},
		},
	}
	if outcome, err := inspectJournalPivotGraph(context.Background(), &fakeStateVolumeQMP{graph: committed}, journal); err != nil || outcome != StateVolumePivotCommitted {
		t.Fatalf("exact committed graph was rejected: outcome=%q err=%v", outcome, err)
	}

	cloneGraph := func() StateVolumeQMPSnapshotGraph {
		graph := StateVolumeQMPSnapshotGraph{Nodes: make(map[string]StateVolumeQMPNode), Exports: make(map[string]StateVolumeQMPExport)}
		for name, node := range committed.Nodes {
			graph.Nodes[name] = node
		}
		for id, export := range committed.Exports {
			graph.Exports[id] = export
		}
		return graph
	}
	tests := map[string]func(*StateVolumeQMPSnapshotGraph){
		"wrapper points elsewhere": func(graph *StateVolumeQMPSnapshotGraph) {
			graph.Nodes["unrelated"] = StateVolumeQMPNode{Name: "unrelated"}
			graph.Nodes["root-b"] = StateVolumeQMPNode{Name: "root-b", ChildNode: "unrelated"}
		},
		"export points at wrong wrapper": func(graph *StateVolumeQMPSnapshotGraph) {
			graph.Exports["export-b"] = StateVolumeQMPExport{ID: "export-b", NodeName: "root-a"}
		},
		"export is shutting down": func(graph *StateVolumeQMPSnapshotGraph) {
			graph.Exports["export-b"] = StateVolumeQMPExport{ID: "export-b", NodeName: "root-b", ShuttingDown: true}
		},
		"mixed transaction": func(graph *StateVolumeQMPSnapshotGraph) {
			graph.Nodes["root-b"] = StateVolumeQMPNode{Name: "root-b", ChildNode: "old-b", Driver: "raw"}
		},
		"pivot file path mismatch": func(graph *StateVolumeQMPSnapshotGraph) {
			node := graph.Nodes["new-b"]
			node.FilePath = filepath.Join(root, "forged.qcow2")
			graph.Nodes["new-b"] = node
		},
		"pivot backing mismatch": func(graph *StateVolumeQMPSnapshotGraph) {
			node := graph.Nodes["new-b"]
			node.BackingFilePath = filepath.Join(root, "other.qcow2")
			graph.Nodes["new-b"] = node
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			graph := cloneGraph()
			mutate(&graph)
			if outcome, err := inspectJournalPivotGraph(context.Background(), &fakeStateVolumeQMP{graph: graph}, journal); err == nil {
				t.Fatalf("unsafe graph was accepted as %q: %+v", outcome, graph)
			}
		})
	}
}

type observableStateVolumeProcess struct {
	done chan struct{}
	err  error
}

func (p *observableStateVolumeProcess) PID() int                       { return 1 }
func (p *observableStateVolumeProcess) Wait(ctx context.Context) error { <-p.done; return p.err }
func (p *observableStateVolumeProcess) Kill() error                    { return nil }
func (p *observableStateVolumeProcess) Done() <-chan struct{}          { return p.done }
func (p *observableStateVolumeProcess) ExitError() error               { return p.err }

func TestStateVolumeUnexpectedQSDExitFencesGroup(t *testing.T) {
	process := &observableStateVolumeProcess{done: make(chan struct{}), err: errors.New("qsd died")}
	callback := make(chan error, 1)
	group := &stateVolumeGroup{containerID: "container", process: process}
	manager := &StateVolumeManager{
		groups:           map[string]*stateVolumeGroup{"container": group},
		OnUnexpectedExit: func(_ string, err error) { callback <- err },
	}
	manager.monitorStateVolumeProcess(group)
	close(process.done)
	select {
	case err := <-callback:
		if !strings.Contains(err.Error(), "qsd died") {
			t.Fatalf("unexpected exit callback: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("QSD exit did not trigger a fence callback")
	}
	if _, err := manager.Pivot(context.Background(), "container", "operation"); !errors.Is(err, ErrStateVolumeQSDExited) {
		t.Fatalf("failed QSD group still accepted operations: %v", err)
	}
}

type staticStateVolumeImageTool struct{ size int64 }

func (s staticStateVolumeImageTool) Create(context.Context, string, int64, string) error { return nil }
func (s staticStateVolumeImageTool) Rebase(context.Context, string, string) error        { return nil }
func (s staticStateVolumeImageTool) Check(_ context.Context, path string) error {
	_, err := os.Stat(path)
	return err
}
func (s staticStateVolumeImageTool) Flatten(context.Context, string, string) error { return nil }
func (s staticStateVolumeImageTool) Info(context.Context, string) (StateVolumeImageInfo, error) {
	return StateVolumeImageInfo{
		Format: "qcow2", VirtualSizeBytes: s.size, ClusterSizeBytes: StateVolumeClusterSize, Compat: "1.1",
	}, nil
}

func TestBlockV1GraphCacheRebuildsStructurallyValidByteCorruption(t *testing.T) {
	root := t.TempDir()
	pristine := filepath.Join(root, "pristine.qcow2")
	original := []byte("structurally-valid-qcow-payload")
	if err := os.WriteFile(pristine, original, 0600); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "graph", "layer.qcow2")
	digest := strings.Repeat("a", 64)
	images := staticStateVolumeImageTool{size: 1024}
	if err := materializeBlockV1GraphLayer(context.Background(), pristine, destination, "", 1024, digest, images); err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), original...)
	corrupt[len(corrupt)-1] ^= 1
	if err := os.WriteFile(destination, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	if err := materializeBlockV1GraphLayer(context.Background(), pristine, destination, "", 1024, digest, images); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rebuilt, original) {
		t.Fatalf("corrupt graph cache was reused: %q", rebuilt)
	}
}

func testBlockManifest(volumeID, generationID, parentID string, depth int) BlockV1Manifest {
	return BlockV1Manifest{
		Version: BlockV1Format, Format: "qcow2", VolumeID: volumeID, GenerationID: generationID,
		Generation:         int64(depth),
		ParentGenerationID: parentID, VirtualSizeBytes: 1, LayerFileSizeBytes: 1,
		QCOW2ClusterSize: StateVolumeClusterSize, QCOW2Compat: "1.1", QCOW2LazyRefcounts: false,
		ChunkSizeBytes: BlockV1ChunkSize, Depth: depth,
	}
}

type mapManifestResolver struct{ manifests map[string]BlockV1Manifest }

func (r mapManifestResolver) ResolveBlockV1Manifest(_ context.Context, generationID string) (BlockV1Manifest, error) {
	manifest, ok := r.manifests[generationID]
	if !ok {
		return BlockV1Manifest{}, os.ErrNotExist
	}
	return manifest, nil
}

type fakeStateVolumeImages struct{ virtualSize int64 }

type observingStateVolumeImages struct {
	fakeStateVolumeImages
	beforeCreate func(path string) error
}

func (i observingStateVolumeImages) Create(ctx context.Context, path string, virtualSize int64, backingPath string) error {
	if i.beforeCreate != nil {
		if err := i.beforeCreate(path); err != nil {
			return err
		}
	}
	return i.fakeStateVolumeImages.Create(ctx, path, virtualSize, backingPath)
}

type failingImmutableCheckImages struct {
	fakeStateVolumeImages
	failPath string
}

func (i failingImmutableCheckImages) Check(ctx context.Context, path string) error {
	if path == i.failPath {
		return errors.New("injected qcow2 refcount corruption")
	}
	return i.fakeStateVolumeImages.Check(ctx, path)
}

func (fakeStateVolumeImages) Create(_ context.Context, path string, virtualSize int64, backingPath string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(fmt.Sprintf("QFI:%d:%s", virtualSize, backingPath)), 0600)
}
func (fakeStateVolumeImages) Rebase(_ context.Context, path, backingPath string) error {
	info, err := fakeStateVolumeImageInfo(path)
	if err != nil {
		return err
	}
	return os.WriteFile(path, []byte(fmt.Sprintf("QFI:%d:%s", info.VirtualSizeBytes, backingPath)), 0600)
}
func (fakeStateVolumeImages) Check(_ context.Context, path string) error {
	_, err := os.Stat(path)
	return err
}
func (fakeStateVolumeImages) Flatten(_ context.Context, sourcePath, destinationPath string) error {
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}
	return os.WriteFile(destinationPath, data, 0600)
}

func (f fakeStateVolumeImages) Info(_ context.Context, path string) (StateVolumeImageInfo, error) {
	info, err := fakeStateVolumeImageInfo(path)
	if err == nil {
		return info, nil
	}
	if f.virtualSize <= 0 {
		return StateVolumeImageInfo{}, err
	}
	return StateVolumeImageInfo{
		Format: "qcow2", VirtualSizeBytes: f.virtualSize, ClusterSizeBytes: StateVolumeClusterSize,
		Compat: "1.1",
	}, nil
}

func fakeStateVolumeImageInfo(path string) (StateVolumeImageInfo, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return StateVolumeImageInfo{}, err
	}
	parts := strings.SplitN(string(data), ":", 3)
	if len(parts) != 3 || parts[0] != "QFI" {
		return StateVolumeImageInfo{}, fmt.Errorf("invalid fake qcow2 metadata")
	}
	virtualSize, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return StateVolumeImageInfo{}, err
	}
	backingFormat := ""
	if parts[2] != "" {
		backingFormat = "qcow2"
	}
	return StateVolumeImageInfo{
		Format: "qcow2", VirtualSizeBytes: virtualSize, ClusterSizeBytes: StateVolumeClusterSize,
		Compat: "1.1", BackingPath: parts[2], BackingFormat: backingFormat,
	}, nil
}

type fakeStateVolumeMounts struct {
	mu             sync.Mutex
	events         []string
	thawFailures   int
	unmountFailure error
}

type phaseFailingStateVolumeSecurePaths struct {
	stateVolumeSecurePathOps
	mu        sync.Mutex
	phase     string
	remaining int
}

func (p *phaseFailingStateVolumeSecurePaths) AtomicReplaceRegular(path string, data []byte, perm os.FileMode) error {
	p.mu.Lock()
	shouldFail := p.remaining > 0 && bytes.Contains(data, []byte(`"phase": "`+p.phase+`"`))
	if shouldFail {
		p.remaining--
	}
	p.mu.Unlock()
	if shouldFail {
		return fmt.Errorf("injected %s journal write failure", p.phase)
	}
	return p.stateVolumeSecurePathOps.AtomicReplaceRegular(path, data, perm)
}

func (m *fakeStateVolumeMounts) event(name, path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, name+":"+filepath.Base(path))
}
func (m *fakeStateVolumeMounts) Format(_ context.Context, path string) error {
	m.event("format", path)
	return nil
}
func (m *fakeStateVolumeMounts) Mount(_ context.Context, _, path string, _ bool) error {
	m.event("mount", path)
	return nil
}
func (m *fakeStateVolumeMounts) Sync(_ context.Context, path string) error {
	m.event("sync", path)
	return nil
}
func (m *fakeStateVolumeMounts) Freeze(_ context.Context, path string) error {
	m.event("freeze", path)
	return nil
}
func (m *fakeStateVolumeMounts) Thaw(_ context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, "thaw:"+filepath.Base(path))
	if m.thawFailures > 0 {
		m.thawFailures--
		return fmt.Errorf("injected thaw failure")
	}
	return nil
}
func (m *fakeStateVolumeMounts) Unmount(_ context.Context, path string) error {
	m.event("unmount", path)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.unmountFailure
}

type fakeStateVolumeConnector struct {
	mu          sync.Mutex
	disconnects int
}

func (*fakeStateVolumeConnector) Connect(context.Context, string, string, string) error { return nil }
func (c *fakeStateVolumeConnector) Disconnect(context.Context, string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.disconnects++
	return nil
}

type ambiguousAttachStateVolumeKernel struct {
	fakeStateVolumeNBDKernel
	mu                  sync.Mutex
	busy                bool
	contender           *StateVolumeNBDAllocator
	flockHeldDuringWait bool
}

func (k *ambiguousAttachStateVolumeKernel) setBusy(busy bool) {
	k.mu.Lock()
	k.busy = busy
	k.mu.Unlock()
}

func (k *ambiguousAttachStateVolumeKernel) WaitConnected(context.Context, string, int64) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.busy {
		return errors.New("NBD is not connected")
	}
	return nil
}

func (k *ambiguousAttachStateVolumeKernel) WaitDisconnected(context.Context, string) error {
	if k.contender != nil {
		lease, err := k.contender.Acquire()
		k.mu.Lock()
		k.flockHeldDuringWait = errors.Is(err, ErrStateVolumeNBDUnavailable)
		k.mu.Unlock()
		if lease != nil {
			_ = lease.Release()
		}
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.busy {
		return errors.New("NBD kernel attachment remains active")
	}
	return nil
}

type ambiguousAttachStateVolumeConnector struct {
	kernel            *ambiguousAttachStateVolumeKernel
	connectErr        error
	disconnectErr     error
	clearOnDisconnect bool
	mu                sync.Mutex
	disconnects       int
}

func (c *ambiguousAttachStateVolumeConnector) Connect(context.Context, string, string, string) error {
	c.kernel.setBusy(true)
	return c.connectErr
}

func (c *ambiguousAttachStateVolumeConnector) Disconnect(context.Context, string) error {
	c.mu.Lock()
	c.disconnects++
	clear := c.clearOnDisconnect
	err := c.disconnectErr
	c.mu.Unlock()
	if clear {
		c.kernel.setBusy(false)
	}
	return err
}

func TestStartConnectErrorAfterKernelAttachUsesKernelDetachPostconditionUnderLease(t *testing.T) {
	for _, test := range []struct {
		name          string
		disconnectErr error
	}{
		{name: "disconnect command succeeds"},
		{name: "disconnect reply fails after kernel clear", disconnectErr: errors.New("lost disconnect reply")},
	} {
		t.Run(test.name, func(t *testing.T) {
			allocator, sysRoot, devRoot, mountInfo := setupTestNBD(t, 1)
			kernel := &ambiguousAttachStateVolumeKernel{}
			allocator.Kernel = kernel
			kernel.contender = &StateVolumeNBDAllocator{
				SysBlockRoot: sysRoot, DevRoot: devRoot, LockRoot: allocator.LockRoot,
				MountInfoPath: mountInfo, Kernel: kernel,
			}
			connector := &ambiguousAttachStateVolumeConnector{
				kernel: kernel, connectErr: errors.New("lost connect reply after kernel attach"),
				disconnectErr: test.disconnectErr, clearOnDisconnect: true,
			}
			root := t.TempDir()
			qmp := &fakeStateVolumeQMP{}
			manager := &StateVolumeManager{
				StateRoot: root, RuntimeRoot: filepath.Join(root, "runtime"),
				Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
				NBD:      allocator, Connector: connector, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
				QMPDialer: fakeStateVolumeQMPDialer{qmp: qmp}, Launcher: &inheritedFDLauncher{},
			}
			_, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: "ambiguous-connect", Volumes: []StateVolumeSpec{{
				ID: "root", Name: "root", ContainerMountPath: "/", Root: true,
				BackingDir: filepath.Join(root, "graph"), MountPath: filepath.Join(root, "mount"),
				SizeBytes: 1024, Format: true,
				AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
			}}})
			if err == nil || !strings.Contains(err.Error(), "lost connect reply") {
				t.Fatalf("expected indeterminate Connect reply failure, got %v", err)
			}
			connector.mu.Lock()
			disconnects := connector.disconnects
			connector.mu.Unlock()
			kernel.mu.Lock()
			busy, lockHeld := kernel.busy, kernel.flockHeldDuringWait
			kernel.mu.Unlock()
			if disconnects != 1 || busy || !lockHeld {
				t.Fatalf("kernel-authoritative cleanup failed: disconnects=%d busy=%v lock-held=%v", disconnects, busy, lockHeld)
			}
			lease, err := kernel.contender.Acquire()
			if err != nil {
				t.Fatalf("NBD flock was not released after the clear kernel postcondition: %v", err)
			}
			_ = lease.Release()
			journals, err := manager.Journals.List()
			if err != nil || len(journals) != 0 {
				t.Fatalf("failed Start left a journal after proven detach: journals=%+v err=%v", journals, err)
			}
			qmp.mu.Lock()
			quits := qmp.quits
			qmp.mu.Unlock()
			if quits != 1 {
				t.Fatalf("QSD was not stopped after exact detach proof: quits=%d", quits)
			}
		})
	}
}

func TestStartConnectErrorRetainsLeaseJournalAndQSDWhenKernelDetachIsAmbiguous(t *testing.T) {
	allocator, sysRoot, devRoot, mountInfo := setupTestNBD(t, 1)
	kernel := &ambiguousAttachStateVolumeKernel{}
	allocator.Kernel = kernel
	kernel.contender = &StateVolumeNBDAllocator{
		SysBlockRoot: sysRoot, DevRoot: devRoot, LockRoot: allocator.LockRoot,
		MountInfoPath: mountInfo, Kernel: kernel,
	}
	connector := &ambiguousAttachStateVolumeConnector{
		kernel: kernel, connectErr: errors.New("lost connect reply after kernel attach"),
		disconnectErr: errors.New("disconnect failed"), clearOnDisconnect: false,
	}
	root := t.TempDir()
	qmp := &fakeStateVolumeQMP{}
	manager := &StateVolumeManager{
		StateRoot: root, RuntimeRoot: filepath.Join(root, "runtime"),
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		NBD:      allocator, Connector: connector, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: qmp}, Launcher: &inheritedFDLauncher{},
	}
	_, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: "ambiguous-connect-retained", Volumes: []StateVolumeSpec{{
		ID: "root", Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "graph"), MountPath: filepath.Join(root, "mount"),
		SizeBytes: 1024, Format: true,
		AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}})
	if err == nil || !strings.Contains(err.Error(), "NBD kernel attachment remains active") {
		t.Fatalf("ambiguous kernel attachment was not surfaced: %v", err)
	}
	if _, err := kernel.contender.Acquire(); !errors.Is(err, ErrStateVolumeNBDUnavailable) {
		t.Fatalf("ambiguous NBD flock became available: %v", err)
	}
	journals, journalErr := manager.Journals.List()
	if journalErr != nil || len(journals) != 1 {
		t.Fatalf("ambiguous cleanup did not retain its journal: journals=%+v err=%v", journals, journalErr)
	}
	qmp.mu.Lock()
	quits := qmp.quits
	qmp.mu.Unlock()
	if quits != 0 {
		t.Fatalf("QSD stopped before kernel detach proof: quits=%d", quits)
	}

	connector.mu.Lock()
	connector.clearOnDisconnect = true
	connector.disconnectErr = nil
	connector.mu.Unlock()
	if err := manager.Stop(context.Background(), "ambiguous-connect-retained"); err != nil {
		t.Fatalf("exact teardown retry failed: %v", err)
	}
	lease, err := kernel.contender.Acquire()
	if err != nil {
		t.Fatalf("NBD flock was not released after retry proved detach: %v", err)
	}
	_ = lease.Release()
}

type fakeStateVolumeQMP struct {
	mu                          sync.Mutex
	actions                     [][]StateVolumeSnapshotAction
	nodes                       map[string]struct{}
	graph                       StateVolumeQMPSnapshotGraph
	blockJobs                   map[string]StateVolumeQMPBlockJob
	blockStreamAutoPending      bool
	blockStreamFinalizeFlatNode string
	blockStreamFinalizeError    error
	blockStreamStarts           int
	blockStreamFinalizes        int
	blockStreamDismisses        int
	blockStreamCancels          int
	blockJobQueryStarted        chan struct{}
	blockJobQueryRelease        chan struct{}
	blockJobQueryOnce           sync.Once
	err                         error
	quits                       int
}

func snapshotTestGraph(rootNode, childNode, exportName string, otherNodes ...string) StateVolumeQMPSnapshotGraph {
	nodes := map[string]StateVolumeQMPNode{
		rootNode:  {Name: rootNode, ChildNode: childNode, Driver: "raw"},
		childNode: {Name: childNode, Driver: "qcow2"},
	}
	for _, node := range otherNodes {
		nodes[node] = StateVolumeQMPNode{Name: node, Driver: "qcow2"}
	}
	return StateVolumeQMPSnapshotGraph{
		Nodes: nodes,
		Exports: map[string]StateVolumeQMPExport{
			exportName: {ID: exportName, NodeName: rootNode},
		},
	}
}

func (q *fakeStateVolumeQMP) ProbeSnapshotSupport(context.Context) error { return nil }
func (q *fakeStateVolumeQMP) TransactionSnapshot(_ context.Context, actions []StateVolumeSnapshotAction) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.actions = append(q.actions, append([]StateVolumeSnapshotAction(nil), actions...))
	return q.err
}
func (q *fakeStateVolumeQMP) QueryNodeNames(context.Context) (map[string]struct{}, error) {
	return q.nodes, nil
}
func (q *fakeStateVolumeQMP) QuerySnapshotGraph(context.Context) (StateVolumeQMPSnapshotGraph, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.graph, nil
}
func (q *fakeStateVolumeQMP) VerifyStateVolumeRuntimeGraph(context.Context, *stateVolumeRuntime) error {
	return nil
}
func (q *fakeStateVolumeQMP) StartBlockStream(_ context.Context, nodeName, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return q.err
	}
	if q.blockJobs == nil {
		q.blockJobs = make(map[string]StateVolumeQMPBlockJob)
	}
	q.blockStreamStarts++
	q.blockJobs[jobID] = StateVolumeQMPBlockJob{ID: jobID, Status: "running"}
	return nil
}
func (q *fakeStateVolumeQMP) QueryBlockJob(ctx context.Context, jobID string) (*StateVolumeQMPBlockJob, error) {
	q.mu.Lock()
	if q.err != nil {
		q.mu.Unlock()
		return nil, q.err
	}
	job, ok := q.blockJobs[jobID]
	started, release := q.blockJobQueryStarted, q.blockJobQueryRelease
	q.mu.Unlock()
	if !ok {
		return nil, nil
	}
	if started != nil {
		q.blockJobQueryOnce.Do(func() { close(started) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	job = q.blockJobs[jobID]
	if q.blockStreamAutoPending && job.Status == "running" {
		job.Status = "pending"
		q.blockJobs[jobID] = job
	}
	copy := job
	return &copy, nil
}
func (q *fakeStateVolumeQMP) FinalizeBlockJob(_ context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return q.err
	}
	q.blockStreamFinalizes++
	job := q.blockJobs[jobID]
	job.ID = jobID
	job.Status = "concluded"
	q.blockJobs[jobID] = job
	if nodeName := q.blockStreamFinalizeFlatNode; nodeName != "" {
		node := q.graph.Nodes[nodeName]
		node.BackingFilePath = ""
		node.BackingFileDepth = 0
		q.graph.Nodes[nodeName] = node
	}
	return q.blockStreamFinalizeError
}
func (q *fakeStateVolumeQMP) DismissBlockJob(_ context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return q.err
	}
	q.blockStreamDismisses++
	delete(q.blockJobs, jobID)
	return nil
}
func (q *fakeStateVolumeQMP) CancelBlockJob(_ context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.blockStreamCancels++
	if q.err != nil {
		return q.err
	}
	delete(q.blockJobs, jobID)
	return nil
}
func (q *fakeStateVolumeQMP) Quit(context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.quits++
	return nil
}
func (q *fakeStateVolumeQMP) Close() error { return nil }

type fakeStateVolumeQMPDialer struct{ qmp StateVolumeQMP }

func (d fakeStateVolumeQMPDialer) Dial(context.Context, string) (StateVolumeQMP, error) {
	return d.qmp, nil
}

type fakeStateVolumeProcess struct{ pid int }

func (p fakeStateVolumeProcess) PID() int                   { return p.pid }
func (p fakeStateVolumeProcess) Wait(context.Context) error { return nil }
func (p fakeStateVolumeProcess) Kill() error                { return nil }
func (p fakeStateVolumeProcess) ExpectedStateVolumeProcessIdentity() (string, uint64) {
	return "fake-state-volume-qsd", uint64(p.pid)
}

type deathProofStateVolumeProcess struct {
	mu          sync.Mutex
	pid         int
	alive       bool
	clearOnKill bool
	waits       int
	kills       int
}

func (p *deathProofStateVolumeProcess) PID() int { return p.pid }
func (p *deathProofStateVolumeProcess) Wait(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.waits++
	if p.alive {
		return errors.New("QSD wait outcome is indeterminate")
	}
	return errors.New("QSD exited after SIGKILL")
}
func (p *deathProofStateVolumeProcess) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.kills++
	if p.clearOnKill {
		p.alive = false
	}
	return nil
}
func (p *deathProofStateVolumeProcess) ExpectedStateVolumeProcessIdentity() (string, uint64) {
	return "/usr/bin/qemu-storage-daemon", 77
}

func TestStopGroupSIGKILLRequiresSecondWaitAndExactProcessDeathProof(t *testing.T) {
	for _, test := range []struct {
		name             string
		clearOnKill      bool
		wantErrSubstring string
	}{
		{name: "kernel identity clears after kill", clearOnKill: true},
		{name: "kernel identity remains after kill", wantErrSubstring: "is still alive after SIGKILL"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			process := &deathProofStateVolumeProcess{pid: 4242, alive: true, clearOnKill: test.clearOnKill}
			identity := func(pid int) (string, uint64, error) {
				process.mu.Lock()
				defer process.mu.Unlock()
				if pid != process.pid || !process.alive {
					return "", 0, &os.PathError{Op: "readlink", Path: fmt.Sprintf("/proc/%d/exe", pid), Err: os.ErrNotExist}
				}
				return "/usr/bin/qemu-storage-daemon", 77, nil
			}
			qmp := &fakeStateVolumeQMP{}
			manager := &StateVolumeManager{
				Journals:        StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
				ProcessIdentity: identity,
			}
			group := &stateVolumeGroup{
				containerID: "death-proof", process: process, qmp: qmp,
				qmpSocket: filepath.Join(root, "runtime", "qmp.sock"),
				nbdSocket: filepath.Join(root, "runtime", "nbd.sock"),
				volumes: []*stateVolumeRuntime{{
					spec: StateVolumeSpec{
						ID: "root", Name: "root", Root: true, ContainerMountPath: "/",
						BackingDir: filepath.Join(root, "graph"), MountPath: filepath.Join(root, "mount"),
						SizeBytes: 1024, ActiveLayerPath: filepath.Join(root, "graph", "active.qcow2"),
					},
					devicePath: "/dev/nbd0", exportName: "export-root", fileNode: "file-root",
					rootNode: "root-root", activeNode: "active-root",
				}},
			}
			if err := manager.saveGroupJournal(group, "running", ""); err != nil {
				t.Fatal(err)
			}
			err := manager.stopGroup(context.Background(), group, true)
			process.mu.Lock()
			waits, kills := process.waits, process.kills
			process.mu.Unlock()
			if waits != 2 || kills != 1 {
				t.Fatalf("QSD shutdown did not wait both before and after SIGKILL: waits=%d kills=%d", waits, kills)
			}
			journals, journalErr := manager.Journals.List()
			if journalErr != nil {
				t.Fatal(journalErr)
			}
			if test.wantErrSubstring == "" {
				if err != nil || len(journals) != 0 {
					t.Fatalf("proven process death did not complete teardown: err=%v journals=%+v", err, journals)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErrSubstring) || len(journals) != 1 {
				t.Fatalf("ambiguous process death did not retain recovery state: err=%v journals=%+v", err, journals)
			}

			process.mu.Lock()
			process.clearOnKill = true
			process.mu.Unlock()
			if err := manager.stopGroup(context.Background(), group, true); err != nil {
				t.Fatalf("exact shutdown retry failed: %v", err)
			}
			journals, err = manager.Journals.List()
			if err != nil || len(journals) != 0 {
				t.Fatalf("exact shutdown retry did not retire journal: journals=%+v err=%v", journals, err)
			}
		})
	}
}

type inheritedFDLauncher struct {
	mu             sync.Mutex
	args           []string
	verifiedSocket string
}

type blockingInheritedFDLauncher struct {
	delegate inheritedFDLauncher
	started  chan struct{}
	release  chan struct{}
	mu       sync.Mutex
	starts   int
}

func (l *blockingInheritedFDLauncher) Start(args []string, extraFiles []*os.File, logPath string) (StateVolumeProcess, error) {
	l.mu.Lock()
	l.starts++
	l.mu.Unlock()
	select {
	case l.started <- struct{}{}:
	default:
	}
	<-l.release
	return l.delegate.Start(args, extraFiles, logPath)
}

func (l *blockingInheritedFDLauncher) VerifyStateVolumeNBDSocket(path string) error {
	return l.delegate.VerifyStateVolumeNBDSocket(path)
}

func (l *inheritedFDLauncher) Start(args []string, extraFiles []*os.File, _ string) (StateVolumeProcess, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.args = append([]string(nil), args...)
	if len(extraFiles) != 0 {
		return nil, fmt.Errorf("Unix NBD QSD must not inherit listener file descriptors")
	}
	return fakeStateVolumeProcess{pid: 99}, nil
}

func (l *inheritedFDLauncher) VerifyStateVolumeNBDSocket(path string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.verifiedSocket = path
	return nil
}

func TestStateVolumeManagerOwnerPrivateUnixSocketAndAtomicMultiVolumePivot(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 2)
	root := t.TempDir()
	mounts := &fakeStateVolumeMounts{}
	qmp := &fakeStateVolumeQMP{}
	launcher := &inheritedFDLauncher{}
	connector := &fakeStateVolumeConnector{}
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		NBD: allocator, Connector: connector, Images: fakeStateVolumeImages{}, Mounts: mounts,
		QMPDialer: fakeStateVolumeQMPDialer{qmp: qmp}, Launcher: launcher,
	}
	spec := StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{
		{ID: "root", Name: "root", ContainerMountPath: "/", Root: true, BackingDir: filepath.Join(root, "backing-root"), MountPath: filepath.Join(root, "mount-root"), SizeBytes: 1024, Format: true, AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1},
		{ID: "data", Name: "data", ContainerMountPath: "/data", BackingDir: filepath.Join(root, "backing-data"), MountPath: filepath.Join(root, "mount-data"), SizeBytes: 1024, Format: true, AttachmentToken: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", FencingToken: 2},
	}}
	if _, err := manager.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	expectedSocket := filepath.Join(root, "runtime", stateVolumeToken("container-", "container"), "nbd.sock")
	if launcher.verifiedSocket != expectedSocket {
		t.Fatalf("QSD Unix socket verifier got %q, want %q", launcher.verifiedSocket, expectedSocket)
	}
	joinedArgs := strings.Join(launcher.args, " ")
	for _, required := range []string{"addr.type=unix,addr.path=" + expectedSocket, `"driver":"raw"`, "--export"} {
		if !strings.Contains(joinedArgs, required) {
			t.Fatalf("QSD args missing %q: %s", required, joinedArgs)
		}
	}
	mounts.mu.Lock()
	mounts.events = nil
	mounts.thawFailures = 1
	mounts.mu.Unlock()
	receipt, err := manager.Pivot(context.Background(), "container", "operation-1")
	if err == nil || receipt == nil {
		t.Fatalf("expected committed receipt with an injected thaw failure: receipt=%+v err=%v", receipt, err)
	}
	receipt, err = manager.Pivot(context.Background(), "container", "operation-1")
	if err != nil || receipt == nil {
		t.Fatalf("same-operation retry did not force thaw before returning receipt: receipt=%+v err=%v", receipt, err)
	}
	if len(receipt.Generations) != 2 || len(qmp.actions) != 1 || len(qmp.actions[0]) != 2 {
		t.Fatalf("pivot was not one two-volume transaction: receipt=%+v actions=%+v", receipt, qmp.actions)
	}
	for _, action := range qmp.actions[0] {
		if action.Mode != "existing" {
			t.Fatalf("pivot did not use precreated qcow2: %+v", action)
		}
		if _, err := os.Stat(action.NewPath); err != nil {
			t.Fatalf("pivot target missing after commit: %v", err)
		}
	}
	mounts.mu.Lock()
	events := append([]string(nil), mounts.events...)
	mounts.mu.Unlock()
	want := []string{"sync:mount-data", "sync:mount-root", "freeze:mount-data", "freeze:mount-root", "thaw:mount-root", "thaw:mount-data", "thaw:mount-root"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("unexpected freeze fence events\n got: %v\nwant: %v", events, want)
	}
	if err := manager.AcknowledgePending("container", "operation-1"); err != nil {
		t.Fatal(err)
	}
	mounts.mu.Lock()
	mounts.unmountFailure = fmt.Errorf("busy")
	mounts.mu.Unlock()
	if err := manager.Stop(context.Background(), "container"); err == nil {
		t.Fatal("expected injected unmount failure")
	}
	connector.mu.Lock()
	disconnects := connector.disconnects
	connector.mu.Unlock()
	qmp.mu.Lock()
	quits := qmp.quits
	qmp.mu.Unlock()
	if disconnects != 0 || quits != 0 {
		t.Fatalf("unsafe teardown proceeded below mounted ext4: disconnects=%d quits=%d", disconnects, quits)
	}
	mounts.mu.Lock()
	mounts.unmountFailure = nil
	mounts.mu.Unlock()
	if err := manager.Stop(context.Background(), "container"); err != nil {
		t.Fatal(err)
	}
}

func TestPivotJournalFailureNeverResumesFrozenWritersOrExposesUncommittedReceipt(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	mounts := &fakeStateVolumeMounts{}
	qmp := &fakeStateVolumeQMP{}
	secure := &phaseFailingStateVolumeSecurePaths{
		stateVolumeSecurePathOps: newStateVolumeSecurePathOps(), phase: "pivot-frozen",
	}
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), SecurePaths: secure,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals"), SecurePaths: secure},
		NBD:      allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: mounts,
		QMPDialer: fakeStateVolumeQMPDialer{qmp: qmp}, Launcher: &inheritedFDLauncher{},
	}
	_, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
		ID: "root", Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"),
		SizeBytes: 1024, Format: true,
		AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	secure.mu.Lock()
	secure.remaining = 1
	secure.mu.Unlock()
	mounts.mu.Lock()
	mounts.thawFailures = 1
	mounts.mu.Unlock()
	var resumeMu sync.Mutex
	resumeOutcomes := make([]bool, 0, 2)
	hooks := StateVolumePivotHooks{
		Quiesce: func(context.Context) error { return nil },
		Complete: func(_ context.Context, committed bool) error {
			resumeMu.Lock()
			resumeOutcomes = append(resumeOutcomes, committed)
			resumeMu.Unlock()
			return nil
		},
	}
	if receipt, err := manager.PivotWithHooks(context.Background(), "container", "operation", hooks); err == nil || receipt != nil ||
		!strings.Contains(err.Error(), "pivot-frozen journal write failure") || !strings.Contains(err.Error(), "injected thaw failure") {
		t.Fatalf("expected pre-transaction journal+thaw failure without receipt: receipt=%+v err=%v", receipt, err)
	}
	resumeMu.Lock()
	if len(resumeOutcomes) != 0 {
		t.Fatalf("writer resumed before every filesystem thawed: %v", resumeOutcomes)
	}
	resumeMu.Unlock()
	qmp.mu.Lock()
	transactions := len(qmp.actions)
	qmp.mu.Unlock()
	if transactions != 0 {
		t.Fatalf("QMP transaction ran after the pre-transaction journal failed: %d", transactions)
	}
	if _, err := manager.UploadPending(context.Background(), "container", "operation", nil); err == nil || !strings.Contains(err.Error(), "no committed-outcome proof") {
		t.Fatalf("uncommitted active layer was exposed to upload: %v", err)
	}

	receipt, err := manager.PivotWithHooks(context.Background(), "container", "operation", hooks)
	if err != nil || receipt == nil {
		t.Fatalf("same-operation retry did not roll back then execute a real transaction: receipt=%+v err=%v", receipt, err)
	}
	qmp.mu.Lock()
	transactions = len(qmp.actions)
	qmp.mu.Unlock()
	if transactions != 1 {
		t.Fatalf("same-operation retry executed %d QMP transactions, want exactly one", transactions)
	}
	resumeMu.Lock()
	if !reflect.DeepEqual(resumeOutcomes, []bool{false, true}) {
		t.Fatalf("unexpected resume calls across rollback+commit: %v", resumeOutcomes)
	}
	resumeMu.Unlock()
	if err := manager.AcknowledgePending("container", "operation"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Stop(context.Background(), "container"); err != nil {
		t.Fatal(err)
	}
}

func TestCommittedPivotJournalFailureThawsBeforeResumeAndReplaysWithoutSecondTransaction(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	mounts := &fakeStateVolumeMounts{}
	qmp := &fakeStateVolumeQMP{}
	secure := &phaseFailingStateVolumeSecurePaths{
		stateVolumeSecurePathOps: newStateVolumeSecurePathOps(), phase: "pivoted",
	}
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), SecurePaths: secure,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals"), SecurePaths: secure},
		NBD:      allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: mounts,
		QMPDialer: fakeStateVolumeQMPDialer{qmp: qmp}, Launcher: &inheritedFDLauncher{},
	}
	_, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
		ID: "root", Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"),
		SizeBytes: 1024, Format: true,
		AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	secure.mu.Lock()
	secure.remaining = 1
	secure.mu.Unlock()
	resumed := 0
	hooks := StateVolumePivotHooks{
		Quiesce: func(context.Context) error { return nil },
		Resume:  func(context.Context) error { resumed++; return nil },
	}
	receipt, err := manager.PivotWithHooks(context.Background(), "container", "operation", hooks)
	if err == nil || receipt == nil || !strings.Contains(err.Error(), "pivoted journal write failure") {
		t.Fatalf("expected committed receipt with post-transaction journal failure: receipt=%+v err=%v", receipt, err)
	}
	if resumed != 1 {
		t.Fatalf("committed pivot did not thaw then resume exactly once: %d", resumed)
	}
	replayed, err := manager.PivotWithHooks(context.Background(), "container", "operation", hooks)
	if err != nil || replayed == nil {
		t.Fatalf("committed same-operation replay failed: receipt=%+v err=%v", replayed, err)
	}
	qmp.mu.Lock()
	transactions := len(qmp.actions)
	qmp.mu.Unlock()
	if transactions != 1 || resumed != 1 {
		t.Fatalf("committed replay repeated mutation: transactions=%d resumes=%d", transactions, resumed)
	}
	if err := manager.AcknowledgePending("container", "operation"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Stop(context.Background(), "container"); err != nil {
		t.Fatal(err)
	}
}

func TestCommittedTerminalPivotPersistentThawFailureNeverCompletesAsRollback(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	mounts := &fakeStateVolumeMounts{}
	qmp := &fakeStateVolumeQMP{}
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		NBD: allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: mounts,
		QMPDialer: fakeStateVolumeQMPDialer{qmp: qmp}, Launcher: &inheritedFDLauncher{},
	}
	_, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
		ID: "root", Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"),
		SizeBytes: 1024, Format: true,
		AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	mounts.mu.Lock()
	// The direct post-commit thaw, deferred determinate-exit thaw, and first
	// retry all fail. Only the second retry may complete the terminal writer.
	mounts.thawFailures = 3
	mounts.mu.Unlock()
	var completeMu sync.Mutex
	var completeOutcomes []bool
	hooks := StateVolumePivotHooks{
		Quiesce: func(context.Context) error { return nil },
		Complete: func(_ context.Context, committed bool) error {
			completeMu.Lock()
			completeOutcomes = append(completeOutcomes, committed)
			completeMu.Unlock()
			return nil
		},
	}
	receipt, err := manager.PivotWithHooks(context.Background(), "container", "terminal-operation", hooks)
	if err == nil || receipt == nil || !strings.Contains(err.Error(), "injected thaw failure") {
		t.Fatalf("committed terminal pivot did not retain its thaw obligation: receipt=%+v err=%v", receipt, err)
	}
	if replay, err := manager.PivotWithHooks(context.Background(), "container", "terminal-operation", hooks); err == nil || replay == nil {
		t.Fatalf("persistent first thaw retry unexpectedly completed: receipt=%+v err=%v", replay, err)
	}
	completeMu.Lock()
	if len(completeOutcomes) != 0 {
		t.Fatalf("terminal completion ran while a filesystem remained frozen: %v", completeOutcomes)
	}
	completeMu.Unlock()
	replayed, err := manager.PivotWithHooks(context.Background(), "container", "terminal-operation", hooks)
	if err != nil || replayed == nil {
		t.Fatalf("terminal committed replay failed after verified thaw: receipt=%+v err=%v", replayed, err)
	}
	completeMu.Lock()
	outcomes := append([]bool(nil), completeOutcomes...)
	completeMu.Unlock()
	if !reflect.DeepEqual(outcomes, []bool{true}) {
		t.Fatalf("committed terminal pivot completed with the wrong outcome: %v", outcomes)
	}
	qmp.mu.Lock()
	transactions := len(qmp.actions)
	qmp.mu.Unlock()
	if transactions != 1 {
		t.Fatalf("terminal thaw retry repeated the QMP transaction %d times", transactions)
	}
	group, err := manager.group("container")
	if err != nil {
		t.Fatal(err)
	}
	group.mu.Lock()
	resumeRequired, completionRequired := group.resumeRequired, group.terminalCompletionRequired
	group.mu.Unlock()
	if resumeRequired || completionRequired {
		t.Fatalf("successful Complete(true) retained terminal obligations: resume=%v completion=%v", resumeRequired, completionRequired)
	}
	if err := manager.AcknowledgePending("container", "terminal-operation"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Stop(context.Background(), "container"); err != nil {
		t.Fatal(err)
	}
}

func TestDeterminateTerminalRollbackJournalFailureKeepsWritersStoppedUntilNonpublishableIntent(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	mounts := &fakeStateVolumeMounts{}
	qmp := &fakeStateVolumeQMP{}
	secure := &phaseFailingStateVolumeSecurePaths{
		stateVolumeSecurePathOps: newStateVolumeSecurePathOps(), phase: "terminal-rollback-intent", remaining: 1,
	}
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), SecurePaths: secure,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals"), SecurePaths: secure},
		NBD:      allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: mounts,
		QMPDialer: fakeStateVolumeQMPDialer{qmp: qmp}, Launcher: &inheritedFDLauncher{},
	}
	_, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
		ID: "root", Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"),
		SizeBytes: 1024, Format: true,
		AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	var completeMu sync.Mutex
	var completeOutcomes []bool
	hooks := StateVolumePivotHooks{
		Quiesce: func(context.Context) error { return nil },
		Complete: func(_ context.Context, committed bool) error {
			completeMu.Lock()
			completeOutcomes = append(completeOutcomes, committed)
			completeMu.Unlock()
			return nil
		},
	}
	prior, err := manager.PivotWithHooks(context.Background(), "container", "prior-committed-operation", hooks)
	if err != nil || prior == nil {
		t.Fatalf("failed to establish the prior committed operation: receipt=%+v err=%v", prior, err)
	}
	if err := manager.AcknowledgePending("container", "prior-committed-operation"); err != nil {
		t.Fatal(err)
	}
	completeMu.Lock()
	if !reflect.DeepEqual(completeOutcomes, []bool{true}) {
		t.Fatalf("prior operation did not establish committed completion state: %v", completeOutcomes)
	}
	completeMu.Unlock()
	recovery := StateVolumeRecoveryEnvelope{
		StateSnapshotID: "snapshot", RecoveryProofToken: stateVolumeTestRecoveryProofToken,
		OperationID: "terminal-operation", WorkspaceID: "workspace", WorkspaceName: "workspace-name",
		StubID: "stub", StubName: "stub-name", StubType: "pod",
		ImageID: "image", ImageDigest: "sha256:image", RuntimeProfile: "runc",
		Mode: string(StateSnapshotModeTerminal), WorkspaceStorageID: 1,
		WorkspaceStorageBucket: "bucket", WorkspaceStorageRegion: "us-east-1",
	}
	if err := manager.BindSnapshotRecovery("container", recovery); err != nil {
		t.Fatal(err)
	}
	qmp.mu.Lock()
	qmp.err = errors.New("injected determinate QMP rollback")
	qmp.mu.Unlock()
	receipt, err := manager.PivotWithHooks(context.Background(), "container", recovery.OperationID, hooks)
	if err == nil || receipt != nil || !strings.Contains(err.Error(), "injected determinate QMP rollback") ||
		!strings.Contains(err.Error(), "terminal-rollback-intent journal write failure") {
		t.Fatalf("lost rollback-intent write did not fail closed: receipt=%+v err=%v", receipt, err)
	}
	completeMu.Lock()
	if !reflect.DeepEqual(completeOutcomes, []bool{true}) {
		t.Fatalf("terminal writer resumed before a nonpublishable rollback intent was durable: %v", completeOutcomes)
	}
	completeMu.Unlock()
	group, err := manager.group("container")
	if err != nil {
		t.Fatal(err)
	}
	group.mu.Lock()
	frozen, resumeRequired, persistNeeded := group.volumes[0].frozen, group.resumeRequired, group.rollbackIntentPersistNeeded
	group.mu.Unlock()
	if !frozen || !resumeRequired || !persistNeeded {
		t.Fatalf("lost rollback-intent write released the consistency boundary: frozen=%v resume=%v persist=%v", frozen, resumeRequired, persistNeeded)
	}
	journal, err := manager.Journals.Load("container")
	if err != nil {
		t.Fatal(err)
	}
	if journal.Phase != "terminal-quiesced" {
		t.Fatalf("failed rollback-intent write unexpectedly changed the durable phase: %+v", journal)
	}
	if pending, _, err := manager.PendingReceipt("container", recovery.OperationID); err == nil || pending != nil ||
		!strings.Contains(err.Error(), "no committed-outcome proof") {
		t.Fatalf("lost rollback-intent write exposed a publishable receipt: pending=%+v err=%v", pending, err)
	}

	qmp.mu.Lock()
	qmp.err = nil
	qmp.mu.Unlock()
	reconciled, err := manager.ReconcilePendingOperation(context.Background(), "container", recovery.OperationID)
	if err != nil || reconciled != nil {
		t.Fatalf("exact retry did not durably mark, thaw, resume, and retire rollback: receipt=%+v err=%v", reconciled, err)
	}
	completeMu.Lock()
	outcomes := append([]bool(nil), completeOutcomes...)
	completeMu.Unlock()
	if !reflect.DeepEqual(outcomes, []bool{true, false}) {
		t.Fatalf("determinate rollback completed with an unsafe outcome: %v", outcomes)
	}
	journal, err = manager.Journals.Load("container")
	if err != nil {
		t.Fatal(err)
	}
	if journal.Phase != "recovery-bound" || journal.OperationID != recovery.OperationID {
		t.Fatalf("rollback retry did not restore the exact armed pre-pivot obligation: %+v", journal)
	}
	qmp.mu.Lock()
	transactions := len(qmp.actions)
	qmp.mu.Unlock()
	if transactions != 2 {
		t.Fatalf("rollback reconciliation retried the QMP transaction %d times", transactions)
	}
	if err := manager.CancelSnapshotRecovery("container", recovery.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Stop(context.Background(), "container"); err != nil {
		t.Fatal(err)
	}
}

func restoredTestVolumeSpec(root, id, name, mountPath, backingPath, activePath, token string, fencing int64, isRoot bool) StateVolumeSpec {
	return StateVolumeSpec{
		ID: id, Name: name, ContainerMountPath: mountPath, Root: isRoot,
		Generation: 3, LineageSourceGenerationID: "source-generation-" + id,
		SourceVolumeID: "source-volume-" + id, SourceGeneration: 3,
		SourceParentGenerationID: "source-parent-" + id, SourceDepth: 3,
		BackingDir: filepath.Join(root, "volumes", id, "graph"),
		MountPath:  filepath.Join(root, "mounts", id), SizeBytes: 1024,
		ActiveLayerPath: activePath, ActiveBackingPath: backingPath,
		ParentGenerationID: "source-generation-" + id, Depth: 4,
		AttachmentToken: token, FencingToken: fencing, CreateLayer: true,
	}
}

func TestStateVolumeStartJournalFailurePrecedesEveryGroupFilesystemMutation(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	blockedJournalRoot := filepath.Join(root, "journal-root-is-a-file")
	if err := os.WriteFile(blockedJournalRoot, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	backing := filepath.Join(root, "graphs", "volume", "active")
	mount := filepath.Join(root, "mounts", "volume")
	runtimeRoot := filepath.Join(root, "runtime")
	manager := &StateVolumeManager{
		RuntimeRoot: runtimeRoot, StateRoot: root, Journals: StateVolumeJournalStore{RootDir: blockedJournalRoot},
		NBD: allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: &fakeStateVolumeQMP{}}, Launcher: &inheritedFDLauncher{},
	}
	_, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
		ID: "root", Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: backing, MountPath: mount, SizeBytes: 1024, Format: true,
		AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}})
	if err == nil {
		t.Fatal("start unexpectedly survived a durable journal failure")
	}
	for _, path := range []string{backing, mount, runtimeRoot} {
		if _, statErr := os.Lstat(path); !os.IsNotExist(statErr) {
			t.Fatalf("path %q was mutated before the initialization journal became durable: %v", path, statErr)
		}
	}
}

func TestStateVolumeRestoreIntentPrecedesChildrenAndRollsBackPartialPreparation(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 2)
	root := t.TempDir()
	baseImages := fakeStateVolumeImages{}
	cacheRoot := filepath.Join(root, "block-cache")
	rootBacking := filepath.Join(cacheRoot, "root-source.qcow2")
	dataBacking := filepath.Join(cacheRoot, "data-source.qcow2")
	for _, path := range []string{rootBacking, dataBacking} {
		if err := baseImages.Create(context.Background(), path, 1024, ""); err != nil {
			t.Fatal(err)
		}
	}
	rootActive := filepath.Join(root, "volumes", "a-root", "graph", "active", "restore.qcow2")
	dataActive := filepath.Join(root, "volumes", "b-data", "graph", "active", "restore.qcow2")
	store := StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")}
	createCalls := 0
	images := observingStateVolumeImages{fakeStateVolumeImages: baseImages}
	images.beforeCreate = func(_ string) error {
		createCalls++
		journal, err := store.Load("container")
		if err != nil {
			return fmt.Errorf("restore child was created before its journal: %w", err)
		}
		if journal.Phase != "restore-intent" && journal.Phase != "restore-preparing" {
			return fmt.Errorf("restore child observed unsafe journal phase %q", journal.Phase)
		}
		if journal.SourceStateSnapshotID != "snapshot" || len(journal.Volumes) != 2 ||
			!journal.Volumes[0].CreateLayer || !journal.Volumes[1].CreateLayer {
			return fmt.Errorf("restore intent did not bind the complete destination group: %+v", journal)
		}
		if createCalls == 2 {
			return fmt.Errorf("injected corrupt second source child")
		}
		return nil
	}
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), StateRoot: root, Journals: store,
		NBD: allocator, Connector: &fakeStateVolumeConnector{}, Images: images, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: &fakeStateVolumeQMP{}}, Launcher: &inheritedFDLauncher{},
	}
	spec := StateVolumeGroupSpec{ContainerID: "container", SourceStateSnapshotID: "snapshot", Volumes: []StateVolumeSpec{
		restoredTestVolumeSpec(root, "a-root", "root", "/", rootBacking, rootActive, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", 1, true),
		restoredTestVolumeSpec(root, "b-data", "data", "/data", dataBacking, dataActive, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", 2, false),
	}}
	if _, err := manager.Start(context.Background(), spec); err == nil || !strings.Contains(err.Error(), "corrupt second source child") {
		t.Fatalf("partial restore preparation unexpectedly succeeded: %v", err)
	}
	if createCalls != 2 {
		t.Fatalf("expected two prepared-child attempts, got %d", createCalls)
	}
	for _, path := range []string{rootActive, dataActive} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("partial restored child %q survived rollback: %v", path, err)
		}
	}
	if _, err := store.Load("container"); !os.IsNotExist(err) {
		t.Fatalf("determinate rollback left a live restore journal: %v", err)
	}
}

func TestStateVolumeRestorePreparationCrashQuarantinesOnlyCreatedChildren(t *testing.T) {
	for createdCount := 1; createdCount <= 2; createdCount++ {
		t.Run(fmt.Sprintf("after-child-%d", createdCount), func(t *testing.T) {
			allocator, _, _, _ := setupTestNBD(t, 3)
			root := t.TempDir()
			cacheRoot := filepath.Join(root, "block-cache")
			images := fakeStateVolumeImages{}
			rootBacking := filepath.Join(cacheRoot, "root-source.qcow2")
			dataBacking := filepath.Join(cacheRoot, "data-source.qcow2")
			readOnlySource := filepath.Join(cacheRoot, "models-source.qcow2")
			for _, path := range []string{rootBacking, dataBacking, readOnlySource} {
				if err := images.Create(context.Background(), path, 1024, ""); err != nil {
					t.Fatal(err)
				}
			}
			rootActive := filepath.Join(root, "volumes", "a-root", "graph", "active", "restore.qcow2")
			dataActive := filepath.Join(root, "volumes", "b-data", "graph", "active", "restore.qcow2")
			createdPaths := []string{rootActive, dataActive}
			for index := 0; index < createdCount; index++ {
				backing := []string{rootBacking, dataBacking}[index]
				if err := images.Create(context.Background(), createdPaths[index], 1024, backing); err != nil {
					t.Fatal(err)
				}
			}
			store := StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")}
			manager := &StateVolumeManager{RuntimeRoot: filepath.Join(root, "runtime"), StateRoot: root, Journals: store, NBD: allocator, Images: images}
			qmpSocket := filepath.Join(manager.RuntimeRoot, stateVolumeToken("container-", "container"), "qmp.sock")
			journal := StateVolumeJournal{
				ContainerID: "container", SourceStateSnapshotID: "snapshot", QMPSocket: qmpSocket,
				NBDSocket: filepath.Join(filepath.Dir(qmpSocket), "nbd.sock"),
				Phase:     "restore-preparing", Volumes: []StateVolumeJournalVolume{
					restoredTestJournalVolume(root, "a-root", "root", "/", rootBacking, rootActive, true, createdCount >= 1),
					restoredTestJournalVolume(root, "b-data", "data", "/data", dataBacking, dataActive, false, createdCount >= 2),
					{
						ID: "c-models", Name: "models", ContainerMountPath: "/models", ReadOnly: true,
						Generation: 5, CurrentGenerationID: "models-generation", LineageSourceGenerationID: "models-generation",
						SourceVolumeID: "models-volume", SourceGeneration: 5, SourceParentGenerationID: "models-parent", SourceDepth: 5,
						ExportName: "export-models", BackingDir: filepath.Join(root, "containers", "models"), MountPath: filepath.Join(root, "mounts", "models"),
						SizeBytes: 1024, RootNode: "root-models", FileNode: "file-models", ActiveNode: "active-models",
						ActiveLayerPath: readOnlySource, Depth: 5, Prepared: true,
					},
				},
			}
			if err := store.Save(journal); err != nil {
				t.Fatal(err)
			}
			if err := manager.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			for index, path := range createdPaths {
				_, err := os.Lstat(path)
				if index < createdCount && !os.IsNotExist(err) {
					t.Fatalf("created child %q was not quarantined: %v", path, err)
				}
			}
			if _, err := os.Stat(readOnlySource); err != nil {
				t.Fatalf("immutable read-only source was moved during restore recovery: %v", err)
			}
			if _, err := store.Load("container"); !os.IsNotExist(err) {
				t.Fatalf("reconciled restore journal remained active: %v", err)
			}
		})
	}
}

func restoredTestJournalVolume(root, id, name, mountPath, backingPath, activePath string, isRoot, prepared bool) StateVolumeJournalVolume {
	return StateVolumeJournalVolume{
		ID: id, Name: name, ContainerMountPath: mountPath, Root: isRoot, CreateLayer: true, Prepared: prepared,
		Generation: 3, LineageSourceGenerationID: "source-generation-" + id,
		SourceVolumeID: "source-volume-" + id, SourceGeneration: 3, SourceParentGenerationID: "source-parent-" + id, SourceDepth: 3,
		ExportName: "export-" + id, BackingDir: filepath.Join(root, "volumes", id, "graph"), MountPath: filepath.Join(root, "mounts", id),
		SizeBytes: 1024, RootNode: "root-" + id, FileNode: "file-" + id, ActiveNode: "active-" + id,
		ActiveLayerPath: activePath, ActiveBackingPath: backingPath, ParentGenerationID: "source-generation-" + id, Depth: 4,
	}
}

func TestStateVolumeStartReservesContainerAndJournalsIntentBeforeQSD(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := shortStateVolumeTestRoot(t)
	launcher := &blockingInheritedFDLauncher{started: make(chan struct{}, 1), release: make(chan struct{})}
	journalStore := StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")}
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), Journals: journalStore,
		NBD: allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: &fakeStateVolumeQMP{}}, Launcher: launcher,
	}
	rootVolumeID := "b351a81d-da96-4662-a45e-3e707c7fb74c"
	spec := StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
		ID: rootVolumeID, Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"),
		SizeBytes: 1024, Format: true, AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}}
	type startResult struct {
		handle *StateVolumeGroupHandle
		err    error
	}
	result := make(chan startResult, 1)
	go func() {
		handle, err := manager.Start(context.Background(), spec)
		result <- startResult{handle: handle, err: err}
	}()
	select {
	case <-launcher.started:
	case <-time.After(time.Second):
		t.Fatal("first start did not reach QSD launch")
	}
	journal, err := journalStore.Load("container")
	if err != nil {
		t.Fatal(err)
	}
	if journal.Phase != "start-intent" || journal.QSDPID != 0 || len(journal.Volumes) != 1 || journal.Volumes[0].DevicePath == "" {
		t.Fatalf("pre-exec start intent is incomplete: %+v", journal)
	}
	if _, err := manager.Start(context.Background(), spec); !errors.Is(err, ErrStateVolumeGroupExists) {
		t.Fatalf("concurrent duplicate start was not rejected: %v", err)
	}
	launcher.mu.Lock()
	starts := launcher.starts
	launcher.mu.Unlock()
	if starts != 1 {
		t.Fatalf("duplicate start launched %d QSD processes", starts)
	}
	close(launcher.release)
	started := <-result
	if started.err != nil || started.handle == nil {
		t.Fatalf("reserved start did not finish: handle=%+v err=%v", started.handle, started.err)
	}
	if err := manager.Stop(context.Background(), "container"); err != nil {
		t.Fatal(err)
	}
}

func TestIndeterminatePivotCannotUploadUntilGraphReconciles(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	qmp := &fakeStateVolumeQMP{err: ErrStateVolumePivotIndeterminate}
	secure := &phaseFailingStateVolumeSecurePaths{
		stateVolumeSecurePathOps: newStateVolumeSecurePathOps(), phase: "pivot-rollback-intent",
	}
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), SecurePaths: secure,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals"), SecurePaths: secure},
		NBD:      allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: qmp}, Launcher: &inheritedFDLauncher{},
	}
	if _, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
		ID: "root", Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"),
		SizeBytes: 1024, Format: true, AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}}); err != nil {
		t.Fatal(err)
	}
	var completeOutcomes []bool
	if _, err := manager.PivotWithHooks(context.Background(), "container", "operation", StateVolumePivotHooks{
		Quiesce: func(context.Context) error { return nil },
		Complete: func(_ context.Context, committed bool) error {
			completeOutcomes = append(completeOutcomes, committed)
			return nil
		},
	}); !errors.Is(err, ErrStateVolumePivotIndeterminate) {
		t.Fatalf("lost QMP reply did not become indeterminate: %v", err)
	}
	if _, _, err := manager.PendingReceipt("container", "operation"); !errors.Is(err, ErrStateVolumePivotIndeterminate) {
		t.Fatalf("indeterminate receipt escaped pending barrier: %v", err)
	}
	if _, err := manager.UploadPending(context.Background(), "container", "operation", &memoryBlockCAS{}); !errors.Is(err, ErrStateVolumePivotIndeterminate) {
		t.Fatalf("indeterminate layer was uploadable: %v", err)
	}
	qmp.mu.Lock()
	firstAction := qmp.actions[0][0]
	journal, err := manager.Journals.Load("container")
	if err != nil {
		qmp.mu.Unlock()
		t.Fatal(err)
	}
	entry := journal.Volumes[0]
	qmp.graph = snapshotTestGraph(entry.RootNode, firstAction.CurrentNode, entry.ExportName, firstAction.NewNode)
	qmp.graph.Nodes[firstAction.CurrentNode] = StateVolumeQMPNode{
		Name: firstAction.CurrentNode, Driver: "qcow2", FilePath: entry.ActiveLayerPath,
	}
	qmp.err = nil
	qmp.mu.Unlock()
	secure.mu.Lock()
	secure.remaining = 1
	secure.mu.Unlock()
	receipt, err := manager.ReconcilePendingOperation(context.Background(), "container", "operation")
	if err == nil || receipt != nil || !strings.Contains(err.Error(), "pivot-rollback-intent journal write failure") {
		t.Fatalf("resolved rollback escaped before its nonpublishable journal was durable: receipt=%+v err=%v", receipt, err)
	}
	if len(completeOutcomes) != 0 {
		t.Fatalf("resolved rollback resumed writers before its journal fence: %v", completeOutcomes)
	}
	if _, _, err := manager.PendingReceipt("container", "operation"); err == nil || !strings.Contains(err.Error(), "no committed-outcome proof") {
		t.Fatalf("resolved but unpersisted rollback exposed its receipt: %v", err)
	}
	if err := manager.ResumeIndeterminateWriters(context.Background(), "container", "operation"); err == nil ||
		!strings.Contains(err.Error(), "rollback intent is durable") {
		t.Fatalf("resolved but unpersisted rollback allowed writer resume: %v", err)
	}
	receipt, err = manager.ReconcilePendingOperation(context.Background(), "container", "operation")
	if err != nil || receipt != nil {
		t.Fatalf("exact rollback-journal retry did not clear the unsafe receipt: receipt=%+v err=%v", receipt, err)
	}
	if !reflect.DeepEqual(completeOutcomes, []bool{false}) {
		t.Fatalf("resolved rollback completed with an unsafe outcome: %v", completeOutcomes)
	}
	if _, err := os.Stat(firstAction.NewPath); !os.IsNotExist(err) {
		t.Fatalf("rolled-back pivot target survived reconciliation: %v", err)
	}
	receipt, err = manager.Pivot(context.Background(), "container", "operation")
	if err != nil || receipt == nil {
		t.Fatalf("exact operation could not repivot after proven rollback: receipt=%+v err=%v", receipt, err)
	}
}

func TestIndeterminateWriterResumeTaintsOriginalGenerationAgainstLaterPublish(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	qmp := &fakeStateVolumeQMP{err: ErrStateVolumePivotIndeterminate}
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		NBD: allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: qmp}, Launcher: &inheritedFDLauncher{},
	}
	if _, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
		ID: "root", Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"), SizeBytes: 1024, Format: true,
		AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}}); err != nil {
		t.Fatal(err)
	}
	resumes, deletes := 0, 0
	_, err := manager.PivotWithHooks(context.Background(), "container", "operation", StateVolumePivotHooks{
		Quiesce: func(context.Context) error { return nil },
		Complete: func(_ context.Context, committed bool) error {
			if committed {
				deletes++
			} else {
				resumes++
			}
			return nil
		},
	})
	if !errors.Is(err, ErrStateVolumePivotIndeterminate) {
		t.Fatalf("lost transaction reply was not indeterminate: %v", err)
	}
	if _, err := manager.ReconcilePendingOperation(context.Background(), "container", "operation"); err == nil {
		t.Fatal("unauthenticated empty graph resolved an indeterminate pivot")
	}
	if err := manager.ResumeIndeterminateWriters(context.Background(), "container", "operation"); err != nil {
		t.Fatal(err)
	}
	if resumes != 1 || deletes != 0 {
		t.Fatalf("temporary recovery did not resume exactly once: resumes=%d deletes=%d", resumes, deletes)
	}
	journal, err := manager.Journals.Load("container")
	if err != nil {
		t.Fatal(err)
	}
	if journal.Phase != "writers-resumed-indeterminate" {
		t.Fatalf("writer resume taint was not durable before SIGCONT: phase=%q", journal.Phase)
	}
	entry := journal.Volumes[0]
	qmp.mu.Lock()
	qmp.err = nil
	qmp.graph = StateVolumeQMPSnapshotGraph{
		Nodes: map[string]StateVolumeQMPNode{
			entry.RootNode: {Name: entry.RootNode, ChildNode: entry.PivotNode, Driver: "raw"},
			entry.ActiveNode: {
				Name: entry.ActiveNode, Driver: "qcow2", FilePath: entry.ActiveLayerPath,
			},
			entry.PivotNode: {
				Name: entry.PivotNode, Driver: "qcow2", FilePath: entry.PivotLayerPath,
				BackingFilePath: entry.ActiveLayerPath, BackingFileDepth: 1,
			},
		},
		Exports: map[string]StateVolumeQMPExport{
			entry.ExportName: {ID: entry.ExportName, NodeName: entry.RootNode},
		},
	}
	qmp.mu.Unlock()
	reconciled, err := manager.ReconcilePendingOperation(context.Background(), "container", "operation")
	if err == nil || reconciled != nil || !strings.Contains(err.Error(), "permanently tainted") {
		t.Fatalf("post-resume committed graph was incorrectly publishable: receipt=%+v err=%v", reconciled, err)
	}
	if resumes != 1 || deletes != 0 {
		t.Fatalf("tainted original operation executed a terminal delete: resumes=%d deletes=%d", resumes, deletes)
	}
	if _, err := manager.UploadPending(context.Background(), "container", "operation", &memoryBlockCAS{}); !errors.Is(err, ErrStateVolumePivotIndeterminate) {
		t.Fatalf("tainted original generation became uploadable: %v", err)
	}
}

func TestStartupAutonomouslyResumesTerminalPendingAcrossTwoWorkerCrashes(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	mounts := &fakeStateVolumeMounts{}
	qmp := &fakeStateVolumeQMP{}
	connector := &fakeStateVolumeConnector{}
	journalStore := StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")}
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), Journals: journalStore,
		NBD: allocator, Connector: connector, Images: fakeStateVolumeImages{}, Mounts: mounts,
		QMPDialer: fakeStateVolumeQMPDialer{qmp: qmp}, Launcher: &inheritedFDLauncher{},
	}
	rootVolumeID := "b351a81d-da96-4662-a45e-3e707c7fb74c"
	spec := StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
		ID: rootVolumeID, Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"),
		SizeBytes: 1024, Format: true, AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}}
	handle, err := manager.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	recovery := StateVolumeRecoveryEnvelope{
		StateSnapshotID: "snapshot", RecoveryProofToken: stateVolumeTestRecoveryProofToken, OperationID: "operation", WorkspaceID: "workspace", WorkspaceName: "workspace-name",
		StubID: "stub", StubName: "stub-name", StubType: "pod",
		ImageID: "image", ImageDigest: "sha256:image", RuntimeProfile: "runc",
		Mode: string(StateSnapshotModeTerminal), IncludeMemory: true, Visible: false,
		WorkspaceStorageID: 1, WorkspaceStorageExternalID: "storage", WorkspaceStorageBucket: "bucket",
		WorkspaceStorageEndpoint: "http://object-store", WorkspaceStorageRegion: "us-east-1",
	}
	if err := manager.BindSnapshotRecovery("container", recovery); err != nil {
		t.Fatal(err)
	}
	receipt, err := manager.Pivot(context.Background(), "container", "operation")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.SealAndDetachTerminalPending(context.Background(), "container", "operation"); err != nil {
		t.Fatal(err)
	}
	journal, err := journalStore.Load("container")
	if err != nil {
		t.Fatal(err)
	}
	if journal.Phase != "detached-pending" || journal.Volumes[0].DevicePath == "" || journal.QSDPID != 0 {
		t.Fatalf("terminal detach journal lost replay identity: %+v", journal)
	}
	connector.mu.Lock()
	disconnects := connector.disconnects
	connector.mu.Unlock()
	if disconnects != 1 {
		t.Fatalf("terminal detach disconnected %d devices, want 1", disconnects)
	}
	journalPath, err := journalStore.journalPath("container")
	if err != nil {
		t.Fatal(err)
	}
	journalBytes, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(journalBytes, []byte("access-key")) || bytes.Contains(journalBytes, []byte("secret-key")) {
		t.Fatalf("state recovery journal persisted storage credentials: %s", journalBytes)
	}

	newManager := func() *StateVolumeManager {
		return &StateVolumeManager{
			RuntimeRoot: filepath.Join(root, "runtime"), Journals: journalStore,
			NBD: allocator, Connector: connector, Images: fakeStateVolumeImages{}, Mounts: mounts,
			QMPDialer: fakeStateVolumeQMPDialer{qmp: qmp}, Launcher: &inheritedFDLauncher{},
		}
	}
	// First replacement adopts without reconnecting QSD/NBD. A second
	// replacement repeats that adoption to model a crash before upload.
	firstReplacement := newManager()
	if err := firstReplacement.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if receipt, detached, err := firstReplacement.PendingReceipt("container", "operation"); err != nil || receipt == nil || !detached {
		t.Fatalf("first offline adoption failed: receipt=%+v detached=%t err=%v", receipt, detached, err)
	}
	secondReplacement := newManager()
	if err := secondReplacement.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if receipt, detached, err := secondReplacement.PendingReceipt("container", "operation"); err != nil || receipt == nil || !detached {
		t.Fatalf("second offline adoption failed: receipt=%+v detached=%t err=%v", receipt, detached, err)
	}
	plannedMembers := make([]*pb.StateGeneration, 0, len(receipt.Generations))
	for _, generation := range receipt.Generations {
		plannedMembers = append(plannedMembers, &pb.StateGeneration{
			VolumeId: generation.VolumeID, GenerationId: generation.GenerationID,
			ParentGenerationId: generation.ParentGenerationID, CloneParentGenerationId: generation.CloneParentGenerationID,
			Generation: generation.Generation, Name: generation.Name, MountPath: generation.MountPath,
			ReadOnly: generation.ReadOnly, Root: generation.Root,
		})
	}
	pending := &pb.StateSnapshot{
		ExternalId: "snapshot", OperationId: recovery.OperationID, SourceContainerId: "container",
		Status: string(types.StateSnapshotStatusPending), ImageId: recovery.ImageID, ImageDigest: recovery.ImageDigest,
		RuntimeProfile: recovery.RuntimeProfile, RestoreMode: stateRestoreModeCold,
		Mode: recovery.Mode, IncludeMemory: recovery.IncludeMemory, Visible: recovery.Visible,
		SourceStubExternalId: recovery.StubID, SourceStubName: recovery.StubName, SourceStubType: recovery.StubType,
	}
	repository := &offlineStateSnapshotRepository{operation: &pb.GetStateSnapshotResponse{
		Ok: true, Snapshot: pending, WorkspaceId: recovery.WorkspaceID, StubId: recovery.StubID, PlannedMembers: plannedMembers,
	}}
	cas := &memoryBlockCAS{}
	instances := common.NewSafeMap[*ContainerInstance]()
	hold := &terminalStateSnapshotHold{
		operationID: recovery.OperationID, mode: StateSnapshotModeTerminal, includeMemory: recovery.IncludeMemory,
		done: make(chan struct{}), runtimeStopped: true,
	}
	attachmentDone := make(chan struct{})
	close(attachmentDone)
	request := &types.ContainerRequest{
		ContainerId: "container", Workspace: types.Workspace{ExternalId: recovery.WorkspaceID},
		PersistentRoot: &types.PersistentRoot{Size: "1Gi"},
	}
	retained := &ContainerInstance{
		Id: "container", Request: request, StateVolumes: handle, ExitCode: 23,
		terminalStateSnapshot: hold,
		StateFinalCommitError: errors.New("origin was unavailable"),
		StateVolumeAttachments: &stateVolumeAttachmentState{
			leases: []*pb.StateVolumeLease{{
				VolumeId: rootVolumeID, AttachmentToken: spec.Volumes[0].AttachmentToken, FencingToken: spec.Volumes[0].FencingToken,
			}},
			cancel: func() {}, done: attachmentDone,
		},
	}
	instances.Set("container", retained)
	containerRepository := &fakeContainerRepoClient{}
	genericCredentials := &fakeRuntimeCredentialsWorkerRepo{resp: &pb.GetContainerRuntimeCredentialsResponse{
		Ok: true, WorkspaceStorage: &pb.CacheWorkspaceStorageCredentials{
			EndpointUrl: "http://object-store", Region: "us-east-1", BucketName: "bucket",
			AccessKey: "generic-access-must-not-be-used", SecretKey: "generic-secret-must-not-be-used",
		},
	}}
	reporter := &cacheContentReporter{
		ctx: context.Background(), eventRepo: &fakeEventRepo{}, metadata: cache.NewMockCacheMetadataStore(), locality: "test-locality",
		pending: make(map[reporterKey]map[string]types.CacheRequiredContentItem),
		recent:  make(map[reporterStubKey]struct{}), reported: make(map[string]struct{}),
	}
	worker := &Worker{
		workerId:            "replacement-worker",
		machineID:           "storage-node",
		backendRepoClient:   repository,
		containerRepoClient: containerRepository,
		containerInstances:  instances,
		completedRequests:   make(chan *types.ContainerRequest, 1),
		workerRepoClient:    genericCredentials,
		cacheManager:        &WorkerCacheManager{reporter: reporter},
		stateVolumeManager:  secondReplacement,
		stateVolumeCASFactory: func(_ context.Context, request *types.ContainerRequest) (BlockV1CAS, string, error) {
			if request.Workspace.Storage == nil || request.Workspace.Storage.AccessKey == nil || *request.Workspace.Storage.AccessKey != "access" {
				return nil, "", fmt.Errorf("offline recovery did not rehydrate storage credentials")
			}
			return cas, "bucket", nil
		},
	}
	// This is the startup reconciliation stage, not a manually re-delivered
	// snapshot RPC. The replacement worker must finish publication before it
	// can advertise readiness.
	if err := worker.reconcileTerminalStateSnapshotJournals(context.Background()); err != nil {
		t.Fatal(err)
	}
	if genericCredentials.lastReq != nil {
		t.Fatal("offline recovery used the generic workspace credential endpoint")
	}
	if _, err := journalStore.Load("container"); !os.IsNotExist(err) {
		t.Fatalf("completed offline replay retained journal: %v", err)
	}
	if _, exists := instances.Get("container"); exists {
		t.Fatal("successful startup replay retained ContainerInstance")
	}
	select {
	case <-hold.done:
	default:
		t.Fatal("successful startup replay did not close terminal teardown hold")
	}
	if retained.StateVolumeAttachments != nil || retained.StateVolumes != nil || retained.StateFinalCommitError != nil {
		t.Fatalf("successful startup replay retained state resources: %+v", retained)
	}
	if containerRepository.containerExitCodeCalls() != 1 || containerRepository.deleteContainerStateCalls() != 1 {
		t.Fatalf("terminal completion was not published and removed exactly once: exits=%d deletes=%d", containerRepository.containerExitCodeCalls(), containerRepository.deleteContainerStateCalls())
	}
	select {
	case completed := <-worker.completedRequests:
		if completed != request {
			t.Fatalf("wrong completed request: %+v", completed)
		}
	default:
		t.Fatal("successful startup replay did not close the worker completion path")
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if len(repository.commitRequests) != 1 || len(repository.commitRequests[0].Generations) != 1 {
		t.Fatalf("startup did not atomically publish exactly one generation/group: %+v", repository.commitRequests)
	}
	if len(repository.commitRequests[0].Leases) != 0 {
		t.Fatalf("replacement replay leaked or required journaled lease secrets: %+v", repository.commitRequests[0].Leases)
	}
	if len(repository.releaseRequests) != 0 {
		t.Fatalf("terminal cleanup opened a second release path after atomic snapshot Commit: %+v", repository.releaseRequests)
	}
	if repository.commitRequests[0].Snapshot.RestoreMode != stateRestoreModeCold || repository.commitRequests[0].Snapshot.FallbackReason == "" {
		t.Fatalf("lost in-flight memory was not published as a truthful cold fallback: %+v", repository.commitRequests[0].Snapshot)
	}
	cas.mu.Lock()
	defer cas.mu.Unlock()
	if len(cas.objects) < 2 {
		t.Fatalf("offline recovery did not publish chunk(s) and manifest last: objects=%d", len(cas.objects))
	}
}

func TestStateVolumeShutdownBoundaryRequiresVerifiedDetachedReplayJournal(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := shortStateVolumeTestRoot(t)
	manager := &StateVolumeManager{
		RuntimeRoot: filepath.Join(root, "runtime"), Journals: StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")},
		NBD: allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
		QMPDialer: fakeStateVolumeQMPDialer{qmp: &fakeStateVolumeQMP{}}, Launcher: &inheritedFDLauncher{},
	}
	containerID := "shutdown-boundary"
	volumeID := "6270942b-07ac-43dd-91f9-29efecc42736"
	if _, err := manager.Start(context.Background(), StateVolumeGroupSpec{ContainerID: containerID, Volumes: []StateVolumeSpec{{
		ID: volumeID, Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"),
		SizeBytes: 1024, Format: true, AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ShutdownSafeContainers(); err == nil {
		t.Fatal("mounted running group was accepted as a shutdown boundary")
	}
	recovery := StateVolumeRecoveryEnvelope{
		StateSnapshotID: "snapshot", RecoveryProofToken: stateVolumeTestRecoveryProofToken, OperationID: "operation", WorkspaceID: "workspace", WorkspaceName: "workspace",
		StubID: "stub", ImageID: "image", ImageDigest: "sha256:image", RuntimeProfile: "runc",
		Mode: string(StateSnapshotModeTerminal), WorkspaceStorageID: 1, WorkspaceStorageBucket: "bucket", WorkspaceStorageRegion: "us-east-1",
	}
	if err := manager.BindSnapshotRecovery(containerID, recovery); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Pivot(context.Background(), containerID, recovery.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := manager.SealAndDetachTerminalPending(context.Background(), containerID, recovery.OperationID); err != nil {
		t.Fatal(err)
	}
	safe, err := manager.ShutdownSafeContainers()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := safe[containerID]; !ok {
		t.Fatal("verified detached terminal journal was not accepted as a shutdown boundary")
	}
}

func TestStartupFinalizesAvailableTerminalOperationAcrossAckCrashBoundaries(t *testing.T) {
	for _, acknowledgeBeforeCrash := range []bool{false, true} {
		name := "before-local-ack"
		if acknowledgeBeforeCrash {
			name = "after-local-ack"
		}
		t.Run(name, func(t *testing.T) {
			allocator, _, _, _ := setupTestNBD(t, 1)
			root := t.TempDir()
			mounts := &fakeStateVolumeMounts{}
			qmp := &fakeStateVolumeQMP{}
			connector := &fakeStateVolumeConnector{}
			journalStore := StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")}
			newManager := func() *StateVolumeManager {
				return &StateVolumeManager{
					RuntimeRoot: filepath.Join(root, "runtime"), Journals: journalStore,
					NBD: allocator, Connector: connector, Images: fakeStateVolumeImages{}, Mounts: mounts,
					QMPDialer: fakeStateVolumeQMPDialer{qmp: qmp}, Launcher: &inheritedFDLauncher{},
				}
			}
			manager := newManager()
			volumeID := "5f429690-3239-4a53-bc44-7c59054719af"
			spec := StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
				ID: volumeID, Name: "root", ContainerMountPath: "/", Root: true,
				BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"),
				SizeBytes: 1024, Format: true, AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
			}}}
			if _, err := manager.Start(context.Background(), spec); err != nil {
				t.Fatal(err)
			}
			recovery := StateVolumeRecoveryEnvelope{
				StateSnapshotID: "snapshot", RecoveryProofToken: stateVolumeTestRecoveryProofToken, OperationID: "operation", WorkspaceID: "workspace", WorkspaceName: "workspace-name",
				StubID: "stub", ImageID: "image", ImageDigest: "sha256:image", RuntimeProfile: "runc",
				Mode: string(StateSnapshotModeTerminal), WorkspaceStorageID: 1,
				WorkspaceStorageBucket: "bucket", WorkspaceStorageRegion: "us-east-1",
			}
			if err := manager.BindSnapshotRecovery("container", recovery); err != nil {
				t.Fatal(err)
			}
			receipt, err := manager.Pivot(context.Background(), "container", recovery.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.SealAndDetachTerminalPending(context.Background(), "container", recovery.OperationID); err != nil {
				t.Fatal(err)
			}
			if acknowledgeBeforeCrash {
				if err := manager.AcknowledgePending("container", recovery.OperationID); err != nil {
					t.Fatal(err)
				}
				journal, err := journalStore.Load("container")
				if err != nil {
					t.Fatal(err)
				}
				if journal.Phase != "terminal-committed" || journal.Recovery == nil {
					t.Fatalf("post-commit cleanup obligation was not durable across local Ack: %+v", journal)
				}
			}
			replacement := newManager()
			if err := replacement.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			member := receipt.Generations[0]
			manifest := testBlockManifest(member.VolumeID, member.GenerationID, member.ParentGenerationID, max(member.Depth, 1))
			manifest.Generation = member.Generation
			chunk := []byte{'x'}
			chunkHash := sha256.Sum256(chunk)
			chunkDigest := hex.EncodeToString(chunkHash[:])
			manifest.Chunks = []BlockV1Chunk{{Index: 0, OffsetBytes: 0, SizeBytes: 1, Digest: chunkDigest}}
			manifestData, manifestDigest, err := EncodeBlockV1ManifestCanonical(manifest)
			if err != nil {
				t.Fatal(err)
			}
			manifestKey, _ := stateBlockObjectKey(manifestDigest)
			cas := &memoryBlockCAS{objects: map[string][]byte{manifestDigest: manifestData, chunkDigest: chunk}}
			generation := &pb.VolumeGeneration{
				ExternalId: member.GenerationID, VolumeId: member.VolumeID, Generation: member.Generation,
				ParentGenerationId: member.ParentGenerationID, ManifestDigest: manifestDigest, ManifestKey: manifestKey,
				ManifestSizeBytes: int64(len(manifestData)), LogicalSizeBytes: manifest.VirtualSizeBytes,
				ChunkCount: 1, StoredSizeBytes: 1, BucketName: "bucket",
			}
			available := &pb.StateSnapshot{
				ExternalId: "snapshot", OperationId: recovery.OperationID, SourceContainerId: "container",
				Status: string(types.StateSnapshotStatusAvailable), ImageId: recovery.ImageID, ImageDigest: recovery.ImageDigest,
				RuntimeProfile: recovery.RuntimeProfile, RestoreMode: stateRestoreModeCold, Mode: recovery.Mode,
				Generations: []*pb.StateGeneration{{
					VolumeId: member.VolumeID, GenerationId: member.GenerationID, Generation: member.Generation,
					ParentGenerationId: member.ParentGenerationID, Name: member.Name, MountPath: member.MountPath, Root: member.Root,
				}},
			}
			repository := &offlineStateSnapshotRepository{
				operation: &pb.GetStateSnapshotResponse{
					Ok: true, Snapshot: available, WorkspaceId: recovery.WorkspaceID, StubId: recovery.StubID,
				},
				generations: map[string]*pb.VolumeGeneration{member.GenerationID: generation},
			}
			reporter := &cacheContentReporter{
				ctx: context.Background(), eventRepo: &fakeEventRepo{}, metadata: cache.NewMockCacheMetadataStore(), locality: "test-locality",
				pending: make(map[reporterKey]map[string]types.CacheRequiredContentItem),
				recent:  make(map[reporterStubKey]struct{}), reported: make(map[string]struct{}),
			}
			worker := &Worker{
				backendRepoClient: repository, stateVolumeManager: replacement,
				workerId: "replacement-worker", workerInstanceId: "replacement-instance", machineID: "storage-node",
				cacheManager: &WorkerCacheManager{reporter: reporter},
				stateVolumeCASFactory: func(context.Context, *types.ContainerRequest) (BlockV1CAS, string, error) {
					return cas, "bucket", nil
				},
			}
			if err := worker.reconcileTerminalStateSnapshotJournals(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := journalStore.Load("container"); !os.IsNotExist(err) {
				t.Fatalf("available operation retained cleanup journal: %v", err)
			}
			repository.mu.Lock()
			defer repository.mu.Unlock()
			if len(repository.commitRequests) != 0 {
				t.Fatalf("available operation was republished: commits=%d", len(repository.commitRequests))
			}
			cas.mu.Lock()
			if len(cas.objects) != 2 {
				t.Fatalf("available finalization unexpectedly rewrote block CAS: objects=%d", len(cas.objects))
			}
			cas.mu.Unlock()
		})
	}
}

func TestStartupReplaysRequiredContentAfterCommitBeforeReportCrash(t *testing.T) {
	allocator, _, _, _ := setupTestNBD(t, 1)
	root := t.TempDir()
	journalStore := StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")}
	qmp := &fakeStateVolumeQMP{}
	newManager := func() *StateVolumeManager {
		return &StateVolumeManager{
			RuntimeRoot: filepath.Join(root, "runtime"), Journals: journalStore,
			NBD: allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
			QMPDialer: fakeStateVolumeQMPDialer{qmp: qmp}, Launcher: &inheritedFDLauncher{},
		}
	}
	manager := newManager()
	volumeID := "79f2ade3-40c0-450a-96ea-acbf64bd202d"
	spec := StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{{
		ID: volumeID, Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(root, "backing"), MountPath: filepath.Join(root, "mount"),
		SizeBytes: 1, Format: true, AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1,
	}}}
	if _, err := manager.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	recovery := StateVolumeRecoveryEnvelope{
		StateSnapshotID: "snapshot", RecoveryProofToken: stateVolumeTestRecoveryProofToken, OperationID: "operation", WorkspaceID: "workspace", WorkspaceName: "workspace-name",
		StubID: "stub", StubName: "stub-name", StubType: "pod", ImageID: "image", ImageDigest: "sha256:image",
		RuntimeProfile: "runc", Mode: string(StateSnapshotModeTerminal), WorkspaceStorageID: 1,
		WorkspaceStorageBucket: "bucket", WorkspaceStorageRegion: "us-east-1",
	}
	if err := manager.BindSnapshotRecovery("container", recovery); err != nil {
		t.Fatal(err)
	}
	receipt, err := manager.Pivot(context.Background(), "container", recovery.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.SealAndDetachTerminalPending(context.Background(), "container", recovery.OperationID); err != nil {
		t.Fatal(err)
	}
	member := receipt.Generations[0]
	manifest := testBlockManifest(member.VolumeID, member.GenerationID, member.ParentGenerationID, max(member.Depth, 1))
	manifest.Generation = member.Generation
	chunkData := []byte{'x'}
	chunkDigest := sha256.Sum256(chunkData)
	manifest.Chunks = []BlockV1Chunk{{Index: 0, OffsetBytes: 0, SizeBytes: 1, Digest: hex.EncodeToString(chunkDigest[:])}}
	cas := &memoryBlockCAS{objects: make(map[string][]byte)}
	manifestData, manifestDigest, err := EncodeBlockV1ManifestCanonical(manifest)
	if err != nil {
		t.Fatal(err)
	}
	cas.objects[manifestDigest] = manifestData
	manifestKey, _ := stateBlockObjectKey(manifestDigest)
	generation := &pb.VolumeGeneration{
		ExternalId: member.GenerationID, VolumeId: member.VolumeID, Generation: member.Generation,
		ParentGenerationId: member.ParentGenerationID, ManifestDigest: manifestDigest, ManifestKey: manifestKey,
		ManifestSizeBytes: int64(len(manifestData)), LogicalSizeBytes: manifest.VirtualSizeBytes,
		ChunkCount: 1, StoredSizeBytes: 1, BucketName: "bucket",
	}
	available := &pb.StateSnapshot{
		ExternalId: "snapshot", OperationId: recovery.OperationID, SourceContainerId: "container",
		Status: string(types.StateSnapshotStatusAvailable), ImageId: recovery.ImageID, ImageDigest: recovery.ImageDigest,
		RuntimeProfile: recovery.RuntimeProfile, RestoreMode: stateRestoreModeCold, Mode: recovery.Mode,
		Generations: []*pb.StateGeneration{{
			VolumeId: member.VolumeID, GenerationId: member.GenerationID, Generation: member.Generation,
			ParentGenerationId: member.ParentGenerationID, Name: member.Name, MountPath: member.MountPath, Root: member.Root,
		}},
	}
	repository := &offlineStateSnapshotRepository{
		operation:   &pb.GetStateSnapshotResponse{Ok: true, Snapshot: available, WorkspaceId: recovery.WorkspaceID, StubId: recovery.StubID},
		generations: map[string]*pb.VolumeGeneration{member.GenerationID: generation},
	}
	events := &fakeEventRepo{err: errors.New("injected durable event outage")}
	metadata := &failingRecentMetadataStore{
		CacheMetadataStore: cache.NewMockCacheMetadataStore(),
		err:                errors.New("injected reconciliation index outage"),
	}
	newReporter := func() *cacheContentReporter {
		return &cacheContentReporter{
			ctx: context.Background(), eventRepo: events, metadata: metadata, locality: "test-locality",
			pending: make(map[reporterKey]map[string]types.CacheRequiredContentItem),
			recent:  make(map[reporterStubKey]struct{}), reported: make(map[string]struct{}),
		}
	}
	newWorker := func(manager *StateVolumeManager) *Worker {
		return &Worker{
			backendRepoClient: repository, stateVolumeManager: manager,
			workerId: "worker", workerInstanceId: "worker-instance", machineID: "node",
			cacheManager: &WorkerCacheManager{reporter: newReporter()},
			workerRepoClient: &fakeRuntimeCredentialsWorkerRepo{resp: &pb.GetContainerRuntimeCredentialsResponse{
				Ok: true, WorkspaceStorage: &pb.CacheWorkspaceStorageCredentials{
					Region: "us-east-1", BucketName: "bucket", AccessKey: "access", SecretKey: "secret",
				},
			}},
			stateVolumeCASFactory: func(context.Context, *types.ContainerRequest) (BlockV1CAS, string, error) {
				return cas, "bucket", nil
			},
		}
	}
	if err := newWorker(manager).reconcileTerminalStateSnapshotJournals(context.Background()); err == nil {
		t.Fatal("Commit-before-report outage retired the local finalization obligation")
	}
	if pendingID, pending := manager.PendingOperation("container"); !pending || pendingID != recovery.OperationID {
		t.Fatalf("failed report did not retain exact pending operation: id=%q pending=%t", pendingID, pending)
	}
	if _, err := journalStore.Load("container"); err != nil {
		t.Fatalf("failed report did not retain durable journal: %v", err)
	}
	// The durable event append can recover while Redis discoverability remains
	// unavailable. This is still not an Ack boundary: without the recent-stub
	// index the reconciler cannot find the now-durable stream.
	events.mu.Lock()
	events.err = nil
	events.mu.Unlock()
	if err := newWorker(manager).reconcileTerminalStateSnapshotJournals(context.Background()); err == nil || !strings.Contains(err.Error(), "reconciliation index") {
		t.Fatalf("index outage did not retain the finalization obligation: %v", err)
	}
	if _, err := journalStore.Load("container"); err != nil {
		t.Fatalf("index outage retired durable journal: %v", err)
	}

	// A replacement worker reconstructs the report solely from the available
	// repository row plus authenticated manifests, publishes it, then Ack/Stop
	// removes the local obligation. It must not reupload or recommit state.
	metadata.mu.Lock()
	metadata.err = nil
	metadata.mu.Unlock()
	replacement := newManager()
	if err := replacement.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := newWorker(replacement).reconcileTerminalStateSnapshotJournals(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := journalStore.Load("container"); !os.IsNotExist(err) {
		t.Fatalf("replacement retained reported journal: %v", err)
	}
	events.mu.Lock()
	pushed := append([]types.EventStubCacheRequiredContentSchema(nil), events.pushed...)
	events.mu.Unlock()
	if len(pushed) != 4 {
		t.Fatalf("durable index retry+replacement published %d cache records, want two idempotent part+commit revisions", len(pushed))
	}
	parts, commits := 0, 0
	for _, event := range pushed {
		if event.WorkspaceID != recovery.WorkspaceID || event.StubID != recovery.StubID || event.Scope != volumeID ||
			event.Kind != "" || !event.Replace || event.PartCount != 1 || event.ItemCount != 2 || event.SetDigest == "" {
			t.Fatalf("replacement published malformed report: %+v", event)
		}
		if event.Commit {
			commits++
			if len(event.Items) != 0 {
				t.Fatalf("scoped commit marker contains items: %+v", event)
			}
			continue
		}
		parts++
		if len(event.Items) != 2 || event.Items[0].Kind == event.Items[1].Kind {
			t.Fatalf("scoped revision did not mix manifest and chunk content: %+v", event)
		}
	}
	if parts != 2 || commits != 2 {
		t.Fatalf("replacement did not publish two complete scoped revisions: parts=%d commits=%d", parts, commits)
	}
}

func TestPodReplacementFailsArmedPrePivotEscrowQuarantinesAndRetries(t *testing.T) {
	allocatorTemplate, _, _, _ := setupTestNBD(t, 2)
	root := t.TempDir()
	journalStore := StateVolumeJournalStore{RootDir: filepath.Join(root, "journals")}
	newManager := func(instanceID string) *StateVolumeManager {
		allocator := &StateVolumeNBDAllocator{
			SysBlockRoot: allocatorTemplate.SysBlockRoot, DevRoot: allocatorTemplate.DevRoot,
			LockRoot: allocatorTemplate.LockRoot, MountInfoPath: allocatorTemplate.MountInfoPath,
			MaxDevices: allocatorTemplate.MaxDevices, Kernel: allocatorTemplate.Kernel,
		}
		return &StateVolumeManager{
			WorkerID: "worker", WorkerInstanceID: instanceID, StorageNodeID: "node",
			StateRoot: root, RuntimeRoot: filepath.Join(root, "runtime"), Journals: journalStore,
			NBD: allocator, Connector: &fakeStateVolumeConnector{}, Images: fakeStateVolumeImages{}, Mounts: &fakeStateVolumeMounts{},
			QMPDialer: fakeStateVolumeQMPDialer{qmp: &fakeStateVolumeQMP{}}, Launcher: &inheritedFDLauncher{},
		}
	}
	manager := newManager("source-instance")
	spec := StateVolumeGroupSpec{ContainerID: "container", Volumes: []StateVolumeSpec{
		{ID: "root", Name: "root", ContainerMountPath: "/", Root: true, BackingDir: filepath.Join(root, "backing-root"), MountPath: filepath.Join(root, "mount-root"), SizeBytes: 1024, Format: true, AttachmentToken: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", FencingToken: 1},
		{ID: "data", Name: "data", ContainerMountPath: "/data", BackingDir: filepath.Join(root, "backing-data"), MountPath: filepath.Join(root, "mount-data"), SizeBytes: 1024, Format: true, AttachmentToken: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", FencingToken: 2},
	}}
	if _, err := manager.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	recovery := StateVolumeRecoveryEnvelope{
		StateSnapshotID: "snapshot", RecoveryProofToken: stateVolumeTestRecoveryProofToken, OperationID: "operation", WorkspaceID: "workspace", WorkspaceName: "workspace-name", StubID: "stub",
		ImageID: "image", ImageDigest: "sha256:image", RuntimeProfile: "runc", Mode: string(StateSnapshotModeTerminal),
		WorkspaceStorageID: 1, WorkspaceStorageBucket: "bucket", WorkspaceStorageRegion: "us-east-1",
	}
	if err := manager.BindSnapshotRecovery("container", recovery); err != nil {
		t.Fatal(err)
	}
	journal, err := journalStore.Load("container")
	if err != nil {
		t.Fatal(err)
	}
	if journal.Phase != "recovery-bound" || len(journal.Volumes) != 2 {
		t.Fatalf("pre-quiesce operation did not retain exact unsealed membership: %+v", journal)
	}
	// Model pod death closing the source process's node-global flock file
	// descriptors. The replacement must not call the dead manager's Stop path,
	// but a real process exit releases these leases before startup recovery.
	sourceGroup, groupErr := manager.group("container")
	if groupErr != nil {
		t.Fatal(groupErr)
	}
	sourceGroup.mu.Lock()
	for _, volume := range sourceGroup.volumes {
		if volume.lease != nil {
			if err := volume.lease.Release(); err != nil {
				sourceGroup.mu.Unlock()
				t.Fatal(err)
			}
			volume.lease = nil
		}
	}
	sourceGroup.mu.Unlock()
	// A replacement may retain this as a cleanup obligation, but it may never
	// construct/upload a generation from the pre-quiesce active layers.
	replacement := newManager("replacement-instance")
	if err := replacement.Reconcile(context.Background()); err != nil {
		t.Fatalf("replacement could not retain pre-pivot cleanup obligation: %v", err)
	}
	if _, _, exists := replacement.ExistingGroup("container"); !exists {
		t.Fatal("replacement lost the pre-pivot cleanup obligation")
	}
	pending := &pb.StateSnapshot{
		ExternalId: recovery.StateSnapshotID, OperationId: recovery.OperationID, SourceContainerId: "container",
		Status: string(types.StateSnapshotStatusPending), Armed: true,
		SourceWorkerId: "worker", SourceWorkerInstanceId: "source-instance", StorageNodeId: "node",
		ImageId: recovery.ImageID, ImageDigest: recovery.ImageDigest, RuntimeProfile: recovery.RuntimeProfile,
		Mode: recovery.Mode, IncludeMemory: recovery.IncludeMemory, Visible: recovery.Visible,
	}
	repository := &offlineStateSnapshotRepository{operation: &pb.GetStateSnapshotResponse{
		Ok: true, Snapshot: pending, WorkspaceId: recovery.WorkspaceID, StubId: recovery.StubID,
	}}
	worker := &Worker{
		workerId: "worker", workerInstanceId: "replacement-instance", machineID: "node",
		backendRepoClient: repository, stateVolumeManager: replacement,
	}
	if err := worker.reconcileTerminalStateSnapshotJournals(context.Background()); err != nil {
		t.Fatalf("replacement did not fail and quarantine armed pre-pivot operation: %v", err)
	}
	repository.mu.Lock()
	if len(repository.failRequests) != 1 || repository.failRequests[0].RecoveryClaimGeneration != 1 {
		t.Fatalf("replacement did not fail the exact claimed escrow: %+v", repository.failRequests)
	}
	repository.mu.Unlock()
	if _, loadErr := journalStore.Load("container"); !os.IsNotExist(loadErr) {
		t.Fatalf("completed pre-pivot cleanup retained active journal: %v", loadErr)
	}
	if _, _, exists := replacement.ExistingGroup("container"); exists {
		t.Fatal("completed pre-pivot cleanup retained an active group")
	}
	quarantineRoot := filepath.Join(root, "quarantine", stateVolumeToken("prepivot-", "container\x00operation"))
	for _, volumeID := range []string{"root", "data"} {
		if info, statErr := os.Stat(filepath.Join(quarantineRoot, stateVolumeToken("volume-", volumeID))); statErr != nil || !info.IsDir() {
			t.Fatalf("pre-pivot graph %q was not quarantined: %v", volumeID, statErr)
		}
	}
	if _, err := replacement.Start(context.Background(), spec); err != nil {
		t.Fatalf("fresh retry remained bricked after pre-pivot cleanup: %v", err)
	}
}
