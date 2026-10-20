package disk

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompactReleasesQEMUOverlays(t *testing.T) {
	ctx, volume := newQEMUVolume(t)
	// Keep one daemon alive beyond the original 1,024-snapshot failure point.
	// Its enforced 128-FD limit also catches unnamed monitor/job references.
	for i := 1; i <= 1_104; i++ {
		sealed, _, err := volume.Seal(ctx, true)
		require.NoError(t, err)
		require.Len(t, sealed, 1)
		require.NoError(t, volume.MarkPublished(sealed[0].Path, fmt.Sprintf("snap-%d", i)))
		if i%8 == 0 {
			require.NoError(t, volume.Compact(ctx))
			client, err := dialQMP(ctx, volume.state.QMPSocket)
			require.NoError(t, err)
			nodes, err := client.namedBlockNodes(ctx)
			client.Close()
			require.NoError(t, err)
			require.Len(t, nodes, 4, "only the base, head and their file children may remain after snapshot %d", i)
		}
	}
}

// The merged layer is uploaded from its file, so Merge must leave the commit's
// writes on disk rather than only in the daemon's metadata cache.
func TestMergedQEMULayerIsCompleteOnDisk(t *testing.T) {
	ctx, volume := newQEMUVolume(t)
	if _, err := exec.LookPath("qemu-io"); err != nil {
		t.Skip("qemu-io is not installed")
	}
	qemuIO := func(args ...string) {
		out, err := exec.CommandContext(ctx, "qemu-io", args...).CombinedOutput()
		require.NoError(t, err, string(out))
	}
	pattern := func(i int) string { return fmt.Sprintf("-P %d %d 64k", i, i<<16) }
	export := fmt.Sprintf("nbd+unix:///%s?socket=%s", qsdExportName, filepath.Join(volume.dir, "run", "nbd.sock"))
	for i := 1; i <= 3; i++ {
		qemuIO("-f", "raw", export, "-c", "write "+pattern(i))
		sealed, _, err := volume.Seal(ctx, true)
		require.NoError(t, err)
		require.NoError(t, volume.MarkPublished(sealed[0].Path, fmt.Sprint(i)))
	}

	merged, err := volume.Merge(ctx, 1)
	require.NoError(t, err)
	alone, err := json.Marshal(map[string]any{"driver": "qcow2", "backing": nil, "file": map[string]any{"driver": "file", "filename": merged.Path}})
	require.NoError(t, err)
	for i := 1; i <= 3; i++ {
		qemuIO("-U", "-r", "json:"+string(alone), "-c", "read "+pattern(i))
	}
	client, err := dialQMP(ctx, volume.state.QMPSocket)
	require.NoError(t, err)
	defer client.Close()
	require.Error(t, client.checkReadOnly(ctx, volume.state.HeadPath), "the writable head must not pass for a flushed layer")
}

func TestSealRecoversExhaustedQEMUDaemon(t *testing.T) {
	ctx, volume := newQEMUVolume(t)
	client, err := dialQMP(ctx, volume.state.QMPSocket)
	require.NoError(t, err)
	defer client.Close()
	// Reproduce an older worker's already-unlinked, monitor-owned overlays.
	// Fill the daemon's real descriptor table without deepening its live chain.
	for i := 10_000; i < 10_200; i++ {
		path := headLayerPath(filepath.Join(volume.dir, layersSubdir), i)
		require.NoError(t, volume.manager.createQcowBase(ctx, path, 8<<20))
		name := fmtNodeName(i)
		err = client.addOverlay(ctx, name, qsdFileNodePrefix+name, path)
		require.NoError(t, os.Remove(path))
		if err != nil {
			break
		}
	}
	require.ErrorContains(t, err, "Too many open files")
	client.Close()

	sealed, _, err := volume.Seal(ctx, true)
	require.NoError(t, err, "recover without restarting the daemon or its VM")
	require.Len(t, sealed, 1)
	client, err = dialQMP(ctx, volume.state.QMPSocket)
	require.NoError(t, err)
	defer client.Close()
	nodes, err := client.namedBlockNodes(ctx)
	require.NoError(t, err)
	require.Len(t, nodes, 6, "only the base, original head, new head and their file nodes remain")
}

func newQEMUVolume(t *testing.T) (context.Context, *Volume) {
	t.Helper()
	qsd, err := exec.LookPath("qemu-storage-daemon")
	if err != nil {
		t.Skip("qemu-storage-daemon is not installed")
	}
	qemuImg, err := exec.LookPath("qemu-img")
	if err != nil {
		t.Skip("qemu-img is not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	dir, err := os.MkdirTemp("", "qcow")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	require.NoError(t, os.Mkdir(filepath.Join(dir, layersSubdir), 0o700))
	head := headLayerPath(filepath.Join(dir, layersSubdir), 0)
	// No filesystem is mounted; only the daemon and block graph are real.
	manager := NewManager(Config{Root: dir, Binaries: Binaries{
		QemuImg: qemuImg, QSD: qsd, Fsfreeze: "true",
	}, Runner: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == qsd {
			// Keep both regressions below the real process's soft limit.
			args = append([]string{"-c", `ulimit -n 128; exec "$@"`, "qsd", qsd}, args...)
			name = "sh"
		}
		return execRunner(ctx, name, args...)
	}})
	base := filepath.Join(dir, layersSubdir, "base.qcow2")
	require.NoError(t, manager.createQcowBase(ctx, base, 8<<20))
	require.NoError(t, manager.createQcowOverlay(ctx, head, base, 8<<20))
	proc, err := manager.startQSD(ctx, filepath.Join(dir, "run"), head, "fmt-0", false, ExportNBD)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.stopQSD(context.Background(), proc)) })
	return ctx, &Volume{manager: manager, dir: dir, fmtNode: "fmt-0", state: &volumeState{
		Key: "test", VirtualSizeBytes: 8 << 20, Attached: true, Formatted: true,
		HeadPath: head, QMPSocket: proc.qmpSocket,
		Chain: []stateLayer{{SnapshotID: "base", Path: base}},
	}}
}

// Submit a real daemon job, then lose the caller before it can dismiss it.
// A later seal must release that job's graph references without a restart.
func TestSealFinishesInterruptedQEMUCommit(t *testing.T) {
	ctx, volume := newQEMUVolume(t)
	sealed, _, err := volume.Seal(ctx, true)
	require.NoError(t, err)
	require.NoError(t, volume.MarkPublished(sealed[0].Path, "snap-1"))
	client, err := dialQMP(ctx, volume.state.QMPSocket)
	require.NoError(t, err)
	defer client.Close()
	_, err = client.execute(ctx, "block-commit", map[string]any{
		"job-id": "interrupted", "device": volume.fmtNode,
		"top": volume.state.Chain[1].Path, "base": volume.state.Chain[0].Path, "auto-dismiss": false,
	})
	require.NoError(t, err)
	jobs, err := client.jobs(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.NoError(t, client.Close())
	_, _, err = volume.Seal(ctx, true)
	require.NoError(t, err, "join and dismiss the existing job before pivoting")
	client, err = dialQMP(ctx, volume.state.QMPSocket)
	require.NoError(t, err)
	defer client.Close()
	jobs, err = client.jobs(ctx)
	require.NoError(t, err)
	require.Empty(t, jobs)
}

func TestSealResolvesLostQEMUPivotReply(t *testing.T) {
	for _, scenario := range []struct {
		name              string
		commit, failProbe bool
	}{
		{"committed", true, false}, {"uncommitted", false, false}, {"probe-unavailable", true, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			parent, volume := newQEMUVolume(t)
			ctx, cancel := context.WithCancel(parent)
			defer cancel()
			oldHead := volume.state.HeadPath
			volume.state.QMPSocket = lostPivotReplyProxy(t, volume.state.QMPSocket, scenario.commit, scenario.failProbe, cancel)
			sealed, _, err := volume.Seal(ctx, true)
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			if scenario.failProbe {
				require.Error(t, err)
				require.FileExists(t, oldHead)
				require.FileExists(t, volume.state.HeadPath, "an unknown outcome must retain the possible active head")
				require.Equal(t, "fmt-0", volume.fmtNode)
				sealed, err = volume.Unpublished(parent)
				require.Error(t, err, "a failed probe must not expose a potentially writable layer")
				require.Empty(t, sealed)
				sealed, err = volume.Unpublished(parent)
				require.NoError(t, err, "reconcile before exposing a sealed layer to its publisher")
			} else if !scenario.commit {
				require.Error(t, err)
				require.Equal(t, oldHead, volume.state.HeadPath)
				require.Empty(t, volume.state.Pending)
				require.Equal(t, "fmt-0", volume.fmtNode)
			} else {
				require.NoError(t, err, "confirm a committed pivot despite the cancelled caller")
			}
			if scenario.commit {
				require.Len(t, sealed, 1)
				require.Equal(t, oldHead, sealed[0].Path)
				require.Equal(t, "fmt-1", volume.fmtNode)
			}
			_, _, err = volume.Seal(parent, true)
			require.NoError(t, err, "the same live daemon must remain usable")
		})
	}
}

// A real QEMU transaction can commit after its caller loses the reply. This
// proxy cancels that caller and delivers the delayed reply after reconnection,
// so recovery must use both a fresh context and a distinct command identity.
func lostPivotReplyProxy(t *testing.T, upstream string, commit, failProbe bool, cancel context.CancelFunc) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "qmp-proxy")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "qmp.sock")
	listener, err := net.Listen("unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	var dropped atomic.Bool
	var delayed atomic.Pointer[map[string]json.RawMessage]
	var probeFailures atomic.Int32
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer client.Close()
				server, err := net.Dial("unix", upstream)
				if err != nil {
					return
				}
				defer server.Close()
				fromServer, fromClient := json.NewDecoder(server), json.NewDecoder(client)
				toServer, toClient := json.NewEncoder(server), json.NewEncoder(client)
				var greeting json.RawMessage
				if fromServer.Decode(&greeting) != nil || toClient.Encode(greeting) != nil {
					return
				}
				for {
					var request map[string]json.RawMessage
					if fromClient.Decode(&request) != nil {
						return
					}
					var command string
					if json.Unmarshal(request["execute"], &command) != nil {
						return
					}
					loseReply := command == "transaction" && dropped.CompareAndSwap(false, true)
					if loseReply && !commit {
						cancel()
						return
					}
					if toServer.Encode(request) != nil {
						return
					}
					for {
						var response map[string]json.RawMessage
						if fromServer.Decode(&response) != nil {
							return
						}
						if _, event := response["event"]; event {
							if toClient.Encode(response) != nil {
								return
							}
							continue
						}
						if loseReply {
							delayed.Store(&response)
							cancel()
							return
						}
						if command == "qmp_capabilities" {
							if late := delayed.Swap(nil); late != nil && toClient.Encode(*late) != nil {
								return
							}
						}
						if command == "query-named-block-nodes" && failProbe && dropped.Load() && probeFailures.Add(1) <= 2 {
							response = map[string]json.RawMessage{"id": request["id"], "error": json.RawMessage(`{"class":"GenericError","desc":"probe unavailable"}`)}
						}
						if toClient.Encode(response) != nil {
							return
						}
						break
					}
				}
			}()
		}
	}()
	return path
}
