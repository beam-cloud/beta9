package runtime

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestMicroVMSnapshotSealsDisksBeforeCapture(t *testing.T) {
	var mu sync.Mutex
	var actions []string
	inst := snapshotTestInstance(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		actions = append(actions, r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	})
	mustStop, err := snapshotMicroVM(context.Background(), inst, t.TempDir(), func(context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		actions = append(actions, "seal")
		return nil
	})
	require.NoError(t, err)
	require.True(t, mustStop)
	require.True(t, inst.isPaused())
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"/api/v1/vm.pause", "seal", "/api/v1/vm.snapshot"}, actions)
}

func TestMicroVMSnapshotOtherErrorsAreNotSafeToRetry(t *testing.T) {
	var mu sync.Mutex
	var snapshots, resumes int
	inst := snapshotTestInstance(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/api/v1/vm.snapshot":
			snapshots++
			w.WriteHeader(http.StatusInternalServerError)
			return
		case "/api/v1/vm.resume":
			resumes++
		}
		w.WriteHeader(http.StatusNoContent)
	})
	captured, err := snapshotMicroVM(context.Background(), inst, t.TempDir(), nil)
	mu.Lock()
	defer mu.Unlock()
	require.Error(t, err)
	require.True(t, captured)
	require.True(t, inst.isPaused())
	require.Equal(t, 1, snapshots)
	require.Zero(t, resumes)
}

func TestMicroVMSnapshotSealFailureResumesBeforeAnyCapture(t *testing.T) {
	var mu sync.Mutex
	var actions []string
	inst := snapshotTestInstance(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		actions = append(actions, r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	})
	sealErr := errors.New("disk publication failed")
	captured, err := snapshotMicroVM(context.Background(), inst, t.TempDir(), func(context.Context) error { return sealErr })
	mu.Lock()
	defer mu.Unlock()
	require.ErrorIs(t, err, sealErr)
	require.False(t, captured)
	require.False(t, inst.isPaused())
	require.Equal(t, []string{"/api/v1/vm.pause", "/api/v1/vm.resume"}, actions)
}

func TestMicroVMSnapshotLostPauseResponseResolvesActualState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var actions []string
	inst := snapshotTestInstance(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		actions = append(actions, r.URL.Path)
		mu.Unlock()
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
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"/api/v1/vm.pause", "/api/v1/vm.info", "/api/v1/vm.resume"}, actions)
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
	inst := &microVMInstance{canvas: t.TempDir()}
	spec := &specs.Spec{}
	require.NoError(t, rt.prepareCanvas(inst, spec, microvm.Network{}, microVMDisk{}, nil, checkpoint))
	data, err := os.ReadFile(filepath.Join(inst.canvas, microvm.InitPath))
	require.NoError(t, err)
	require.Equal(t, "captured agent", string(data))
	require.NoError(t, rt.prepareCanvas(inst, spec, microvm.Network{}, microVMDisk{}, nil, ""))
	data, err = os.ReadFile(filepath.Join(inst.canvas, microvm.InitPath))
	require.NoError(t, err)
	require.Equal(t, "upgraded agent", string(data))
	require.NoError(t, os.Remove(filepath.Join(checkpoint, checkpointInitFile)))
	require.Error(t, rt.prepareCanvas(inst, spec, microvm.Network{}, microVMDisk{}, nil, checkpoint), "never substitute another executable into captured memory")
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
	require.False(t, caps.CheckpointRestore)
	for _, value := range []string{"../77", "01:00.0", "0000:01:20.0", "0000:01:00.8", "0000:01:00.0,path=/tmp/evil", "0000:01:00.0,0000:01:00.0"} {
		_, err := (microVMVFIOHost{devices: []string{"0000:01:00.0"}}).acquire(value)
		require.ErrorContains(t, err, "invalid or duplicate", value)
	}
	claim, err := (microVMVFIOHost{}).acquire("")
	require.NoError(t, err, "CPU guests do not need host VFIO support")
	require.Empty(t, claim.paths)
	claim.close()

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

	base := microVMHypervisorArgs("/state", "/kernel", "", 4, 8<<30, "02:00:00:00:00:01", microVMDisk{}, nil)
	args := microVMHypervisorArgs("/state", "/kernel", "", 4, 8<<30, "02:00:00:00:00:01", microVMDisk{}, nil, claim.paths...)
	require.Contains(t, base, "--balloon")
	require.NotContains(t, args, "--balloon", "DMA-pinned RAM cannot be reclaimed")
	require.Contains(t, strings.Join(args, " "), "--device path="+claim.paths[0]+" path="+claim.paths[1])
	_, err = rewriteSnapshotConfig([]byte(`{"devices":[{"path":"/sys/bus/pci/devices/0000:01:00.0"}]}`), "/state", microvm.Network{}, microVMDisk{}, nil)
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

func TestMicroVMVFIORestoreRequiresColdBoot(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		rt := &MicroVM{cfg: Config{MicroVMVFIODevices: []string{"0000:01:00.0"}}}
		spec := &specs.Spec{Annotations: map[string]string{MicroVMGPUCountAnnotation: "1"}}
		if explicit {
			rt.cfg.MicroVMVFIODevices = nil
			delete(spec.Annotations, MicroVMGPUCountAnnotation)
			spec.Annotations[MicroVMVFIOAnnotation] = "0000:01:00.0"
		}
		inst := &microVMInstance{stateDir: filepath.Join(t.TempDir(), "state")}
		_, _, _, err := rt.prepare(context.Background(), inst, spec, "checkpoint")
		require.ErrorContains(t, err, "cold boot")
		require.Nil(t, inst.vfio, "memory restore must reject GPU assignment before claiming devices")
		require.NoDirExists(t, inst.stateDir)
	}
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
	return &MicroVM{cfg: Config{MicroVMKernelPath: workerKernel}},
		&microVMInstance{canvas: canvas, stateDir: t.TempDir(), vfio: &microVMVFIO{paths: []string{"gpu"}}}, image
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
				files, err := os.ReadDir(inst.stateDir)
				require.NoError(t, err)
				require.Empty(t, files)
				return
			}
			require.Equal(t, inst.stateDir, filepath.Dir(kernel))
			require.NotEqual(t, rt.cfg.MicroVMKernelPath, kernel, "the image kernel takes precedence over an invalid worker kernel")
			require.NoError(t, os.WriteFile(image, []byte("changed"), 0o644))
			copied, err := os.ReadFile(kernel)
			require.NoError(t, err)
			require.Equal(t, content, copied, "the VMM uses a private copy, not a mutable image path")
			info, err := os.Stat(kernel)
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
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
			files, err := os.ReadDir(inst.stateDir)
			require.NoError(t, err)
			require.Empty(t, files, "unsafe kernels must be rejected before writing VM state")
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
	require.NoError(t, os.MkdirAll(inst.stateDir, 0o700))
	kernel, err := rt.kernelPath(inst)
	require.NoError(t, err)
	copied, err := os.ReadFile(kernel)
	require.NoError(t, err)
	require.Equal(t, "image kernel", string(copied))
}
