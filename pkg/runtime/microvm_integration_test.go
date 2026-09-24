//go:build linux && microvm

// Stage 1 mechanics for the microvm runtime. These tests boot real Cloud
// Hypervisor VMs and therefore need KVM, the worker image's tooling on PATH
// (cloud-hypervisor, virtiofsd, mkfs.ext4, iptables, qemu-storage-daemon), root,
// and rootfs tarballs exported by hack/microvm-smoke.sh. They are compiled
// only with `-tags microvm` and skip themselves when the environment is not
// there. Each test builds exactly what the worker builds: an overlay merged
// directory holding config.json as the bundle, and a named network namespace
// with a veth on a bridge.
package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/microvm"
	goproc "github.com/beam-cloud/goproc/pkg"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

const (
	testBridgeName = "b9test0"
	testSubnet4    = "10.200.0.0/24"
	testBridgeIP4  = "10.200.0.1"
	testSubnet6    = "fd00:beef::/64"
	testBridgeIP6  = "fd00:beef::1"
	testComment    = "b9-microvm-test"
	goprocPort     = 7111
)

var (
	testIPCounter atomic.Int32
	testEnvOnce   sync.Once
	testEnvErr    error
	testRootfsDir = envOr("MICROVM_TEST_ROOTFS_DIR", "/microvm/rootfs")
	testGoproc    = envOr("MICROVM_TEST_GOPROC", "/usr/local/bin/goproc")
	testWorkRoot  = envOr("MICROVM_TEST_WORK_DIR", "/tmp/microvm-test")
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// --- environment -----------------------------------------------------------------

func requireMicroVMEnv(t *testing.T) *MicroVM {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("microvm integration tests must run as root")
	}
	if _, err := os.Stat("/dev/kvm"); err != nil {
		t.Skip("/dev/kvm is not available")
	}
	if _, err := os.Stat(testRootfsDir); err != nil {
		t.Skipf("rootfs tarballs not found in %s (run hack/microvm-smoke.sh rootfs)", testRootfsDir)
	}
	testEnvOnce.Do(func() { testEnvErr = setupTestBridge() })
	require.NoError(t, testEnvErr, "test bridge")

	rt, err := NewMicroVM(Config{Type: "microvm", MicroVMStateRoot: filepath.Join(testWorkRoot, "state")})
	require.NoError(t, err)
	return rt.(*MicroVM)
}

// setupTestBridge creates b9test0 with IPv4/IPv6 addresses, MASQUERADE for
// its subnet, and FORWARD accepts so guests reach the uplink even with
// Docker's FORWARD DROP policy on the host. Everything carries testComment
// and is idempotent across runs.
func setupTestBridge() error {
	bridge, err := netlink.LinkByName(testBridgeName)
	if err != nil {
		if err := netlink.LinkAdd(&netlink.Bridge{LinkAttrs: netlink.LinkAttrs{Name: testBridgeName}}); err != nil {
			return fmt.Errorf("create bridge: %w", err)
		}
		if bridge, err = netlink.LinkByName(testBridgeName); err != nil {
			return err
		}
	}
	for _, cidr := range []string{testBridgeIP4 + "/24", testBridgeIP6 + "/64"} {
		addr, err := netlink.ParseAddr(cidr)
		if err != nil {
			return err
		}
		if err := netlink.AddrReplace(bridge, addr); err != nil {
			return fmt.Errorf("address %s: %w", cidr, err)
		}
	}
	if err := netlink.LinkSetUp(bridge); err != nil {
		return err
	}
	_ = os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1"), 0o644)
	_ = os.WriteFile("/proc/sys/net/ipv6/conf/all/forwarding", []byte("1"), 0o644)

	rules := [][]string{
		{"iptables", "-t", "nat", "-A", "POSTROUTING", "-s", testSubnet4, "!", "-o", testBridgeName, "-j", "MASQUERADE", "-m", "comment", "--comment", testComment},
		{"iptables", "-I", "FORWARD", "1", "-s", testSubnet4, "-j", "ACCEPT", "-m", "comment", "--comment", testComment},
		{"iptables", "-I", "FORWARD", "1", "-d", testSubnet4, "-j", "ACCEPT", "-m", "comment", "--comment", testComment},
		{"ip6tables", "-t", "nat", "-A", "POSTROUTING", "-s", testSubnet6, "!", "-o", testBridgeName, "-j", "MASQUERADE", "-m", "comment", "--comment", testComment},
		{"ip6tables", "-I", "FORWARD", "1", "-s", testSubnet6, "-j", "ACCEPT", "-m", "comment", "--comment", testComment},
		{"ip6tables", "-I", "FORWARD", "1", "-d", testSubnet6, "-j", "ACCEPT", "-m", "comment", "--comment", testComment},
	}
	for _, rule := range rules {
		check := append([]string{}, rule...)
		check[indexOf(check, "-A", "-I")] = "-C"
		// -I takes a position argument that -C does not.
		check = removeAt(check, indexOf(check, "1"))
		if exec.Command(check[0], check[1:]...).Run() == nil {
			continue
		}
		if out, err := exec.Command(rule[0], rule[1:]...).CombinedOutput(); err != nil {
			if strings.HasPrefix(rule[0], "ip6") {
				continue // IPv6 nat may be unavailable; IPv6 assertions then skip
			}
			return fmt.Errorf("%v: %w: %s", rule, err, out)
		}
	}
	return nil
}

func indexOf(args []string, candidates ...string) int {
	for i, arg := range args {
		for _, c := range candidates {
			if arg == c {
				return i
			}
		}
	}
	return -1
}

func removeAt(args []string, i int) []string {
	if i < 0 || i >= len(args) || i < 3 {
		return args
	}
	return append(append([]string{}, args[:i]...), args[i+1:]...)
}

// --- per-VM fixture ------------------------------------------------------------------

type testVM struct {
	t        *testing.T
	rt       *MicroVM
	id       string
	rootfs   string // extracted image
	canvas   string // overlay merged dir == bundle
	netnsDir string
	netns    string
	ip4      net.IP
	ip6      net.IP
	vethMAC  net.HardwareAddr
	spec     *specs.Spec
	output   *lockedBuffer
	started  chan int
	result   chan runResult
	cancel   context.CancelFunc
	runCtx   context.Context
	vethHost string
}

type runResult struct {
	code int
	err  error
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type vmOptions struct {
	image       string // alpine | dind | python
	args        []string
	annotations map[string]string
	memoryMiB   int64
	cpuCores    int64
	binds       []specs.Mount
	docker      bool
	goproc      bool
	hostname    string
}

func newTestVM(t *testing.T, rt *MicroVM, opts vmOptions) *testVM {
	t.Helper()
	n := testIPCounter.Add(1) + 9
	id := fmt.Sprintf("mvm%d-%d", os.Getpid()%1000, n)
	vm := &testVM{
		t:       t,
		rt:      rt,
		id:      id,
		ip4:     net.ParseIP(fmt.Sprintf("10.200.0.%d", n)),
		ip6:     net.ParseIP(fmt.Sprintf("fd00:beef::%x", n)),
		output:  &lockedBuffer{},
		started: make(chan int, 1),
		result:  make(chan runResult, 1),
	}
	vm.rootfs = extractRootfs(t, opts.image)
	vm.canvas = mountCanvas(t, id, vm.rootfs)
	vm.netns, vm.vethHost, vm.vethMAC = createTestNetwork(t, id, vm.ip4, vm.ip6)
	vm.spec = vm.buildSpec(opts)

	data, err := json.MarshalIndent(vm.spec, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(vm.canvas, "config.json"), data, 0o644))
	t.Cleanup(vm.cleanup)
	return vm
}

func (vm *testVM) buildSpec(opts vmOptions) *specs.Spec {
	memory := opts.memoryMiB
	if memory == 0 {
		memory = 1024
	}
	cores := opts.cpuCores
	if cores == 0 {
		cores = 1
	}
	quota := cores * 100000
	period := uint64(100000)
	limit := memory << 20

	args := opts.args
	if opts.goproc {
		args = []string{"/usr/bin/goproc"}
	}
	spec := &specs.Spec{
		Version:  specs.Version,
		Hostname: opts.hostname,
		Root:     &specs.Root{Path: vm.canvas},
		Process: &specs.Process{
			Cwd:  "/",
			Args: args,
			Env:  []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "TERM=xterm"},
		},
		Mounts: []specs.Mount{
			{Destination: "/proc", Type: "proc", Source: "proc"},
			{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
			{Destination: "/dev/pts", Type: "devpts", Source: "devpts"},
			{Destination: "/dev/shm", Type: "tmpfs", Source: "shm"},
			{Destination: "/sys", Type: "sysfs", Source: "sysfs"},
			{Destination: "/sys/fs/cgroup", Type: "cgroup", Source: "cgroup"},
		},
		Linux: &specs.Linux{
			Resources: &specs.LinuxResources{
				CPU:    &specs.LinuxCPU{Quota: &quota, Period: &period},
				Memory: &specs.LinuxMemory{Limit: &limit},
			},
			Namespaces: []specs.LinuxNamespace{{Type: specs.NetworkNamespace, Path: vm.netns}},
		},
		Annotations: map[string]string{},
	}
	for k, v := range opts.annotations {
		spec.Annotations[k] = v
	}
	if opts.docker {
		spec.Annotations[MicroVMDockerAnnotation] = "true"
	}

	// resolv.conf and the process manager: the two binds every sandbox has.
	resolv := filepath.Join(testWorkRoot, "resolv.conf")
	require.NoError(vm.t, os.MkdirAll(testWorkRoot, 0o755))
	require.NoError(vm.t, os.WriteFile(resolv, []byte("nameserver 1.1.1.1\nnameserver 8.8.8.8\n"), 0o644))
	spec.Mounts = append(spec.Mounts, specs.Mount{Destination: "/etc/resolv.conf", Type: "none", Source: resolv, Options: []string{"ro", "rbind", "rprivate", "nosuid", "noexec", "nodev"}})
	if opts.goproc {
		spec.Mounts = append(spec.Mounts, specs.Mount{Destination: "/usr/bin/goproc", Type: "bind", Source: testGoproc, Options: []string{"ro", "rbind", "rprivate", "nosuid", "nodev"}})
	}
	spec.Mounts = append(spec.Mounts, opts.binds...)
	return spec
}

// start launches Run in the background and waits for the hypervisor pid.
func (vm *testVM) start() int {
	vm.t.Helper()
	vm.runCtx, vm.cancel = context.WithCancel(context.Background())
	go func() {
		code, err := vm.rt.Run(vm.runCtx, vm.id, vm.canvas, &RunOpts{OutputWriter: vm.output, Started: vm.started})
		vm.result <- runResult{code: code, err: err}
	}()
	select {
	case pid := <-vm.started:
		return pid
	case res := <-vm.result:
		vm.t.Fatalf("Run returned before start: code=%d err=%v\n%s", res.code, res.err, vm.output.String())
	case <-time.After(60 * time.Second):
		vm.t.Fatalf("hypervisor did not start within 60s\n%s", vm.output.String())
	}
	return 0
}

func (vm *testVM) wait(timeout time.Duration) runResult {
	vm.t.Helper()
	select {
	case res := <-vm.result:
		return res
	case <-time.After(timeout):
		vm.t.Fatalf("VM did not exit within %s\n%s", timeout, vm.output.String())
	}
	return runResult{}
}

// goprocClient dials the process manager at the guest's IP, exactly as the
// worker does, and waits for Ready.
func (vm *testVM) goprocClient(timeout time.Duration) *goproc.GoProcClient {
	vm.t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case res := <-vm.result:
			vm.t.Fatalf("VM exited while waiting for goproc: code=%d err=%v\n%s", res.code, res.err, vm.output.String())
		default:
		}
		// The client keeps its dial context for every later RPC, so it must
		// outlive the readiness probe.
		clientCtx, cancelClient := context.WithCancel(context.Background())
		client, err := goproc.NewGoProcClient(clientCtx, vm.ip4.String(), goprocPort)
		if err == nil {
			probeCtx, cancelProbe := context.WithTimeout(clientCtx, 2*time.Second)
			err = client.ReadyContext(probeCtx)
			cancelProbe()
			if err == nil {
				vm.t.Cleanup(func() {
					_ = client.Cleanup()
					cancelClient()
				})
				return client
			}
			_ = client.Cleanup()
		}
		cancelClient()
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	vm.t.Fatalf("goproc at %s:%d never became ready: %v\n%s", vm.ip4, goprocPort, lastErr, vm.output.String())
	return nil
}

// exec runs a command in the guest through goproc and returns its exit code
// and combined output.
func (vm *testVM) exec(client *goproc.GoProcClient, args ...string) (int, string) {
	vm.t.Helper()
	pid, err := client.Exec(args, "/", []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}, false)
	require.NoError(vm.t, err, "exec %v", args)
	code, err := client.Wait(pid)
	if err != nil {
		// goproc reports a non-zero exit as an error string.
		var parsed int
		if _, scanErr := fmt.Sscanf(err.Error(), "exit status %d", &parsed); scanErr == nil {
			code, err = parsed, nil
		}
	}
	require.NoError(vm.t, err, "wait %v", args)
	stdout, _ := client.Stdout(pid)
	stderr, _ := client.Stderr(pid)
	return code, stdout + stderr
}

func (vm *testVM) sh(client *goproc.GoProcClient, script string) (int, string) {
	return vm.exec(client, "sh", "-c", script)
}

func (vm *testVM) cleanup() {
	if vm.cancel != nil {
		vm.cancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = vm.rt.Delete(ctx, vm.id, &DeleteOpts{Force: true})
	_ = unix.Unmount(vm.canvas, unix.MNT_DETACH)
	_ = netns.DeleteNamed(filepath.Base(vm.netns))
	if link, err := netlink.LinkByName(vm.vethHost); err == nil {
		_ = netlink.LinkDel(link)
	}
	_ = os.RemoveAll(filepath.Dir(vm.canvas))
}

// extractRootfs unpacks <image>.tar once per process and returns the directory.
func extractRootfs(t *testing.T, image string) string {
	t.Helper()
	tarball := filepath.Join(testRootfsDir, image+".tar")
	if _, err := os.Stat(tarball); err != nil {
		t.Skipf("rootfs %s missing", tarball)
	}
	dir := filepath.Join(testWorkRoot, "rootfs", image)
	marker := filepath.Join(dir, ".extracted")
	if _, err := os.Stat(marker); err == nil {
		return dir
	}
	require.NoError(t, os.RemoveAll(dir))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	out, err := exec.Command("tar", "-xf", tarball, "-C", dir, "--exclude=dev/*").CombinedOutput()
	require.NoError(t, err, string(out))
	require.NoError(t, os.WriteFile(marker, nil, 0o644))
	return dir
}

// mountCanvas builds what the worker's ContainerOverlay builds: an overlay
// merged directory over the image, writable on the host.
func mountCanvas(t *testing.T, id, lower string) string {
	t.Helper()
	base := filepath.Join(testWorkRoot, "vms", id)
	upper, work, merged := filepath.Join(base, "upper"), filepath.Join(base, "work"), filepath.Join(base, "merged")
	for _, dir := range []string{upper, work, merged} {
		require.NoError(t, os.MkdirAll(dir, 0o755))
	}
	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lower, upper, work)
	require.NoError(t, unix.Mount("overlay", merged, "overlay", 0, opts), "mount overlay %s", opts)
	return merged
}

// createTestNetwork mirrors network.go: a veth pair with the host side on the
// bridge and the peer configured inside a named namespace.
func createTestNetwork(t *testing.T, id string, ip4, ip6 net.IP) (nsPath, hostVeth string, peerMAC net.HardwareAddr) {
	t.Helper()
	goruntime.LockOSThread()
	defer goruntime.UnlockOSThread()

	hostNS, err := netns.Get()
	require.NoError(t, err)
	defer hostNS.Close()

	suffix := id[len(id)-6:]
	hostVeth, peerVeth := "b9th"+suffix, "b9tc"+suffix
	if link, err := netlink.LinkByName(hostVeth); err == nil {
		_ = netlink.LinkDel(link)
	}
	_ = netns.DeleteNamed(id)

	bridge, err := netlink.LinkByName(testBridgeName)
	require.NoError(t, err)
	require.NoError(t, netlink.LinkAdd(&netlink.Veth{LinkAttrs: netlink.LinkAttrs{Name: hostVeth, MTU: 1500}, PeerName: peerVeth}))
	host, err := netlink.LinkByName(hostVeth)
	require.NoError(t, err)
	require.NoError(t, netlink.LinkSetMaster(host, bridge))
	require.NoError(t, netlink.LinkSetUp(host))

	newNS, err := netns.NewNamed(id)
	require.NoError(t, err)
	defer newNS.Close()
	require.NoError(t, netns.Set(hostNS))

	peer, err := netlink.LinkByName(peerVeth)
	require.NoError(t, err)
	peerMAC = peer.Attrs().HardwareAddr
	require.NoError(t, netlink.LinkSetNsFd(peer, int(newNS)))

	require.NoError(t, netns.Set(newNS))
	defer func() { require.NoError(t, netns.Set(hostNS)) }()
	peer, err = netlink.LinkByName(peerVeth)
	require.NoError(t, err)
	_ = os.WriteFile("/proc/sys/net/ipv6/conf/"+peerVeth+"/disable_ipv6", []byte("0"), 0o644)
	require.NoError(t, netlink.LinkSetUp(peer))
	if lo, err := netlink.LinkByName("lo"); err == nil {
		_ = netlink.LinkSetUp(lo)
	}
	addr4, err := netlink.ParseAddr(ip4.String() + "/24")
	require.NoError(t, err)
	require.NoError(t, netlink.AddrAdd(peer, addr4))
	require.NoError(t, netlink.RouteAdd(&netlink.Route{LinkIndex: peer.Attrs().Index, Gw: net.ParseIP(testBridgeIP4)}))
	addr6, err := netlink.ParseAddr(ip6.String() + "/64")
	require.NoError(t, err)
	addr6.Flags = unix.IFA_F_NODAD
	require.NoError(t, netlink.AddrAdd(peer, addr6))
	_, def6, _ := net.ParseCIDR("::/0")
	require.NoError(t, netlink.RouteAdd(&netlink.Route{LinkIndex: peer.Attrs().Index, Dst: def6, Gw: net.ParseIP(testBridgeIP6)}))

	return filepath.Join("/var/run/netns", id), hostVeth, peerMAC
}

// --- helpers ---------------------------------------------------------------------------

func processesMatching(pattern string) []string {
	out, _ := exec.Command("sh", "-c", "ps -eo pid,args | grep -F -- '"+pattern+"' | grep -v grep || true").Output()
	return strings.Fields(strings.TrimSpace(string(out)))
}

func mountsUnder(path string) []string {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil
	}
	var mounts []string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 4 && strings.HasPrefix(fields[4], path+"/") {
			mounts = append(mounts, fields[4])
		}
	}
	return mounts
}

// bridgeHTTPServer answers "ok" on the bridge's v4 and v6 addresses so
// guests can prove reachability of the host over each family.
func bridgeHTTPServer(t *testing.T) (port int) {
	t.Helper()
	ln4, err := net.Listen("tcp4", testBridgeIP4+":0")
	require.NoError(t, err)
	port = ln4.Addr().(*net.TCPAddr).Port
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	srv4 := &http.Server{Handler: handler}
	go func() { _ = srv4.Serve(ln4) }()
	t.Cleanup(func() { _ = srv4.Close() })
	if ln6, err := net.Listen("tcp6", "["+testBridgeIP6+"]:"+strconv.Itoa(port)); err == nil {
		srv6 := &http.Server{Handler: handler}
		go func() { _ = srv6.Serve(ln6) }()
		t.Cleanup(func() { _ = srv6.Close() })
	}
	return port
}

// --- tests -------------------------------------------------------------------------------

func TestMicroVMBootAndExit(t *testing.T) {
	rt := requireMicroVMEnv(t)
	vm := newTestVM(t, rt, vmOptions{image: "alpine", args: []string{"sh", "-c", "uname -r; echo hello-from-guest; exit 7"}})

	startedAt := time.Now()
	pid := vm.start()
	require.Greater(t, pid, 0)

	state, err := rt.State(context.Background(), vm.id)
	require.NoError(t, err)
	require.Equal(t, pid, state.Pid)

	res := vm.wait(90 * time.Second)
	t.Logf("boot-to-exit: %s", time.Since(startedAt))
	require.NoError(t, res.err, vm.output.String())
	require.Equal(t, 7, res.code, vm.output.String())
	require.Contains(t, vm.output.String(), "hello-from-guest")
	require.Contains(t, vm.output.String(), "6.12.", "guest kernel version must appear in console output")

	cgroup := microVMCgroupPath(vm.spec, vm.id)
	scratch := filepath.Join(filepath.Dir(vm.canvas), "scratch.ext4")
	require.FileExists(t, scratch, "scratch disk exists until Delete")

	require.NoError(t, rt.Delete(context.Background(), vm.id, &DeleteOpts{Force: true}))
	require.Empty(t, processesMatching(filepath.Join(rt.cfg.MicroVMStateRoot, vm.id)), "hypervisor/virtiofsd processes must be gone")
	require.Empty(t, mountsUnder(vm.canvas), "canvas submounts must be unmounted before the overlay")
	require.NoFileExists(t, scratch)
	require.NoDirExists(t, cgroup)
	require.NoDirExists(t, filepath.Join(rt.cfg.MicroVMStateRoot, vm.id))
	_ = inNetworkNamespace(vm.netns, func() error {
		_, err := netlink.LinkByName(microVMTapName)
		require.Error(t, err, "tap must be removed from the namespace")
		return nil
	})
	_, err = rt.State(context.Background(), vm.id)
	require.ErrorAs(t, err, &ErrContainerNotFound{})
}

func TestMicroVMGoprocOverTheWire(t *testing.T) {
	rt := requireMicroVMEnv(t)
	vm := newTestVM(t, rt, vmOptions{image: "alpine", goproc: true, hostname: "sandbox-vm"})
	vm.start()

	startedAt := time.Now()
	client := vm.goprocClient(60 * time.Second)
	t.Logf("goproc ready after %s", time.Since(startedAt))

	code, out := vm.exec(client, "uname", "-r")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "6.12.")

	code, out = vm.exec(client, "hostname")
	require.Equal(t, 0, code)
	require.Equal(t, "sandbox-vm", strings.TrimSpace(out))

	code, out = vm.sh(client, "cat /etc/resolv.conf && test -x /usr/bin/goproc && echo binds-ok")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "nameserver 1.1.1.1")
	require.Contains(t, out, "binds-ok")

	// Writes land on the block device, not the virtio-fs share.
	code, out = vm.sh(client, "echo hi > /written && df -T / | tail -1 && grep -w vda /proc/partitions && df -T "+microvm.DiskMount+" | tail -1")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "overlay")
	require.Contains(t, out, "vda")
	require.Contains(t, out, "ext4")

	killedAt := time.Now()
	require.NoError(t, rt.Kill(context.Background(), vm.id, syscall.SIGTERM, &KillOpts{All: true}))
	res := vm.wait(30 * time.Second)
	require.NoError(t, res.err, vm.output.String())
	// goproc shuts down gracefully on SIGTERM (exit 0); a process without a
	// handler would report 143. Either way the signal crossed into the guest.
	require.Contains(t, []int{0, 128 + int(syscall.SIGTERM)}, res.code, "SIGTERM must reach the process manager through vminit")
	require.Less(t, time.Since(killedAt), 10*time.Second, "VM must exit promptly after the process ends")
}

func TestMicroVMNetworkParity(t *testing.T) {
	rt := requireMicroVMEnv(t)
	port := bridgeHTTPServer(t)
	vm := newTestVM(t, rt, vmOptions{image: "alpine", goproc: true})
	vm.start()
	client := vm.goprocClient(60 * time.Second)

	// The guest answers at the veth's addresses with the veth's MAC.
	code, out := vm.sh(client, "ip -o link show eth0 && ip -o addr show eth0")
	require.Equal(t, 0, code, out)
	require.Contains(t, strings.ToLower(out), strings.ToLower(vm.vethMAC.String()))
	require.Contains(t, out, vm.ip4.String()+"/24")
	require.Contains(t, out, vm.ip6.String()+"/64")

	bridge, err := netlink.LinkByName(testBridgeName)
	require.NoError(t, err)
	neighbors, err := netlink.NeighList(bridge.Attrs().Index, netlink.FAMILY_V4)
	require.NoError(t, err)
	var seenMAC string
	for _, n := range neighbors {
		if n.IP.Equal(vm.ip4) {
			seenMAC = n.HardwareAddr.String()
		}
	}
	require.Equal(t, vm.vethMAC.String(), seenMAC, "bridge must learn the guest at the veth's MAC")

	// Guest -> host over both families.
	code, out = vm.sh(client, fmt.Sprintf("wget -qO- -T 5 http://%s:%d/", testBridgeIP4, port))
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "ok")
	code, out = vm.sh(client, fmt.Sprintf("wget -qO- -T 5 http://[%s]:%d/", testBridgeIP6, port))
	if code != 0 {
		t.Logf("IPv6 guest->host failed (host ip6tables may be unavailable): %s", out)
	} else {
		require.Contains(t, out, "ok")
	}

	// Egress through MASQUERADE, then blocked by a host FORWARD rule keyed on
	// the guest's IP, exactly like block_network.
	code, out = vm.sh(client, "nc -z -w 5 1.1.1.1 443 && echo egress-ok")
	require.Equal(t, 0, code, "egress via MASQUERADE: %s\n%s", out, vm.output.String())
	drop := []string{"-I", "FORWARD", "1", "-s", vm.ip4.String(), "!", "-o", testBridgeName, "-j", "DROP", "-m", "comment", "--comment", testComment}
	require.NoError(t, exec.Command("iptables", drop...).Run())
	t.Cleanup(func() { _ = exec.Command("iptables", append([]string{"-D", "FORWARD"}, drop[3:]...)...).Run() })
	code, _ = vm.sh(client, "nc -z -w 3 1.1.1.1 443")
	require.NotEqual(t, 0, code, "FORWARD DROP on the guest IP must block egress")

	// Anti-spoof: a frame sourced from another address never leaves the tap.
	code, out = vm.sh(client, fmt.Sprintf("nc -z -w 3 -s %s %s %d && echo own-ok", vm.ip4, testBridgeIP4, port))
	require.Equal(t, 0, code, out)
	code, out = vm.sh(client, fmt.Sprintf("ip addr add 10.200.0.250/24 dev eth0 && nc -z -w 3 -s 10.200.0.250 %s %d", testBridgeIP4, port))
	require.NotEqual(t, 0, code, "spoofed source must be dropped: %s", out)

	require.NoError(t, rt.Kill(context.Background(), vm.id, syscall.SIGKILL, nil))
	vm.wait(30 * time.Second)
	require.NoError(t, rt.Delete(context.Background(), vm.id, &DeleteOpts{Force: true}))
	_ = inNetworkNamespace(vm.netns, func() error {
		links, _ := netlink.LinkList()
		for _, link := range links {
			require.NotEqual(t, microVMTapName, link.Attrs().Name)
		}
		return nil
	})
}

func TestMicroVMDocker(t *testing.T) {
	rt := requireMicroVMEnv(t)
	vm := newTestVM(t, rt, vmOptions{image: "dind", goproc: true, docker: true, memoryMiB: 2048, cpuCores: 2})
	vm.start()
	client := vm.goprocClient(60 * time.Second)

	code, out := vm.sh(client, "mount | grep ' /var/lib/docker ' && stat -f -c %T /var/lib/docker")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "ext", "docker data root must sit on the block device")

	pid, err := client.Exec([]string{"dockerd"}, "/", []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}, false)
	require.NoError(t, err)

	ready := false
	for i := 0; i < 90 && !ready; i++ {
		code, _ = vm.exec(client, "docker", "info")
		ready = code == 0
		if !ready {
			time.Sleep(time.Second)
		}
	}
	if !ready {
		logs, _ := client.Stderr(pid)
		t.Fatalf("dockerd never became ready:\n%s\n%s", logs, vm.output.String())
	}

	code, out = vm.exec(client, "docker", "info")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "overlay2", "default storage driver on a real ext4")

	code, out = vm.exec(client, "docker", "run", "--rm", "hello-world")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "Hello from Docker!")

	// Real bridge networking inside the guest: a published port answers.
	code, out = vm.sh(client, "docker run -d --rm -p 8080:80 --name web nginx:alpine >/dev/null && sleep 2 && wget -qO- -T 5 http://127.0.0.1:8080/ | head -3")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "html")
	_, _ = vm.exec(client, "docker", "rm", "-f", "web")

	require.NoError(t, rt.Kill(context.Background(), vm.id, syscall.SIGKILL, nil))
	vm.wait(30 * time.Second)
}

func TestMicroVMDurableDiskOverVhostUser(t *testing.T) {
	rt := requireMicroVMEnv(t)
	// QSD 8.x has no `--export help`; an unsupported type fails parameter
	// validation ("does not accept value") before the node lookup does.
	out, _ := exec.Command("qemu-storage-daemon", "--export", "type=vhost-user-blk,id=probe,node-name=nope,addr.type=unix,addr.path=/tmp/probe.sock").CombinedOutput()
	require.NotContains(t, string(out), "does not accept value", "qemu-storage-daemon must be built with the vhost-user-blk export: %s", out)

	manager := disk.NewManager(disk.Config{Root: filepath.Join(testWorkRoot, "qcow")})
	ctx := context.Background()
	key := fmt.Sprintf("mvmdisk-%d", time.Now().UnixNano()%1_000_000)
	t.Cleanup(func() { _ = manager.Detach(context.Background(), key) })

	var freezeTarget atomic.Value
	freezeTarget.Store("")
	freeze := func(ctx context.Context) (func(), error) {
		return rt.FreezeDisk(ctx, freezeTarget.Load().(string), "")
	}
	volume, err := manager.Attach(ctx, disk.AttachSpec{Key: key, VirtualSizeBytes: 2 << 30, Export: disk.ExportVhostUser, Freeze: freeze}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, volume.ExportSocket())
	require.Empty(t, volume.Mountpoint(), "nothing is mounted on the host")

	// First VM writes persistent data and keeps writing while we seal.
	vm := newTestVM(t, rt, vmOptions{
		image:       "alpine",
		args:        []string{"sh", "-c", "echo persisted > /data.txt; sync; while true; do echo x >> /loop.txt; done"},
		annotations: map[string]string{MicroVMRootDiskAnnotation: volume.ExportSocket()},
	})
	freezeTarget.Store(vm.id)
	vm.start()
	require.NoFileExists(t, filepath.Join(filepath.Dir(vm.canvas), "scratch.ext4"), "durable root disk replaces the scratch disk")

	deadline := time.Now().Add(60 * time.Second)
	for !rt.vms[vm.id].ctrl.connected() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	require.True(t, rt.vms[vm.id].ctrl.connected(), "guest never connected: %s", vm.output.String())
	time.Sleep(2 * time.Second)

	sealed, skipped, err := volume.Seal(ctx, true)
	require.NoError(t, err)
	require.False(t, skipped)
	require.Len(t, sealed, 1)
	// The guest keeps running after the freeze/thaw cycle.
	require.True(t, rt.vms[vm.id].alive(), "VM must survive the seal")

	check, err := exec.Command("qemu-img", "check", sealed[0].Path).CombinedOutput()
	require.NoError(t, err, string(check))

	require.NoError(t, rt.Kill(ctx, vm.id, syscall.SIGKILL, nil))
	vm.wait(30 * time.Second)
	require.NoError(t, rt.Delete(ctx, vm.id, &DeleteOpts{Force: true}))
	require.NoError(t, manager.Detach(ctx, key))
	require.Empty(t, processesMatching("qemu-storage-daemon"), "detach must stop the daemon")

	// Re-attach the same key: local layers are reused and the data is there.
	volume, err = manager.Attach(ctx, disk.AttachSpec{Key: key, VirtualSizeBytes: 2 << 30, Export: disk.ExportVhostUser}, nil)
	require.NoError(t, err)
	vm2 := newTestVM(t, rt, vmOptions{
		image:       "alpine",
		args:        []string{"sh", "-c", "cat /data.txt; wc -l < /loop.txt"},
		annotations: map[string]string{MicroVMRootDiskAnnotation: volume.ExportSocket()},
	})
	vm2.start()
	res := vm2.wait(90 * time.Second)
	require.NoError(t, res.err, vm2.output.String())
	require.Equal(t, 0, res.code)
	require.Contains(t, vm2.output.String(), "persisted")
}

func TestMicroVMResourcesAndOOM(t *testing.T) {
	rt := requireMicroVMEnv(t)
	vm := newTestVM(t, rt, vmOptions{image: "alpine", goproc: true, memoryMiB: 512, cpuCores: 2})
	vm.start()
	client := vm.goprocClient(60 * time.Second)

	code, out := vm.exec(client, "nproc")
	require.Equal(t, 0, code)
	require.Equal(t, "2", strings.TrimSpace(out))

	code, out = vm.sh(client, "free -m | awk '/Mem:/{print $2}'")
	require.Equal(t, 0, code, out)
	total, err := strconv.Atoi(strings.TrimSpace(out))
	require.NoError(t, err, out)
	require.InDelta(t, 512, total, 80, "guest sees roughly the requested memory")

	cgroup := microVMCgroupPath(vm.spec, vm.id)
	max, err := os.ReadFile(filepath.Join(cgroup, "memory.max"))
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt((512<<20)+microVMMemoryHeadroom, 10), strings.TrimSpace(string(max)))
	cpuMax, err := os.ReadFile(filepath.Join(cgroup, "cpu.max"))
	require.NoError(t, err)
	require.Equal(t, "200000 100000", strings.TrimSpace(string(cpuMax)))
	procs, err := os.ReadFile(filepath.Join(cgroup, "cgroup.procs"))
	require.NoError(t, err)
	require.NotEmpty(t, strings.TrimSpace(string(procs)), "hypervisor must be in the VM cgroup")

	// Allocating past guest RAM is contained by the guest's own OOM killer;
	// the VMM (and its cgroup) are unaffected.
	code, out = vm.sh(client, "head -c 1200m /dev/zero | tail; echo exit=$?")
	require.Contains(t, out, "exit=137", "guest OOM killer must kill the allocator: %s", out)
	require.True(t, rt.vms[vm.id].alive(), "VM must survive a guest OOM")

	require.NoError(t, rt.Kill(context.Background(), vm.id, syscall.SIGKILL, nil))
	vm.wait(30 * time.Second)
}

func TestMicroVMTimingBaseline(t *testing.T) {
	rt := requireMicroVMEnv(t)

	var boots []time.Duration
	for i := 0; i < 3; i++ {
		vm := newTestVM(t, rt, vmOptions{image: "alpine", goproc: true})
		startedAt := time.Now()
		vm.start()
		vm.goprocClient(60 * time.Second)
		boots = append(boots, time.Since(startedAt))
		require.NoError(t, rt.Kill(context.Background(), vm.id, syscall.SIGKILL, nil))
		vm.wait(30 * time.Second)
		vm.cleanup()
	}
	t.Logf("boot-to-goproc-ready: %v", boots)

	if _, err := os.Stat(filepath.Join(testRootfsDir, "python.tar")); err == nil {
		vm := newTestVM(t, rt, vmOptions{image: "python", goproc: true, memoryMiB: 1024, cpuCores: 2})
		vm.start()
		client := vm.goprocClient(60 * time.Second)
		script := "import json, asyncio, http.client, unittest, xml.dom.minidom, email, sqlite3, decimal, dataclasses, typing"
		for i := 0; i < 3; i++ {
			startedAt := time.Now()
			code, out := vm.exec(client, "python3", "-c", script)
			require.Equal(t, 0, code, out)
			t.Logf("python stdlib import loop run %d: %s", i+1, time.Since(startedAt))
		}
		startedAt := time.Now()
		code, out := vm.sh(client, "dd if=/dev/zero of=/bench bs=1M count=256 conv=fsync 2>&1 | tail -1 && rm /bench")
		require.Equal(t, 0, code, out)
		t.Logf("256 MiB fsync write on the overlay upper: %s (%s)", time.Since(startedAt), strings.TrimSpace(out))
		require.NoError(t, rt.Kill(context.Background(), vm.id, syscall.SIGKILL, nil))
		vm.wait(30 * time.Second)
	}
}
