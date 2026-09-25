//go:build linux

// vminit is PID 1 inside a beam microvm. The kernel boots with the container
// rootfs (the worker's overlay "canvas") mounted read-only over virtio-fs and
// execs this binary from it. vminit then:
//
//  1. mounts the writable block device and layers an overlay (lower = the
//     virtio-fs share, upper = the disk) as the real root,
//  2. re-applies the OCI spec's bind and tmpfs mounts, binds the disk's docker
//     directory over /var/lib/docker when asked, mounts extra disks,
//  3. pivots into the new root, mounts the usual pseudo filesystems and
//     cgroup2, configures the NIC statically, and
//  4. runs the OCI process, reporting its pid and exit code to the host over
//     vsock while accepting signals and filesystem freeze requests.
//
// Any failure powers the VM off so the host sees a prompt exit instead of a
// hung guest. Everything here is the guest's own kernel and root; the VM is
// the isolation boundary, so the process runs with full privileges.
package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

const (
	deviceWaitTimeout  = 20 * time.Second
	controlDialTimeout = 30 * time.Second
	controlDialBackoff = 50 * time.Millisecond
	controlHeartbeat   = 2 * time.Second

	// linux/fs.h: _IOWR('X', 119, int) and _IOWR('X', 120, int).
	ioctlFIFREEZE = 0xC0045877
	ioctlFITHAW   = 0xC0045878
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Println("beam-vminit")
		return
	}
	if os.Getpid() != 1 {
		fmt.Fprintln(os.Stderr, "beam-vminit must run as PID 1 inside a microvm")
		os.Exit(2)
	}
	code, err := run()
	if err != nil {
		logf("fatal: %v", err)
	}
	powerOff(code)
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stdout, "vminit: "+format+"\n", args...)
}

// powerOff flushes and halts the VM; the host reads the exit code from the
// control channel, not from here.
func powerOff(code int) {
	logf("powering off (exit code %d)", code)
	unix.Sync()
	_ = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF)
	// Reboot only returns on failure; give the host something to kill.
	for {
		time.Sleep(time.Hour)
	}
}

func run() (int, error) {
	if err := mountEarly(); err != nil {
		return -1, err
	}
	vm, err := readVMSpec()
	if err != nil {
		return -1, err
	}
	spec, err := readOCISpec()
	if err != nil {
		return -1, err
	}

	if err := assembleRoot(vm); err != nil {
		return -1, fmt.Errorf("assemble root: %w", err)
	}
	if err := pivotRoot(); err != nil {
		return -1, fmt.Errorf("pivot root: %w", err)
	}
	if err := mountPseudo(); err != nil {
		return -1, fmt.Errorf("mount pseudo filesystems: %w", err)
	}
	hostname := firstNonEmpty(spec.Hostname, vm.Hostname)
	if hostname != "" {
		if err := unix.Sethostname([]byte(hostname)); err != nil {
			logf("set hostname %q: %v", hostname, err)
		}
	}
	if err := configureNetwork(vm.Network); err != nil {
		return -1, fmt.Errorf("configure network: %w", err)
	}

	if err := serveFS(microvm.FSPort); err != nil {
		return -1, err
	}
	ctrl, err := dialControl(vm.ControlPort)
	if err != nil {
		return -1, err
	}
	defer ctrl.close()

	return runProcess(spec.Process, ctrl)
}

// --- early boot ------------------------------------------------------------------

func mountEarly() error {
	// devtmpfs is usually auto-mounted; the rest are ours.
	_ = unix.Mount("devtmpfs", "/dev", "devtmpfs", unix.MS_NOSUID, "mode=0755")
	if err := mountIfMissing("proc", "/proc", "proc", unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC, ""); err != nil {
		return err
	}
	if err := mountIfMissing("sysfs", "/sys", "sysfs", unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC, ""); err != nil {
		return err
	}
	// Nothing propagates into a VM, but pivot_root refuses a shared root.
	return unix.Mount("", "/", "", unix.MS_PRIVATE|unix.MS_REC, "")
}

func mountIfMissing(source, target, fstype string, flags uintptr, data string) error {
	if isMountpoint(target) {
		return nil
	}
	if err := unix.Mount(source, target, fstype, flags, data); err != nil && !errors.Is(err, unix.EBUSY) {
		return fmt.Errorf("mount %s on %s: %w", fstype, target, err)
	}
	return nil
}

func isMountpoint(path string) bool {
	var self, parent unix.Stat_t
	if err := unix.Stat(path, &self); err != nil {
		return false
	}
	if err := unix.Stat(filepath.Dir(path), &parent); err != nil {
		return false
	}
	return self.Dev != parent.Dev
}

func readVMSpec() (*microvm.Spec, error) {
	data, err := os.ReadFile(microvm.SpecFile)
	if err != nil {
		return nil, fmt.Errorf("read vm spec: %w", err)
	}
	var vm microvm.Spec
	if err := json.Unmarshal(data, &vm); err != nil {
		return nil, fmt.Errorf("decode vm spec: %w", err)
	}
	if vm.RootDisk == "" {
		return nil, errors.New("vm spec has no root disk")
	}
	if vm.ControlPort == 0 {
		vm.ControlPort = microvm.ControlPort
	}
	return &vm, nil
}

func readOCISpec() (*specs.Spec, error) {
	data, err := os.ReadFile(microvm.OCISpecFile)
	if err != nil {
		return nil, fmt.Errorf("read oci spec: %w", err)
	}
	var spec specs.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("decode oci spec: %w", err)
	}
	if spec.Process == nil || len(spec.Process.Args) == 0 {
		return nil, errors.New("oci spec has no process to run")
	}
	return &spec, nil
}

// --- root assembly -----------------------------------------------------------------

func assembleRoot(vm *microvm.Spec) error {
	if err := waitForDevice(vm.RootDisk); err != nil {
		return err
	}
	if err := unix.Mount(vm.RootDisk, microvm.DiskMount, "ext4", 0, ""); err != nil {
		return fmt.Errorf("mount %s: %w", vm.RootDisk, err)
	}
	upper := filepath.Join(microvm.DiskMount, microvm.DiskOverlayUpper)
	work := filepath.Join(microvm.DiskMount, microvm.DiskOverlayWork)
	docker := filepath.Join(microvm.DiskMount, microvm.DiskDockerDir)
	for _, dir := range []string{upper, docker} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	// overlayfs wants a work dir it created; one left by a previous mount
	// (possibly by a container runtime on another host) is rejected.
	if err := os.RemoveAll(work); err != nil {
		return err
	}
	if err := os.MkdirAll(work, 0o755); err != nil {
		return err
	}

	// A non-recursive bind of the virtio-fs root gives the overlay a lower
	// without the pseudo filesystems and submounts stacked on it.
	if err := unix.Mount("/", microvm.ImageMount, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind image root: %w", err)
	}
	overlayOpts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", microvm.ImageMount, upper, work)
	if err := unix.Mount("overlay", microvm.NewRoot, "overlay", 0, overlayOpts); err != nil {
		return fmt.Errorf("mount overlay (%s): %w", overlayOpts, err)
	}

	newRoot := microvm.NewRoot
	// Keep the disk reachable in the final root for FIFREEZE and Docker.
	if err := os.MkdirAll(filepath.Join(newRoot, microvm.DiskMount), 0o755); err != nil {
		return err
	}
	if err := unix.Mount(microvm.DiskMount, filepath.Join(newRoot, microvm.DiskMount), "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind disk into new root: %w", err)
	}

	for _, bind := range vm.Binds {
		if err := applyBind(newRoot, bind); err != nil {
			return err
		}
	}
	for _, tmpfs := range vm.Tmpfs {
		if err := applyTmpfs(newRoot, tmpfs); err != nil {
			return err
		}
	}
	if vm.Docker {
		target := filepath.Join(newRoot, microvm.DockerDataRoot)
		if err := os.MkdirAll(target, 0o710); err != nil {
			return err
		}
		if err := unix.Mount(docker, target, "", unix.MS_BIND, ""); err != nil {
			return fmt.Errorf("bind docker data root: %w", err)
		}
	}
	for _, disk := range vm.Disks {
		if err := waitForDevice(disk.Device); err != nil {
			return err
		}
		target := filepath.Join(newRoot, disk.MountPath)
		if err := os.MkdirAll(target, 0o755); err != nil {
			return err
		}
		var flags uintptr
		if disk.ReadOnly {
			flags = unix.MS_RDONLY
		}
		if err := unix.Mount(disk.Device, target, "ext4", flags, ""); err != nil {
			return fmt.Errorf("mount %s on %s: %w", disk.Device, disk.MountPath, err)
		}
	}
	return nil
}

func waitForDevice(device string) error {
	deadline := time.Now().Add(deviceWaitTimeout)
	for {
		if _, err := os.Stat(device); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("block device %s did not appear within %s", device, deviceWaitTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// applyBind binds a host-provided mount from the virtio-fs root (where it is
// a submount the kernel auto-mounts on first access, under BindsDir) to its
// destination in the new root. The source path is resolved through the
// virtio-fs mount, never through the overlay, which cannot cross submounts.
func applyBind(newRoot string, bind microvm.Bind) error {
	source := firstNonEmpty(bind.Source, bind.Destination)
	target := filepath.Join(newRoot, bind.Destination)
	if bind.File {
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if _, err := os.Stat(target); err != nil {
			if err := os.WriteFile(target, nil, 0o644); err != nil {
				return err
			}
		}
	} else if err := os.MkdirAll(target, 0o755); err != nil {
		return err
	}
	// Touch the source so the virtio-fs submount is instantiated.
	if _, err := os.Stat(source); err != nil {
		return fmt.Errorf("bind source %s: %w", source, err)
	}
	if err := unix.Mount(source, target, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
		return fmt.Errorf("bind %s: %w", bind.Destination, err)
	}
	if bind.ReadOnly {
		if err := unix.Mount("", target, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, ""); err != nil {
			return fmt.Errorf("remount %s read-only: %w", bind.Destination, err)
		}
	}
	return nil
}

func applyTmpfs(newRoot string, tmpfs microvm.Tmpfs) error {
	target := filepath.Join(newRoot, tmpfs.Destination)
	if err := os.MkdirAll(target, 0o755); err != nil {
		return err
	}
	flags, data := parseMountOptions(tmpfs.Options)
	if err := unix.Mount("tmpfs", target, "tmpfs", flags, data); err != nil {
		return fmt.Errorf("mount tmpfs on %s: %w", tmpfs.Destination, err)
	}
	return nil
}

// parseMountOptions turns OCI mount options into mount(2) flags plus the
// filesystem-specific data string.
func parseMountOptions(options []string) (uintptr, string) {
	var flags uintptr
	var data []string
	for _, option := range options {
		switch option {
		case "ro":
			flags |= unix.MS_RDONLY
		case "nosuid":
			flags |= unix.MS_NOSUID
		case "nodev":
			flags |= unix.MS_NODEV
		case "noexec":
			flags |= unix.MS_NOEXEC
		case "noatime":
			flags |= unix.MS_NOATIME
		case "relatime":
			flags |= unix.MS_RELATIME
		case "strictatime":
			flags |= unix.MS_STRICTATIME
		case "sync":
			flags |= unix.MS_SYNCHRONOUS
		case "rw", "bind", "rbind", "private", "rprivate", "shared", "rshared", "slave", "rslave", "unbindable", "runbindable", "defaults":
		default:
			data = append(data, option)
		}
	}
	return flags, strings.Join(data, ",")
}

func pivotRoot() error {
	newRoot := microvm.NewRoot
	oldRoot := filepath.Join(newRoot, strings.TrimPrefix(microvm.OldRoot, "/"))
	if err := os.MkdirAll(oldRoot, 0o755); err != nil {
		return err
	}
	if err := unix.Chdir(newRoot); err != nil {
		return err
	}
	if err := unix.PivotRoot(".", strings.TrimPrefix(microvm.OldRoot, "/")); err != nil {
		return fmt.Errorf("pivot_root: %w", err)
	}
	if err := unix.Chroot("."); err != nil {
		return err
	}
	if err := unix.Chdir("/"); err != nil {
		return err
	}
	// The overlay keeps its own references to the lower; the old tree can go.
	if err := unix.Unmount(microvm.OldRoot, unix.MNT_DETACH); err != nil {
		return fmt.Errorf("detach old root: %w", err)
	}
	return os.Remove(microvm.OldRoot)
}

func mountPseudo() error {
	mounts := []struct {
		source, target, fstype string
		flags                  uintptr
		data                   string
	}{
		{"proc", "/proc", "proc", unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC, ""},
		{"sysfs", "/sys", "sysfs", unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC, ""},
		{"devtmpfs", "/dev", "devtmpfs", unix.MS_NOSUID, "mode=0755"},
		{"devpts", "/dev/pts", "devpts", unix.MS_NOSUID | unix.MS_NOEXEC, "newinstance,ptmxmode=0666,mode=0620,gid=5"},
		{"shm", "/dev/shm", "tmpfs", unix.MS_NOSUID | unix.MS_NODEV, "mode=1777"},
		{"mqueue", "/dev/mqueue", "mqueue", unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC, ""},
		{"run", "/run", "tmpfs", unix.MS_NOSUID | unix.MS_NODEV, "mode=0755"},
		{"cgroup2", "/sys/fs/cgroup", "cgroup2", unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC, "nsdelegate"},
	}
	for _, m := range mounts {
		if err := os.MkdirAll(m.target, 0o755); err != nil {
			return err
		}
		if isMountpoint(m.target) {
			continue
		}
		if err := unix.Mount(m.source, m.target, m.fstype, m.flags, m.data); err != nil {
			if m.fstype == "mqueue" || m.fstype == "devpts" {
				logf("optional mount %s failed: %v", m.target, err)
				continue
			}
			return fmt.Errorf("mount %s on %s: %w", m.fstype, m.target, err)
		}
	}
	// /dev/ptmx must point at this instance's devpts.
	_ = os.Remove("/dev/ptmx")
	_ = os.Symlink("pts/ptmx", "/dev/ptmx")

	// Delegate every controller so Docker (and the process manager's cgroup
	// setup) can create subgroups with limits.
	if controllers, err := os.ReadFile("/sys/fs/cgroup/cgroup.controllers"); err == nil {
		for _, controller := range strings.Fields(string(controllers)) {
			_ = os.WriteFile("/sys/fs/cgroup/cgroup.subtree_control", []byte("+"+controller), 0o644)
		}
	}
	return nil
}

// --- network -----------------------------------------------------------------------

// reconfigureNetwork replaces the NIC's addresses with cfg's. After a
// restore the guest still holds the checkpointed container's addresses and
// the old gateway's neighbour entry; both must go before the new ones work.
func reconfigureNetwork(cfg microvm.Network) error {
	link, err := findNIC(cfg.MAC)
	if err != nil {
		return err
	}
	for _, family := range []int{netlink.FAMILY_V4, netlink.FAMILY_V6} {
		addrs, _ := netlink.AddrList(link, family)
		for _, addr := range addrs {
			if addr.Scope != int(netlink.SCOPE_LINK) {
				_ = netlink.AddrDel(link, &addr)
			}
		}
		neighs, _ := netlink.NeighList(link.Attrs().Index, family)
		for _, neigh := range neighs {
			_ = netlink.NeighDel(&neigh)
		}
	}
	return configureNetwork(cfg)
}

func configureNetwork(cfg microvm.Network) error {
	if lo, err := netlink.LinkByName("lo"); err == nil {
		_ = netlink.LinkSetUp(lo)
	}
	link, err := findNIC(cfg.MAC)
	if err != nil {
		return err
	}
	name := link.Attrs().Name
	if cfg.MTU > 0 {
		if err := netlink.LinkSetMTU(link, cfg.MTU); err != nil {
			return fmt.Errorf("set mtu: %w", err)
		}
	}
	// Match what the worker does for containers: IPv6 usable, no DAD delay,
	// forwarding on so Docker's bridge works.
	writeSysctl("/proc/sys/net/ipv4/ip_forward", "1")
	writeSysctl("/proc/sys/net/ipv6/conf/all/disable_ipv6", "0")
	writeSysctl("/proc/sys/net/ipv6/conf/"+name+"/disable_ipv6", "0")
	writeSysctl("/proc/sys/net/ipv6/conf/"+name+"/accept_dad", "0")
	writeSysctl("/proc/sys/net/ipv6/ip_nonlocal_bind", "1")

	if err := netlink.LinkSetUp(link); err != nil {
		return fmt.Errorf("bring %s up: %w", name, err)
	}
	if cfg.IPv4 != "" {
		addr, err := netlink.ParseAddr(cfg.IPv4)
		if err != nil {
			return fmt.Errorf("parse ipv4 %q: %w", cfg.IPv4, err)
		}
		if err := netlink.AddrReplace(link, addr); err != nil {
			return fmt.Errorf("add %s: %w", cfg.IPv4, err)
		}
		if cfg.Gateway4 != "" {
			gw := net.ParseIP(cfg.Gateway4)
			if err := netlink.RouteReplace(&netlink.Route{LinkIndex: link.Attrs().Index, Gw: gw, Scope: netlink.SCOPE_UNIVERSE}); err != nil {
				return fmt.Errorf("add default route via %s: %w", cfg.Gateway4, err)
			}
		}
	}
	if cfg.IPv6 != "" {
		addr, err := netlink.ParseAddr(cfg.IPv6)
		if err != nil {
			return fmt.Errorf("parse ipv6 %q: %w", cfg.IPv6, err)
		}
		addr.Flags = unix.IFA_F_NODAD
		if err := netlink.AddrReplace(link, addr); err != nil {
			return fmt.Errorf("add %s: %w", cfg.IPv6, err)
		}
		if cfg.Gateway6 != "" {
			gw := net.ParseIP(cfg.Gateway6)
			_, def, _ := net.ParseCIDR("::/0")
			if err := netlink.RouteReplace(&netlink.Route{LinkIndex: link.Attrs().Index, Dst: def, Gw: gw}); err != nil {
				return fmt.Errorf("add default ipv6 route via %s: %w", cfg.Gateway6, err)
			}
		}
	}
	return nil
}

func findNIC(mac string) (netlink.Link, error) {
	links, err := netlink.LinkList()
	if err != nil {
		return nil, err
	}
	// Only the virtio NIC qualifies as a fallback: the kernel's dummy0 and
	// anything Docker creates in the guest are software links. A restored
	// guest keeps the snapshot's MAC, so the fallback is what finds eth0
	// when the host pushes the new container's addresses.
	var fallback netlink.Link
	for _, link := range links {
		attrs := link.Attrs()
		if attrs.Flags&net.FlagLoopback != 0 || attrs.Name == "lo" || link.Type() != "device" {
			continue
		}
		if mac != "" && strings.EqualFold(attrs.HardwareAddr.String(), mac) {
			return link, nil
		}
		if fallback == nil && attrs.HardwareAddr != nil {
			fallback = link
		}
	}
	if fallback != nil {
		return fallback, nil
	}
	return nil, errors.New("no network interface found")
}

func writeSysctl(path, value string) {
	if err := os.WriteFile(path, []byte(value), 0o644); err != nil && !os.IsNotExist(err) {
		logf("sysctl %s=%s: %v", path, value, err)
	}
}

// --- control channel -------------------------------------------------------------------

type control struct {
	port uint32
	mu   sync.Mutex
	file *os.File
	enc  *microvm.Encoder
}

// controlReconnectTimeout bounds how long init keeps trying to reach the
// host after its connection dies; a restored VM's host listener is up
// before the guest resumes, so this only has to absorb the restore itself.
const controlReconnectTimeout = 2 * time.Minute

func dialControl(port uint32) (*control, error) {
	c := &control{port: port}
	if err := c.connect(controlDialTimeout); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *control) connect(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			return fmt.Errorf("vsock socket: %w", err)
		}
		err = unix.Connect(fd, &unix.SockaddrVM{CID: microvm.HostCID, Port: c.port})
		if err == nil {
			file := os.NewFile(uintptr(fd), "vsock")
			c.mu.Lock()
			if c.file != nil {
				_ = c.file.Close()
			}
			c.file, c.enc = file, microvm.NewEncoder(file)
			c.mu.Unlock()
			return nil
		}
		unix.Close(fd)
		if time.Now().After(deadline) {
			return fmt.Errorf("connect to host vsock port %d: %w", c.port, err)
		}
		time.Sleep(controlDialBackoff)
	}
}

func (c *control) current() *os.File {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.file
}

func (c *control) send(msg microvm.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.enc.Encode(msg); err != nil {
		logf("control send %s: %v", msg.Type, err)
	}
}

func (c *control) ack(id uint64, err error) {
	msg := microvm.Message{Type: microvm.MsgAck, ID: id, OK: err == nil}
	if err != nil {
		msg.Error = err.Error()
	}
	c.send(msg)
}

func (c *control) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.file.Close()
}

// heartbeat keeps a trickle of traffic on the connection so a restored VM,
// whose connection died with the snapshot, finds out and reconnects.
func (c *control) heartbeat(interval time.Duration) {
	for range time.Tick(interval) {
		c.send(microvm.Message{Type: microvm.MsgPing})
	}
}

// serve handles host commands. Signals go to the container process;
// freeze/thaw act on a mounted filesystem, the root disk by default. When
// the stream dies the guest was most likely restored from a snapshot: init
// reconnects to the (new) host, reports itself started again and takes the
// container's new network configuration from the host.
func (c *control) serve(childPid func() int) {
	frozen := map[string]*os.File{}
	dec := microvm.NewDecoder(c.current())
	for {
		msg, err := dec.Decode()
		if err != nil {
			for path, dir := range frozen {
				_ = unix.IoctlSetInt(int(dir.Fd()), ioctlFITHAW, 0)
				dir.Close()
				delete(frozen, path)
			}
			logf("control connection lost (%v); reconnecting", err)
			if err := c.connect(controlReconnectTimeout); err != nil {
				logf("control reconnect failed: %v", err)
				return
			}
			logf("control reconnected")
			c.send(microvm.Message{Type: microvm.MsgStarted, Pid: childPid()})
			dec = microvm.NewDecoder(c.current())
			continue
		}
		switch msg.Type {
		case microvm.MsgPing:
		case microvm.MsgNetwork:
			if msg.Network == nil {
				c.ack(msg.ID, errors.New("network config is missing"))
				continue
			}
			err := reconfigureNetwork(*msg.Network)
			logf("network reconfigured to %s %s: %v", msg.Network.IPv4, msg.Network.IPv6, err)
			c.ack(msg.ID, err)
		case microvm.MsgSignal:
			pid := childPid()
			if pid <= 0 {
				c.ack(msg.ID, errors.New("container process is not running"))
				continue
			}
			c.ack(msg.ID, unix.Kill(pid, syscall.Signal(msg.Signal)))
		case microvm.MsgFreeze:
			path := firstNonEmpty(msg.Text, microvm.DiskMount)
			if _, ok := frozen[path]; ok {
				c.ack(msg.ID, nil)
				continue
			}
			dir, err := os.Open(path)
			if err == nil {
				unix.Sync()
				err = unix.IoctlSetInt(int(dir.Fd()), ioctlFIFREEZE, 0)
				if err != nil {
					dir.Close()
				} else {
					frozen[path] = dir
				}
			}
			c.ack(msg.ID, err)
		case microvm.MsgThaw:
			path := firstNonEmpty(msg.Text, microvm.DiskMount)
			dir, ok := frozen[path]
			if !ok {
				c.ack(msg.ID, nil)
				continue
			}
			err := unix.IoctlSetInt(int(dir.Fd()), ioctlFITHAW, 0)
			dir.Close()
			delete(frozen, path)
			c.ack(msg.ID, err)
		default:
			c.ack(msg.ID, fmt.Errorf("unknown command %q", msg.Type))
		}
	}
}

// --- guest filesystem service ----------------------------------------------------------

const maxSearchLineBytes = 16 << 20

// serveFS accepts one vsock connection per filesystem operation from the
// host: a JSON header line, raw payload bytes for writes, then a JSON reply
// line and raw file bytes for reads. Started before the container process so
// the worker's file RPCs work as soon as the sandbox is reachable.
func serveFS(port uint32) error {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("vsock socket: %w", err)
	}
	if err := unix.Bind(fd, &unix.SockaddrVM{CID: unix.VMADDR_CID_ANY, Port: port}); err != nil {
		unix.Close(fd)
		return fmt.Errorf("bind vsock port %d: %w", port, err)
	}
	if err := unix.Listen(fd, 16); err != nil {
		unix.Close(fd)
		return fmt.Errorf("listen vsock port %d: %w", port, err)
	}
	go func() {
		for {
			nfd, _, err := unix.Accept(fd)
			if err != nil {
				if errors.Is(err, unix.EINTR) {
					continue
				}
				logf("fs accept: %v", err)
				return
			}
			go func(conn *os.File) {
				defer conn.Close()
				if err := handleFSConn(conn); err != nil {
					logf("fs: %v", err)
				}
			}(os.NewFile(uintptr(nfd), "vsock-fs"))
		}
	}()
	return nil
}

func handleFSConn(conn *os.File) error {
	reader := bufio.NewReaderSize(conn, 64<<10)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("read fs header: %w", err)
	}
	var req microvm.FSRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return fmt.Errorf("decode fs header: %w", err)
	}

	reply, body := handleFS(req, reader)
	header, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	if _, err := conn.Write(append(header, '\n')); err != nil {
		return err
	}
	if body != nil {
		defer body.Close()
		if reply.Length == microvm.FSStreamUntilEOF {
			if _, err := io.Copy(conn, body); err != nil {
				return fmt.Errorf("stream %s: %w", req.Path, err)
			}
			return nil
		}
		if _, err := io.CopyN(conn, body, reply.Length); err != nil {
			return fmt.Errorf("stream %s: %w", req.Path, err)
		}
	}
	return nil
}

// handleFS performs one filesystem operation inside the guest. Paths are the
// container's own paths: the guest is the sandbox, so there is nothing to
// escape from. For reads it returns the open file to stream; for writes it
// consumes exactly req.Length bytes from payload.
func handleFS(req microvm.FSRequest, payload io.Reader) (microvm.FSResponse, io.ReadCloser) {
	reply, body, err := doFS(req, payload)
	if err != nil {
		if body != nil {
			body.Close()
		}
		return microvm.FSResponse{Error: err.Error()}, nil
	}
	reply.OK = true
	return reply, body
}

func doFS(req microvm.FSRequest, payload io.Reader) (microvm.FSResponse, io.ReadCloser, error) {
	var none microvm.FSResponse
	path := filepath.Clean(req.Path)
	if !filepath.IsAbs(path) {
		return none, nil, fmt.Errorf("path %q must be absolute", req.Path)
	}
	switch req.Op {
	case microvm.FSOpRead:
		file, err := os.Open(path)
		if err != nil {
			return none, nil, err
		}
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return none, nil, err
		}
		if req.Offset > 0 {
			if _, err := file.Seek(req.Offset, io.SeekStart); err != nil {
				file.Close()
				return none, nil, err
			}
		}
		length := max(info.Size()-req.Offset, 0)
		if req.Length > 0 && req.Length < length {
			length = req.Length
		}
		return microvm.FSResponse{Length: length}, file, nil
	case microvm.FSOpWrite:
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return none, nil, err
		}
		flags := os.O_CREATE | os.O_WRONLY
		if req.Offset == 0 {
			flags |= os.O_TRUNC
		}
		mode := os.FileMode(req.Mode)
		if mode == 0 {
			mode = 0o644
		}
		file, err := os.OpenFile(path, flags, mode)
		if err != nil {
			return none, nil, err
		}
		if req.Offset > 0 {
			if _, err := file.Seek(req.Offset, io.SeekStart); err != nil {
				file.Close()
				return none, nil, err
			}
		}
		if _, err := io.CopyN(file, payload, req.Length); err != nil {
			file.Close()
			return none, nil, fmt.Errorf("write payload: %w", err)
		}
		if err := file.Close(); err != nil {
			return none, nil, err
		}
		if req.Mode != 0 {
			_ = os.Chmod(path, mode)
		}
		return none, nil, nil
	case microvm.FSOpMkdir:
		mode := os.FileMode(req.Mode)
		if mode == 0 {
			mode = 0o755
		}
		return none, nil, os.MkdirAll(path, mode)
	case microvm.FSOpRemove:
		return none, nil, os.RemoveAll(path)
	case microvm.FSOpStat:
		info, err := os.Stat(path)
		if err != nil {
			return none, nil, err
		}
		fi := fsFileInfo(info)
		return microvm.FSResponse{Info: &fi}, nil, nil
	case microvm.FSOpList:
		entries, err := os.ReadDir(path)
		if err != nil {
			return none, nil, err
		}
		infos := make([]microvm.FSFileInfo, 0, len(entries))
		for _, entry := range entries {
			info, err := os.Lstat(filepath.Join(path, entry.Name()))
			if err != nil {
				return none, nil, err
			}
			infos = append(infos, fsFileInfo(info))
		}
		return microvm.FSResponse{Entries: infos}, nil, nil
	case microvm.FSOpReplace:
		regex, err := regexp.Compile(req.Pattern)
		if err != nil {
			return none, nil, err
		}
		err = walkRegularFiles(path, func(name string) error {
			content, err := os.ReadFile(name)
			if err != nil {
				return err
			}
			if !regex.Match(content) {
				return nil
			}
			return os.WriteFile(name, regex.ReplaceAll(content, []byte(req.Replacement)), 0o644)
		})
		return none, nil, err
	case microvm.FSOpFind:
		regex, err := regexp.Compile(req.Pattern)
		if err != nil {
			return none, nil, err
		}
		var results []microvm.FSSearchResult
		err = walkRegularFiles(path, func(name string) error {
			file, err := os.Open(name)
			if err != nil {
				return err
			}
			defer file.Close()
			matches, err := searchFile(file, regex)
			if len(matches) > 0 {
				results = append(results, microvm.FSSearchResult{Path: name, Matches: matches})
			}
			return err
		})
		return microvm.FSResponse{Results: results}, nil, err
	case microvm.FSOpArchive:
		info, err := os.Stat(path)
		if err != nil {
			return none, nil, err
		}
		if !info.IsDir() {
			return none, nil, fmt.Errorf("%s is not a directory", path)
		}
		pr, pw := io.Pipe()
		go func() { pw.CloseWithError(writeTree(pw, path, req.Exclude)) }()
		return microvm.FSResponse{Length: microvm.FSStreamUntilEOF}, pr, nil
	default:
		return none, nil, fmt.Errorf("unknown fs op %q", req.Op)
	}
}

// writeTree streams root as a PAX tar the host can replay into an overlay
// upper directory: ownership, modes, mtimes, symlinks, FIFOs, character and
// block devices (overlay whiteouts are 0:0 character devices) and every
// xattr (trusted.overlay.opaque marks opaque directories). Sockets are
// skipped. Hard links become independent copies. A file that changes size
// mid-stream is truncated or zero-padded to the size in its header.
func writeTree(w io.Writer, root string, exclude []string) error {
	tw := tar.NewWriter(w)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || rel == "." {
			return err
		}
		for _, ex := range exclude {
			ex = strings.Trim(filepath.Clean("/"+ex), "/")
			if rel == ex || strings.HasPrefix(rel, ex+"/") {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSocket != 0 {
			return nil
		}
		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			if link, err = os.Readlink(path); err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if info.IsDir() {
			hdr.Name += "/"
		}
		hdr.Format = tar.FormatPAX
		hdr.Uname, hdr.Gname = "", ""
		if names, err := listXattrs(path); err == nil {
			for _, name := range names {
				value, err := getXattr(path, name)
				if err != nil {
					continue
				}
				if hdr.PAXRecords == nil {
					hdr.PAXRecords = map[string]string{}
				}
				hdr.PAXRecords["SCHILY.xattr."+name] = string(value)
			}
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if !info.Mode().IsRegular() || hdr.Size == 0 {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		n, err := io.CopyN(tw, file, hdr.Size)
		if err == io.EOF {
			_, err = io.CopyN(tw, zeroReader{}, hdr.Size-n)
		}
		return err
	})
	if err != nil {
		return err
	}
	return tw.Close()
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func listXattrs(path string) ([]string, error) {
	buf := make([]byte, 4096)
	for {
		n, err := unix.Llistxattr(path, buf)
		if err == unix.ERANGE {
			buf = make([]byte, len(buf)*2)
			continue
		}
		if err != nil {
			return nil, err
		}
		var names []string
		for _, name := range bytes.Split(buf[:n], []byte{0}) {
			if len(name) > 0 {
				names = append(names, string(name))
			}
		}
		return names, nil
	}
}

func getXattr(path, name string) ([]byte, error) {
	buf := make([]byte, 4096)
	for {
		n, err := unix.Lgetxattr(path, name, buf)
		if err == unix.ERANGE {
			buf = make([]byte, len(buf)*2)
			continue
		}
		if err != nil {
			return nil, err
		}
		return buf[:n], nil
	}
}

func fsFileInfo(info os.FileInfo) microvm.FSFileInfo {
	fi := microvm.FSFileInfo{
		Name:    info.Name(),
		Size:    info.Size(),
		Mode:    uint32(info.Mode()),
		ModTime: info.ModTime().Unix(),
		IsDir:   info.IsDir(),
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		fi.UID, fi.GID = st.Uid, st.Gid
	}
	return fi
}

func walkRegularFiles(base string, visit func(string) error) error {
	return filepath.WalkDir(base, func(name string, entry os.DirEntry, err error) error {
		if err == nil && entry.Type().IsRegular() {
			return visit(name)
		}
		return err
	})
}

// searchFile reports regex matches as 1-based line and column ranges, the
// same shape the worker computes for container runtimes. A file with a NUL
// byte is binary and has no matches.
func searchFile(r io.Reader, regex *regexp.Regexp) ([]microvm.FSMatch, error) {
	var matches []microvm.FSMatch
	scanner := bufio.NewScanner(r)
	scanner.Buffer(nil, maxSearchLineBytes)
	for line := int32(1); scanner.Scan(); line++ {
		text := scanner.Bytes()
		if bytes.IndexByte(text, 0) >= 0 {
			return nil, nil
		}
		for _, loc := range regex.FindAllIndex(text, -1) {
			matches = append(matches, microvm.FSMatch{Line: line, StartCol: int32(loc[0] + 1), EndCol: int32(loc[1]), Content: string(text[loc[0]:loc[1]])})
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, bufio.ErrTooLong) {
		return nil, err
	}
	return matches, nil
}

// --- container process --------------------------------------------------------------------

func runProcess(proc *specs.Process, ctrl *control) (int, error) {
	env := proc.Env
	if !envHas(env, "PATH=") {
		env = append(env, "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	}
	// exec.LookPath consults this process's PATH, not the child's.
	for _, kv := range env {
		if key, value, ok := strings.Cut(kv, "="); ok && key == "PATH" {
			os.Setenv("PATH", value)
		}
	}
	binary, err := exec.LookPath(proc.Args[0])
	if err != nil {
		return -1, fmt.Errorf("resolve %q: %w", proc.Args[0], err)
	}

	cmd := exec.Command(binary, proc.Args[1:]...)
	cmd.Env = env
	cmd.Dir = firstNonEmpty(proc.Cwd, "/")
	cmd.Stdin = nil
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
		Credential: &syscall.Credential{
			Uid:    proc.User.UID,
			Gid:    proc.User.GID,
			Groups: proc.User.AdditionalGids,
		},
	}

	// Reap everything as PID 1; the container process's own status is what
	// we report.
	sigchld := make(chan os.Signal, 16)
	signal.Notify(sigchld, unix.SIGCHLD)

	if err := cmd.Start(); err != nil {
		return -1, fmt.Errorf("start %v: %w", proc.Args, err)
	}
	pid := cmd.Process.Pid
	ctrl.send(microvm.Message{Type: microvm.MsgStarted, Pid: pid})
	go ctrl.serve(func() int { return pid })
	go ctrl.heartbeat(controlHeartbeat)
	logf("started %v as pid %d", proc.Args, pid)

	for range sigchld {
		for {
			var status unix.WaitStatus
			reaped, err := unix.Wait4(-1, &status, unix.WNOHANG, nil)
			if err != nil || reaped <= 0 {
				break
			}
			if reaped != pid {
				continue
			}
			code := exitCode(status)
			// Flush before the host learns the process is gone: sync(2) sends
			// FUSE_SYNCFS for every virtiofs superblock, which reaches the host
			// filesystems behind the bind mounts (volumes on FUSE-backed object
			// storage flush to the bucket here). Reporting first would let the
			// host tear the VM down while that flush is still in flight.
			syncStart := time.Now()
			unix.Sync()
			if took := time.Since(syncStart); took > time.Second {
				logf("synced filesystems in %s", took.Round(time.Millisecond))
			}
			ctrl.send(microvm.Message{Type: microvm.MsgExit, Code: code})
			return code, nil
		}
	}
	return -1, errors.New("signal channel closed")
}

func exitCode(status unix.WaitStatus) int {
	if status.Exited() {
		return status.ExitStatus()
	}
	if status.Signaled() {
		return 128 + int(status.Signal())
	}
	return -1
}

func envHas(env []string, prefix string) bool {
	for _, kv := range env {
		if strings.HasPrefix(kv, prefix) {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
