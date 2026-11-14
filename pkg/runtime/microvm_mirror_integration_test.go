//go:build linux && microvm

package runtime

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/mirror"
	"github.com/beam-cloud/clip/pkg/clip"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netlink/nl"
	"golang.org/x/sys/unix"
)

// prepareBridgedTap stands in for plumbNetwork on hosts whose kernel has no
// tc mirred action (MICROVM_TEST_BRIDGED_TAP=1): the tap and the veth share a
// bridge inside the namespace instead of redirecting into each other. The
// guest still owns the veth's addresses and MAC, so the veth takes another
// MAC or the bridge would keep the guest's frames for itself. Test-only: it
// has none of the anti-spoof filters.
func prepareBridgedTap(rt *MicroVM, nsPath string) error {
	var prepared preparedNetwork
	network := &prepared.Network
	err := inNetworkNamespace(nsPath, func() error {
		veth, err := findVeth()
		if err != nil {
			return err
		}
		attrs := veth.Attrs()
		network.MAC = attrs.HardwareAddr.String()
		network.MTU = attrs.MTU
		v4, err := netlink.AddrList(veth, netlink.FAMILY_V4)
		if err != nil {
			return err
		}
		v6, err := netlink.AddrList(veth, netlink.FAMILY_V6)
		if err != nil {
			return err
		}
		if len(v4) == 0 {
			return fmt.Errorf("veth %s has no IPv4 address", attrs.Name)
		}
		network.IPv4 = v4[0].IPNet.String()
		for _, addr := range v6 {
			if !addr.IP.IsLinkLocalUnicast() {
				network.IPv6 = addr.IPNet.String()
				break
			}
		}
		network.Gateway4 = defaultGateway(veth, netlink.FAMILY_V4)
		if network.IPv6 != "" {
			network.Gateway6 = defaultGateway(veth, netlink.FAMILY_V6)
		}
		_ = netlink.LinkSetIP6AddrGenMode(veth, nl.IN6_ADDR_GEN_MODE_NONE)
		for _, addr := range append(v4, v6...) {
			if err := netlink.AddrDel(veth, &addr); err != nil && !errors.Is(err, unix.EADDRNOTAVAIL) {
				return err
			}
		}

		tap := &netlink.Tuntap{
			LinkAttrs: netlink.LinkAttrs{Name: microVMTapName, MTU: attrs.MTU},
			Mode:      netlink.TUNTAP_MODE_TAP,
			Flags:     netlink.TUNTAP_NO_PI | netlink.TUNTAP_VNET_HDR,
		}
		if err := netlink.LinkAdd(tap); err != nil && !errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("create tap: %w", err)
		}
		tapLink, err := netlink.LinkByName(microVMTapName)
		if err != nil {
			return err
		}
		if err := netlink.LinkSetMTU(tapLink, network.MTU); err != nil {
			return err
		}
		_ = netlink.LinkSetIP6AddrGenMode(tapLink, nl.IN6_ADDR_GEN_MODE_NONE)
		prepared.TapIndex = tapLink.Attrs().Index

		mac := make(net.HardwareAddr, 6)
		if _, err := rand.Read(mac); err != nil {
			return err
		}
		mac[0] = mac[0]&0xfe | 0x02
		if err := netlink.LinkSetDown(veth); err != nil {
			return err
		}
		if err := netlink.LinkSetHardwareAddr(veth, mac); err != nil {
			return fmt.Errorf("re-address veth: %w", err)
		}
		bridge := &netlink.Bridge{LinkAttrs: netlink.LinkAttrs{Name: "b9sw0", MTU: attrs.MTU}}
		if err := netlink.LinkAdd(bridge); err != nil {
			return fmt.Errorf("create namespace bridge: %w", err)
		}
		_ = netlink.LinkSetIP6AddrGenMode(bridge, nl.IN6_ADDR_GEN_MODE_NONE)
		for _, link := range []netlink.Link{veth, tapLink} {
			if err := netlink.LinkSetMaster(link, bridge); err != nil {
				return err
			}
			if err := netlink.LinkSetUp(link); err != nil {
				return err
			}
		}
		return netlink.LinkSetUp(bridge)
	})
	if err != nil {
		return err
	}
	data, err := json.Marshal(prepared)
	if err != nil {
		return err
	}
	path := rt.preparedNetworkPath(nsPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// mirrorLayers is the pair of read-only FUSE mounts a mirrored VM's host
// overlay stacks: the image as the worker mounts it (a stateless CLIP mount)
// and, above it, the mirror filesystem serving one generation under the
// guest homes.
type mirrorLayers struct {
	t        *testing.T
	base     string
	manifest *mirror.Manifest
	objects  mirrorObjects
	image    *fuse.Server
	mirror   *fuse.Server
	sums     map[string]string // guest path -> sha256
}

type mirrorObjects map[mirror.Digest][]byte

func (o mirrorObjects) ReadObject(_ context.Context, object mirror.Object, offset int64, dest []byte) error {
	data, ok := o[object.Digest]
	if !ok {
		return fmt.Errorf("object %s missing", object.Digest)
	}
	copy(dest, data[offset:])
	return nil
}

func (l *mirrorLayers) imageMount() string  { return filepath.Join(l.base, "image") }
func (l *mirrorLayers) mirrorMount() string { return filepath.Join(l.base, "mirror") }
func (l *mirrorLayers) lowerdir() string    { return l.mirrorMount() + ":" + l.imageMount() }

func newMirrorLayers(t *testing.T, rootfs string) *mirrorLayers {
	t.Helper()
	base, err := os.MkdirTemp(testWorkRoot, "mirror-layers-")
	require.NoError(t, err)
	layers := &mirrorLayers{t: t, base: base, objects: mirrorObjects{}, sums: map[string]string{}}
	t.Cleanup(func() {
		layers.unmount()
		_ = os.RemoveAll(base)
	})

	writer, err := mirror.NewWriter(mirror.DefaultWriterOptions(), func(object mirror.Object, data []byte) error {
		layers.objects[object.Digest] = bytes.Clone(data)
		return nil
	})
	require.NoError(t, err)
	dirs := map[string]bool{}
	write := func(path string, data []byte, mode uint32) {
		parts := strings.Split(path, "/")
		for i := 1; i < len(parts); i++ {
			if dir := strings.Join(parts[:i], "/"); !dirs[dir] {
				dirs[dir] = true
				require.NoError(t, writer.AddDir(dir, 0o755, 0))
			}
		}
		require.NoError(t, writer.AddFile(path, mode, 1_700_000_000_000_000_000, bytes.NewReader(data)))
		sum := sha256.Sum256(data)
		layers.sums["/root/"+path] = hex.EncodeToString(sum[:])
	}
	random := func(size int) []byte {
		data := make([]byte, size)
		_, err := rand.Read(data)
		require.NoError(t, err)
		return data
	}
	write(".zshrc", []byte("export EDITOR=vim\n"), 0o644)
	write(".ssh/config", []byte("Host *\n  ServerAliveInterval 60\n"), 0o600)
	write("projects/app/main.go", []byte("package main\n\nfunc main() {}\n"), 0o644)
	write("projects/app/internal/lib/lib.go", []byte("package lib\n"), 0o644)
	write("projects/notes.txt", bytes.Repeat([]byte("compressible line of text\n"), 100_000), 0o644)
	write("big.bin", random(32<<20), 0o644)
	for i := range 2000 {
		write(fmt.Sprintf("small/%04d.txt", i), random(4096), 0o644)
	}
	for i := range 400 {
		write(fmt.Sprintf("later/%04d.txt", i), random(4096), 0o644)
	}
	require.NoError(t, writer.AddSymlink("projects/current", "app", 0))
	_, layers.manifest, err = writer.Finish(nil)
	require.NoError(t, err)

	// The image carries the same small files so the guest can compare a read
	// through the mirror layer with one through the image layer.
	image := filepath.Join(base, "rootfs")
	out, err := exec.Command("cp", "-a", rootfs, image).CombinedOutput()
	require.NoError(t, err, string(out))
	require.NoError(t, os.MkdirAll(filepath.Join(image, "opt/small"), 0o755))
	for i := range 2000 {
		require.NoError(t, os.WriteFile(filepath.Join(image, fmt.Sprintf("opt/small/%04d.txt", i)), random(4096), 0o644))
	}
	archive := filepath.Join(base, "image.clip")
	require.NoError(t, clip.SetLogLevel("warn"))
	require.NoError(t, clip.CreateArchive(clip.CreateOptions{InputPath: image, OutputPath: archive}))
	layers.mount()
	return layers
}

func (l *mirrorLayers) mount() {
	l.t.Helper()
	for _, dir := range []string{l.imageMount(), l.mirrorMount()} {
		require.NoError(l.t, os.MkdirAll(dir, 0o755))
	}
	start, _, image, err := clip.MountArchive(clip.MountOptions{
		Stateless:   true,
		ArchivePath: filepath.Join(l.base, "image.clip"),
		MountPoint:  l.imageMount(),
	})
	require.NoError(l.t, err)
	require.NoError(l.t, start())
	l.image = image

	fsys, err := mirror.NewFS(l.manifest, l.objects, mirror.FSOptions{})
	require.NoError(l.t, err)
	l.mirror, err = mirror.Mount(l.mirrorMount(), fsys)
	require.NoError(l.t, err)
}

func (l *mirrorLayers) unmount() {
	for _, server := range []*fuse.Server{l.mirror, l.image} {
		if server != nil {
			_ = server.Unmount()
		}
	}
	l.mirror, l.image = nil, nil
}

var uptimePair = regexp.MustCompile(`(\w+) ([0-9.]+) ([0-9.]+) (\d+)`)

// perFile reports the mean wall time of the guest loop that printed
// "<label> <start uptime> <end uptime> <files>".
func perFile(output, label string) (time.Duration, int) {
	for _, match := range uptimePair.FindAllStringSubmatch(output, -1) {
		if match[1] != label {
			continue
		}
		start, _ := strconv.ParseFloat(match[2], 64)
		end, _ := strconv.ParseFloat(match[3], 64)
		files, _ := strconv.Atoi(match[4])
		if files == 0 {
			return 0, 0
		}
		return time.Duration((end - start) / float64(files) * float64(time.Second)), files
	}
	return 0, 0
}

// The host overlay takes the mirror as a second read-only FUSE lower above
// the image. The guest reads both through one virtio-fs share, changes land
// on its root disk, and a cold boot on the same disk (fresh overlay, fresh
// FUSE mounts, as after a worker restart) sees its changes over an unchanged
// lower.
func TestMicroVMMirrorLayerBootAndColdBoot(t *testing.T) {
	rt := requireMicroVMEnv(t)
	layers := newMirrorLayers(t, extractRootfs(t, "alpine"))

	manager := disk.NewManager(disk.Config{Root: filepath.Join(testWorkRoot, "qcow")})
	ctx := context.Background()
	key := fmt.Sprintf("mirrordisk-%d", time.Now().UnixNano()%1_000_000)
	t.Cleanup(func() { _ = manager.Close(ctx) })
	volume, err := manager.Attach(ctx, disk.AttachSpec{Key: key, VirtualSizeBytes: 2 << 30, Export: disk.ExportVhostUser}, nil)
	require.NoError(t, err)

	first := newTestVM(t, rt, vmOptions{
		image: "alpine",
		lower: layers.lowerdir(),
		args: []string{"sh", "-c", `
set -eu
echo "zshrc=$(sha256sum /root/.zshrc | cut -d' ' -f1)"
echo "big=$(sha256sum /root/big.bin | cut -d' ' -f1)"
echo "release=$(cat /etc/alpine-release)"
echo "file=$(stat -c '%u:%g:%a' /root/.ssh/config)"
echo "home=$(stat -c '%u:%g:%a' /root)"
echo "beam=$(stat -c '%u:%g:%a' /home/beam/.ssh/config)"
echo "beamsum=$(sha256sum /home/beam/.zshrc | cut -d' ' -f1)"
echo "notes=$(sha256sum /root/projects/notes.txt | cut -d' ' -f1)"
echo "link=$(readlink /root/projects/current):$(sha256sum /root/projects/current/main.go | cut -d' ' -f1)"
echo "entries=$(ls /root/small | wc -l)"
t0=$(cut -d' ' -f1 /proc/uptime)
cat /root/small/* > /dev/null
echo "mirrorsmall $t0 $(cut -d' ' -f1 /proc/uptime) $(ls /root/small | wc -l)"
t0=$(cut -d' ' -f1 /proc/uptime)
cat /opt/small/* > /dev/null
echo "imagesmall $t0 $(cut -d' ' -f1 /proc/uptime) $(ls /opt/small | wc -l)"
t0=$(cut -d' ' -f1 /proc/uptime)
cat /root/small/* > /dev/null
echo "mirrorwarm $t0 $(cut -d' ' -f1 /proc/uptime) $(ls /root/small | wc -l)"
echo local-edit >> /root/.zshrc
mv /root/projects/app /root/projects/app-renamed
rm /root/.ssh/config
echo new > /root/projects/new.txt
sync
echo first-done
`},
		annotations: map[string]string{MicroVMRootDiskAnnotation: volume.ExportSocket()},
	})
	first.start()
	res := first.wait(120 * time.Second)
	output := first.output.String()
	require.NoError(t, res.err, output)
	require.Equal(t, 0, res.code, output)
	require.Contains(t, output, "zshrc="+layers.sums["/root/.zshrc"])
	require.Contains(t, output, "big="+layers.sums["/root/big.bin"])
	require.Contains(t, output, "release=3.20")
	require.Contains(t, output, "file=0:0:600")
	require.Contains(t, output, "home=0:0:700", "the mirror layer's home directory replaces the image's")
	require.Contains(t, output, "beam=1000:1000:600", "the second placement belongs to the desktop user")
	require.Contains(t, output, "beamsum="+layers.sums["/root/.zshrc"])
	require.Contains(t, output, "notes="+layers.sums["/root/projects/notes.txt"])
	require.Contains(t, output, "link=app:"+layers.sums["/root/projects/app/main.go"])
	require.Contains(t, output, "entries=2000")
	for _, label := range []string{"mirrorsmall", "imagesmall", "mirrorwarm"} {
		each, files := perFile(output, label)
		t.Logf("guest small-file read (%s): %s per file over %d files", label, each, files)
	}
	// Nothing the guest did reached the mirror layer.
	data, err := os.ReadFile(filepath.Join(layers.mirrorMount(), "root/.zshrc"))
	require.NoError(t, err)
	require.Equal(t, "export EDITOR=vim\n", string(data))
	require.FileExists(t, filepath.Join(layers.mirrorMount(), "root/.ssh/config"))

	require.NoError(t, rt.Delete(ctx, first.id, &DeleteOpts{Force: true}))
	require.NoError(t, manager.Detach(ctx, key))
	_ = unix.Unmount(first.canvas, unix.MNT_DETACH)
	layers.unmount()
	layers.mount()

	volume, err = manager.Attach(ctx, disk.AttachSpec{Key: key, VirtualSizeBytes: 2 << 30, Export: disk.ExportVhostUser}, nil)
	require.NoError(t, err)
	second := newTestVM(t, rt, vmOptions{
		image: "alpine",
		lower: layers.lowerdir(),
		args: []string{"sh", "-c", `
set -eu
echo "edit=$(tail -n1 /root/.zshrc)"
echo "head=$(head -n1 /root/.zshrc)"
echo "renamed=$(sha256sum /root/projects/app-renamed/main.go | cut -d' ' -f1)"
echo "nested=$(sha256sum /root/projects/app-renamed/internal/lib/lib.go | cut -d' ' -f1)"
test ! -e /root/.ssh/config
test ! -e /root/projects/app
echo "new=$(cat /root/projects/new.txt)"
echo "big=$(sha256sum /root/big.bin | cut -d' ' -f1)"
echo "later=$(sha256sum /root/later/0007.txt | cut -d' ' -f1)"
echo second-done
`},
		annotations: map[string]string{MicroVMRootDiskAnnotation: volume.ExportSocket()},
	})
	second.start()
	res = second.wait(120 * time.Second)
	output = second.output.String()
	require.NoError(t, res.err, output)
	require.Equal(t, 0, res.code, output)
	require.Contains(t, output, "edit=local-edit")
	require.Contains(t, output, "head=export EDITOR=vim")
	require.Contains(t, output, "renamed="+layers.sums["/root/projects/app/main.go"], "a directory renamed in the guest still resolves its lower contents after a cold boot")
	require.Contains(t, output, "nested="+layers.sums["/root/projects/app/internal/lib/lib.go"])
	require.Contains(t, output, "new=new")
	require.Contains(t, output, "big="+layers.sums["/root/big.bin"])
	require.Contains(t, output, "later="+layers.sums["/root/later/0007.txt"])
	require.Contains(t, output, "second-done")
}

// A paused and resumed VM, and a fork restored from its memory image onto a
// new host overlay, both keep reading the mirror layer: files already in the
// guest page cache and files never touched before the pause.
func TestMicroVMMirrorLayerPauseResumeAndFork(t *testing.T) {
	rt := requireMicroVMEnv(t)
	layers := newMirrorLayers(t, extractRootfs(t, "alpine"))

	vm := newTestVM(t, rt, vmOptions{image: "alpine", lower: layers.lowerdir(), goproc: true})
	vm.start()
	client := vm.goprocClient(60 * time.Second)
	sum := func(owner *testVM, path string) string {
		code, out := owner.exec(client, "sha256sum", path)
		require.Equal(t, 0, code, out)
		return strings.Fields(out)[0]
	}
	require.Equal(t, layers.sums["/root/small/0001.txt"], sum(vm, "/root/small/0001.txt"))
	code, out := vm.sh(client, "echo before-pause >> /root/.zshrc && cat /root/.zshrc")
	require.Equal(t, 0, code, out)

	// Pause and resume in place.
	live := filepath.Join(testWorkRoot, "checkpoint-mirror-live-"+vm.id)
	require.NoError(t, os.RemoveAll(live))
	require.NoError(t, rt.Checkpoint(context.Background(), vm.id, &CheckpointOpts{ImagePath: live, LeaveRunning: true}))
	require.Equal(t, layers.sums["/root/small/0001.txt"], sum(vm, "/root/small/0001.txt"), "cached before the pause")
	require.Equal(t, layers.sums["/root/small/0002.txt"], sum(vm, "/root/small/0002.txt"), "first read after the resume")
	require.Equal(t, layers.sums["/root/big.bin"], sum(vm, "/root/big.bin"))
	code, out = vm.sh(client, "tail -n1 /root/.zshrc && ls /root/later | wc -l")
	require.Equal(t, 0, code, out)
	require.Equal(t, "before-pause\n400\n", out)

	// Fork: a terminal checkpoint restored as a new VM on a new overlay.
	image := filepath.Join(testWorkRoot, "checkpoint-mirror-"+vm.id)
	require.NoError(t, os.RemoveAll(image))
	require.NoError(t, rt.Checkpoint(context.Background(), vm.id, &CheckpointOpts{ImagePath: image, LeaveRunning: false}))
	res := vm.wait(30 * time.Second)
	require.NoError(t, res.err)
	require.Equal(t, 128+int(syscall.SIGKILL), res.code)
	require.NoError(t, rt.Delete(context.Background(), vm.id, &DeleteOpts{Force: true}))

	fork := newTestVM(t, rt, vmOptions{image: "alpine", lower: layers.lowerdir(), goproc: true})
	fork.restore(image)
	client = fork.goprocClient(60 * time.Second)
	require.Equal(t, layers.sums["/root/small/0001.txt"], sum(fork, "/root/small/0001.txt"), "cached in the forked memory image")
	require.Equal(t, layers.sums["/root/later/0003.txt"], sum(fork, "/root/later/0003.txt"), "never read before the fork")
	require.Equal(t, layers.sums["/root/big.bin"], sum(fork, "/root/big.bin"))
	code, out = fork.sh(client, "tail -n1 /root/.zshrc && ls /root/small | wc -l && cat /etc/alpine-release")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "before-pause\n2000\n3.20")

	require.NoError(t, rt.Kill(context.Background(), fork.id, syscall.SIGKILL, &KillOpts{All: true}))
	require.NoError(t, fork.wait(30*time.Second).err)
	require.NoError(t, rt.Delete(context.Background(), fork.id, &DeleteOpts{Force: true}))
}
