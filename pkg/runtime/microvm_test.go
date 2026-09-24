package runtime

import (
	"bytes"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/microvm"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMicroVMMountPlanSplitsHostBindsFromGuestMounts(t *testing.T) {
	binds, tmpfs := microVMMountPlan([]specs.Mount{
		{Destination: "/proc", Type: "proc", Source: "proc"},
		{Destination: "/dev/shm", Type: "tmpfs", Source: "shm"},
		{Destination: "/sys/fs/cgroup", Type: "cgroup", Source: "cgroup"},
		{Destination: "/tmp", Type: "tmpfs", Source: "none", Options: []string{"size=1g"}},
		{Destination: "/usr/bin/goproc", Type: "bind", Source: "/usr/local/bin/goproc", Options: []string{"ro", "rbind"}},
		{Destination: "/etc/resolv.conf", Type: "none", Source: "/workspace/etc/resolv.conf", Options: []string{"ro", "rbind"}},
		{Destination: "/volumes/data", Type: "bind", Source: "/data/volumes/x", Options: []string{"rbind", "rw"}},
		{Destination: "relative", Type: "bind", Source: "/x"},
		{Destination: "/dev/nvidia0", Type: "bind", Source: "/dev/nvidia0"},
	})

	require.Len(t, binds, 3)
	assert.Equal(t, "/usr/bin/goproc", binds[0].Destination)
	assert.Equal(t, "/etc/resolv.conf", binds[1].Destination)
	assert.Equal(t, "/volumes/data", binds[2].Destination)

	require.Len(t, tmpfs, 1, "/dev/shm is the guest's own; only /tmp is forwarded")
	assert.Equal(t, "/tmp", tmpfs[0].Destination)
}

func TestMicroVMDiskPlanScratchRoot(t *testing.T) {
	spec := &specs.Spec{Annotations: map[string]string{}}
	root, extra, err := microVMDiskPlan(spec, "/tmp/x/scratch.ext4")
	require.NoError(t, err)
	assert.Equal(t, "path=/tmp/x/scratch.ext4,image_type=raw", root.arg)
	assert.Equal(t, "/dev/vda", root.device)
	assert.Empty(t, extra)
}

func TestMicroVMDiskPlanDurableDisks(t *testing.T) {
	spec := &specs.Spec{Annotations: map[string]string{
		MicroVMRootDiskAnnotation:         "/run/qcow/root/vhost.sock",
		MicroVMDiskAnnotationPrefix + "1": "/run/qcow/b/vhost.sock:/data:ro",
		MicroVMDiskAnnotationPrefix + "0": "/run/qcow/a/vhost.sock:/mnt/models",
		MicroVMDockerAnnotation:           "true",
	}}
	root, extra, err := microVMDiskPlan(spec, "/unused")
	require.NoError(t, err)
	assert.Equal(t, "vhost_user=on,socket=/run/qcow/root/vhost.sock,num_queues=1,queue_size=128", root.arg)

	require.Len(t, extra, 2)
	assert.Equal(t, "/dev/vdb", extra[0].device)
	assert.Equal(t, "/mnt/models", extra[0].mountPath)
	assert.False(t, extra[0].readOnly)
	assert.Equal(t, "/dev/vdc", extra[1].device)
	assert.Equal(t, "/data", extra[1].mountPath)
	assert.True(t, extra[1].readOnly)
}

func TestMicroVMDiskPlanRejectsMalformedAnnotations(t *testing.T) {
	for _, value := range []string{"nosocket", "/sock:", ":/data", "/sock:relative"} {
		spec := &specs.Spec{Annotations: map[string]string{MicroVMDiskAnnotationPrefix + "0": value}}
		_, _, err := microVMDiskPlan(spec, "/scratch")
		assert.Error(t, err, value)
	}
}

func TestMicroVMResourcesFromSpec(t *testing.T) {
	quota := int64(250000)
	period := uint64(100000)
	limit := int64(3 << 30)
	spec := &specs.Spec{Linux: &specs.Linux{Resources: &specs.LinuxResources{
		CPU:    &specs.LinuxCPU{Quota: &quota, Period: &period},
		Memory: &specs.LinuxMemory{Limit: &limit},
	}}}
	assert.Equal(t, 3, microVMVCPUs(spec), "2.5 cores rounds up to 3 vCPUs")
	assert.Equal(t, limit, microVMMemoryBytes(spec))
	assert.Equal(t, "250000 100000", microVMCPUMax(spec))
}

func TestMicroVMResourcesFallbacks(t *testing.T) {
	spec := &specs.Spec{Linux: &specs.Linux{Resources: &specs.LinuxResources{CPU: &specs.LinuxCPU{Cpus: "0-3,8"}}}}
	assert.Equal(t, 5, microVMVCPUs(spec))
	assert.Equal(t, int64(microVMDefaultMemoryMiB<<20), microVMMemoryBytes(spec))
	assert.Equal(t, "", microVMCPUMax(spec))

	spec.Annotations = map[string]string{MicroVMVCPUAnnotation: "2", MicroVMMemoryMiBAnnotation: "1025"}
	assert.Equal(t, 2, microVMVCPUs(spec))
	assert.Equal(t, int64(1026<<20), microVMMemoryBytes(spec), "memory is rounded up to a 2 MiB multiple")

	assert.Equal(t, 1, microVMVCPUs(&specs.Spec{Linux: &specs.Linux{}}))
}

func TestMicroVMCgroupPath(t *testing.T) {
	assert.Equal(t, "/sys/fs/cgroup/abc", microVMCgroupPath(&specs.Spec{Linux: &specs.Linux{}}, "abc"))
	assert.Equal(t, "/sys/fs/cgroup/beta9/abc", microVMCgroupPath(&specs.Spec{Linux: &specs.Linux{CgroupsPath: "/beta9/abc"}}, "abc"))
}

func TestMicroVMHypervisorArgs(t *testing.T) {
	root := microVMDisk{arg: "path=/tmp/scratch.ext4", device: "/dev/vda"}
	extra := []microVMDisk{{arg: "vhost_user=on,socket=/s,num_queues=1,queue_size=128", device: "/dev/vdb"}}
	args := microVMHypervisorArgs("/run/beam/microvm/c1", "/vmlinux", "console=ttyS0", 2, 1<<30, "02:00:00:00:00:01", root, extra)
	joined := strings.Join(args, " ")

	assert.Contains(t, joined, "--kernel /vmlinux")
	assert.Contains(t, joined, "--cpus boot=2")
	assert.Contains(t, joined, "--memory size=1073741824,shared=on")
	assert.Contains(t, joined, "--fs tag=beamfs,socket=/run/beam/microvm/c1/virtiofs.sock")
	assert.Contains(t, joined, "--disk path=/tmp/scratch.ext4 vhost_user=on,socket=/s")
	assert.Contains(t, joined, "--net tap=b9tap0,mac=02:00:00:00:00:01")
	assert.Contains(t, joined, "--vsock cid=3,socket=/run/beam/microvm/c1/vsock.sock")
	assert.Contains(t, joined, "--serial tty --console off")

	cmdline := microVMKernelCmdline()
	assert.Contains(t, cmdline, "root=beamfs rootfstype=virtiofs rw")
	assert.Contains(t, cmdline, "init="+microvm.InitPath)
	assert.Contains(t, cmdline, "console=ttyS0")
}

func TestMicroVMGuestSpec(t *testing.T) {
	spec := &specs.Spec{Hostname: "sb-1", Annotations: map[string]string{MicroVMDockerAnnotation: "true"}}
	network := microvm.Network{MAC: "02:00:00:00:00:01", IPv4: "192.168.1.5/20", Gateway4: "192.168.0.1"}
	root := microVMDisk{device: "/dev/vda"}
	extra := []microVMDisk{{device: "/dev/vdb", mountPath: "/data", readOnly: true}}
	binds := []microvm.Bind{{Destination: "/usr/bin/goproc", File: true, ReadOnly: true}}
	tmpfs := []specs.Mount{{Destination: "/tmp/", Type: "tmpfs", Options: []string{"size=1g"}}}

	got := microVMGuestSpec(spec, network, root, extra, binds, tmpfs)
	assert.Equal(t, "sb-1", got.Hostname)
	assert.True(t, got.Docker)
	assert.Equal(t, "/dev/vda", got.RootDisk)
	assert.Equal(t, []microvm.Disk{{Device: "/dev/vdb", MountPath: "/data", ReadOnly: true}}, got.Disks)
	assert.Equal(t, binds, got.Binds)
	assert.Equal(t, []microvm.Tmpfs{{Destination: "/tmp", Options: []string{"size=1g"}}}, got.Tmpfs)
	assert.Equal(t, uint32(microvm.ControlPort), got.ControlPort)
}

func TestCpusetSize(t *testing.T) {
	assert.Equal(t, 0, cpusetSize(""))
	assert.Equal(t, 1, cpusetSize("3"))
	assert.Equal(t, 4, cpusetSize("0-3"))
	assert.Equal(t, 6, cpusetSize("0-3, 7,9"))
	assert.Equal(t, 0, cpusetSize("3-1"))
	assert.Equal(t, 0, cpusetSize("x"))
}

func TestTailWriterKeepsLastLines(t *testing.T) {
	w := newTailWriter(2)
	_, _ = w.Write([]byte("one\ntwo\nthr"))
	_, _ = w.Write([]byte("ee\nfour"))
	assert.Equal(t, "two\nthree\nfour", w.String(), "last two complete lines plus the unterminated one")
}

type recordingWriter struct{ records []string }

func (r *recordingWriter) Write(p []byte) (int, error) {
	r.records = append(r.records, string(p))
	return len(p), nil
}

func TestLineWriterEmitsWholeLines(t *testing.T) {
	dst := &recordingWriter{}
	w := newLineWriter(dst)
	for _, chunk := range []string{"[ ", "0.1", "] boot", "ing\nvminit: ", "ready\npar"} {
		_, err := w.Write([]byte(chunk))
		require.NoError(t, err)
	}
	assert.Equal(t, []string{"[ 0.1] booting\n", "vminit: ready\n"}, dst.records, "byte-sized console writes become one record per line")
	require.NoError(t, w.Close())
	assert.Equal(t, "par", dst.records[len(dst.records)-1], "Close flushes the trailing partial line")

	dst = &recordingWriter{}
	w = newLineWriter(dst)
	_, err := w.Write(bytes.Repeat([]byte("x"), lineWriterMaxLine))
	require.NoError(t, err)
	assert.Len(t, dst.records, 1, "an over-long line without a newline is flushed rather than buffered forever")
}

func TestMicroVMProtocolRoundTrip(t *testing.T) {
	var buf strings.Builder
	enc := microvm.NewEncoder(&buf)
	require.NoError(t, enc.Encode(microvm.Message{Type: microvm.MsgSignal, ID: 7, Signal: 15}))
	require.NoError(t, enc.Encode(microvm.Message{Type: microvm.MsgExit, Code: 3}))

	dec := microvm.NewDecoder(strings.NewReader(buf.String()))
	first, err := dec.Decode()
	require.NoError(t, err)
	assert.Equal(t, microvm.Message{Type: microvm.MsgSignal, ID: 7, Signal: 15}, first)
	second, err := dec.Decode()
	require.NoError(t, err)
	assert.Equal(t, microvm.Message{Type: microvm.MsgExit, Code: 3}, second)
}
