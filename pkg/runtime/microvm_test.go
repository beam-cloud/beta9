package runtime

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCPUMaxString(t *testing.T) {
	quota := func(q, p int64) *specs.LinuxCPU {
		c := &specs.LinuxCPU{Quota: &q}
		if p > 0 {
			period := uint64(p)
			c.Period = &period
		}
		return c
	}
	assert.Equal(t, "", cpuMaxString(nil), "no CPU spec means no limit (boots unthrottled)")
	assert.Equal(t, "", cpuMaxString(&specs.LinuxCPU{}), "no quota means no limit")
	assert.Equal(t, "10000 100000", cpuMaxString(quota(10000, 0)), "0.1 core with the default period")
	assert.Equal(t, "200000 100000", cpuMaxString(quota(200000, 100000)), "2 cores")
	assert.Equal(t, "50000 50000", cpuMaxString(quota(50000, 50000)), "1 core with a custom period")
}

func TestMicroVMMountPlanKeepsSpecOrder(t *testing.T) {
	plan := microVMMountPlan([]specs.Mount{
		{Destination: "/proc", Type: "proc", Source: "proc"},
		{Destination: "/dev/shm", Type: "tmpfs", Source: "shm"},
		{Destination: "/sys/fs/cgroup", Type: "cgroup", Source: "cgroup"},
		{Destination: "/tmp", Type: "tmpfs", Source: "none", Options: []string{"size=1g"}},
		{Destination: "/usr/bin/goproc", Type: "bind", Source: "/usr/local/bin/goproc", Options: []string{"ro", "rbind"}},
		{Destination: "/volumes", Type: "tmpfs", Source: "none"},
		{Destination: "/volumes/data", Type: "bind", Source: "/workspace/data/workspace/volumes/x", Options: []string{"rbind", "rw"}},
		{Destination: "relative", Type: "bind", Source: "/x"},
		{Destination: "/dev/nvidia0", Type: "bind", Source: "/dev/nvidia0"},
	})

	var order []string
	for _, mount := range plan {
		order = append(order, mount.Type+" "+mount.Destination)
	}
	// /dev/shm is the guest's own; the /volumes tmpfs must stay ahead of the
	// bind beneath it or the guest would mount it over the volume.
	assert.Equal(t, []string{"tmpfs /tmp", "bind /usr/bin/goproc", "tmpfs /volumes", "bind /volumes/data"}, order)
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
	assert.Equal(t, limit, microVMWorkloadMemoryBytes(spec))
	assert.Equal(t, limit+microvm.GuestMemoryHeadroom, microVMMemoryBytes(spec))
	assert.Equal(t, "250000 100000", microVMCPUMax(spec))
}

func TestMicroVMResourcesFallbacks(t *testing.T) {
	spec := &specs.Spec{Linux: &specs.Linux{Resources: &specs.LinuxResources{CPU: &specs.LinuxCPU{Cpus: "0-3,8"}}}}
	assert.Equal(t, 5, microVMVCPUs(spec))
	assert.Equal(t, int64(microVMDefaultMemoryMiB<<20)+microvm.GuestMemoryHeadroom, microVMMemoryBytes(spec))
	assert.Equal(t, "", microVMCPUMax(spec))

	spec.Annotations = map[string]string{MicroVMVCPUAnnotation: "2", MicroVMMemoryMiBAnnotation: "1025"}
	assert.Equal(t, 2, microVMVCPUs(spec))
	assert.Equal(t, int64(1025<<20), microVMWorkloadMemoryBytes(spec), "the workload budget remains exact")
	assert.Equal(t, int64(1026<<20)+microvm.GuestMemoryHeadroom, microVMMemoryBytes(spec), "guest RAM includes headroom and rounds to 2 MiB")

	assert.Equal(t, 1, microVMVCPUs(&specs.Spec{Linux: &specs.Linux{}}))
}

func TestMicroVMWorkloadBudgetExcludesHostBuffer(t *testing.T) {
	limit := int64(22 << 30)
	spec := &specs.Spec{
		Annotations: map[string]string{MicroVMMemoryMiBAnnotation: "16384"},
		Linux:       &specs.Linux{Resources: &specs.LinuxResources{Memory: &specs.LinuxMemory{Limit: &limit}}},
	}
	assert.Equal(t, int64(16<<30), microVMWorkloadMemoryBytes(spec))
	assert.Equal(t, int64(16<<30), microVMGuestSpec(spec, microvm.Network{}, microVMDisk{}, nil, nil).WorkloadMemoryBytes)
}

func TestMicroVMCgroupPath(t *testing.T) {
	assert.Equal(t, "/sys/fs/cgroup/abc", microVMCgroupPath(&specs.Spec{Linux: &specs.Linux{}}, "abc"))
	assert.Equal(t, "/sys/fs/cgroup/beta9/abc", microVMCgroupPath(&specs.Spec{Linux: &specs.Linux{CgroupsPath: "/beta9/abc"}}, "abc"))
}

func TestMicroVMHypervisorArgs(t *testing.T) {
	root := microVMDisk{arg: "path=/tmp/scratch.ext4", device: "/dev/vda"}
	extra := []microVMDisk{{arg: "vhost_user=on,socket=/s,num_queues=1,queue_size=128", device: "/dev/vdb"}}
	args := microVMHypervisorArgs("/run/beam/microvm/c1", "/vmlinux", "console=ttyS0", 2, 1<<30, "", "02:00:00:00:00:01", root, extra, nil, false)
	joined := strings.Join(args, " ")

	assert.Contains(t, joined, "--kernel /vmlinux")
	assert.Contains(t, joined, "--cpus boot=2")
	assert.Contains(t, args, "size=1073741824,shared=on")
	assert.Contains(t, joined, "--fs tag=beamfs,socket=/run/beam/microvm/c1/virtiofs.sock")
	assert.Contains(t, joined, "--disk path=/tmp/scratch.ext4 vhost_user=on,socket=/s")
	assert.Contains(t, joined, "--net tap=b9tap0,mac=02:00:00:00:00:01")
	assert.Contains(t, joined, "--vsock cid=3,socket=/run/beam/microvm/c1/vsock.sock")
	assert.Contains(t, joined, "--balloon size=0,free_page_reporting=on")
	assert.Contains(t, joined, "--serial tty --console off")
	assert.NotContains(t, args, "--memory-zone")
	assert.NotContains(t, joined, "affinity=")

	cmdline := microVMKernelCmdline()
	assert.Contains(t, cmdline, "root=beamfs rootfstype=virtiofs rw")
	assert.Contains(t, cmdline, "init="+microvm.InitPath)
	assert.Contains(t, cmdline, "console=ttyS0")
}

func TestMicroVMGPUPlacementArgs(t *testing.T) {
	for _, test := range []struct {
		name      string
		placement *microVMPlacement
		vcpus     int
		cpuArg    string
	}{
		{"node zero", &microVMPlacement{node: 0, cpus: []int{0, 2}}, 2, "boot=2,affinity=[0@[0,2],1@[0,2]]"},
		{"sparse CPUs", &microVMPlacement{node: 1, cpus: []int{64, 66, 128}}, 2, "boot=2,affinity=[0@[64,66,128],1@[64,66,128]]"},
		{"shared CPU", &microVMPlacement{node: 1, cpus: []int{64}}, 3, "boot=3,affinity=[0@[64],1@[64],2@[64]]"},
		{"unknown locality", nil, 2, "boot=2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			memory := microVMMemoryBytes(&specs.Spec{Annotations: map[string]string{MicroVMMemoryMiBAnnotation: "16384"}})
			args := microVMHypervisorArgs("/state", "/kernel", "", test.vcpus, memory, "", "mac", microVMDisk{}, nil, test.placement, true)
			joined := strings.Join(args, " ")
			assert.Contains(t, args, test.cpuArg)
			assert.NotContains(t, args, "--device", "VFIO devices are attached while the guest boots")
			assert.NotContains(t, args, "--balloon")
			assert.NotContains(t, args, "--numa", "host locality must not change guest NUMA topology")
			if test.placement == nil {
				assert.Contains(t, joined, "--memory size=17716740096,shared=on")
				assert.NotContains(t, args, "--memory-zone")
			} else {
				assert.Contains(t, joined, "--memory size=0 --memory-zone id=ram,size=17716740096,shared=on,host_numa_node="+strconv.Itoa(test.placement.node))
			}
			prepared := strings.Join(microVMHypervisorArgs("/state", "/kernel", "", test.vcpus, memory, "/proc/self/fd/4", "mac", microVMDisk{}, nil, test.placement, true), " ")
			assert.Contains(t, prepared, "--memory size=0 --memory-zone id=ram,size=17716740096,shared=on,file=/proc/self/fd/4 ")
			assert.NotContains(t, prepared, "host_numa_node", "prepared RAM is already on the GPU's node")
		})
	}
}

func TestMicroVMGuestSpec(t *testing.T) {
	spec := &specs.Spec{Hostname: "sb-1", Annotations: map[string]string{MicroVMDockerAnnotation: "true"}}
	network := microvm.Network{MAC: "02:00:00:00:00:01", IPv4: "192.168.1.5/20", Gateway4: "192.168.0.1"}
	root := microVMDisk{device: "/dev/vda"}
	extra := []microVMDisk{{device: "/dev/vdb", mountPath: "/data", readOnly: true}}
	mounts := []microvm.Mount{
		tmpfsMount(specs.Mount{Destination: "/tmp/", Type: "tmpfs", Options: []string{"size=1g"}}),
		{Type: microvm.MountBind, Source: "/.beam/binds/1", Destination: "/usr/bin/goproc", File: true, ReadOnly: true},
	}

	got := microVMGuestSpec(spec, network, root, extra, mounts)
	assert.True(t, got.Docker)
	assert.Equal(t, "/dev/vda", got.RootDisk)
	assert.Equal(t, []microvm.Disk{{Device: "/dev/vdb", MountPath: "/data", ReadOnly: true}}, got.Disks)
	assert.Equal(t, microvm.Mount{Type: microvm.MountTmpfs, Destination: "/tmp", Options: []string{"size=1g"}}, got.Mounts[0])
	assert.Equal(t, mounts[1], got.Mounts[1])
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

func TestRewriteSnapshotConfigRetargetsDevices(t *testing.T) {
	snapshot := []byte(`{
	  "cpus": {"boot_vcpus": 2},
	  "memory": {"size": 1073741824, "shared": true, "thp": true},
	  "disks": [
	    {"path": "/old/scratch.ext4", "image_type": "raw", "id": "_disk0"},
	    {"vhost_user": true, "vhost_socket": "/old/run/aaa/vhost-user-blk.sock", "id": "_disk1"}
	  ],
	  "fs": [{"tag": "beamfs", "socket": "/old/state/virtiofs.sock"}],
	  "vsock": {"cid": 3, "socket": "/old/state/vsock.sock"},
	  "net": [{"tap": "b9tap0", "mac": "02:00:00:00:00:01"}]
	}`)
	root := microVMDisk{path: "/new/scratch.ext4"}
	extra := []microVMDisk{{socket: "/new/run/bbb/vhost-user-blk.sock"}}
	network := microvm.Network{MAC: "02:00:00:00:00:02"}
	out, err := rewriteSnapshotConfig(snapshot, "/new/state", network, root, extra, snapshotHost{})
	require.NoError(t, err)

	var cfg map[string]any
	require.NoError(t, json.Unmarshal(out, &cfg))
	assert.Equal(t, false, cfg["memory"].(map[string]any)["thp"])
	assert.Equal(t, float64(1073741824), cfg["memory"].(map[string]any)["size"])
	disks := cfg["disks"].([]any)
	assert.Equal(t, "/new/scratch.ext4", disks[0].(map[string]any)["path"])
	assert.Equal(t, "raw", disks[0].(map[string]any)["image_type"], "unrelated keys survive")
	assert.Equal(t, "/new/run/bbb/vhost-user-blk.sock", disks[1].(map[string]any)["vhost_socket"])
	assert.Equal(t, "/new/state/virtiofs.sock", cfg["fs"].([]any)[0].(map[string]any)["socket"])
	assert.Equal(t, "/new/state/vsock.sock", cfg["vsock"].(map[string]any)["socket"])
	assert.Equal(t, "02:00:00:00:00:02", cfg["net"].([]any)[0].(map[string]any)["mac"], "the guest takes the new slot's MAC")

	_, err = rewriteSnapshotConfig(snapshot, "/new/state", network, root, nil, snapshotHost{})
	assert.ErrorContains(t, err, "snapshot has 2 disks, this VM has 1")
	_, err = rewriteSnapshotConfig(snapshot, "/new/state", network, microVMDisk{socket: "/x"}, extra, snapshotHost{})
	assert.ErrorContains(t, err, "not a vhost-user export", "a scratch-root snapshot cannot be restored onto a durable root")
}

// A GPU guest's snapshot names what it had on the host it was taken on: the
// descriptors its hypervisor inherited and the CPUs and NUMA node next to
// its GPU. It is restored next to whichever GPU is free.
func TestRewriteSnapshotConfigMovesGuestToAnotherGPU(t *testing.T) {
	snapshot := []byte(`{
	  "cpus": {"boot_vcpus": 2, "affinity": [{"vcpu": 0, "host_cpus": [1, 2]}, {"vcpu": 1, "host_cpus": [1, 2]}]},
	  "memory": {"size": 0, "thp": true, "zones": [
	    {"id": "ram", "size": 2147483648, "file": "/proc/self/fd/4", "shared": true, "host_numa_node": null}
	  ]},
	  "payload": {"kernel": "/proc/self/fd/5", "cmdline": "console=ttyS0"},
	  "disks": [{"path": "/old/scratch.ext4"}],
	  "devices": []
	}`)
	rewrite := func(t *testing.T, config []byte, host snapshotHost) (cpus, zone, payload map[string]any) {
		t.Helper()
		out, err := rewriteSnapshotConfig(config, "/state", microvm.Network{}, microVMDisk{path: "/new/scratch.ext4"}, nil, host)
		require.NoError(t, err)
		var cfg map[string]any
		require.NoError(t, json.Unmarshal(out, &cfg))
		if zones, _ := cfg["memory"].(map[string]any)["zones"].([]any); len(zones) == 1 {
			zone = zones[0].(map[string]any)
		}
		payload, _ = cfg["payload"].(map[string]any)
		return cfg["cpus"].(map[string]any), zone, payload
	}
	there := &microVMPlacement{node: 1, cpus: []int{64, 65, 66}}
	near := []any{float64(64), float64(65), float64(66)}

	cpus, zone, payload := rewrite(t, snapshot, snapshotHost{kernel: "/proc/self/fd/7", ram: "/proc/self/fd/6", ramBytes: 2 << 30, placement: there})
	assert.Equal(t, []any{
		map[string]any{"vcpu": float64(0), "host_cpus": near},
		map[string]any{"vcpu": float64(1), "host_cpus": near},
	}, cpus["affinity"])
	assert.Equal(t, "/proc/self/fd/6", zone["file"], "the snapshot's descriptor numbers mean nothing to another hypervisor")
	assert.Equal(t, true, zone["shared"])
	assert.Nil(t, zone["host_numa_node"], "prepared huge pages are already on the GPU's node")
	assert.Equal(t, "/proc/self/fd/7", payload["kernel"])
	assert.Equal(t, "console=ttyS0", payload["cmdline"])

	// Without free huge pages the guest's RAM is ordinary memory near the GPU.
	cpus, zone, _ = rewrite(t, snapshot, snapshotHost{kernel: "/kernel", placement: there})
	assert.Len(t, cpus["affinity"], 2)
	assert.Nil(t, zone["file"])
	assert.Equal(t, float64(1), zone["host_numa_node"])

	// A host that does not know where its GPUs are pins nothing.
	cpus, zone, payload = rewrite(t, snapshot, snapshotHost{kernel: "/kernel"})
	assert.Nil(t, cpus["affinity"], "the CPUs next to the other host's GPU are not this guest's")
	assert.Nil(t, zone["file"])
	assert.Nil(t, zone["host_numa_node"])
	assert.Equal(t, "/kernel", payload["kernel"])

	_, err := rewriteSnapshotConfig(snapshot, "/state", microvm.Network{}, microVMDisk{path: "/new/scratch.ext4"}, nil,
		snapshotHost{ram: "/proc/self/fd/6", ramBytes: 4 << 30})
	assert.ErrorContains(t, err, "snapshot has 2147483648 bytes of RAM, this VM has 4294967296")

	assert.True(t, snapshotRAMInFile(snapshot))
	for _, config := range []string{
		`{"memory": {"size": 1073741824, "shared": true, "zones": null}}`,
		`{"memory": {"size": 0, "zones": [{"id": "ram", "size": 1073741824, "file": null, "host_numa_node": 0}]}}`,
		`not a snapshot`,
	} {
		assert.False(t, snapshotRAMInFile([]byte(config)), config)
	}

	// A CPU guest's snapshot has no zones and keeps the kernel it names
	// only when this host has none to give.
	cpuGuest := []byte(`{"cpus": {"boot_vcpus": 1, "affinity": null}, "memory": {"size": 1073741824}, "payload": {"kernel": "/old/vmlinux"}, "disks": [{"path": "/old"}]}`)
	cpus, zone, payload = rewrite(t, cpuGuest, snapshotHost{})
	assert.Nil(t, cpus["affinity"])
	assert.Nil(t, zone)
	assert.Equal(t, "/old/vmlinux", payload["kernel"])
}
