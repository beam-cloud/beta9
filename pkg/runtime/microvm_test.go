package runtime

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
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
	args := microVMHypervisorArgs("/run/beam/microvm/c1", "/vmlinux", "console=ttyS0", 2, 1<<30, "02:00:00:00:00:01", root, extra)
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
	out, err := rewriteSnapshotConfig(snapshot, "/new/state", network, root, extra)
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

	_, err = rewriteSnapshotConfig(snapshot, "/new/state", network, root, nil)
	assert.ErrorContains(t, err, "snapshot has 2 disks, this VM has 1")
	_, err = rewriteSnapshotConfig(snapshot, "/new/state", network, microVMDisk{socket: "/x"}, extra)
	assert.ErrorContains(t, err, "not a vhost-user export", "a scratch-root snapshot cannot be restored onto a durable root")
}

// This shape matches Cloud Hypervisor's native config/state metadata; guest
// memory ranges are concatenated in listed order, not sorted by GPA.
func microVMSnapshotQueueFixture(t *testing.T) (string, map[string]any, map[string]any, []byte) {
	t.Helper()
	config := map[string]any{"disks": []any{map[string]any{"id": "_disk0", "vhost_user": true, "num_queues": 1, "queue_size": 128}}}
	section := func(state any) map[string]any {
		data, err := json.Marshal(state)
		require.NoError(t, err)
		return map[string]any{"snapshots": map[string]any{}, "snapshot_data": map[string]any{"state": string(data)}}
	}
	state := map[string]any{"snapshots": map[string]any{
		"memory-manager": section(map[string]any{"memory_ranges": map[string]any{"data": []any{
			map[string]any{"gpa": 8192, "length": 2048}, map[string]any{"gpa": 4096, "length": 2048},
		}}}),
		"device-manager": map[string]any{"snapshots": map[string]any{
			"_disk0": section(map[string]any{"acked_protocol_features": 33291}),
			"_virtio-pci-_disk0": section(map[string]any{"queues": []any{map[string]any{
				"ready": true, "size": 128, "avail_ring": 8198, "used_ring": 4108,
			}}}),
			// A virtio-fs queue is unrelated to the configured block disk.
			"_virtio-pci-_fs2": section(map[string]any{"queues": []any{map[string]any{"ready": false}}}),
		}},
	}}
	memory := make([]byte, 4096)
	binary.LittleEndian.PutUint16(memory[8:], 65535)
	binary.LittleEndian.PutUint16(memory[2062:], 65535)
	return t.TempDir(), config, state, memory
}

func writeMicroVMSnapshotQueueFixture(t *testing.T, dir string, config, state map[string]any, memory []byte) {
	t.Helper()
	for name, value := range map[string]any{"config.json": config, "state.json": state} {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory-ranges"), memory, 0600))
}

func mutateMicroVMSnapshotSection(t *testing.T, state map[string]any, name string, change func(map[string]any)) {
	t.Helper()
	var visit func(map[string]any)
	visit = func(parent map[string]any) {
		children, _ := parent["snapshots"].(map[string]any)
		for key, raw := range children {
			child := raw.(map[string]any)
			if key == name {
				data := child["snapshot_data"].(map[string]any)
				var decoded map[string]any
				require.NoError(t, json.Unmarshal([]byte(data["state"].(string)), &decoded))
				change(decoded)
				encoded, err := json.Marshal(decoded)
				require.NoError(t, err)
				data["state"] = string(encoded)
				return
			}
			visit(child)
		}
	}
	visit(state)
}

func TestValidateMicroVMSnapshotBlockQueues(t *testing.T) {
	t.Run("drained_discontiguous_ranges_in_file_order", func(t *testing.T) {
		dir, config, state, memory := microVMSnapshotQueueFixture(t)
		writeMicroVMSnapshotQueueFixture(t, dir, config, state, memory)
		require.NoError(t, validateMicroVMSnapshotBlockQueues(dir))
	})
	t.Run("high_guest_address_maps_to_small_native_file_offset", func(t *testing.T) {
		dir, config, state, memory := microVMSnapshotQueueFixture(t)
		const base = uint64(math.MaxUint64 - 4095)
		mutateMicroVMSnapshotSection(t, state, "memory-manager", func(v map[string]any) {
			v["memory_ranges"] = map[string]any{"data": []any{map[string]any{"gpa": base, "length": 2048}, map[string]any{"gpa": 4096, "length": 2048}}}
		})
		mutateMicroVMSnapshotSection(t, state, "_virtio-pci-_disk0", func(v map[string]any) {
			v["queues"].([]any)[0].(map[string]any)["avail_ring"] = base + 6
		})
		writeMicroVMSnapshotQueueFixture(t, dir, config, state, memory)
		require.NoError(t, validateMicroVMSnapshotBlockQueues(dir))
	})
	t.Run("observed_13_outstanding_requests_with_uint16_wrap", func(t *testing.T) {
		dir, config, state, memory := microVMSnapshotQueueFixture(t)
		binary.LittleEndian.PutUint16(memory[8:], 3)
		binary.LittleEndian.PutUint16(memory[2062:], 65526)
		writeMicroVMSnapshotQueueFixture(t, dir, config, state, memory)
		require.ErrorContains(t, validateMicroVMSnapshotBlockQueues(dir), "13 outstanding requests")
	})
	t.Run("every_configured_disk_and_queue_is_checked", func(t *testing.T) {
		dir, config, state, memory := microVMSnapshotQueueFixture(t)
		config["disks"] = append(config["disks"].([]any), map[string]any{"id": "data", "vhost_user": true, "num_queues": 2, "queue_size": 128})
		transport := map[string]any{"queues": []any{
			map[string]any{"ready": true, "size": 128, "avail_ring": 8198, "used_ring": 4108},
			map[string]any{"ready": true, "size": 128, "avail_ring": 8222, "used_ring": 4132},
		}}
		encoded, err := json.Marshal(transport)
		require.NoError(t, err)
		children := state["snapshots"].(map[string]any)["device-manager"].(map[string]any)["snapshots"].(map[string]any)
		children["_virtio-pci-data"] = map[string]any{"snapshot_data": map[string]any{"state": string(encoded)}}
		writeMicroVMSnapshotQueueFixture(t, dir, config, state, memory)
		require.NoError(t, validateMicroVMSnapshotBlockQueues(dir))
		memory[32] = 1
		writeMicroVMSnapshotQueueFixture(t, dir, config, state, memory)
		require.ErrorContains(t, validateMicroVMSnapshotBlockQueues(dir), `disk "data" queue 1`)
	})
	t.Run("captured_native_queue_733_available_720_used", func(t *testing.T) {
		dir, config, state, _ := microVMSnapshotQueueFixture(t)
		mutateMicroVMSnapshotSection(t, state, "memory-manager", func(v map[string]any) {
			v["memory_ranges"] = map[string]any{"data": []any{map[string]any{"gpa": 0, "length": 1476395008}}}
		})
		mutateMicroVMSnapshotSection(t, state, "_virtio-pci-_disk0", func(v map[string]any) {
			v["queues"] = []any{map[string]any{"ready": true, "size": 128, "avail_ring": 73537536, "used_ring": 73537856}}
		})
		writeMicroVMSnapshotQueueFixture(t, dir, config, state, nil)
		file, err := os.OpenFile(filepath.Join(dir, "memory-ranges"), os.O_RDWR, 0600)
		require.NoError(t, err)
		defer file.Close()
		require.NoError(t, file.Truncate(1476395008)) // Sparse fixture: only four bytes are written/read.
		var index [2]byte
		binary.LittleEndian.PutUint16(index[:], 733)
		_, err = file.WriteAt(index[:], 73537538)
		require.NoError(t, err)
		binary.LittleEndian.PutUint16(index[:], 720)
		_, err = file.WriteAt(index[:], 73537858)
		require.NoError(t, err)
		require.ErrorContains(t, validateMicroVMSnapshotBlockQueues(dir), "13 outstanding requests")
		binary.LittleEndian.PutUint16(index[:], 733)
		_, err = file.WriteAt(index[:], 73537858)
		require.NoError(t, err)
		require.NoError(t, validateMicroVMSnapshotBlockQueues(dir))
	})
	t.Run("non_vhost_disks_need_no_backend_drain", func(t *testing.T) {
		dir, config, _, _ := microVMSnapshotQueueFixture(t)
		config["disks"].([]any)[0].(map[string]any)["vhost_user"] = false
		encoded, err := json.Marshal(config)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "config.json"), encoded, 0600))
		require.NoError(t, validateMicroVMSnapshotBlockQueues(dir))
	})
}

func TestValidateMicroVMSnapshotBlockQueuesRejectsInvalidState(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(map[string]any, map[string]any)
	}{
		{"missing_disks", func(c, s map[string]any) { delete(c, "disks") }},
		{"missing_vhost_flag", func(c, s map[string]any) { delete(c["disks"].([]any)[0].(map[string]any), "vhost_user") }},
		{"duplicate_disk", func(c, s map[string]any) { c["disks"] = append(c["disks"].([]any), c["disks"].([]any)[0]) }},
		{"missing_id", func(c, s map[string]any) { delete(c["disks"].([]any)[0].(map[string]any), "id") }},
		{"missing_queue_count", func(c, s map[string]any) { delete(c["disks"].([]any)[0].(map[string]any), "num_queues") }},
		{"wrong_queue_count", func(c, s map[string]any) { c["disks"].([]any)[0].(map[string]any)["num_queues"] = 2 }},
		{"invalid_config_capacity", func(c, s map[string]any) { c["disks"].([]any)[0].(map[string]any)["queue_size"] = 129 }},
		{"missing_transport", func(c, s map[string]any) {
			delete(s["snapshots"].(map[string]any)["device-manager"].(map[string]any)["snapshots"].(map[string]any), "_virtio-pci-_disk0")
		}},
		{"missing_memory_metadata", func(c, s map[string]any) { delete(s["snapshots"].(map[string]any), "memory-manager") }},
		{"duplicate_transport", func(c, s map[string]any) {
			s["snapshots"].(map[string]any)["_virtio-pci-_disk0"] = s["snapshots"].(map[string]any)["device-manager"].(map[string]any)["snapshots"].(map[string]any)["_virtio-pci-_disk0"]
		}},
		{"empty_queues", func(c, s map[string]any) {
			mutateMicroVMSnapshotSection(t, s, "_virtio-pci-_disk0", func(v map[string]any) { v["queues"] = []any{} })
		}},
		{"inactive_queue", func(c, s map[string]any) {
			mutateMicroVMSnapshotSection(t, s, "_virtio-pci-_disk0", func(v map[string]any) { v["queues"].([]any)[0].(map[string]any)["ready"] = false })
		}},
		{"missing_ring", func(c, s map[string]any) {
			mutateMicroVMSnapshotSection(t, s, "_virtio-pci-_disk0", func(v map[string]any) { delete(v["queues"].([]any)[0].(map[string]any), "avail_ring") })
		}},
		{"zero_size", func(c, s map[string]any) {
			mutateMicroVMSnapshotSection(t, s, "_virtio-pci-_disk0", func(v map[string]any) { v["queues"].([]any)[0].(map[string]any)["size"] = 0 })
		}},
		{"ring_outside_memory", func(c, s map[string]any) {
			mutateMicroVMSnapshotSection(t, s, "_virtio-pci-_disk0", func(v map[string]any) { v["queues"].([]any)[0].(map[string]any)["avail_ring"] = 11000 })
		}},
		{"ring_index_crosses_range", func(c, s map[string]any) {
			mutateMicroVMSnapshotSection(t, s, "_virtio-pci-_disk0", func(v map[string]any) { v["queues"].([]any)[0].(map[string]any)["avail_ring"] = 10238 })
		}},
		{"ring_overflow", func(c, s map[string]any) {
			mutateMicroVMSnapshotSection(t, s, "_virtio-pci-_disk0", func(v map[string]any) { v["queues"].([]any)[0].(map[string]any)["avail_ring"] = uint64(math.MaxUint64) })
		}},
		{"overlapping_ranges", func(c, s map[string]any) {
			mutateMicroVMSnapshotSection(t, s, "memory-manager", func(v map[string]any) {
				v["memory_ranges"] = map[string]any{"data": []any{map[string]any{"gpa": 0, "length": 128}, map[string]any{"gpa": 64, "length": 128}}}
			})
		}},
		{"range_overflow", func(c, s map[string]any) {
			mutateMicroVMSnapshotSection(t, s, "memory-manager", func(v map[string]any) {
				v["memory_ranges"] = map[string]any{"data": []any{map[string]any{"gpa": uint64(math.MaxUint64), "length": 1}}}
			})
		}},
		{"empty_ranges", func(c, s map[string]any) {
			mutateMicroVMSnapshotSection(t, s, "memory-manager", func(v map[string]any) { v["memory_ranges"] = map[string]any{"data": []any{}} })
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, config, state, memory := microVMSnapshotQueueFixture(t)
			test.change(config, state)
			writeMicroVMSnapshotQueueFixture(t, dir, config, state, memory)
			require.Error(t, validateMicroVMSnapshotBlockQueues(dir))
		})
	}
	for _, name := range []string{"config.json", "state.json", "memory-ranges"} {
		t.Run("missing_"+name, func(t *testing.T) {
			dir, config, state, memory := microVMSnapshotQueueFixture(t)
			writeMicroVMSnapshotQueueFixture(t, dir, config, state, memory)
			require.NoError(t, os.Remove(filepath.Join(dir, name)))
			require.Error(t, validateMicroVMSnapshotBlockQueues(dir))
		})
		if name != "memory-ranges" {
			t.Run("malformed_"+name, func(t *testing.T) {
				dir, config, state, memory := microVMSnapshotQueueFixture(t)
				writeMicroVMSnapshotQueueFixture(t, dir, config, state, memory)
				require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("{"), 0600))
				require.Error(t, validateMicroVMSnapshotBlockQueues(dir))
			})
		}
	}
	for _, size := range []int{4095, 4097} {
		t.Run(fmt.Sprintf("memory_length_%d", size), func(t *testing.T) {
			dir, config, state, _ := microVMSnapshotQueueFixture(t)
			writeMicroVMSnapshotQueueFixture(t, dir, config, state, make([]byte, size))
			require.ErrorContains(t, validateMicroVMSnapshotBlockQueues(dir), "memory file length")
		})
	}
}
