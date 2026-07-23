package worker

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
)

var eightGPUOutput = `0x0000, 00000000:23:00.0, 0, GPU-afb8c77a-62ef-a631-48d0-edc9670fef25
0x0000, 00000000:41:00.0, 1, GPU-c79e0183-59ff-a978-4cf6-5bf40338045b
0x0000, 00000000:61:00.0, 2, GPU-bebdb9f9-f79f-1757-74f8-5e633319af12
0x0000, 00000000:81:00.0, 3, GPU-c3a5fd20-3426-6869-e9cd-d9a80d6cfd0f
0x0000, 00000000:A1:00.0, 4, GPU-df0e69ce-6dbd-a6cf-89f3-92ac8be4e7c6
0x0000, 00000000:C1:00.0, 5, GPU-77d7560d-1290-8018-3a05-bf85cae3b504
0x0000, 00000000:C2:00.0, 6, GPU-fa89e45e-d56c-a9fd-6672-652c04f2818e
0x0000, 00000000:E1:00.0, 7, GPU-0fe76e55-b048-6fa9-826e-c71d2266d0cb`

func withMockDevices(output string, gpuExists bool) func() {
	origQuery := queryDevices
	origCheck := checkGPUExists

	queryDevices = func() ([]byte, error) {
		return []byte(output), nil
	}
	checkGPUExists = func(busId string) (bool, error) {
		return gpuExists, nil
	}

	return func() {
		queryDevices = origQuery
		checkGPUExists = origCheck
	}
}

func TestAvailableGPUDevicesSomeVisibleDevices(t *testing.T) {
	cleanup := withMockDevices(eightGPUOutput, true)
	defer cleanup()

	client := &NvidiaInfoClient{
		visibleDevices: "GPU-c3a5fd20-3426-6869-e9cd-d9a80d6cfd0f,GPU-df0e69ce-6dbd-a6cf-89f3-92ac8be4e7c6",
	}

	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Equal(t, []int{3, 4}, devices)
}

func TestAvailableGPUDevicesAllVisibleDevices(t *testing.T) {
	cleanup := withMockDevices(eightGPUOutput, true)
	defer cleanup()

	client := &NvidiaInfoClient{visibleDevices: "all"}

	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, devices)
}

func TestNvidiaInfoClientReusesDeviceInventory(t *testing.T) {
	originalQueryDevices := queryDevices
	originalCheckGPUExists := checkGPUExists
	defer func() {
		queryDevices = originalQueryDevices
		checkGPUExists = originalCheckGPUExists
	}()

	queryCount := 0
	queryDevices = func() ([]byte, error) {
		queryCount++
		return []byte(eightGPUOutput), nil
	}
	checkGPUExists = func(string) (bool, error) { return true, nil }

	client := &NvidiaInfoClient{visibleDevices: "all"}
	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Len(t, devices, 8)
	uuids, err := client.DeviceUUIDs()
	assert.NoError(t, err)
	assert.Len(t, uuids, 8)
	_, err = client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Equal(t, 1, queryCount)
}

func TestNvidiaInfoClientRetriesFailedInventoryQuery(t *testing.T) {
	originalQueryDevices := queryDevices
	originalCheckGPUExists := checkGPUExists
	defer func() {
		queryDevices = originalQueryDevices
		checkGPUExists = originalCheckGPUExists
	}()

	queryCount := 0
	queryDevices = func() ([]byte, error) {
		queryCount++
		if queryCount == 1 {
			return nil, errors.New("temporary nvidia-smi failure")
		}
		return []byte(eightGPUOutput), nil
	}
	checkGPUExists = func(string) (bool, error) { return true, nil }

	client := &NvidiaInfoClient{visibleDevices: "all"}
	_, err := client.AvailableGPUDevices()
	assert.Error(t, err)
	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Len(t, devices, 8)
	_, err = client.DeviceUUIDs()
	assert.NoError(t, err)
	assert.Equal(t, 2, queryCount)
}

func TestAvailableGPUDevicesWithNonZeroPCIDomains(t *testing.T) {
	originalQueryDevices := queryDevices
	defer func() { queryDevices = originalQueryDevices }()

	originalCheckGPUExists := checkGPUExists
	defer func() { checkGPUExists = originalCheckGPUExists }()

	queryDevices = func() ([]byte, error) {
		mockOutput := `0x0001, 00000001:00:1E.0, 0, GPU-afcdd0c4-4e05-0d70-b751-6ffb42883041`
		return []byte(mockOutput), nil
	}

	checkGPUExists = func(busId string) (bool, error) {
		assert.Equal(t, "0001:00:1e.0", busId)
		return true, nil
	}

	client := &NvidiaInfoClient{visibleDevices: "all"}

	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Equal(t, []int{0}, devices)
}

func TestAvailableGPUDevicesReturnsEmptyWhenVisibleDevicesDoNotMatch(t *testing.T) {
	cleanup := withMockDevices(
		`0x0000, 00000000:23:00.0, 0, GPU-afb8c77a-62ef-a631-48d0-edc9670fef25`,
		true,
	)
	defer cleanup()

	client := &NvidiaInfoClient{visibleDevices: "GPU-some-other-device"}

	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Empty(t, devices)
}

func TestAvailableGPUDevicesVoidReturnsEmpty(t *testing.T) {
	cleanup := withMockDevices(
		`0x0000, 00000000:E1:00.0, 7, GPU-97b84d1d-7956-1a63-4443-ac95d3d11db1`,
		true,
	)
	defer cleanup()

	client := &NvidiaInfoClient{visibleDevices: "void"}

	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Empty(t, devices)
}

func TestAvailableGPUDevicesEmptyReturnsEmpty(t *testing.T) {
	cleanup := withMockDevices(
		`0x0000, 00000000:E1:00.0, 7, GPU-97b84d1d-7956-1a63-4443-ac95d3d11db1`,
		true,
	)
	defer cleanup()

	client := &NvidiaInfoClient{visibleDevices: ""}

	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Empty(t, devices)
}

func TestAvailableGPUDevicesReturnsEmptyWhenProcEntryMissing(t *testing.T) {
	cleanup := withMockDevices(
		`0x0000, 00000000:23:00.0, 0, GPU-afb8c77a-62ef-a631-48d0-edc9670fef25`,
		false,
	)
	defer cleanup()

	client := &NvidiaInfoClient{visibleDevices: "all"}

	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Empty(t, devices)
}

func TestAvailableGPUDevicesIgnoresProcCheckErrors(t *testing.T) {
	originalQueryDevices := queryDevices
	defer func() { queryDevices = originalQueryDevices }()

	originalCheckGPUExists := checkGPUExists
	defer func() { checkGPUExists = originalCheckGPUExists }()

	queryDevices = func() ([]byte, error) {
		return []byte(`0x0000, 00000000:23:00.0, 0, GPU-afb8c77a-62ef-a631-48d0-edc9670fef25`), nil
	}

	checkGPUExists = func(busId string) (bool, error) {
		return false, errors.New("procfs unavailable")
	}

	client := &NvidiaInfoClient{visibleDevices: "all"}

	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Empty(t, devices)
}

func TestAvailableGPUDevicesReturnsQueryErrors(t *testing.T) {
	originalQueryDevices := queryDevices
	defer func() { queryDevices = originalQueryDevices }()

	queryDevices = func() ([]byte, error) {
		return nil, errors.New("nvidia-smi failed")
	}

	client := &NvidiaInfoClient{visibleDevices: "all"}

	devices, err := client.AvailableGPUDevices()
	assert.Error(t, err)
	assert.Nil(t, devices)
}

func TestAvailableGPUDevicesSkipsFailedProcAdapter(t *testing.T) {
	originalQueryDevices := queryDevices
	defer func() { queryDevices = originalQueryDevices }()
	originalCheckGPUExists := checkGPUExists
	defer func() { checkGPUExists = originalCheckGPUExists }()

	queryDevices = func() ([]byte, error) {
		return []byte(`0x0000, 00000000:01:00.0, 0, GPU-bad
0x0000, 00000000:24:00.0, 1, GPU-good`), nil
	}
	root := t.TempDir()
	writeWorkerNvidiaProcGPUInfo(t, root, "0000:01:00.0", "GPU-bad", "N/A", "??.??.??.??.?")
	writeWorkerNvidiaProcGPUInfo(t, root, "0000:24:00.0", "GPU-good", "580.126.18", "95.02.3c.40.b8")
	withNvidiaProcGPUInfoRoot(t, root)

	client := &NvidiaInfoClient{visibleDevices: "all"}

	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Equal(t, []int{1}, devices)
}

func TestAvailableGPUDevicesSingleGPUUUID(t *testing.T) {
	cleanup := withMockDevices(eightGPUOutput, true)
	defer cleanup()

	client := &NvidiaInfoClient{
		visibleDevices: "GPU-0fe76e55-b048-6fa9-826e-c71d2266d0cb",
	}

	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Equal(t, []int{7}, devices)
}

func TestAvailableGPUDevicesMatchesByIndex(t *testing.T) {
	cleanup := withMockDevices(eightGPUOutput, true)
	defer cleanup()

	// Agent GPU assignments may be device indices instead of UUIDs.
	client := &NvidiaInfoClient{visibleDevices: "3,4"}

	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Equal(t, []int{3, 4}, devices)
}

func TestResolveVisibleDevicesPrefersWorkerGPUDevices(t *testing.T) {
	t.Setenv(types.WorkerPodUIDEnv, "")
	t.Setenv(types.WorkerGPUDevicesEnv, "all")
	// The NVIDIA container toolkit rewrites NVIDIA_VISIBLE_DEVICES to "void"
	// inside the container after injecting devices.
	t.Setenv(types.NvidiaVisibleDevicesEnv, "void")

	assert.Equal(t, "all", resolveVisibleDevices())
}

func TestResolveVisibleDevicesVoidMapsToAllForAgentWorkers(t *testing.T) {
	t.Setenv(types.WorkerPodUIDEnv, "")
	t.Setenv(types.WorkerGPUDevicesEnv, "")
	t.Setenv(types.NvidiaVisibleDevicesEnv, "void")
	t.Setenv(types.WorkerPersistentEnv, "true")
	t.Setenv(types.WorkerMachineEnv, "machine-a")

	assert.Equal(t, "all", resolveVisibleDevices())
}

func TestResolveVisibleDevicesVoidStaysVoidForClusterWorkers(t *testing.T) {
	t.Setenv(types.WorkerPodUIDEnv, "")
	t.Setenv(types.WorkerGPUDevicesEnv, "")
	t.Setenv(types.NvidiaVisibleDevicesEnv, "void")
	t.Setenv(types.WorkerPersistentEnv, "")
	t.Setenv(types.WorkerMachineEnv, "")

	assert.Equal(t, "void", resolveVisibleDevices())
}

func writeCheckpointFile(t *testing.T, dir string, entries []podDeviceEntry) string {
	t.Helper()
	checkpoint := kubeletCheckpoint{}
	checkpoint.Data.PodDeviceEntries = entries
	data, err := json.Marshal(checkpoint)
	assert.NoError(t, err)
	path := filepath.Join(dir, "kubelet_internal_checkpoint")
	assert.NoError(t, os.WriteFile(path, data, 0644))
	return path
}

func TestResolveVisibleDevicesFromCheckpoint(t *testing.T) {
	origResolve := resolveVisibleDevices
	defer func() { resolveVisibleDevices = origResolve }()

	tmpDir := t.TempDir()
	checkpointPath := writeCheckpointFile(t, tmpDir, []podDeviceEntry{
		{
			PodUID:       "test-pod-uid-1",
			ResourceName: "nvidia.com/gpu",
			DeviceIDs:    map[string][]string{"0": {"GPU-aaaa-bbbb-cccc"}},
		},
		{
			PodUID:       "test-pod-uid-2",
			ResourceName: "nvidia.com/gpu",
			DeviceIDs:    map[string][]string{"1": {"GPU-dddd-eeee-ffff"}},
		},
	})

	resolveVisibleDevices = func() string {
		podUID := "test-pod-uid-1"
		data, err := os.ReadFile(checkpointPath)
		if err != nil {
			return "fallback"
		}
		var cp kubeletCheckpoint
		if err := json.Unmarshal(data, &cp); err != nil {
			return "fallback"
		}
		for _, entry := range cp.Data.PodDeviceEntries {
			if entry.PodUID != podUID || entry.ResourceName != "nvidia.com/gpu" {
				continue
			}
			for _, uuids := range entry.DeviceIDs {
				if len(uuids) > 0 {
					return uuids[0]
				}
			}
		}
		return "fallback"
	}

	result := resolveVisibleDevices()
	assert.Equal(t, "GPU-aaaa-bbbb-cccc", result)
}

func TestResolveVisibleDevicesFallsBackWithoutPodUID(t *testing.T) {
	origResolve := resolveVisibleDevices
	defer func() { resolveVisibleDevices = origResolve }()

	os.Setenv(types.NvidiaVisibleDevicesEnv, types.NvidiaVisibleDevicesAll)
	defer os.Unsetenv(types.NvidiaVisibleDevicesEnv)

	resolveVisibleDevices = func() string {
		podUID := ""
		if podUID == "" {
			return os.Getenv(types.NvidiaVisibleDevicesEnv)
		}
		return "should-not-reach"
	}

	result := resolveVisibleDevices()
	assert.Equal(t, "all", result)
}

func withNvidiaProcGPUInfoRoot(t *testing.T, root string) {
	t.Helper()
	previous := nvidiaProcGPUInfoRoot
	nvidiaProcGPUInfoRoot = root
	t.Cleanup(func() { nvidiaProcGPUInfoRoot = previous })
}

func writeWorkerNvidiaProcGPUInfo(t *testing.T, root, busID, uuid, firmware, videoBIOS string) {
	t.Helper()
	dir := filepath.Join(root, busID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	info := strings.Join([]string{
		"Model: \t\t NVIDIA GeForce RTX 4090",
		"GPU UUID: \t " + uuid,
		"Video BIOS: \t " + videoBIOS,
		"Bus Location: \t " + busID,
		"Device Minor: \t 0",
		"GPU Firmware: \t " + firmware,
	}, "\n")
	if err := os.WriteFile(filepath.Join(dir, "information"), []byte(info), 0644); err != nil {
		t.Fatal(err)
	}
}
