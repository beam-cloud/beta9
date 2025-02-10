package worker

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAvailableGPUDevicesSomeVisibleDevices(t *testing.T) {
	originalQueryDevices := queryDevices
	defer func() { queryDevices = originalQueryDevices }()

	originalCheckGPUExists := checkGPUExists
	defer func() { checkGPUExists = originalCheckGPUExists }()

	queryDevices = func() ([]byte, error) {
		mockOutput := `0x0000, 00000000:23:00.0, 0, GPU-afb8c77a-62ef-a631-48d0-edc9670fef25
0x0000, 00000000:41:00.0, 1, GPU-c79e0183-59ff-a978-4cf6-5bf40338045b
0x0000, 00000000:61:00.0, 2, GPU-bebdb9f9-f79f-1757-74f8-5e633319af12
0x0000, 00000000:81:00.0, 3, GPU-c3a5fd20-3426-6869-e9cd-d9a80d6cfd0f
0x0000, 00000000:A1:00.0, 4, GPU-df0e69ce-6dbd-a6cf-89f3-92ac8be4e7c6
0x0000, 00000000:C1:00.0, 5, GPU-77d7560d-1290-8018-3a05-bf85cae3b504
0x0000, 00000000:C2:00.0, 6, GPU-fa89e45e-d56c-a9fd-6672-652c04f2818e
0x0000, 00000000:E1:00.0, 7, GPU-0fe76e55-b048-6fa9-826e-c71d2266d0cb`
		return []byte(mockOutput), nil
	}

	checkGPUExists = func(busId string) (bool, error) {
		return true, nil
	}

	client := &NvidiaInfoClient{}
	os.Setenv("NVIDIA_VISIBLE_DEVICES", "GPU-c3a5fd20-3426-6869-e9cd-d9a80d6cfd0f, GPU-df0e69ce-6dbd-a6cf-89f3-92ac8be4e7c6")

	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Equal(t, []int{3, 4}, devices)
}

func TestAvailableGPUDevicesAllVisibleDevices(t *testing.T) {
	originalQueryDevices := queryDevices
	defer func() { queryDevices = originalQueryDevices }()

	originalCheckGPUExists := checkGPUExists
	defer func() { checkGPUExists = originalCheckGPUExists }()

	queryDevices = func() ([]byte, error) {
		mockOutput := `0x0000, 00000000:23:00.0, 0, GPU-afb8c77a-62ef-a631-48d0-edc9670fef25
0x0000, 00000000:41:00.0, 1, GPU-c79e0183-59ff-a978-4cf6-5bf40338045b
0x0000, 00000000:61:00.0, 2, GPU-bebdb9f9-f79f-1757-74f8-5e633319af12
0x0000, 00000000:81:00.0, 3, GPU-c3a5fd20-3426-6869-e9cd-d9a80d6cfd0f
0x0000, 00000000:A1:00.0, 4, GPU-df0e69ce-6dbd-a6cf-89f3-92ac8be4e7c6
0x0000, 00000000:C1:00.0, 5, GPU-77d7560d-1290-8018-3a05-bf85cae3b504
0x0000, 00000000:C2:00.0, 6, GPU-fa89e45e-d56c-a9fd-6672-652c04f2818e
0x0000, 00000000:E1:00.0, 7, GPU-0fe76e55-b048-6fa9-826e-c71d2266d0cb`
		return []byte(mockOutput), nil
	}

	checkGPUExists = func(busId string) (bool, error) {
		return true, nil
	}

	client := &NvidiaInfoClient{}
	os.Setenv("NVIDIA_VISIBLE_DEVICES", "all")

	devices, err := client.AvailableGPUDevices()
	assert.NoError(t, err)
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, devices)
}
