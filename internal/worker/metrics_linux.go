//go:build linux
// +build linux

package worker

import (
	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/okteto/okteto/pkg/log"
)

func (wm *WorkerMetrics) InitNvml() {
	// TODO: investigate segmentation violation
	// wm.nvmlActive = nvml.Init() == nvml.SUCCESS
	wm.nvmlActive = false
}

func (wm *WorkerMetrics) Shutdown() {
	if wm.nvmlActive {
		if ret := nvml.Shutdown(); ret != nvml.SUCCESS {
			log.Printf("Failed to shutdown nvml: %v\n", ret)
		}
	}
}
