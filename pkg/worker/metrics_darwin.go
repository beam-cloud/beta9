//go:build darwin
// +build darwin

package worker

func (wm *WorkerMetrics) InitNvml() {}

func (wm *WorkerMetrics) Shutdown() {}
