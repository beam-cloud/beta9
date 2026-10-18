//go:build !linux

package worker

import "github.com/shirou/gopsutil/v4/process"

func childProcesses(p *process.Process) ([]*process.Process, error) {
	return p.Children()
}
