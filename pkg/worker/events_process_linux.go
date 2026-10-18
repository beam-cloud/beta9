//go:build linux

package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/v4/process"
)

// Read this process's children rather than scanning every PID on the worker
// for every VM sample. Children can belong to any of the parent's threads.
func childProcesses(p *process.Process) ([]*process.Process, error) {
	taskDir := fmt.Sprintf("/proc/%d/task", p.Pid)
	tasks, err := os.ReadDir(taskDir)
	if err != nil {
		return nil, err
	}
	children := []*process.Process{}
	seen := map[int32]bool{}
	readAny := false
	for _, task := range tasks {
		data, err := os.ReadFile(filepath.Join(taskDir, task.Name(), "children"))
		if os.IsNotExist(err) {
			continue // The thread exited, or the kernel lacks this proc file.
		}
		if err != nil {
			return nil, err
		}
		readAny = true
		for _, value := range strings.Fields(string(data)) {
			pid, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return nil, err
			}
			if seen[int32(pid)] {
				continue
			}
			seen[int32(pid)] = true
			if child, err := process.NewProcess(int32(pid)); err == nil {
				children = append(children, child)
			}
		}
	}
	if !readAny {
		return p.Children()
	}
	return children, nil
}
