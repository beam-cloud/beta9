package common

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/v4/process"
)

var (
	ErrInvalidScore = errors.New("oom score must be between -1000 and 1000")
)

type PID interface {
	int | int32 | uint32
}

func MatchParentOOMScoreAdj[T PID](pid T) error {
	score, err := GetOOMScoreAdj(os.Getpid())
	if err != nil {
		return err
	}

	procs, err := FindProcesses(pid)
	if err != nil {
		return err
	}

	var errs []error
	for _, p := range procs {
		if err := SetOOMScoreAdj(int(p.Pid), score); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func GetOOMScoreAdj[T PID](pid T) (int, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid pid: %d", pid)
	}

	path := fmt.Sprintf("/proc/%d/oom_score_adj", pid)
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("failed to read oom_score_adj: %v", err)
	}

	score, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("failed to parse oom_score_adj: %v", err)
	}

	return score, nil
}

func SetOOMScoreAdj(pid, score int) error {
	if pid <= 0 {
		return fmt.Errorf("invalid pid: %d", pid)
	}
	if score < -1000 || score > 1000 {
		return ErrInvalidScore
	}

	path := fmt.Sprintf("/proc/%d/oom_score_adj", pid)
	if err := os.WriteFile(path, []byte(strconv.Itoa(score)), 0644); err != nil {
		return fmt.Errorf("failed to set oom_score_adj: %v", err)
	}

	return nil
}

func FindProcesses[T PID](pid T) ([]*process.Process, error) {
	processes, err := process.Processes()
	if err != nil {
		return nil, err
	}

	for _, p := range processes {
		if p.Pid == int32(pid) {
			return append(FindChildProcesses(p), p), nil
		}
	}

	return nil, fmt.Errorf("failed to find processes for pid %v", pid)
}

func FindChildProcesses(p *process.Process) []*process.Process {
	children, err := p.Children()
	if err != nil {
		// An error will occur when there are no children (pgrep -P <pid>)
		return nil
	}

	processes := []*process.Process{}
	processes = append(processes, children...)

	for _, child := range children {
		grandChildren := FindChildProcesses(child)
		if grandChildren == nil {
			continue
		}
		processes = append(processes, grandChildren...)
	}

	return processes
}
