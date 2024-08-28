package worker

import (
	"runtime"

	"github.com/prometheus/procfs"
)

func GetSystemCPU() (float64, error) {
	fs, err := procfs.NewFS("/proc")
	if err != nil {
		return 0, err
	}

	stats, err := fs.Stat()
	if err != nil {
		return 0, err
	}

	systemUptime :=
		stats.CPUTotal.User +
			stats.CPUTotal.System +
			stats.CPUTotal.Nice +
			stats.CPUTotal.Idle +
			stats.CPUTotal.IRQ +
			stats.CPUTotal.SoftIRQ +
			stats.CPUTotal.Steal +
			stats.CPUTotal.Guest +
			stats.CPUTotal.GuestNice
	return systemUptime, nil
}

func GetProcCurrentCPUMillicores(cpuTime float64, prevCPUTime float64, systemCPUTime float64, prevSystemCPUTime float64) float64 {
	totalMillicores := float64(runtime.NumCPU() * 1000)
	utilizationPercent := (cpuTime - prevCPUTime) / (systemCPUTime - prevSystemCPUTime)

	if utilizationPercent*totalMillicores < 0 {
		return 0
	}

	return utilizationPercent * totalMillicores
}

type ProcUtil struct {
	procfs.Proc
}

func NewProcUtil(pid int) (*ProcUtil, error) {
	proc, err := procfs.NewProc(pid)
	if err != nil {
		return nil, err
	}
	return &ProcUtil{
		Proc: proc,
	}, nil
}

func createPPidToProcMapping(procs procfs.Procs) (map[int][]procfs.Proc, error) {
	ppidToProcs := make(map[int][]procfs.Proc)

	for _, p := range procs {
		pstat, err := p.Stat()
		if err != nil {
			return ppidToProcs, err
		}

		ppidToProcs[int(pstat.PPID)] = append(ppidToProcs[int(pstat.PPID)], p)
	}

	return ppidToProcs, nil
}

func (p *ProcUtil) getAllDescendantProcs() ([]*ProcUtil, error) {
	var descProcs []*ProcUtil

	procs, err := procfs.AllProcs()
	if err != nil {
		return nil, err
	}

	ppidToProcsMapping, err := createPPidToProcMapping(procs)
	if err != nil {
		return nil, err
	}

	queue := []int{int(p.PID)}
	index := 0

	for index < len(queue) {
		pid := queue[index]
		index += 1

		procs := ppidToProcsMapping[pid]
		for _, proc := range procs {
			descProcs = append(descProcs, &ProcUtil{Proc: proc})
			queue = append(queue, int(proc.PID))
		}
	}

	return descProcs, nil
}
