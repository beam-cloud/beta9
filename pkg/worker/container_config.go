package worker

func cgroupV2Parameters() map[string]string {
	return map[string]string{
		// Kill every process in the container cgroup when it runs out of memory.
		"memory.oom.group": "1",
	}
}
