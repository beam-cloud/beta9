package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// JobMonitor watches k3s pods for job activity
type JobMonitor struct {
	state  *AgentState
	cancel context.CancelFunc
}

// PodWatchEvent represents a kubectl watch event
type PodWatchEvent struct {
	Type   string `json:"type"` // ADDED, MODIFIED, DELETED
	Object struct {
		Metadata struct {
			Name              string            `json:"name"`
			Labels            map[string]string `json:"labels"`
			CreationTimestamp string            `json:"creationTimestamp"`
		} `json:"metadata"`
		Status struct {
			Phase             string `json:"phase"` // Pending, Running, Succeeded, Failed
			ContainerStatuses []struct {
				Name  string `json:"name"`
				Ready bool   `json:"ready"`
				State struct {
					Running *struct {
						StartedAt string `json:"startedAt"`
					} `json:"running"`
					Terminated *struct {
						ExitCode   int    `json:"exitCode"`
						FinishedAt string `json:"finishedAt"`
					} `json:"terminated"`
				} `json:"state"`
			} `json:"containerStatuses"`
		} `json:"status"`
	} `json:"object"`
}

// NewJobMonitor creates a new job monitor
func NewJobMonitor(state *AgentState) *JobMonitor {
	return &JobMonitor{
		state: state,
	}
}

// Start begins watching for pods
func (m *JobMonitor) Start(ctx context.Context) {
	watchCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	go m.watchPods(watchCtx)
}

// Stop stops the job monitor
func (m *JobMonitor) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
}

// watchPods watches k3s pods using kubectl
func (m *JobMonitor) watchPods(ctx context.Context) {
	log.Debug().Msg("Starting pod watcher")

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Pod watcher stopped")
			return
		default:
		}

		// Run kubectl watch. Use the cached absolute path (P1-C: PATH
		// hijack) and exit the loop if kubectl isn't installed —
		// non-k3s workers don't need a pod watcher.
		kubectlPath := KubectlPath()
		if kubectlPath == "" {
			log.Debug().Msg("kubectl not installed; pod watcher disabled")
			return
		}
		// Watch for pods with beta9-related labels
		cmd := exec.CommandContext(ctx, kubectlPath, "get", "pods",
			"-n", "default",
			"-l", "app.kubernetes.io/managed-by=beta9",
			"-o", "json",
			"--watch",
			"--output-watch-events")

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			log.Warn().Err(err).Msg("Failed to create stdout pipe for kubectl")
			time.Sleep(5 * time.Second)
			continue
		}

		if err := cmd.Start(); err != nil {
			log.Warn().Err(err).Msg("Failed to start kubectl watch")
			time.Sleep(5 * time.Second)
			continue
		}

		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			m.processWatchEvent(line)
		}

		if err := scanner.Err(); err != nil {
			log.Debug().Err(err).Msg("Scanner error (may be expected on context cancel)")
		}

		cmd.Wait()

		// Brief pause before reconnecting
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// processWatchEvent processes a single watch event
func (m *JobMonitor) processWatchEvent(line string) {
	var event PodWatchEvent
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		log.Debug().Str("line", line).Msg("Failed to parse watch event")
		return
	}

	podName := event.Object.Metadata.Name
	if podName == "" {
		return
	}

	// Extract function name from labels or pod name
	funcName := event.Object.Metadata.Labels["beta9.io/stub-id"]
	if funcName == "" {
		funcName = extractFuncName(podName)
	}

	// Determine job status based on pod phase and container status
	status := m.determineJobStatus(event)

	// Parse timestamps
	startTime := parseK8sTime(event.Object.Metadata.CreationTimestamp)
	var endTime time.Time
	var exitCode int

	if len(event.Object.Status.ContainerStatuses) > 0 {
		cs := event.Object.Status.ContainerStatuses[0]
		if cs.State.Terminated != nil {
			endTime = parseK8sTime(cs.State.Terminated.FinishedAt)
			exitCode = cs.State.Terminated.ExitCode
		}
	}

	job := JobInfo{
		PodName:   podName,
		FuncName:  funcName,
		Status:    status,
		StartTime: startTime,
		EndTime:   endTime,
		ExitCode:  exitCode,
	}

	if !endTime.IsZero() && !startTime.IsZero() {
		job.Duration = endTime.Sub(startTime)
	}

	log.Debug().
		Str("event", event.Type).
		Str("pod", podName).
		Str("status", string(status)).
		Msg("Pod event")

	m.state.AddJob(job)
}

// determineJobStatus maps pod phase to JobStatus
func (m *JobMonitor) determineJobStatus(event PodWatchEvent) JobStatus {
	phase := event.Object.Status.Phase

	// Check container status first
	if len(event.Object.Status.ContainerStatuses) > 0 {
		cs := event.Object.Status.ContainerStatuses[0]
		if cs.State.Terminated != nil {
			if cs.State.Terminated.ExitCode == 0 {
				return JobStatusCompleted
			}
			return JobStatusFailed
		}
		if cs.State.Running != nil {
			return JobStatusRunning
		}
	}

	// Fall back to pod phase
	switch phase {
	case "Pending":
		return JobStatusPending
	case "Running":
		return JobStatusRunning
	case "Succeeded":
		return JobStatusCompleted
	case "Failed":
		return JobStatusFailed
	default:
		return JobStatusPending
	}
}

// extractFuncName extracts function name from pod name
func extractFuncName(podName string) string {
	// Pod names typically like: worker-abc123-hello-xyz
	parts := strings.Split(podName, "-")
	if len(parts) >= 3 {
		return strings.Join(parts[2:len(parts)-1], "-")
	}
	return podName
}

// parseK8sTime parses Kubernetes timestamp
func parseK8sTime(ts string) time.Time {
	if ts == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return time.Time{}
	}
	return t
}

// RefreshPods does a one-time refresh of current pods
func (m *JobMonitor) RefreshPods(ctx context.Context) {
	kubectlPath := KubectlPath()
	if kubectlPath == "" {
		log.Debug().Msg("kubectl not installed; skipping RefreshPods")
		return
	}
	cmd := exec.CommandContext(ctx, kubectlPath, "get", "pods",
		"-n", "default",
		"-l", "app.kubernetes.io/managed-by=beta9",
		"-o", "json")

	output, err := cmd.Output()
	if err != nil {
		log.Debug().Err(err).Msg("Failed to get initial pods")
		return
	}

	var podList struct {
		Items []struct {
			Metadata struct {
				Name              string            `json:"name"`
				Labels            map[string]string `json:"labels"`
				CreationTimestamp string            `json:"creationTimestamp"`
			} `json:"metadata"`
			Status struct {
				Phase             string `json:"phase"`
				ContainerStatuses []struct {
					State struct {
						Running *struct {
							StartedAt string `json:"startedAt"`
						} `json:"running"`
						Terminated *struct {
							ExitCode   int    `json:"exitCode"`
							FinishedAt string `json:"finishedAt"`
						} `json:"terminated"`
					} `json:"state"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}

	if err := json.Unmarshal(output, &podList); err != nil {
		log.Debug().Err(err).Msg("Failed to parse pod list")
		return
	}

	for _, pod := range podList.Items {
		funcName := pod.Metadata.Labels["beta9.io/stub-id"]
		if funcName == "" {
			funcName = extractFuncName(pod.Metadata.Name)
		}

		status := JobStatusPending
		switch pod.Status.Phase {
		case "Running":
			status = JobStatusRunning
		case "Succeeded":
			status = JobStatusCompleted
		case "Failed":
			status = JobStatusFailed
		}

		var endTime time.Time
		var exitCode int
		if len(pod.Status.ContainerStatuses) > 0 {
			cs := pod.Status.ContainerStatuses[0]
			if cs.State.Terminated != nil {
				endTime = parseK8sTime(cs.State.Terminated.FinishedAt)
				exitCode = cs.State.Terminated.ExitCode
				if exitCode == 0 {
					status = JobStatusCompleted
				} else {
					status = JobStatusFailed
				}
			}
		}

		job := JobInfo{
			PodName:   pod.Metadata.Name,
			FuncName:  funcName,
			Status:    status,
			StartTime: parseK8sTime(pod.Metadata.CreationTimestamp),
			EndTime:   endTime,
			ExitCode:  exitCode,
		}

		m.state.AddJob(job)
	}
}
