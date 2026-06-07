package agent

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type dockerContainerInspect struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	State struct {
		Status string `json:"Status"`
	} `json:"State"`
	Config struct {
		Labels map[string]string `json:"Labels"`
		Env    []string          `json:"Env"`
	} `json:"Config"`
}

func removeManagedWorkerContainer(name string, slot *pb.AgentWorkerSlot) error {
	if slot == nil {
		return fmt.Errorf("worker slot is required to remove managed container %q", name)
	}
	owned, exists, err := dockerContainerOwnedByAgent(name, slot)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if !owned {
		return fmt.Errorf("docker container %q already exists and is not managed by the Beam agent", name)
	}

	return removeDockerContainer(name)
}

func removeStaleManagedWorkerContainers(slot *pb.AgentWorkerSlot) error {
	if slot == nil {
		return nil
	}
	out, err := exec.Command("docker", "ps", "-aq", "--filter", "label="+types.AgentDockerLabelManaged+"=true").CombinedOutput()
	if err != nil {
		return fmt.Errorf("list managed worker containers: %w: %s", err, strings.TrimSpace(string(out)))
	}

	var failures []string
	for _, id := range strings.Fields(string(out)) {
		inspect, exists, err := inspectDockerContainer(id)
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}
		if !exists || !dockerContainerLabelsMatchMachine(inspect.Config.Labels, slot) || !dockerContainerStopped(inspect.State.Status) {
			continue
		}
		if err := removeDockerContainer(id); err != nil {
			failures = append(failures, err.Error())
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%s", strings.Join(failures, "; "))
	}
	return nil
}

func removeDockerContainer(name string) error {
	out, err := exec.Command("docker", "rm", "-f", name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("remove worker container %q: %w: %s", name, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func dockerContainerOwnedByAgent(name string, slot *pb.AgentWorkerSlot) (bool, bool, error) {
	inspect, exists, err := inspectDockerContainer(name)
	if err != nil || !exists {
		return false, exists, err
	}

	owned, err := dockerContainerInspectMatchesSlot(inspect, slot)
	if err != nil {
		return false, true, fmt.Errorf("inspect docker container %q: %w", name, err)
	}
	return owned, true, nil
}

func inspectDockerContainer(name string) (*dockerContainerInspect, bool, error) {
	out, err := exec.Command("docker", "inspect", "--format", "{{json .}}", name).CombinedOutput()
	if err != nil {
		msg := strings.ToLower(string(out) + err.Error())
		if strings.Contains(msg, "no such object") || strings.Contains(msg, "no such container") {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("inspect docker container %q: %w: %s", name, err, strings.TrimSpace(string(out)))
	}

	var inspect dockerContainerInspect
	if err := json.Unmarshal(out, &inspect); err != nil {
		return nil, true, err
	}
	return &inspect, true, nil
}

func dockerContainerInspectOwnedByAgent(data []byte, slot *pb.AgentWorkerSlot) (bool, error) {
	var inspect dockerContainerInspect
	if err := json.Unmarshal(data, &inspect); err != nil {
		return false, err
	}
	return dockerContainerInspectMatchesSlot(&inspect, slot)
}

func dockerContainerInspectMatchesSlot(inspect *dockerContainerInspect, slot *pb.AgentWorkerSlot) (bool, error) {
	if inspect == nil {
		return false, nil
	}

	if inspect.Config.Labels[types.AgentDockerLabelManaged] == "true" {
		return dockerContainerLabelsMatchSlot(inspect.Config.Labels, slot), nil
	}
	return dockerContainerEnvMatchesSlot(inspect.Config.Env, slot), nil
}

func dockerContainerLabelsMatchMachine(labels map[string]string, slot *pb.AgentWorkerSlot) bool {
	if labels[types.AgentDockerLabelManaged] != "true" {
		return false
	}
	if slot == nil {
		return false
	}
	return labels[types.AgentDockerLabelMachineID] == slot.MachineId &&
		labels[types.AgentDockerLabelPoolName] == slot.PoolName
}

func dockerContainerLabelsMatchSlot(labels map[string]string, slot *pb.AgentWorkerSlot) bool {
	if labels[types.AgentDockerLabelManaged] != "true" {
		return false
	}
	if slot == nil {
		return false
	}
	return labels[types.AgentDockerLabelWorkerID] == slot.WorkerId &&
		labels[types.AgentDockerLabelMachineID] == slot.MachineId &&
		labels[types.AgentDockerLabelPoolName] == slot.PoolName
}

func dockerContainerEnvMatchesSlot(env []string, slot *pb.AgentWorkerSlot) bool {
	if slot == nil {
		return false
	}

	values := map[string]string{}
	for _, item := range env {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			values[key] = value
		}
	}
	return values[types.WorkerIDEnv] == slot.WorkerId &&
		values[types.WorkerMachineEnv] == slot.MachineId &&
		values[types.WorkerPoolEnv] == slot.PoolName
}

func dockerContainerStopped(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "dead", "exited":
		return true
	default:
		return false
	}
}
