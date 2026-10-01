package worker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"tags.cncf.io/container-device-interface/pkg/cdi"
	cdispecs "tags.cncf.io/container-device-interface/specs-go"
)

// Equality is deliberately conservative: CPU feature supersets need not have
// compatible XSAVE layouts, and logical GPU labels can hide different hardware.
type checkpointHostProfile struct {
	CPU, GPU, Mounts []string
	Binaries         map[string]string
	WorkerImage      string
	ResourceLimits   types.ContainerResourceLimitsConfig
	Runtime          string
	Platform         string
	RunscArgs        []string
}

func (p checkpointHostProfile) key() string {
	data, _ := json.Marshal(p)
	return fmt.Sprintf("v1:%x", sha256.Sum256(data))
}

func checkpointSortedUnique(values []string) []string {
	slices.Sort(values)
	return slices.Compact(values)
}

func checkpointCPUProfile(cpuinfo string) ([]string, error) {
	var profiles []string
	for _, processor := range strings.Split(strings.TrimSpace(cpuinfo), "\n\n") {
		fields := map[string]string{}
		for _, line := range strings.Split(processor, "\n") {
			key, value, ok := strings.Cut(line, ":")
			if !ok {
				continue
			}
			key, value = strings.TrimSpace(key), strings.TrimSpace(value)
			switch key {
			case "vendor_id", "cpu family", "model", "stepping", "model name":
				fields[key] = value
			case "flags":
				fields[key] = strings.Join(checkpointSortedUnique(strings.Fields(value)), " ")
			}
		}
		if fields["vendor_id"] == "" || fields["cpu family"] == "" || fields["model"] == "" || fields["flags"] == "" {
			return nil, fmt.Errorf("missing x86 CPU identity or features")
		}
		data, _ := json.Marshal(fields)
		profiles = append(profiles, string(data))
	}
	return checkpointSortedUnique(profiles), nil
}

func checkpointGPUProfile(output string) ([]string, error) {
	var profiles []string
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		model, driver, ok := strings.Cut(line, ",")
		model, driver = strings.TrimSpace(model), strings.TrimSpace(driver)
		if !ok || model == "" || driver == "" {
			return nil, fmt.Errorf("missing physical GPU model or driver version")
		}
		profiles = append(profiles, model+", "+driver)
	}
	return checkpointSortedUnique(profiles), nil
}

func checkpointCDIMounts(spec *cdispecs.Spec) []string {
	var mounts []string
	add := func(edits cdispecs.ContainerEdits) {
		for _, mount := range edits.Mounts {
			// Sources and device UUIDs are host-specific. gVisor compares the
			// injected destination, type and options across restore.
			options := checkpointSortedUnique(slices.Clone(mount.Options))
			data, _ := json.Marshal([]any{path.Clean(mount.ContainerPath), mount.Type, options})
			mounts = append(mounts, string(data))
		}
	}
	add(spec.ContainerEdits)
	for _, device := range spec.Devices {
		add(device.ContainerEdits)
	}
	return checkpointSortedUnique(mounts)
}

func (s *Worker) readCheckpointHostProfile() (checkpointHostProfile, error) {
	p := checkpointHostProfile{
		Binaries:       map[string]string{},
		WorkerImage:    s.config.Worker.ImageTag,
		ResourceLimits: s.config.Worker.ContainerResourceLimits,
	}
	if s.runtime != nil {
		p.Runtime = s.runtime.Name()
	}
	cpuinfo, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return p, err
	}
	if p.CPU, err = checkpointCPUProfile(string(cpuinfo)); err != nil {
		return p, err
	}
	// The worker binary covers embedded base specs and SDK mount-generation
	// code. Include runc even on gVisor workers, which support forced-runc jobs.
	binaries := map[string]string{"worker": "/proc/self/exe", "runc": "runc", "criu": "criu"}
	if s.gvisorRuntime != nil {
		binaries["runsc"] = "runsc"
		p.Platform = s.poolConfig.ContainerRuntimeConfig.GVisorPlatform
		if p.Platform == "" {
			p.Platform = "systrap"
		}
		p.RunscArgs = slices.Clone(s.poolConfig.ContainerRuntimeConfig.GVisorExtraArgs)
	}
	if s.gpuCount > 0 {
		binaries["cuda-checkpoint"] = "cuda-checkpoint"
	}
	for name, executable := range binaries {
		filename, err := exec.LookPath(executable)
		if err != nil {
			return p, fmt.Errorf("fingerprint %s: %w", name, err)
		}
		if p.Binaries[name], _, err = fileSHA256(filename); err != nil {
			return p, fmt.Errorf("fingerprint %s: %w", name, err)
		}
	}
	if s.gpuCount == 0 {
		return p, nil
	}
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=name,driver_version", "--format=csv,noheader").Output()
	if err != nil {
		return p, fmt.Errorf("read physical GPU and driver: %w", err)
	}
	if p.GPU, err = checkpointGPUProfile(string(output)); err != nil {
		return p, err
	}
	device := cdi.GetDefaultCache().GetDevice(nvidiaFirstDevice)
	if device == nil {
		return p, fmt.Errorf("missing NVIDIA CDI spec")
	}
	p.Mounts = checkpointCDIMounts(device.GetSpec().Spec)
	return p, nil
}

func (s *Worker) initializeCheckpointCompatibility() {
	profile, err := s.readCheckpointHostProfile()
	if err != nil {
		// Never advertise a legacy/empty key when discovery fails.
		s.checkpointCompatibilityKey = "unknown:" + uuid.NewString()
		log.Warn().Err(err).Msg("checkpoint host discovery failed; restricting new checkpoints to this worker process")
	} else {
		s.checkpointCompatibilityKey = profile.key()
	}
	log.Info().Str("worker_id", s.workerId).Str("checkpoint_compatibility_key", s.checkpointCompatibilityKey).
		Strs("physical_gpus", profile.GPU).Msg("checkpoint host compatibility initialized")
}
