package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

var (
	nvidiaProcGPUInfoRoot = "/proc/driver/nvidia/gpus"
	cudaDriverInitProbe   = defaultCUDADriverInitProbe
)

const cudaDriverInitProbeTimeout = 10 * time.Second

type preflightResult struct {
	checks      []check
	gpus        []string
	gpuDevices  []gpuDevice
	schedulable bool
}

func runPreflight(devMode bool, executor string) preflightResult {
	checks := []check{
		{
			Name:     "k3s",
			Ok:       true,
			Message:  "not required for agent worker-container mode",
			Severity: "info",
		},
		{
			Name:     "flux",
			Ok:       true,
			Message:  "not installed or required for agent worker-container mode",
			Severity: "info",
		},
		{
			Name:     "tailscale-daemon",
			Ok:       true,
			Message:  "not installed or required; agent transport is embedded and route-scoped",
			Severity: "info",
		},
	}
	if devMode {
		checks = append(checks, check{
			Name:     "local-dev",
			Ok:       true,
			Message:  "macOS and Linux local joins use the agent route listener without k3s, Flux, or a system Tailscale daemon",
			Severity: "info",
		})
	}
	if envBool(types.AgentInContainerEnv) {
		checks = append(checks, check{
			Name:     "agent-container",
			Ok:       true,
			Message:  "running the Linux agent inside Docker; worker containers are started as siblings through the Docker socket",
			Severity: "info",
		})
	}

	workerContainer := executor == types.DefaultAgentWorkerContainerMode
	production := runtime.GOOS == "linux"
	linuxOK := production || (devMode && !workerContainer)
	checks = append(checks, check{
		Name:     "linux",
		Ok:       linuxOK,
		Message:  "worker-container execution requires Linux; macOS uses the Docker Linux agent path",
		Severity: severity(linuxOK),
	})

	if runtime.GOOS == "linux" && workerContainer {
		root := os.Geteuid() == 0
		checks = append(checks, check{
			Name:     "root",
			Ok:       root,
			Message:  "not required when the user can run rootful Docker; sudo install remains supported",
			Severity: "info",
		})

		runtimeOK := commandExists("docker")
		checks = append(checks, check{
			Name:     "container-runtime",
			Ok:       runtimeOK,
			Message:  "requires rootful Docker for the v1 worker-container executor",
			Severity: severity(runtimeOK),
		})

		if envBool(types.AgentInContainerEnv) {
			socketOK := dockerSocketMounted()
			checks = append(checks, check{
				Name:     "docker-socket",
				Ok:       socketOK,
				Message:  "requires /var/run/docker.sock so the agent can start worker containers as siblings",
				Severity: severity(socketOK),
			})
		}

		daemonOK := runtimeOK && dockerDaemonAvailable()
		checks = append(checks, check{
			Name:     "docker-daemon",
			Ok:       daemonOK,
			Message:  "requires access to a running Docker daemon",
			Severity: severity(daemonOK),
		})

		hostNetworkOK := daemonOK && dockerHostNetworkAvailable()
		checks = append(checks, check{
			Name:     "docker-host-network",
			Ok:       hostNetworkOK,
			Message:  "requires Docker host networking because the agent routes worker and container host ports",
			Severity: severity(hostNetworkOK),
		})

		netns := pathExists("/proc/self/ns/net")
		checks = append(checks, check{
			Name:     "network-namespace",
			Ok:       netns,
			Message:  "required because the worker creates container network namespaces directly",
			Severity: severity(netns),
		})

		netnsRunDir := writableDirOrCreatable(types.HostNetnsPath)
		checks = append(checks, check{
			Name:     "netns-run-dir",
			Ok:       netnsRunDir,
			Message:  "requires writable /var/run/netns for named container network namespaces",
			Severity: severity(netnsRunDir),
		})

		ipForward := sysctlEnabledOrWritable("/proc/sys/net/ipv4/ip_forward")
		checks = append(checks, check{
			Name:     "ip-forward",
			Ok:       ipForward,
			Message:  "requires IPv4 forwarding for worker bridge NAT and exposed container ports",
			Severity: severity(ipForward),
		})

		iptables := iptablesNatAvailable()
		checks = append(checks, check{
			Name:     "iptables",
			Ok:       iptables,
			Message:  "requires usable IPv4 nat/filter tables for worker-managed MASQUERADE and DNAT rules",
			Severity: severity(iptables),
		})

		networkManager := networkManagerPresent()
		checks = append(checks, check{
			Name:     "network-manager",
			Ok:       true,
			Message:  networkManagerMessage(networkManager),
			Severity: "info",
		})

		fuse := pathExists(types.HostFuseDevicePath)
		fuseOK := fuse || devMode
		fuseMessage := "required while the current image path uses CLIP/go-fuse lazy mounts"
		if devMode && !fuse {
			fuseMessage = "not detected on the dev agent host; local Docker worker startup will still run and surface runtime mount errors if FUSE is unavailable"
		}
		checks = append(checks, check{
			Name:     "fuse",
			Ok:       fuseOK,
			Message:  fuseMessage,
			Severity: severity(fuseOK),
		})
	}

	gpuDevices := detectNvidiaGPUDevices()
	gpus := make([]string, 0, len(gpuDevices))
	for _, device := range gpuDevices {
		gpus = append(gpus, device.Name)
	}
	if len(gpuDevices) > 0 {
		nvidiaRuntime := commandExists("nvidia-ctk") || commandExists("nvidia-container-runtime")
		checks = append(checks, check{
			Name:     "nvidia-runtime",
			Ok:       nvidiaRuntime,
			Message:  "required to pin GPUs into worker containers",
			Severity: severity(nvidiaRuntime),
		})

		driverOK, driverMessage := nvidiaProcDriverStateOK(gpuDevices)
		checks = append(checks, check{
			Name:     "nvidia-driver-state",
			Ok:       driverOK,
			Message:  driverMessage,
			Severity: severity(driverOK),
		})

		if cudaOK, cudaMessage, ran := cudaDriverInitProbe(); ran {
			checks = append(checks, check{
				Name:     "cuda-driver-init",
				Ok:       cudaOK,
				Message:  cudaMessage,
				Severity: "info",
			})
		}
	}

	return preflightResult{
		checks:      checks,
		gpus:        gpus,
		gpuDevices:  gpuDevices,
		schedulable: allRequiredChecksPassed(checks),
	}
}

func allRequiredChecksPassed(checks []check) bool {
	for _, c := range checks {
		if c.Severity == "error" && !c.Ok {
			return false
		}
	}
	return true
}

func severity(ok bool) string {
	if ok {
		return "info"
	}
	return "error"
}

func commandExists(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

func nvidiaProcDriverStateOK(gpuDevices []gpuDevice) (bool, string) {
	entries, err := os.ReadDir(nvidiaProcGPUInfoRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return true, "nvidia procfs GPU state not present"
		}
		return false, fmt.Sprintf("unable to read NVIDIA procfs GPU state: %v", err)
	}

	infoCount := 0
	stale := []string{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		infoPath := filepath.Join(nvidiaProcGPUInfoRoot, entry.Name(), "information")
		data, err := os.ReadFile(infoPath)
		if err != nil {
			return false, fmt.Sprintf("unable to read NVIDIA GPU information for %s: %v", entry.Name(), err)
		}

		infoCount++
		text := string(data)
		if nvidiaProcInfoLooksFailed(text) {
			stale = append(stale, entry.Name())
		}
	}

	switch {
	case len(stale) > 0:
		return true, fmt.Sprintf("NVIDIA procfs reports failed adapter state for %s; those adapters are excluded from advertised GPU capacity", strings.Join(stale, ","))
	case infoCount > len(gpuDevices):
		return true, fmt.Sprintf("NVIDIA procfs reports %d GPUs but %d healthy GPUs are advertised", infoCount, len(gpuDevices))
	case infoCount == 0:
		return false, "nvidia-smi detected GPUs but NVIDIA procfs has no GPU entries"
	default:
		return true, "NVIDIA kernel driver state matches detected GPUs"
	}
}

func nvidiaProcGPUInfoHealthy(busID string) (bool, error) {
	if busID == "" {
		return true, nil
	}
	infoPath := filepath.Join(nvidiaProcGPUInfoRoot, busID, "information")
	data, err := os.ReadFile(infoPath)
	if err != nil {
		if os.IsNotExist(err) {
			if _, rootErr := os.Stat(nvidiaProcGPUInfoRoot); os.IsNotExist(rootErr) {
				return true, nil
			}
			return false, nil
		}
		return false, err
	}
	return !nvidiaProcInfoLooksFailed(string(data)), nil
}

func nvidiaProcInfoLooksFailed(info string) bool {
	for _, line := range strings.Split(info, "\n") {
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.TrimSpace(name) {
		case "Video BIOS":
			if value == "" || strings.HasPrefix(value, "??") {
				return true
			}
		}
	}
	return false
}

func defaultCUDADriverInitProbe() (bool, string, bool) {
	pythonPath, err := exec.LookPath("python3")
	if err != nil {
		return true, "", false
	}

	const script = `import ctypes, sys
lib = ctypes.CDLL("libcuda.so.1")
rc = lib.cuInit(0)
count = ctypes.c_int()
rc_count = lib.cuDeviceGetCount(ctypes.byref(count))
print(f"cuInit={rc} cuDeviceGetCount={rc_count} count={count.value}")
sys.exit(0 if rc == 0 and rc_count == 0 else 1)
`
	ctx, cancel := context.WithTimeout(context.Background(), cudaDriverInitProbeTimeout)
	defer cancel()

	output, err := exec.CommandContext(ctx, pythonPath, "-c", script).CombinedOutput()
	message := strings.TrimSpace(string(output))
	if message == "" {
		message = "host CUDA driver init probe completed"
	}
	if ctx.Err() == context.DeadlineExceeded {
		return false, "host CUDA driver init probe timed out", true
	}
	if err != nil {
		return false, message, true
	}
	return true, message, true
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func writableDirOrCreatable(path string) bool {
	info, err := os.Stat(path)
	if err == nil {
		return info.IsDir() && writableDir(path)
	}
	if !os.IsNotExist(err) {
		return false
	}

	parent := filepath.Dir(path)
	return writableDir(parent)
}

func writableDir(path string) bool {
	probe, err := os.CreateTemp(path, ".beam-preflight-*")
	if err != nil {
		return false
	}
	name := probe.Name()
	_ = probe.Close()
	_ = os.Remove(name)
	return true
}

func sysctlEnabledOrWritable(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	if strings.TrimSpace(string(data)) == "1" {
		return true
	}
	return writableFile(path)
}

func writableFile(path string) bool {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return false
	}
	_ = file.Close()
	return true
}

func iptablesNatAvailable() bool {
	for _, name := range []string{"iptables", "iptables-nft", "iptables-legacy"} {
		if !commandExists(name) {
			continue
		}
		if err := exec.Command(name, "-t", "nat", "-L", "-n").Run(); err == nil {
			return true
		}
	}
	return false
}

func dockerSocketMounted() bool {
	info, err := os.Stat("/var/run/docker.sock")
	return err == nil && !info.IsDir()
}

func dockerDaemonAvailable() bool {
	return exec.Command("docker", "version", "--format", "{{.Server.Version}}").Run() == nil
}

func dockerHostNetworkAvailable() bool {
	return exec.Command("docker", "network", "inspect", "host").Run() == nil
}

func networkManagerPresent() bool {
	return commandExists("nmcli") || pathExists("/run/NetworkManager")
}

func networkManagerMessage(present bool) string {
	if !present {
		return "not detected; worker-managed bridge/veth devices will be created directly"
	}
	return "detected; Beam uses b9_br0 and b9h*/b9c* devices directly and does not require NetworkManager profiles"
}

func machineFingerprint(hostname string) string {
	if value := strings.TrimSpace(os.Getenv(types.AgentFingerprintEnv)); value != "" {
		return value
	}
	for _, path := range []string{"/etc/machine-id", "/var/lib/dbus/machine-id"} {
		if data, err := os.ReadFile(path); err == nil {
			value := strings.TrimSpace(string(data))
			if value != "" {
				return value
			}
		}
	}
	sum := sha256.Sum256([]byte(runtime.GOOS + "\x00" + runtime.GOARCH + "\x00" + hostname))
	return hex.EncodeToString(sum[:])
}

func systemMemoryMB() uint64 {
	if runtime.GOOS == "linux" {
		data, err := os.ReadFile("/proc/meminfo")
		if err != nil {
			return 0
		}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == "MemTotal:" {
				kb, err := strconv.ParseUint(fields[1], 10, 64)
				if err != nil {
					return 0
				}
				return kb / 1024
			}
		}
		return 0
	}

	if runtime.GOOS == "darwin" {
		out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
		if err != nil {
			return 0
		}
		bytes, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
		if err != nil {
			return 0
		}
		return bytes / 1024 / 1024
	}

	return 0
}
