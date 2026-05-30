package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

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
	if envBool("BEAM_AGENT_CONTAINER") {
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

		netns := pathExists("/proc/self/ns/net")
		checks = append(checks, check{
			Name:     "network-namespace",
			Ok:       netns,
			Message:  "required because the worker creates container network namespaces directly",
			Severity: severity(netns),
		})

		netnsRunDir := writableDirOrCreatable("/var/run/netns")
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

		fuse := pathExists("/dev/fuse")
		checks = append(checks, check{
			Name:     "fuse",
			Ok:       fuse,
			Message:  "required while the current image path uses CLIP/go-fuse lazy mounts",
			Severity: severity(fuse),
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
	if value := strings.TrimSpace(os.Getenv("BEAM_AGENT_MACHINE_FINGERPRINT")); value != "" {
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
