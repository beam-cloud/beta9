package vast

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

func RunInstall(ctx context.Context, opts InstallOptions) error {
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	opts.GatewayURL = strings.TrimRight(strings.TrimSpace(opts.GatewayURL), "/")
	if opts.GatewayURL == "" {
		return fmt.Errorf("gateway is required")
	}
	if strings.TrimSpace(opts.JoinToken) == "" && strings.TrimSpace(opts.JoinTokenFile) == "" {
		return fmt.Errorf("join-token or join-token-file is required")
	}
	if runtime.GOOS != "linux" && !opts.DryRun {
		return fmt.Errorf("vast install currently supports systemd on linux")
	}
	opts.StateDir = firstNonEmpty(strings.TrimSpace(opts.StateDir), DefaultStateDir)
	opts.ListenAddr = firstNonEmpty(strings.TrimSpace(opts.ListenAddr), DefaultListenAddr)
	opts.HostServiceName = firstNonEmpty(strings.TrimSpace(opts.HostServiceName), DefaultHostServiceName)
	opts.GPUServicePrefix = firstNonEmpty(strings.TrimSpace(opts.GPUServicePrefix), DefaultGPUServicePrefix)
	opts.UnitDir = firstNonEmpty(strings.TrimSpace(opts.UnitDir), types.AgentSystemdUnitDir)
	opts.BinaryPath = firstNonEmpty(strings.TrimSpace(opts.BinaryPath), executablePath())
	opts.SentinelImage = firstNonEmpty(strings.TrimSpace(opts.SentinelImage), DefaultSentinelImage)

	joinTokenFile, err := installJoinToken(opts)
	if err != nil {
		return err
	}
	sentinelToken, sentinelTokenFile, err := installSentinelToken(opts)
	if err != nil {
		return err
	}

	hostUnit := vastHostUnit(opts, sentinelTokenFile)
	gpuUnit := vastGPUUnit(opts, joinTokenFile)
	if opts.DryRun {
		fmt.Fprintf(opts.Stdout, "%s\n", hostUnit)
		fmt.Fprintf(opts.Stdout, "%s\n", gpuUnit)
		printDefjobCommand(opts, sentinelToken)
		return nil
	}

	if err := ensureDir(opts.StateDir, 0700); err != nil {
		return err
	}
	if err := ensureDir(opts.UnitDir, 0755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(opts.UnitDir, opts.HostServiceName+".service"), []byte(hostUnit), 0644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(opts.UnitDir, opts.GPUServicePrefix+"@.service"), []byte(gpuUnit), 0644); err != nil {
		return err
	}
	for _, args := range [][]string{
		{"daemon-reload"},
		{"enable", opts.HostServiceName + ".service"},
		{"start", opts.HostServiceName + ".service"},
	} {
		if err := runSystemctl(ctx, args...); err != nil {
			return err
		}
	}
	fmt.Fprintf(opts.Stdout, "installed %s.service and %s@.service\n", opts.HostServiceName, opts.GPUServicePrefix)
	printDefjobCommand(opts, sentinelToken)
	return nil
}

func installJoinToken(opts InstallOptions) (string, error) {
	if opts.JoinTokenFile != "" {
		return opts.JoinTokenFile, nil
	}
	path := filepath.Join(opts.StateDir, types.AgentJoinTokenFileName)
	if opts.DryRun {
		return path, nil
	}
	if err := ensureDir(opts.StateDir, 0700); err != nil {
		return "", err
	}
	return path, os.WriteFile(path, []byte(strings.TrimSpace(opts.JoinToken)+"\n"), 0600)
}

func installSentinelToken(opts InstallOptions) (string, string, error) {
	if opts.SentinelTokenFile != "" {
		token, err := readSecret("", opts.SentinelTokenFile)
		return token, opts.SentinelTokenFile, err
	}
	token := strings.TrimSpace(opts.SentinelToken)
	if token == "" {
		generated, err := randomToken()
		if err != nil {
			return "", "", err
		}
		token = generated
	}
	path := filepath.Join(opts.StateDir, "sentinel-token")
	if opts.DryRun {
		return token, path, nil
	}
	if err := ensureDir(opts.StateDir, 0700); err != nil {
		return "", "", err
	}
	return token, path, os.WriteFile(path, []byte(token+"\n"), 0600)
}

func vastHostUnit(opts InstallOptions, sentinelTokenFile string) string {
	args := []string{
		opts.BinaryPath,
		"vast", "host",
		"--gateway", opts.GatewayURL,
		"--state-dir", opts.StateDir,
		"--listen", opts.ListenAddr,
		"--sentinel-token-file", sentinelTokenFile,
		"--service-template", opts.GPUServicePrefix + "@%s.service",
	}
	return systemdUnit(systemdUnitSpec{
		Description: "Beam Vast host sidecar",
		WorkingDir:  opts.StateDir,
		ExecStart:   args,
	})
}

func vastGPUUnit(opts InstallOptions, joinTokenFile string) string {
	args := []string{
		opts.BinaryPath,
		"vast", "gpu-agent",
		"--gateway", opts.GatewayURL,
		"--join-token-file", joinTokenFile,
		"--state-dir", opts.StateDir,
		"--gpu-index", systemdInstanceSpecifier,
	}
	args = appendOptional(args, "--max-cpu", opts.MaxCPU)
	args = appendOptional(args, "--max-memory", opts.MaxMemory)
	args = appendOptional(args, "--worker-image", opts.WorkerImage)
	if opts.NetworkSlots > 0 {
		args = append(args, "--network-slots", strconv.FormatUint(uint64(opts.NetworkSlots), 10))
	}
	if opts.ContainerStartConcurrency > 0 {
		args = append(args, "--container-start-concurrency", strconv.FormatUint(uint64(opts.ContainerStartConcurrency), 10))
	}
	return systemdUnit(systemdUnitSpec{
		Description: "Beam Vast per-GPU agent %i",
		WorkingDir:  opts.StateDir,
		ExecStart:   args,
	})
}

type systemdUnitSpec struct {
	Description string
	WorkingDir  string
	ExecStart   []string
}

const systemdInstanceSpecifier = "%i"

func systemdUnit(spec systemdUnitSpec) string {
	var b strings.Builder
	writeUnitLines(&b,
		"[Unit]",
		"Description="+spec.Description,
		"Wants="+types.AgentSystemdNetworkOnlineTarget+" "+types.AgentSystemdDockerService,
		"After="+types.AgentSystemdNetworkOnlineTarget+" "+types.AgentSystemdDockerService,
		"",
		"[Service]",
		"Type=simple",
		"WorkingDirectory="+systemdPath(spec.WorkingDir),
		"Environment="+systemdQuote(types.AgentPathEnv+"="+types.AgentServiceDefaultPath),
		"ExecStart="+systemdCommand(spec.ExecStart),
		"Restart=on-failure",
		"RestartSec=10",
		"KillSignal=SIGINT",
		"TimeoutStopSec=65",
		"LimitNOFILE=1048576",
		"",
		"[Install]",
		"WantedBy=multi-user.target",
	)
	return b.String()
}

func appendOptional(args []string, name, value string) []string {
	if strings.TrimSpace(value) == "" {
		return args
	}
	return append(args, name, value)
}

func writeUnitLines(b *strings.Builder, lines ...string) {
	for _, line := range lines {
		b.WriteString(line + "\n")
	}
}

func systemdCommand(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		if arg == systemdInstanceSpecifier {
			quoted = append(quoted, systemdInstanceSpecifier)
			continue
		}
		quoted = append(quoted, systemdQuote(arg))
	}
	return strings.Join(quoted, " ")
}

func systemdQuote(value string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`"`, `\"`,
		"\n", `\n`,
		"%", "%%",
	)
	return `"` + replacer.Replace(value) + `"`
}

func systemdPath(value string) string {
	return strings.ReplaceAll(value, "%", "%%")
}

func randomToken() (string, error) {
	var data [32]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(data[:]), nil
}

func printDefjobCommand(opts InstallOptions, sentinelToken string) {
	hostURL := firstNonEmpty(strings.TrimSpace(opts.PublicHostURL), "http://"+publicHostFromListen(opts.ListenAddr))
	machineID := firstNonEmpty(strings.TrimSpace(opts.VastMachineID), "<vast-machine-id>")
	fmt.Fprintf(opts.Stdout, "\nVast default-job command:\n")
	fmt.Fprintf(opts.Stdout, "vastai set defjob %s --image %s --args --host-url %s --token %s\n", machineID, opts.SentinelImage, hostURL, sentinelToken)
}
