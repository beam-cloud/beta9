package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type nodeRole string
type runMode string

const (
	roleMaster nodeRole = "master"
	roleGPU    nodeRole = "gpu"
	roleCPU    nodeRole = "cpu"

	modeCheck   runMode = "check"
	modeInstall runMode = "install"
)

type options struct {
	role             nodeRole
	mode             runMode
	host             string
	user             string
	keyPath          string
	remoteDir        string
	noCopy           bool
	k3sURL           string
	k3sToken         string
	k3sTLSSAN        string
	nodeName         string
	nodeIP           string
	flannelIface     string
	flannelBackend   string
	tailscaleAuthKey string
	beamRegistry     string
	gpuType          string
	gpuCount         string
}

func main() {
	opts := parseFlags()
	if err := opts.validate(); err != nil {
		fatal(err)
	}

	root, err := deploymentRoot()
	if err != nil {
		fatal(err)
	}

	if isLocal(opts.host) {
		if err := runLocal(root, opts); err != nil {
			fatal(err)
		}
		return
	}

	if err := runRemote(root, opts); err != nil {
		fatal(err)
	}
}

func parseFlags() options {
	var opts options
	flag.Var((*roleValue)(&opts.role), "role", "node role: master, gpu, or cpu")
	flag.Var((*modeValue)(&opts.mode), "mode", "run mode: check or install")
	flag.StringVar(&opts.host, "host", "local", "target host; use local, localhost, or 127.0.0.1 for this machine")
	flag.StringVar(&opts.user, "user", os.Getenv("USER"), "ssh user for remote hosts")
	flag.StringVar(&opts.keyPath, "key", "", "ssh private key for remote hosts")
	flag.StringVar(&opts.remoteDir, "remote-dir", "/tmp/beam-deployment", "remote deployment directory")
	flag.BoolVar(&opts.noCopy, "no-copy", false, "skip copying scripts and infra before running")
	flag.StringVar(&opts.k3sURL, "k3s-url", os.Getenv("K3S_URL"), "k3s server URL for cpu/gpu workers, for example https://100.73.254.107:6443")
	flag.StringVar(&opts.k3sToken, "k3s-token", os.Getenv("K3S_TOKEN"), "k3s node token for cpu/gpu workers")
	flag.StringVar(&opts.k3sTLSSAN, "k3s-tls-san", os.Getenv("K3S_TLS_SAN"), "extra TLS SAN for the k3s server, usually the master Tailscale IP")
	flag.StringVar(&opts.nodeName, "node-name", os.Getenv("K3S_NODE_NAME"), "Kubernetes node name to register")
	flag.StringVar(&opts.nodeIP, "node-ip", os.Getenv("K3S_NODE_IP"), "Kubernetes node IP to advertise, usually this node's Tailscale IP")
	flag.StringVar(&opts.flannelIface, "flannel-iface", os.Getenv("K3S_FLANNEL_IFACE"), "network interface for flannel traffic, for example tailscale0")
	flag.StringVar(&opts.flannelBackend, "flannel-backend", os.Getenv("K3S_FLANNEL_BACKEND"), "server-side flannel backend, for example wireguard-native")
	flag.StringVar(&opts.tailscaleAuthKey, "ts-authkey", os.Getenv("TS_AUTHKEY"), "optional Tailscale auth key for first-time login")
	flag.StringVar(&opts.beamRegistry, "beam-registry", os.Getenv("BEAM_REGISTRY"), "optional registry prefix for image reachability checks")
	flag.StringVar(&opts.gpuType, "gpu-type", os.Getenv("GPU_TYPE"), "GPU type label to apply on gpu nodes, default A6000")
	flag.StringVar(&opts.gpuCount, "gpu-count", os.Getenv("GPU_COUNT"), "GPU count label and expected allocatable value on gpu nodes, default 8")
	flag.Parse()
	if opts.mode == "" {
		opts.mode = modeCheck
	}
	return opts
}

func (o options) validate() error {
	switch o.role {
	case roleMaster, roleGPU, roleCPU:
	default:
		return fmt.Errorf("unsupported role %q; expected master, gpu, or cpu", o.role)
	}
	switch o.mode {
	case modeCheck, modeInstall:
	default:
		return fmt.Errorf("unsupported mode %q; expected check or install", o.mode)
	}
	if !isLocal(o.host) && strings.TrimSpace(o.user) == "" {
		return errors.New("remote user is required")
	}
	if o.mode == modeInstall && (o.role == roleGPU || o.role == roleCPU) {
		if strings.TrimSpace(o.k3sURL) == "" || strings.TrimSpace(o.k3sToken) == "" {
			return errors.New("--k3s-url and --k3s-token are required when installing cpu/gpu worker nodes")
		}
	}
	return nil
}

func deploymentRoot() (string, error) {
	exe, err := os.Executable()
	if err == nil {
		if root := findRoot(filepath.Dir(exe)); root != "" {
			return root, nil
		}
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	if root := findRoot(wd); root != "" {
		return root, nil
	}
	return "", errors.New("could not find deployment root containing scripts/run.sh")
}

func findRoot(start string) string {
	dir, _ := filepath.Abs(start)
	for {
		if fileExists(filepath.Join(dir, "scripts", "run.sh")) {
			return dir
		}
		next := filepath.Dir(dir)
		if next == dir {
			return ""
		}
		dir = next
	}
}

func runLocal(root string, opts options) error {
	script := filepath.Join(root, "scripts", "run.sh")
	args := []string{"--role", string(opts.role), "--mode", string(opts.mode)}
	return runCommandEnv("", opts.envPairs(), "bash", append([]string{script}, args...)...)
}

func runRemote(root string, opts options) error {
	target := opts.user + "@" + opts.host
	if !opts.noCopy {
		if err := runCommand("", "ssh", append(sshArgs(opts), target, "mkdir -p "+shellQuote(opts.remoteDir))...); err != nil {
			return err
		}
		for _, name := range []string{"scripts", "infra"} {
			src := filepath.Join(root, name)
			if !dirExists(src) {
				continue
			}
			dest := target + ":" + strings.TrimRight(opts.remoteDir, "/") + "/"
			if err := runCommand("", "scp", append(scpArgs(opts), "-r", src, dest)...); err != nil {
				return err
			}
		}
	}
	remoteScript := strings.TrimRight(opts.remoteDir, "/") + "/scripts/run.sh"
	remoteCmd := fmt.Sprintf("%s bash %s --role %s --mode %s", opts.remoteEnvPrefix(), shellQuote(remoteScript), shellQuote(string(opts.role)), shellQuote(string(opts.mode)))
	return runCommand("", "ssh", append(sshArgs(opts), target, strings.TrimSpace(remoteCmd))...)
}

func (o options) remoteEnvPrefix() string {
	return strings.Join(shellEnvAssignments(o.envPairs()), " ")
}

func (o options) envPairs() []string {
	values := map[string]string{
		"K3S_URL":             o.k3sURL,
		"K3S_TOKEN":           o.k3sToken,
		"K3S_TLS_SAN":         o.k3sTLSSAN,
		"K3S_NODE_NAME":       o.nodeName,
		"K3S_NODE_IP":         o.nodeIP,
		"K3S_FLANNEL_IFACE":   o.flannelIface,
		"K3S_FLANNEL_BACKEND": o.flannelBackend,
		"TS_AUTHKEY":          o.tailscaleAuthKey,
		"BEAM_REGISTRY":       o.beamRegistry,
		"GPU_TYPE":            o.gpuType,
		"GPU_COUNT":           o.gpuCount,
	}
	order := []string{"K3S_URL", "K3S_TOKEN", "K3S_TLS_SAN", "K3S_NODE_NAME", "K3S_NODE_IP", "K3S_FLANNEL_IFACE", "K3S_FLANNEL_BACKEND", "TS_AUTHKEY", "BEAM_REGISTRY", "GPU_TYPE", "GPU_COUNT"}
	pairs := []string{}
	for _, key := range order {
		if strings.TrimSpace(values[key]) == "" {
			continue
		}
		pairs = append(pairs, key+"="+values[key])
	}
	return pairs
}

func shellEnvAssignments(pairs []string) []string {
	parts := []string{}
	for _, pair := range pairs {
		key, value, ok := strings.Cut(pair, "=")
		if !ok || strings.TrimSpace(value) == "" {
			continue
		}
		parts = append(parts, key+"="+shellQuote(value))
	}
	return parts
}

func sshArgs(opts options) []string {
	args := []string{"-o", "StrictHostKeyChecking=accept-new"}
	if opts.keyPath != "" {
		args = append(args, "-i", expandHome(opts.keyPath))
	}
	return args
}

func scpArgs(opts options) []string {
	args := []string{"-o", "StrictHostKeyChecking=accept-new"}
	if opts.keyPath != "" {
		args = append(args, "-i", expandHome(opts.keyPath))
	}
	return args
}

func runCommand(dir string, name string, args ...string) error {
	return runCommandEnv(dir, nil, name, args...)
}

func runCommandEnv(dir string, env []string, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

func isLocal(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	return host == "" || host == "local" || host == "localhost" || host == "127.0.0.1" || host == "0.0.0.0" || host == "::1"
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func expandHome(path string) string {
	if path == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			return home
		}
	}
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, strings.TrimPrefix(path, "~/"))
		}
	}
	return path
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

type roleValue nodeRole

func (v *roleValue) String() string { return string(*v) }
func (v *roleValue) Set(s string) error {
	*v = roleValue(strings.TrimSpace(strings.ToLower(s)))
	return nil
}

type modeValue runMode

func (v *modeValue) String() string { return string(*v) }
func (v *modeValue) Set(s string) error {
	*v = modeValue(strings.TrimSpace(strings.ToLower(s)))
	return nil
}
