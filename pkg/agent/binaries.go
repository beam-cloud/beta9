package agent

import (
	"os/exec"
	"sync"

	"github.com/rs/zerolog/log"
)

// Package-level cache of absolute paths for external binaries that the
// agent invokes. We resolve each once via exec.LookPath at first use and
// pin the absolute path for all subsequent exec.CommandContext calls.
//
// Why: exec.Command("kubectl", ...) with no absolute path relies on the
// inherited $PATH. When the agent runs as a systemd service or is started
// by a user with $HOME/bin at the front of PATH, a local attacker who
// can write ~/bin/kubectl executes arbitrary code under the agent's
// privileges — and the k8s watcher reconnects every 2 seconds so the
// attacker binary is invoked continuously.
//
// We deliberately do not bail out if a binary is missing. The agent
// should still run: nvidia-smi is absent on non-GPU machines, tailscale
// is absent in tests, kubectl is absent on non-k3s workers. Each call
// site checks the cached path for "" and degrades gracefully.
var (
	binaryCache     binaryPaths
	binaryCacheOnce sync.Once
)

type binaryPaths struct {
	kubectl   string
	nvidiaSMI string
	tailscale string
}

// resolveBinaries looks up kubectl, nvidia-smi, and tailscale once and
// caches the absolute paths. Subsequent calls are a no-op.
//
// When invoked from tests a caller may want to re-evaluate PATH; pass
// forceReset=true to clear the cache. Production code should call
// resolveBinaries() (no args) at startup.
func resolveBinaries() binaryPaths {
	binaryCacheOnce.Do(func() {
		binaryCache = lookupBinaries()
	})
	return binaryCache
}

// resolveBinariesForTest resets the cache and re-resolves. Only intended
// for use from _test.go files.
func resolveBinariesForTest() binaryPaths {
	binaryCacheOnce = sync.Once{}
	return resolveBinaries()
}

func lookupBinaries() binaryPaths {
	return binaryPaths{
		kubectl:   lookPathWithWarn("kubectl"),
		nvidiaSMI: lookPathWithWarn("nvidia-smi"),
		tailscale: lookPathWithWarn("tailscale"),
	}
}

func lookPathWithWarn(name string) string {
	p, err := exec.LookPath(name)
	if err != nil {
		log.Warn().Str("binary", name).Err(err).Msg("binary not found on $PATH; feature will be skipped")
		return ""
	}
	log.Debug().Str("binary", name).Str("path", p).Msg("resolved binary path")
	return p
}

// KubectlPath returns the cached absolute path to kubectl, or "" if not
// present. Callers must handle the empty case.
func KubectlPath() string { return resolveBinaries().kubectl }

// NvidiaSMIPath returns the cached absolute path to nvidia-smi, or "".
func NvidiaSMIPath() string { return resolveBinaries().nvidiaSMI }

// TailscalePath returns the cached absolute path to tailscale, or "".
func TailscalePath() string { return resolveBinaries().tailscale }
