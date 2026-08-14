package runtime

import (
	"fmt"
	"strings"

	"github.com/opencontainers/runtime-spec/specs-go"
)

const preInitShellName = "beta9-preinit"
const preInitDefaultPath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

type PreInitHook struct {
	Name   string
	Script string
}

func InjectPreInitHooks(spec *specs.Spec, hooks ...PreInitHook) error {
	if spec == nil || spec.Process == nil {
		return fmt.Errorf("container spec is required for pre-init hooks")
	}
	if len(spec.Process.Args) == 0 {
		return fmt.Errorf("container process args are required for pre-init hooks")
	}

	script := preInitScript(hooks)
	if script == "" {
		return nil
	}

	if !envHasKey(spec.Process.Env, "PATH") {
		spec.Process.Env = append(spec.Process.Env, "PATH="+preInitDefaultPath)
	}

	originalArgs := append([]string(nil), spec.Process.Args...)
	spec.Process.Args = append([]string{
		"bash",
		"-o",
		"pipefail",
		"-c",
		script,
		preInitShellName,
	}, originalArgs...)
	return nil
}

func preInitScript(hooks []PreInitHook) string {
	var builder strings.Builder
	builder.WriteString("set -e\n")
	wroteHook := false
	for _, hook := range hooks {
		script := strings.TrimSpace(hook.Script)
		if script == "" {
			continue
		}
		if hook.Name != "" {
			builder.WriteString("# ")
			builder.WriteString(hook.Name)
			builder.WriteByte('\n')
		}
		builder.WriteString(script)
		builder.WriteByte('\n')
		wroteHook = true
	}
	if !wroteHook {
		return ""
	}
	builder.WriteString("exec \"$@\"")
	return builder.String()
}

func envHasKey(env []string, key string) bool {
	prefix := key + "="
	for _, item := range env {
		if strings.HasPrefix(item, prefix) {
			return true
		}
	}
	return false
}
