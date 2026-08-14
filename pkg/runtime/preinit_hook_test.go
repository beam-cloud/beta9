package runtime

import (
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func TestInjectPreInitHooksPrependsBashPreludeAndExecsOriginalArgs(t *testing.T) {
	spec := &specs.Spec{Process: &specs.Process{Args: []string{"python3", "-m", "app"}}}

	err := InjectPreInitHooks(spec, PreInitHook{
		Name:   "install",
		Script: "curl -fsSL https://example.test/install.sh | sh",
	})

	require.NoError(t, err)
	require.Equal(t, []string{
		"bash",
		"-o",
		"pipefail",
		"-c",
		"set -e\n# install\ncurl -fsSL https://example.test/install.sh | sh\nexec \"$@\"",
		preInitShellName,
		"python3",
		"-m",
		"app",
	}, spec.Process.Args)
	require.Contains(t, spec.Process.Env, "PATH="+preInitDefaultPath)
}

func TestInjectPreInitHooksKeepsExistingPath(t *testing.T) {
	spec := &specs.Spec{Process: &specs.Process{Args: []string{"python3"}, Env: []string{"PATH=/custom/bin"}}}

	err := InjectPreInitHooks(spec, PreInitHook{Script: "true"})

	require.NoError(t, err)
	require.Contains(t, spec.Process.Env, "PATH=/custom/bin")
	require.NotContains(t, spec.Process.Env, "PATH="+preInitDefaultPath)
}

func TestInjectPreInitHooksSkipsEmptyHooks(t *testing.T) {
	spec := &specs.Spec{Process: &specs.Process{Args: []string{"python3"}}}

	err := InjectPreInitHooks(spec, PreInitHook{Name: "empty"})

	require.NoError(t, err)
	require.Equal(t, []string{"python3"}, spec.Process.Args)
}

func TestInjectPreInitHooksRequiresProcessArgs(t *testing.T) {
	err := InjectPreInitHooks(&specs.Spec{Process: &specs.Process{}}, PreInitHook{Script: "true"})

	require.Error(t, err)
	require.Contains(t, err.Error(), "process args")
}
