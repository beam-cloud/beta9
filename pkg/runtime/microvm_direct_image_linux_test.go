//go:build linux

package runtime

import (
	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestDirectImageBootstrapKeepsCanvasAndImageUntouched(t *testing.T) {
	canvas, image, state := t.TempDir(), t.TempDir(), t.TempDir()
	config := []byte(`{"actual":"worker spec"}`)
	require.NoError(t, os.WriteFile(filepath.Join(canvas, "config.json"), config, 0644))
	init := filepath.Join(t.TempDir(), "init")
	require.NoError(t, os.WriteFile(init, []byte("static init"), 0755))
	inst := &microVMInstance{canvas: canvas, imageRoot: image, bootstrapRoot: filepath.Join(state, "binds"), stateDir: state, directImage: true}
	m := &MicroVM{cfg: Config{MicroVMInitPath: init}}
	require.NoError(t, m.prepareCanvas(inst, &specs.Spec{}, microvm.Network{}, microVMDisk{device: "/dev/vda"}, nil))
	require.Equal(t, canvas, inst.canvas)
	require.Equal(t, image, inst.imageRoot)
	entries, err := os.ReadDir(image)
	require.NoError(t, err)
	require.Empty(t, entries)
	entries, err = os.ReadDir(canvas)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "config.json", entries[0].Name())
	actual, err := os.ReadFile(filepath.Join(inst.bootstrapRoot, "config.json"))
	require.NoError(t, err)
	require.Equal(t, config, actual)
	metadata := filepath.Join(inst.bootstrapRoot, microvm.MetadataDir)
	actual, err = os.ReadFile(filepath.Join(metadata, "config.json"))
	require.NoError(t, err)
	require.Equal(t, config, actual)
	entries, err = os.ReadDir(metadata)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NotEqual(t, "0", entry.Name(), "numeric external bind roots are siblings, never metadata children")
	}
	vm, err := os.ReadFile(filepath.Join(inst.bootstrapRoot, microvm.SpecFile))
	require.NoError(t, err)
	require.Contains(t, string(vm), `"direct_image": true`)
	require.Contains(t, microVMImageKernelCmdline(true), "root=beambinds rootfstype=virtiofs rw")
	require.Equal(t, microVMKernelCmdline(), microVMImageKernelCmdline(false))
}
