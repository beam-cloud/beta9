package worker

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestNormalizeSandboxRootInitializesMachineIdentity(t *testing.T) {
	root := t.TempDir()
	const imageMachineID = "0123456789abcdef0123456789abcdef\n"
	require.NoError(t, os.MkdirAll(filepath.Join(root, "etc"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "var/lib/dbus"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "etc/machine-id"), nil, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "var/lib/dbus/machine-id"), []byte(imageMachineID), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "etc/hostname"), []byte("localhost.localdomain\n"), 0644))

	require.NoError(t, normalizeSandboxRoot(root, "brisk-canyon-a1b2"))
	machineID := requireFileContents(t, filepath.Join(root, "etc/machine-id"))
	require.Equal(t, imageMachineID, string(machineID))
	require.Equal(t, machineID, requireFileContents(t, filepath.Join(root, "var/lib/dbus/machine-id")))
	require.Equal(t, "brisk-canyon-a1b2\n", string(requireFileContents(t, filepath.Join(root, "etc/hostname"))))

	require.NoError(t, normalizeSandboxRoot(root, "brisk-canyon-a1b2"))
	require.Equal(t, machineID, requireFileContents(t, filepath.Join(root, "etc/machine-id")))
}

func TestNormalizeSandboxRootCreatesMissingIdentityFiles(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "etc"), 0755))

	require.NoError(t, normalizeSandboxRoot(root, "quiet-harbor-a1b2"))
	require.Regexp(t, regexp.MustCompile(`^[0-9a-f]{32}\n$`), string(requireFileContents(t, filepath.Join(root, "etc/machine-id"))))
	require.Equal(t, "quiet-harbor-a1b2\n", string(requireFileContents(t, filepath.Join(root, "etc/hostname"))))
	require.NoFileExists(t, filepath.Join(root, "var/lib/dbus/machine-id"))
}

func TestNormalizeSandboxRootSynchronizesEmptyDBusIdentity(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "etc"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "var/lib/dbus"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "var/lib/dbus/machine-id"), nil, 0644))

	require.NoError(t, normalizeSandboxRoot(root, "localhost"))
	machineID := requireFileContents(t, filepath.Join(root, "etc/machine-id"))
	require.Regexp(t, regexp.MustCompile(`^[0-9a-f]{32}\n$`), string(machineID))
	require.Equal(t, machineID, requireFileContents(t, filepath.Join(root, "var/lib/dbus/machine-id")))
}

func TestNormalizeSandboxRootPreservesNonstandardDBusIdentity(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "etc"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "var/lib/dbus"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "var/lib/dbus/machine-id"), []byte("user-managed-value\n"), 0644))

	require.NoError(t, normalizeSandboxRoot(root, "localhost"))
	require.Regexp(t, regexp.MustCompile(`^[0-9a-f]{32}\n$`), string(requireFileContents(t, filepath.Join(root, "etc/machine-id"))))
	require.Equal(t, "user-managed-value\n", string(requireFileContents(t, filepath.Join(root, "var/lib/dbus/machine-id"))))
}

func TestNormalizeSandboxRootPreservesUserManagedIdentity(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "etc"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "var/lib/dbus"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "etc/machine-id"), []byte("existing-machine-id\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "var/lib/dbus/machine-id"), []byte("existing-dbus-id\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "etc/hostname"), []byte("custom-hostname\n"), 0644))

	require.NoError(t, normalizeSandboxRoot(root, "new-hostname"))
	require.Equal(t, "existing-machine-id\n", string(requireFileContents(t, filepath.Join(root, "etc/machine-id"))))
	require.Equal(t, "existing-dbus-id\n", string(requireFileContents(t, filepath.Join(root, "var/lib/dbus/machine-id"))))
	require.Equal(t, "custom-hostname\n", string(requireFileContents(t, filepath.Join(root, "etc/hostname"))))
}

func TestNormalizeSandboxRootPreservesOversizedIdentityFiles(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "etc"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "var/lib/dbus"), 0755))
	oversizedDBusID := []byte(strings.Repeat("d", maxSandboxIdentityFileSize+1))
	oversizedHostname := []byte(strings.Repeat("h", maxSandboxIdentityFileSize+1))
	require.NoError(t, os.WriteFile(filepath.Join(root, "var/lib/dbus/machine-id"), oversizedDBusID, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "etc/hostname"), oversizedHostname, 0644))

	require.NoError(t, normalizeSandboxRoot(root, "new-hostname"))
	require.Regexp(t, regexp.MustCompile(`^[0-9a-f]{32}\n$`), string(requireFileContents(t, filepath.Join(root, "etc/machine-id"))))
	require.Equal(t, oversizedDBusID, requireFileContents(t, filepath.Join(root, "var/lib/dbus/machine-id")))
	require.Equal(t, oversizedHostname, requireFileContents(t, filepath.Join(root, "etc/hostname")))
}

func TestNormalizeSandboxRootDoesNotFollowIdentitySymlinks(t *testing.T) {
	root := t.TempDir()
	external := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "etc"), 0755))
	externalMachineID := filepath.Join(external, "machine-id")
	externalHostname := filepath.Join(external, "hostname")
	require.NoError(t, os.WriteFile(externalMachineID, []byte("outside-machine-id\n"), 0644))
	require.NoError(t, os.WriteFile(externalHostname, []byte("outside-hostname\n"), 0644))
	require.NoError(t, os.Symlink(externalMachineID, filepath.Join(root, "etc/machine-id")))
	require.NoError(t, os.Symlink(externalHostname, filepath.Join(root, "etc/hostname")))

	require.NoError(t, normalizeSandboxRoot(root, "new-hostname"))
	require.Equal(t, "outside-machine-id\n", string(requireFileContents(t, externalMachineID)))
	require.Equal(t, "outside-hostname\n", string(requireFileContents(t, externalHostname)))
}

func TestNormalizeSandboxRootCannotEscapeThroughParentSymlink(t *testing.T) {
	root := t.TempDir()
	external := t.TempDir()
	externalMachineID := filepath.Join(external, "machine-id")
	externalHostname := filepath.Join(external, "hostname")
	require.NoError(t, os.WriteFile(externalMachineID, nil, 0644))
	require.NoError(t, os.WriteFile(externalHostname, []byte("outside-hostname\n"), 0644))
	require.NoError(t, os.Symlink(external, filepath.Join(root, "etc")))

	require.Error(t, normalizeSandboxRoot(root, "new-hostname"))
	require.Empty(t, requireFileContents(t, externalMachineID))
	require.Equal(t, "outside-hostname\n", string(requireFileContents(t, externalHostname)))
}

func TestShouldNormalizeSandboxRoot(t *testing.T) {
	request := &types.ContainerRequest{Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}}}
	require.True(t, shouldNormalizeSandboxRoot(request, false))
	require.False(t, shouldNormalizeSandboxRoot(request, true))

	request.Stub.Type = types.StubType(types.StubTypePod)
	require.False(t, shouldNormalizeSandboxRoot(request, false))
	require.False(t, shouldNormalizeSandboxRoot(nil, false))
}

func requireFileContents(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	return contents
}
