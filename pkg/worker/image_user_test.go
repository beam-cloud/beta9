package worker

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func TestResolveImageUser(t *testing.T) {
	rootfs, emptyRoot := t.TempDir(), t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(rootfs, "etc"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(rootfs, "etc/passwd"), []byte("app:x:12345:23456::/home/app:/bin/sh\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(rootfs, "etc/group"), []byte("app:x:23456:\nshared:x:34567:app\nrender:x:45678:app\nother:x:56789:\n"), 0o644))

	for _, test := range []struct {
		name    string
		user    string
		root    string
		want    specs.User
		wantErr string
	}{
		{name: "numeric pair without accounts", user: "12345:23456", root: emptyRoot, want: specs.User{UID: 12345, GID: 23456}},
		{name: "numeric uid without accounts", user: "12345", root: emptyRoot, want: specs.User{UID: 12345}},
		{name: "numeric uid resolves groups", user: "12345", root: rootfs, want: specs.User{UID: 12345, GID: 23456, AdditionalGids: []uint32{34567, 45678}}},
		{name: "named user", user: "app", root: rootfs, want: specs.User{UID: 12345, GID: 23456, AdditionalGids: []uint32{34567, 45678}}},
		{name: "named pair", user: "app:other", root: rootfs, want: specs.User{UID: 12345, GID: 56789}},
		{name: "numeric user named group", user: "12345:other", root: rootfs, want: specs.User{UID: 12345, GID: 56789}},
		{name: "named user numeric group", user: "app:99", root: rootfs, want: specs.User{UID: 12345, GID: 99}},
		{name: "missing user", user: "missing", root: rootfs, wantErr: "unable to find user missing"},
		{name: "missing group", user: "app:missing", root: rootfs, wantErr: "unable to find group missing"},
		{name: "named user without accounts", user: "app", root: emptyRoot, wantErr: "unable to find user app"},
		{name: "negative uid", user: "-1", root: emptyRoot, wantErr: "must be in range"},
		{name: "negative gid", user: "12345:-1", root: emptyRoot, wantErr: "must be in range"},
		{name: "oversized uid", user: "4294967296", root: emptyRoot, wantErr: "must be in range"},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity := specs.User{Username: test.user}
			err := resolveImageUser(test.root, &identity)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				require.Equal(t, specs.User{Username: test.user}, identity, "failed lookups must not select root")
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, identity)
		})
	}
}

func TestResolveImageUserPreservesOCIIdentity(t *testing.T) {
	umask := uint32(0o027)
	identity := specs.User{UID: 12345, GID: 23456, AdditionalGids: []uint32{34567}, Umask: &umask}
	want := identity
	require.NoError(t, resolveImageUser("/nonexistent-image-root", &identity))
	require.Equal(t, want, identity)

	identity.Username = "12345:23456"
	require.NoError(t, resolveImageUser(t.TempDir(), &identity))
	require.Equal(t, want, identity, "explicit image groups and umask survive username resolution")
}

func TestResolveImageUserConfinesAccountFiles(t *testing.T) {
	outside := filepath.Join(t.TempDir(), "passwd")
	require.NoError(t, os.WriteFile(outside, []byte("hostonly:x:1:1::/:/bin/sh\n"), 0o644))
	for _, kind := range []string{"outside symlink", "fifo", "directory", "relative symlink", "absolute image symlink"} {
		t.Run(kind, func(t *testing.T) {
			rootfs := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(rootfs, "etc"), 0o755))
			path := filepath.Join(rootfs, "etc/passwd")
			switch kind {
			case "outside symlink":
				require.NoError(t, os.Symlink(outside, path))
			case "fifo":
				require.NoError(t, syscall.Mkfifo(path, 0o600))
			case "directory":
				require.NoError(t, os.Mkdir(path, 0o755))
			case "relative symlink":
				require.NoError(t, os.WriteFile(path+".real", []byte("app:x:12345:23456::/:/bin/sh\n"), 0o644))
				require.NoError(t, os.Symlink("passwd.real", path))
			case "absolute image symlink":
				require.NoError(t, os.MkdirAll(filepath.Join(rootfs, "usr/etc"), 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(rootfs, "usr/etc/passwd"), []byte("app:x:12345:23456::/:/bin/sh\n"), 0o644))
				require.NoError(t, os.Symlink("/usr/etc/passwd", path))
			}
			identity := specs.User{Username: "app"}
			err := resolveImageUser(rootfs, &identity)
			if kind == "relative symlink" || kind == "absolute image symlink" {
				require.NoError(t, err)
				require.Equal(t, specs.User{UID: 12345, GID: 23456}, identity)
			} else {
				require.Error(t, err)
				require.Equal(t, specs.User{Username: "app"}, identity)
			}
		})
	}
}

func TestSpecFromRequestAppliesImageUserOnlyToPods(t *testing.T) {
	worker := &Worker{runtime: &mockRuntime{name: types.ContainerRuntimeRunc.String()}}
	initial := worker.buildSpecFromCLIPMetadata(&clipCommon.ImageMetadata{User: "12345:23456"})
	initial.Process.User.AdditionalGids = []uint32{34567}
	for _, stubType := range []types.StubType{types.StubType(types.StubTypePodDeployment), types.StubType(types.StubTypeSandbox), types.StubType(types.StubTypeVM)} {
		t.Run(string(stubType), func(t *testing.T) {
			request := &types.ContainerRequest{
				ContainerId: "image-user-" + string(stubType),
				EntryPoint:  []string{"/bin/sh"},
				Stub:        types.StubWithRelated{Stub: types.Stub{Type: stubType}},
			}
			t.Cleanup(func() { _ = os.RemoveAll(filepath.Join(baseConfigPath, request.ContainerId)) })
			spec, err := worker.specFromRequest(request, &ContainerOptions{BindPorts: []int{8001}, InitialSpec: initial})
			require.NoError(t, err)
			require.NoError(t, resolveImageUser(t.TempDir(), &spec.Process.User))
			if stubType.Kind() == types.StubTypePod {
				require.Equal(t, specs.User{UID: 12345, GID: 23456, AdditionalGids: []uint32{34567}}, spec.Process.User)
				spec.Process.User.AdditionalGids[0] = 9
				require.Equal(t, []uint32{34567}, initial.Process.User.AdditionalGids)
			} else {
				require.Equal(t, specs.User{}, spec.Process.User, "sandbox managers and VM boot must stay root")
			}
		})
	}
}
