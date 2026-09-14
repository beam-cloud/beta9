package worker

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLayeredBuildIsolatesRunAndKeepsBuildArgsOutOfWorkerEnvironment(t *testing.T) {
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "buildah"), []byte(`#!/bin/sh
test "$BUILDAH_ISOLATION" = oci || exit 31
test -z "$LD_PRELOAD" || exit 32
printf '%s\n' "$@" > "$BUILD_TEST_ARGS"
`), 0755))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("BUILD_TEST_ARGS", argsPath)
	t.Setenv("BUILDAH_ISOLATION", "chroot")
	t.Setenv("LD_PRELOAD", "")
	b := &layeredBuild{
		c: &ImageClient{}, ctx: context.Background(), out: slog.New(slog.NewTextHandler(io.Discard, nil)),
		container: "test-container", storage: "vfs",
		declaredArgs: map[string]struct{}{"BUILDAH_ISOLATION": {}, "LD_PRELOAD": {}},
		buildArgs:    map[string]string{"BUILDAH_ISOLATION": "chroot", "LD_PRELOAD": "/untrusted.so"},
	}
	require.NoError(t, b.step(dockerfileStep{kind: stepRun, exec: []string{"true"}}))
	data, err := os.ReadFile(argsPath)
	require.NoError(t, err)
	args := strings.Split(strings.TrimSpace(string(data)), "\n")
	for _, flag := range []string{"--isolation=oci", "--pid=private", "--ipc=private", "--network=private"} {
		require.Contains(t, args, flag)
	}
	require.Contains(t, args, "BUILDAH_ISOLATION=chroot")
	require.Contains(t, args, "LD_PRELOAD=/untrusted.so")
}
