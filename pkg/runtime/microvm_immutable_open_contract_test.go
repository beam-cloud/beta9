//go:build linux

package runtime

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Exercise the real two-share command path without a VM, cgroup or privileged
// mount. The stand-in records argv and supplies only each daemon's ready socket.
func TestMicroVMImmutableOpenOnlyForAttestedImage(t *testing.T) {
	python, err := exec.LookPath("python3")
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		noXattrs bool
	}{{"attested_clip", true}, {"native_fallback", false}} {
		t.Run(tc.name, func(t *testing.T) {
			// A short root keeps AF_UNIX socket paths below Linux's108-byte limit.
			root, err := os.MkdirTemp("", "cap-share-")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, os.RemoveAll(root)) })
			canvas, state := filepath.Join(root, "canvas"), filepath.Join(root, "state")
			require.NoError(t, os.MkdirAll(canvas, 0755))
			require.NoError(t, os.MkdirAll(filepath.Join(state, "binds"), 0755))
			helper := filepath.Join(root, "daemon-stand-in")
			script := "#!" + python + "\n" + `import json,pathlib,signal,socket,sys
args=sys.argv[1:]
name=next(v.split("=",1)[1] for v in args if v.startswith("--socket-path="))
pathlib.Path(name+".args.json").write_text(json.dumps(args))
sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
sock.bind(name)
sock.listen()
signal.pause()
`
			require.NoError(t, os.WriteFile(helper, []byte(script), 0755))
			inst := &microVMInstance{canvas: canvas, stateDir: state, console: newTailWriter(4096), imageNoXattrs: tc.noXattrs}
			t.Cleanup(func() {
				inst.mu.Lock()
				children := append([]*exec.Cmd(nil), inst.virtiofsd...)
				inst.mu.Unlock()
				for _, child := range children {
					_ = child.Process.Kill()
				}
				for _, child := range children {
					waitProcessGone(child.Process, 5*time.Second)
					require.Error(t, child.Process.Signal(syscall.Signal(0)), "owned daemon stand-in must exit")
				}
			})
			m := &MicroVM{cfg: Config{MicroVMVirtiofsdPath: helper}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, m.startVirtiofsd(ctx, inst))
			inst.mu.Lock()
			count := len(inst.virtiofsd)
			inst.mu.Unlock()
			require.Equal(t, 2, count)
			for _, share := range []struct {
				name, dir, cache string
				immutable        bool
			}{{microVMVirtiofsSocket, canvas, "always", true}, {microVMBindsSocket, filepath.Join(state, "binds"), "auto", false}} {
				data, err := os.ReadFile(filepath.Join(state, share.name) + ".args.json")
				require.NoError(t, err)
				var args []string
				require.NoError(t, json.Unmarshal(data, &args))
				for _, flag := range []string{"--shared-dir=" + share.dir, "--cache=" + share.cache, "--killpriv-v2", "--inode-file-handles=never", "--thread-pool-size=16"} {
					require.Contains(t, args, flag)
				}
				require.Equal(t, share.immutable, containsImmutableOpenArgument(args, "--readonly"))
				require.Equal(t, share.immutable, containsImmutableOpenArgument(args, "--readdirplus-always"))
				require.Equal(t, share.immutable && tc.noXattrs, containsImmutableOpenArgument(args, "--immutable-open"), "mutable binds and native fallback must never use immutable opens")
				require.Equal(t, !share.immutable || !tc.noXattrs, containsImmutableOpenArgument(args, "--xattr"), "only attested immutable CLIP can omit xattrs")
			}
		})
	}
}

func containsImmutableOpenArgument(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}
