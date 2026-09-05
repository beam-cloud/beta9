package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	aptConfMountPath      = "/etc/apt/apt.conf.d/99-beta9-build"
	aptSourcesMountPath   = "/etc/apt/sources.list"
	aptSourcesDeb822Mount = "/etc/apt/sources.list.d/ubuntu.sources"
)

// ubuntuArchiveHosts are the public archives a stock Ubuntu image points at,
// and what BuildAptConfig.Mirror stands in for.
var ubuntuArchiveHosts = []string{
	"http://archive.ubuntu.com/ubuntu",
	"http://security.ubuntu.com/ubuntu",
	"https://archive.ubuntu.com/ubuntu",
	"https://security.ubuntu.com/ubuntu",
}

// aptConfigMarkers are what a RUN step that rewrites the apt configuration
// itself tends to mention. Such a step could not edit a file that is a
// read-only bind mount (sed -i, tee and rm all fail on it), and later steps
// would see the build's copy of the base image's sources rather than the
// edited ones, so a Dockerfile with such a step keeps its own apt setup.
var aptConfigMarkers = []string{"/etc/apt", "sources.list", "ubuntu.sources", "add-apt-repository", "apt-add-repository"}

// runStepsTouchAptConfig reports whether any step of the plan looks like it
// edits the apt configuration: a RUN mentioning it, or a COPY/ADD into it
// (which a later RUN would then not see under the mounts).
func runStepsTouchAptConfig(steps []dockerfileStep) bool {
	for _, step := range steps {
		var text string
		switch step.kind {
		case stepRun:
			text = step.shell
			if step.exec != nil {
				text = strings.Join(step.exec, " ")
			}
		case stepCopy, stepAdd:
			text = step.dest
		default:
			continue
		}
		for _, marker := range aptConfigMarkers {
			if strings.Contains(text, marker) {
				return true
			}
		}
	}
	return false
}

// aptBuildVolumes returns --volume specs that tune apt inside RUN steps of a
// build whose base rootfs (mounted at rootfs) has apt. Files are written under
// tmpdir and bind-mounted read-only, so the image itself is unchanged: a
// container started from it sees the base image's own apt configuration.
//
// Nothing is mounted for images without /etc/apt (Alpine, distroless); buildah
// would otherwise create the mount point directory inside the layer.
func aptBuildVolumes(cfg types.BuildAptConfig, rootfs, tmpdir string) []string {
	if _, err := lstatInRootfs(rootfs, "/etc/apt"); err != nil {
		return nil
	}

	dir := filepath.Join(tmpdir, "apt")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Warn().Err(err).Msg("apt build config directory")
		return nil
	}

	var volumes []string
	if conf := renderAptBuildConf(cfg); conf != "" {
		path := filepath.Join(dir, "99-beta9-build")
		if err := os.WriteFile(path, []byte(conf), 0o644); err == nil {
			volumes = append(volumes, path+":"+aptConfMountPath+":ro")
		} else {
			log.Warn().Err(err).Msg("apt build config")
		}
	}

	if cfg.Mirror != "" {
		for _, sources := range []string{aptSourcesMountPath, aptSourcesDeb822Mount} {
			original, err := readFileInRootfs(rootfs, sources)
			if err != nil {
				continue
			}
			rewritten, changed := rewriteAptSources(string(original), cfg.Mirror)
			if !changed {
				continue
			}
			path := filepath.Join(dir, filepath.Base(sources))
			if err := os.WriteFile(path, []byte(rewritten), 0o644); err != nil {
				log.Warn().Err(err).Msg("apt build sources")
				continue
			}
			volumes = append(volumes, path+":"+sources+":ro")
		}
	}
	return volumes
}

// lstatInRootfs stats path inside rootfs without following symlinks in any
// component: the rootfs is the user's base image, and a symlink in it would
// otherwise point the read at the worker's own filesystem. Symlinked apt
// paths are simply not tuned.
func lstatInRootfs(rootfs, path string) (os.FileInfo, error) {
	current := rootfs
	var info os.FileInfo
	for _, part := range strings.Split(strings.Trim(filepath.Clean(path), "/"), "/") {
		if part == "" || part == ".." {
			return nil, fmt.Errorf("invalid rootfs path %q", path)
		}
		current = filepath.Join(current, part)
		var err error
		if info, err = os.Lstat(current); err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("%s is a symlink", current)
		}
	}
	return info, nil
}

// readFileInRootfs reads a regular file inside rootfs, refusing symlinks.
func readFileInRootfs(rootfs, path string) ([]byte, error) {
	info, err := lstatInRootfs(rootfs, path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	return os.ReadFile(filepath.Join(rootfs, path))
}

// renderAptBuildConf renders the apt.conf.d drop-in; "" when there is nothing
// to set.
func renderAptBuildConf(cfg types.BuildAptConfig) string {
	var b strings.Builder
	if cfg.TimeoutS > 0 {
		fmt.Fprintf(&b, "Acquire::http::Timeout \"%d\";\n", cfg.TimeoutS)
		fmt.Fprintf(&b, "Acquire::https::Timeout \"%d\";\n", cfg.TimeoutS)
	}
	if cfg.Retries > 0 {
		fmt.Fprintf(&b, "Acquire::Retries \"%d\";\n", cfg.Retries)
	}
	if cfg.Proxy != "" {
		fmt.Fprintf(&b, "Acquire::http::Proxy \"%s\";\n", cfg.Proxy)
	}
	return b.String()
}

// rewriteAptSources points the Ubuntu archive entries of a sources.list (or
// deb822 .sources) file at mirror. The second result is false when the file
// has no such entries and should be left alone.
func rewriteAptSources(sources, mirror string) (string, bool) {
	mirror = strings.TrimRight(mirror, "/")
	changed := false
	for _, host := range ubuntuArchiveHosts {
		if strings.Contains(sources, host) {
			sources = strings.ReplaceAll(sources, host, mirror)
			changed = true
		}
	}
	return sources, changed
}
