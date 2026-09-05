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

// aptBuildVolumes returns --volume specs that tune apt inside RUN steps of a
// build whose base rootfs (mounted at rootfs) has apt. Files are written under
// tmpdir and bind-mounted read-only, so the image itself is unchanged: a
// container started from it sees the base image's own apt configuration.
//
// Nothing is mounted for images without /etc/apt (Alpine, distroless); buildah
// would otherwise create the mount point directory inside the layer.
func aptBuildVolumes(cfg types.BuildAptConfig, rootfs, tmpdir string) []string {
	if _, err := os.Stat(filepath.Join(rootfs, "etc", "apt")); err != nil {
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
			original, err := os.ReadFile(filepath.Join(rootfs, sources))
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
