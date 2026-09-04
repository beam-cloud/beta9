package worker

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/rs/zerolog/log"
)

// layeredBuild runs a Dockerfile's instructions against a buildah working
// container and publishes the container's overlay upper directory as new
// layers on the base image, the way a sandbox filesystem snapshot is
// published. Compared with buildah bud + push + index this skips the commit
// (a tar-and-hash pass over the delta), the push's second compression pass
// and the indexer's download and inflation of what was just uploaded; the
// delta is split into layers so the registry, the indexer and the content
// cache each move it several streams wide.
//
// buildah still runs every step, with the same mounts and isolation bud
// would use, so RUN semantics are unchanged. What is lost is bud's per-step
// layer cache, which only ever helped a rebuild on the very node that ran the
// previous build.
type layeredBuild struct {
	c            *ImageClient
	ctx          context.Context
	out          *slog.Logger
	request      *types.ContainerRequest
	plan         *dockerfilePlan
	sourceImage  string // registry reference of FROM, used to fetch the manifest to append to
	fromRef      string // what buildah from is given (may be a cached local layout)
	graphroot    string
	runroot      string
	tmpdir       string
	storage      string
	storageConf  string
	buildCtxPath string
	runVolumes   []string // --volume flags for RUN
	buildArgs    map[string]string

	container string
	stepsDone int
	// Image config accumulated from the instructions.
	env        map[string]string
	envOrder   []string
	workdir    string
	user       string
	shell      []string
	entrypoint []string
	cmd        []string
	labels     map[string]string
	exposed    map[string]struct{}
}

func (b *layeredBuild) storageArgs(sub string) []string {
	return []string{"--root", b.graphroot, "--runroot", b.runroot, "--storage-driver=" + b.storage, sub}
}

// quiet runs a buildah subcommand, returning the last line of its stdout (the
// id or path buildah prints), or both streams folded into the error on
// failure.
func (b *layeredBuild) quiet(sub string, args ...string) (string, error) {
	return b.quietEnv(nil, sub, args...)
}

func (b *layeredBuild) quietEnv(extraEnv []string, sub string, args ...string) (string, error) {
	var stdout, stderr strings.Builder
	env := append(b.c.buildahEnv(b.runroot, b.tmpdir, b.storageConf), extraEnv...)
	cmd := newBuildahCommand(b.ctx, append(b.storageArgs(sub), args...), env, &stdout, &stderr)
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("buildah %s: %w: %s", sub, err, strings.TrimSpace(stderr.String()+"\n"+stdout.String()))
	}
	lines := strings.Fields(strings.TrimSpace(stdout.String()))
	if len(lines) == 0 {
		return "", nil
	}
	return lines[len(lines)-1], nil
}

// execute runs the plan and returns the working container's overlay upper
// directory.
func (b *layeredBuild) execute() (upperDir string, err error) {
	b.env = map[string]string{}
	b.labels = map[string]string{}
	b.exposed = map[string]struct{}{}

	fromArgs := []string{"--name", "b9-build-" + b.request.ImageId, "--pull=missing"}
	if b.c.config.ImageService.BuildRegistryInsecure {
		fromArgs = append(fromArgs, "--tls-verify=false")
	}
	if authArgs := b.c.buildahAuthArgs(b.ctx, b.request, b.sourceImage); len(authArgs) > 0 {
		fromArgs = append(fromArgs, authArgs...)
	}
	fromArgs = append(fromArgs, b.fromRef)
	b.out.Info(fmt.Sprintf("STEP 1/%d: FROM %s\n", len(b.plan.steps)+1, b.sourceImage))
	container, err := b.quiet("from", fromArgs...)
	if err != nil {
		return "", err
	}
	b.container = container

	for i, step := range b.plan.steps {
		b.out.Info(fmt.Sprintf("STEP %d/%d: %s\n", i+2, len(b.plan.steps)+1, step.raw))
		if err := b.step(step); err != nil {
			return "", err
		}
		b.stepsDone++
	}

	mountPoint, err := b.quiet("mount", b.container)
	if err != nil {
		return "", err
	}
	upperDir, err = overlayUpperDir(mountPoint)
	if err != nil {
		return "", fmt.Errorf("locate overlay upper dir of %s: %w", mountPoint, err)
	}
	return upperDir, nil
}

// cleanup unmounts and removes the working container.
func (b *layeredBuild) cleanup() {
	if b.container == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	saved := b.ctx
	b.ctx = ctx
	defer func() { b.ctx = saved }()
	if _, err := b.quiet("umount", b.container); err != nil {
		log.Debug().Err(err).Msg("buildah umount")
	}
	if _, err := b.quiet("rm", b.container); err != nil {
		log.Warn().Err(err).Str("container", b.container).Msg("remove build container")
	}
}

func (b *layeredBuild) step(step dockerfileStep) error {
	switch step.kind {
	case stepRun:
		args := b.storageArgs("run")
		for _, volume := range b.runVolumes {
			args = append(args, "--volume", volume)
		}
		// ARG values reach RUN as environment without persisting in the
		// image. They are handed over through buildah's own environment
		// (--env NAME) so secrets stay off the command line.
		var extraEnv []string
		for name, value := range b.buildArgs {
			if _, isEnv := b.env[name]; !isEnv {
				args = append(args, "--env", name)
				extraEnv = append(extraEnv, name+"="+value)
			}
		}
		args = append(args, b.container, "--")
		if step.exec != nil {
			args = append(args, step.exec...)
		} else {
			shell := b.shell
			if len(shell) == 0 {
				shell = []string{"/bin/sh", "-c"}
			}
			args = append(args, shell...)
			args = append(args, step.shell)
		}
		output := newActiveOutputWriter(b.out)
		stop := startSilentOutputHeartbeat(b.ctx, b.out, time.Now(), output, "Still running build step...")
		env := append(b.c.buildahEnv(b.runroot, b.tmpdir, b.storageConf), extraEnv...)
		err := newBuildahCommand(b.ctx, args, env, output, output).Run()
		stop()
		if err != nil {
			return fmt.Errorf("build step failed: %s: %w", step.raw, err)
		}
	case stepEnv:
		var args []string
		for _, p := range step.pairs {
			if _, seen := b.env[p.key]; !seen {
				b.envOrder = append(b.envOrder, p.key)
			}
			b.env[p.key] = p.value
			args = append(args, "--env", p.key+"="+p.value)
		}
		_, err := b.quiet("config", append(args, b.container)...)
		return err
	case stepArg:
		// Already folded into buildArgs/expansion by the parser; an ARG with a
		// default has to reach RUN too.
		for _, p := range step.pairs {
			if p.set {
				if b.buildArgs == nil {
					b.buildArgs = map[string]string{}
				}
				if _, given := b.buildArgs[p.key]; !given {
					b.buildArgs[p.key] = p.value
				}
			}
		}
	case stepWorkdir:
		b.workdir = step.value
		if _, err := b.quiet("run", b.container, "--", "/bin/sh", "-c", "mkdir -p -- \"$0\"", step.value); err != nil {
			return err
		}
		_, err := b.quiet("config", "--workingdir", step.value, b.container)
		return err
	case stepUser:
		b.user = step.value
		_, err := b.quiet("config", "--user", step.value, b.container)
		return err
	case stepShell:
		b.shell = step.exec
	case stepEntrypoint:
		b.entrypoint = step.exec
		if step.exec == nil {
			b.entrypoint = []string{"/bin/sh", "-c", step.shell}
		}
		// Docker resets CMD when ENTRYPOINT is set.
		b.cmd = nil
	case stepCmd:
		b.cmd = step.exec
		if step.exec == nil {
			b.cmd = []string{"/bin/sh", "-c", step.shell}
		}
	case stepLabel:
		for _, p := range step.pairs {
			b.labels[p.key] = p.value
		}
	case stepExpose:
		for _, port := range step.ports {
			if !strings.Contains(port, "/") {
				port += "/tcp"
			}
			b.exposed[port] = struct{}{}
		}
	case stepCopy, stepAdd:
		sub := "copy"
		if step.kind == stepAdd {
			sub = "add"
		}
		if b.buildCtxPath == "" {
			return fmt.Errorf("%s without a build context", strings.ToUpper(sub))
		}
		args := []string{"--contextdir", b.buildCtxPath}
		if step.chown != "" {
			args = append(args, "--chown", step.chown)
		}
		if step.chmod != "" {
			args = append(args, "--chmod", step.chmod)
		}
		args = append(args, b.container)
		args = append(args, step.sources...)
		args = append(args, step.dest)
		if _, err := b.quiet(sub, args...); err != nil {
			return fmt.Errorf("build step failed: %s: %w", step.raw, err)
		}
	}
	return nil
}

// configFile applies the plan's ENV/WORKDIR/USER/ENTRYPOINT/CMD/LABEL/EXPOSE
// to img's config. img must already carry the new layers so their diff ids
// and history are kept.
func (b *layeredBuild) configFile(img v1.Image) (*v1.ConfigFile, error) {
	cfg, err := img.ConfigFile()
	if err != nil {
		return nil, err
	}
	cfg = cfg.DeepCopy()
	env := map[string]int{}
	for i, kv := range cfg.Config.Env {
		key, _, _ := strings.Cut(kv, "=")
		env[key] = i
	}
	for _, key := range b.envOrder {
		kv := key + "=" + b.env[key]
		if i, ok := env[key]; ok {
			cfg.Config.Env[i] = kv
		} else {
			env[key] = len(cfg.Config.Env)
			cfg.Config.Env = append(cfg.Config.Env, kv)
		}
	}
	if b.workdir != "" {
		cfg.Config.WorkingDir = b.workdir
	}
	if b.user != "" {
		cfg.Config.User = b.user
	}
	if b.entrypoint != nil {
		cfg.Config.Entrypoint = b.entrypoint
		cfg.Config.Cmd = b.cmd
	} else if b.cmd != nil {
		cfg.Config.Cmd = b.cmd
	}
	if len(b.shell) > 0 {
		cfg.Config.Shell = b.shell
	}
	if len(b.labels) > 0 {
		if cfg.Config.Labels == nil {
			cfg.Config.Labels = map[string]string{}
		}
		for k, v := range b.labels {
			cfg.Config.Labels[k] = v
		}
	}
	if len(b.exposed) > 0 {
		if cfg.Config.ExposedPorts == nil {
			cfg.Config.ExposedPorts = map[string]struct{}{}
		}
		for p := range b.exposed {
			cfg.Config.ExposedPorts[p] = struct{}{}
		}
	}
	cfg.Created = v1.Time{Time: time.Now()}
	return cfg, nil
}

// overlayUpperDir finds the upperdir of the overlay mounted at mountPoint by
// reading this process's mount table.
func overlayUpperDir(mountPoint string) (string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		// 5th field is the mount point; the fields after "-" are fstype, source, super options.
		if len(fields) < 10 || unescapeMountField(fields[4]) != mountPoint {
			continue
		}
		sep := -1
		for i, f := range fields {
			if f == "-" {
				sep = i
				break
			}
		}
		if sep < 0 || sep+3 >= len(fields) || fields[sep+1] != "overlay" {
			continue
		}
		for _, opt := range strings.Split(fields[sep+3], ",") {
			if dir, ok := strings.CutPrefix(opt, "upperdir="); ok {
				return unescapeMountField(dir), nil
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("no overlay mount at %s", mountPoint)
}

func unescapeMountField(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var out strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			var c byte
			if _, err := fmt.Sscanf(s[i+1:i+4], "%03o", &c); err == nil {
				out.WriteByte(c)
				i += 3
				continue
			}
		}
		out.WriteByte(s[i])
	}
	return out.String()
}

// buildLayeredImage runs the layered build end to end: execute the plan, pack
// the delta, then push and index it. ok is false when the plan could not be
// executed at all (before any step ran), letting the caller fall back to bud.
func (c *ImageClient) buildLayeredImage(ctx context.Context, outputLogger *slog.Logger, request *types.ContainerRequest, b *layeredBuild) error {
	defer b.cleanup()
	started := time.Now()
	upperDir, err := b.execute()
	if err != nil {
		if b.container == "" || b.stepsDone < len(b.plan.steps) {
			return err
		}
		// Every step ran; only the delta could not be reached (no overlay
		// upper dir). Commit the working container and publish it the
		// classic way rather than rerunning the build.
		log.Warn().Err(err).Str("image_id", request.ImageId).Msg("layered build cannot read the delta, committing instead")
		imageTag := fmt.Sprintf("%s/%s:%s", c.getBuildRegistry(), c.config.ImageService.BuildRepositoryName, request.ImageId)
		if _, err := b.quiet("commit", "--format", "docker", b.container, imageTag); err != nil {
			return err
		}
		outputLogger.Info(fmt.Sprintf("Image built in %.1fs\n", time.Since(started).Seconds()))
		return c.publishFromStorage(ctx, outputLogger, request, imageTag, &buildahStore{graphroot: b.graphroot, runroot: b.runroot, tmpdir: b.tmpdir, driver: b.storage, conf: b.storageConf})
	}
	outputLogger.Info(fmt.Sprintf("Image built in %.1fs\n", time.Since(started).Seconds()))

	base, _, err := c.remoteBaseImage(ctx, request, b.sourceImage)
	if err != nil {
		return fmt.Errorf("fetch base image %s: %w", b.sourceImage, err)
	}

	outputLogger.Info("Publishing image...\n")
	started = time.Now()
	layersDir := filepath.Join(c.layerSpoolDir(), "build-"+request.ImageId)
	defer os.RemoveAll(layersDir)
	layers, err := packOverlayLayers(upperDir, layersDir, layerMediaTypeFor(base))
	if err != nil {
		return fmt.Errorf("pack image layers: %w", err)
	}
	packed := time.Since(started)
	img, err := appendLayers(base, layers, "beta9 image build")
	if err != nil {
		return err
	}
	cfg, err := b.configFile(img)
	if err != nil {
		return err
	}
	if img, err = mutate.ConfigFile(img, cfg); err != nil {
		return err
	}
	compressed, content, sizes := layerStats(layers)
	log.Info().Str("image_id", request.ImageId).Int("layers", len(layers)).Int64("layer_bytes", compressed).
		Int64("content_bytes", content).Ints64("layer_sizes", sizes).Dur("pack", packed).Msg("packed build delta")

	result, err := c.publishLayeredImage(ctx, request, img, layers, request.ImageId, layersDir)
	if err != nil {
		return err
	}
	outputLogger.Info(fmt.Sprintf("Image published in %.1fs (%d layers, %s)\n", packed.Seconds()+result.elapsed.Seconds(), len(layers), formatImageBytes(compressed)))
	return nil
}

// buildArgMap turns NAME=value build secrets into ARG values.
func buildArgMap(secrets []string) map[string]string {
	args := map[string]string{}
	for _, secret := range secrets {
		if name, value, ok := strings.Cut(secret, "="); ok && name != "" {
			args[name] = value
		}
	}
	return args
}
