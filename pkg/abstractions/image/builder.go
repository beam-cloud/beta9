package image

import (
    "context"
    _ "embed"
    "fmt"
    "regexp"
    "strings"
    "time"

    "github.com/pkg/errors"
    "github.com/rs/zerolog/log"

    "github.com/beam-cloud/beta9/pkg/common"
    "github.com/beam-cloud/beta9/pkg/network"
    "github.com/beam-cloud/beta9/pkg/registry"
    "github.com/beam-cloud/beta9/pkg/repository"
    "github.com/beam-cloud/beta9/pkg/scheduler"
    "github.com/beam-cloud/beta9/pkg/types"
)

const (
	defaultImageBuildGracefulShutdownS               = 5 * time.Second
	defaultBuildContainerCpu           int64         = 1000
	defaultBuildContainerMemory        int64         = 1024
	defaultContainerSpinupTimeout      time.Duration = 600 * time.Second
	dockerfileContainerSpinupTimeout   time.Duration = 1 * time.Hour

	pipCommandType        string = "pip"
	shellCommandType      string = "shell"
	micromambaCommandType string = "micromamba"

	dockerHubRegistry string = "docker.io"
)

var buildEnv []string = []string{"DEBIAN_FRONTEND=noninteractive", "PIP_ROOT_USER_ACTION=ignore", "UV_NO_CACHE=true", "UV_COMPILE_BYTECODE=true"}

type Builder struct {
	config        types.AppConfig
	scheduler     *scheduler.Scheduler
	registry      *registry.ImageRegistry
	containerRepo repository.ContainerRepository
	tailscale     *network.Tailscale
	eventBus      *common.EventBus
	skopeoClient  common.SkopeoClient
}

func NewBuilder(config types.AppConfig, registry *registry.ImageRegistry, scheduler *scheduler.Scheduler, tailscale *network.Tailscale, containerRepo repository.ContainerRepository, rdb *common.RedisClient) (*Builder, error) {
	return &Builder{
		config:        config,
		scheduler:     scheduler,
		tailscale:     tailscale,
		registry:      registry,
		containerRepo: containerRepo,
		eventBus:      common.NewEventBus(rdb),
		skopeoClient:  common.NewSkopeoClient(config),
	}, nil
}

var (
	//go:embed base_requirements.txt
	basePythonRequirements string
)

type BaseImage struct {
	Registry string
	Repo     string
	Tag      string
	Digest   string
}

func (i *BaseImage) String() string {
	if i.Digest != "" {
		return fmt.Sprintf("%s/%s@%s", i.Registry, i.Repo, i.Digest)
	}
	return fmt.Sprintf("%s/%s:%s", i.Registry, i.Repo, i.Tag)
}

// startBuildContainer is used to start the build container and wait for it to be ready
func (b *Builder) startBuildContainer(ctx context.Context, build *Build) error {
	if err := build.initializeBuildConfiguration(); err != nil {
		return err
	}

	containerRequest, err := build.generateContainerRequest()
	if err != nil {
		build.log(true, "Error occured while generating container request: "+err.Error())
		return err
	}

	if err = b.scheduler.Run(containerRequest); err != nil {
		build.log(true, err.Error()+"\n")
		return err
	}

	hostname, err := b.containerRepo.GetWorkerAddress(ctx, build.containerID)
	if err != nil {
		build.log(true, "Failed to connect to build container.\n")
		return err
	}

	if err := b.containerRepo.SetBuildContainerTTL(build.containerID, time.Duration(imageContainerTtlS)*time.Second); err != nil {
		build.log(true, "Failed to connect to build container.\n")
		return err
	}

	go b.refreshBuildContainerTTL(ctx, build.containerID)

	return build.connectToHost(hostname, b.tailscale)
}

func (b *Builder) waitForBuildContainer(ctx context.Context, build *Build) error {
	// For Clip v2 builds, the worker builds via buildah and short-circuits without running a runc container.
	// We just wait for the container lifecycle to complete (exit with code 0).
	if b.config.ImageService.ClipVersion == 2 {
		return b.waitForV2Build(ctx, build)
	}

	// V1 path: wait for the build container to start running
	build.log(false, "Setting up build container...\n")
	buildContainerRunning := false

	containerSpinupTimeout := b.calculateContainerSpinupTimeout(ctx, build.opts)
	retryTicker := time.NewTicker(100 * time.Millisecond)
	defer retryTicker.Stop()
	timeoutTicker := time.NewTicker(containerSpinupTimeout)
	defer timeoutTicker.Stop()

	for !buildContainerRunning {
		select {
		case <-ctx.Done():
			log.Info().Str("container_id", build.containerID).Msg("build was aborted")
			build.log(true, "Build was aborted.\n")
			return ctx.Err()

		case <-retryTicker.C:
			r, err := build.getContainerStatus()
			if err != nil {
				build.log(true, "Error occurred while checking container status: "+err.Error())
				return err
			}

			if r.Running {
				buildContainerRunning = true
				continue
			}

			// Check if container exited prematurely
			exitCode, err := b.containerRepo.GetContainerExitCode(build.containerID)
			if err == nil && exitCode != 0 {
				exitCodeMsg := getExitCodeMsg(exitCode)
				time.Sleep(200 * time.Millisecond)
				build.log(true, fmt.Sprintf("Container exited with error: %s\n", exitCodeMsg))
				return errors.New(fmt.Sprintf("container exited with error: %s", exitCodeMsg))
			}

		case <-timeoutTicker.C:
			if err := b.stopBuild(build.containerID); err != nil {
				log.Error().Str("container_id", build.containerID).Err(err).Msg("failed to stop build")
			}
			build.log(true, fmt.Sprintf("Timeout: container not running after %s seconds.\n", containerSpinupTimeout))
			return errors.New(fmt.Sprintf("timeout: container not running after %s seconds", containerSpinupTimeout))
		}
	}

	return nil
}

// waitForV2Build waits for a Clip v2 build to complete (buildah bud + index creation)
func (b *Builder) waitForV2Build(ctx context.Context, build *Build) error {
	build.log(false, "Building image with buildah...\n")

	containerSpinupTimeout := b.calculateContainerSpinupTimeout(ctx, build.opts)
	retryTicker := time.NewTicker(100 * time.Millisecond)
	defer retryTicker.Stop()
	timeoutTicker := time.NewTicker(containerSpinupTimeout)
	defer timeoutTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info().Str("container_id", build.containerID).Msg("build was aborted")
			build.log(true, "Build was aborted.\n")
			return ctx.Err()

		case <-retryTicker.C:
			// For v2 builds, the worker short-circuits after building via buildah.
			// Check if the container has exited (indicating build completion).
			exitCode, err := b.containerRepo.GetContainerExitCode(build.containerID)
			if err == nil {
				if exitCode != 0 {
					exitCodeMsg := getExitCodeMsg(exitCode)
					time.Sleep(200 * time.Millisecond)
					build.log(true, fmt.Sprintf("Build failed: %s\n", exitCodeMsg))
					return errors.New(fmt.Sprintf("build failed: %s", exitCodeMsg))
				}
				// Success: buildah build + index creation completed
				return nil
			}

		case <-timeoutTicker.C:
			if err := b.stopBuild(build.containerID); err != nil {
				log.Error().Str("container_id", build.containerID).Err(err).Msg("failed to stop build")
			}
			build.log(true, fmt.Sprintf("Timeout: build did not complete after %s.\n", containerSpinupTimeout))
			return errors.New(fmt.Sprintf("timeout: build did not complete after %s", containerSpinupTimeout))
		}
	}
}

// Build user image
func (b *Builder) Build(ctx context.Context, opts *BuildOpts, outputChan chan common.OutputMsg) error {
	build, err := NewBuild(ctx, opts, outputChan, b.config)
	if err != nil {
		return err
	}

    // Prepare opts for image ID calculation (renders Dockerfile for v2 if needed)
    if err := prepareOptsForImageID(build.opts, b.config.ImageService.ClipVersion, b.RenderV2Dockerfile); err != nil {
        build.log(true, "Failed to prepare build options.\n")
        return err
    }

    // Calculate final image ID (includes Dockerfile for v2, commands/steps for v1)
    build.imageID, err = getImageID(build.opts)
    if err != nil {
        return err
    }

    // Send a stop-build event to the worker if the user cancels the build
	go b.handleBuildCancellation(ctx, build)
	defer build.killContainer() // Kill and remove container after the build completes

    err = b.startBuildContainer(ctx, build)
	if err != nil {
		build.log(true, "Failed to start build container: "+err.Error())
		return err
	}

	go build.streamLogs()

    // Always wait for the build container lifecycle to progress so logs stream through.
    // For v2, waitForBuildContainer treats clean exit (exitCode==0) as success.
    err = b.waitForBuildContainer(ctx, build)
    if err != nil {
        return err
    }

    // For v2 builds, the worker built the image from the Dockerfile before the container ran.
    // Skip container command execution and archiving.
    if b.config.ImageService.ClipVersion != 2 {
        if err := build.prepareCommands(); err != nil {
            return err
        }

        if err := build.executeCommands(); err != nil {
            return err
        }

        if err := build.archive(); err != nil {
            return err
        }
    } else {
        // Emit a friendly completion line with the final image id
        build.log(false, "=> Build complete 🎉\n")
    }

	build.setSuccess(true)
	build.logWithImageAndPythonVersion(true, "Build completed successfully")
	return nil
}

// RenderV2Dockerfile converts build options into a Dockerfile that can be built by the worker
// using buildah bud. We intentionally avoid executing any commands in a runc container.
func (b *Builder) RenderV2Dockerfile(opts *BuildOpts) (string, error) {
    base := getSourceImage(opts)

    var sb strings.Builder
    sb.WriteString("FROM ")
    sb.WriteString(base)
    sb.WriteString("\n")

    // Determine python install plan without runtime probing
    micromamba := strings.Contains(opts.PythonVersion, "micromamba")
    pythonVersion := opts.PythonVersion
    if pythonVersion == types.Python3.String() {
        pythonVersion = b.config.ImageService.PythonVersion
    }

    // Check if we're using a beta9 base image (which already has Python installed)
    isBeta9BaseImage := opts.BaseImageName == b.config.ImageService.Runner.BaseImageName &&
        opts.BaseImageRegistry == b.config.ImageService.Runner.BaseImageRegistry

    // If not ignoring python, add python install (standalone) or micromamba config
    // Skip Python installation for beta9 base images since they already have Python
    if !(opts.IgnorePython && len(opts.PythonPackages) == 0) {
        if micromamba {
            sb.WriteString("RUN micromamba config set use_lockfiles False\n")
        } else if pythonVersion != "" && !isBeta9BaseImage {
            installCmd, err := getPythonInstallCommand(b.config.ImageService.Runner.PythonStandalone, pythonVersion)
            if err != nil {
                return "", err
            }
            sb.WriteString("RUN ")
            sb.WriteString(installCmd)
            sb.WriteString("\n")
        }

        // Pip install for requested packages (use standard pip for dockerfile builds)
        if len(opts.PythonPackages) > 0 && pythonVersion != "" {
            // Assume virtual env only when micromamba is requested
            pipCmd := generateStandardPipInstallCommand(opts.PythonPackages, pythonVersion, micromamba)
            if strings.TrimSpace(pipCmd) != "" {
                sb.WriteString("RUN ")
                sb.WriteString(pipCmd)
                sb.WriteString("\n")
            }
        }
    }

    // Coalesce build steps (pip/mamba) relative to pythonVersion and micromamba virtual env
    if len(opts.BuildSteps) > 0 {
        steps := parseBuildStepsForDockerfile(opts.BuildSteps, pythonVersion, micromamba)
        for _, line := range steps {
            if strings.TrimSpace(line) == "" {
                continue
            }
            sb.WriteString("RUN ")
            sb.WriteString(line)
            sb.WriteString("\n")
        }
    }

    // Append explicit shell commands provided on the image
    for _, line := range opts.Commands {
        if strings.TrimSpace(line) == "" {
            continue
        }
        sb.WriteString("RUN ")
        sb.WriteString(line)
        sb.WriteString("\n")
    }

    return sb.String(), nil
}

// Check if an image already exists in the registry
func (b *Builder) Exists(ctx context.Context, imageId string) (bool, error) {
	return b.registry.Exists(ctx, imageId)
}

func (b *Builder) refreshBuildContainerTTL(ctx context.Context, containerId string) {
	ticker := time.NewTicker(time.Duration(buildContainerKeepAliveIntervalS) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := b.containerRepo.SetBuildContainerTTL(containerId, time.Duration(imageContainerTtlS)*time.Second); err != nil {
				log.Error().Str("container_id", containerId).Err(err).Msg("failed to set build container ttl")
			}
		}
	}
}

var imageNamePattern = regexp.MustCompile(
	`^` + // Assert position at the start of the string
		`(?:(?P<Registry>(?:(?:localhost|[\w.-]+(?:\.[\w.-]+)+)(?::\d+)?)|[\w]+:\d+)\/)?` + // Optional registry, which can be localhost, a domain with optional port, or a simple registry with port
		`(?P<Repo>(?:[\w][\w.-]*(?:/[\w][\w.-]*)*))?` + // Full repository path including namespace
		`(?::(?P<Tag>[\w][\w.-]{0,127}))?` + // Optional tag, which starts with a word character and can contain word characters, dots, and hyphens
		`(?:@(?P<Digest>[A-Za-z][A-Za-z0-9]*(?:[-_+.][A-Za-z][A-Za-z0-9]*)*:[0-9A-Fa-f]{32,}))?` + // Optional digest, which is a hash algorithm followed by a colon and a hexadecimal hash
		`$`, // Assert position at the end of the string
)

func ExtractImageNameAndTag(imageRef string) (BaseImage, error) {
	if imageRef == "" {
		return BaseImage{}, errors.New("invalid image URI format")
	}

	matches := imageNamePattern.FindStringSubmatch(imageRef)
	if matches == nil {
		return BaseImage{}, errors.New("invalid image URI format")
	}

	result := make(map[string]string)
	for i, name := range imageNamePattern.SubexpNames() {
		if i > 0 && name != "" {
			result[name] = matches[i]
		}
	}

	registry := result["Registry"]
	if registry == "" {
		registry = dockerHubRegistry
	}

	repo := result["Repo"]
	if repo == "" {
		return BaseImage{}, errors.New("invalid image URI format")
	}

	tag, digest := result["Tag"], result["Digest"]
	if tag == "" && digest == "" {
		tag = "latest"
	}

	return BaseImage{
		Registry: registry,
		Repo:     repo,
		Tag:      tag,
		Digest:   digest,
	}, nil
}

func (b *Builder) stopBuild(containerId string) error {
	_, err := b.eventBus.Send(&common.Event{
		Type:          common.StopBuildEventType(containerId),
		Args:          map[string]any{"container_id": containerId},
		LockAndDelete: false,
	})
	if err != nil {
		log.Error().Err(err).Msg("failed to send stop build event")
		return err
	}

	log.Info().Str("container_id", containerId).Msg("sent stop build event")
	return nil
}

func (b *Builder) handleBuildCancellation(ctx context.Context, build *Build) {
	<-ctx.Done()
	if build.success.Load() {
		return
	}
	err := b.stopBuild(build.containerID)
	if err != nil {
		log.Error().Str("container_id", build.containerID).Err(err).Msg("failed to stop build")
	}

	err = b.containerRepo.UpdateContainerStatus(build.containerID, types.ContainerStatusStopping, time.Now().Unix())
	if err != nil {
		log.Error().Str("container_id", build.containerID).Err(err).Msg("failed to update container status")
	}

	if err := build.killContainer(); err != nil {
		log.Error().Str("container_id", build.containerID).Err(err).Msg("failed to kill build container")
	}
}

func (b *Builder) calculateContainerSpinupTimeout(ctx context.Context, opts *BuildOpts) time.Duration {
	switch {
	case opts.Dockerfile != "":
		return dockerfileContainerSpinupTimeout
	case opts.ExistingImageUri != "":
		sourceImage := getSourceImage(opts)
		imageMetadata, err := b.skopeoClient.Inspect(ctx, sourceImage, opts.BaseImageCreds, nil)
		if err != nil {
			log.Error().Err(err).Msg("failed to inspect image")
			return defaultContainerSpinupTimeout
		}

		imageSizeBytes := 0
		for _, layer := range imageMetadata.LayersData {
			imageSizeBytes += layer.Size
		}

		timePerByte := time.Duration(b.config.ImageService.ArchiveNanosecondsPerByte) * time.Nanosecond
		timeLimit := timePerByte * time.Duration(imageSizeBytes) * 10
		log.Info().Int("image_size", imageSizeBytes).Msgf("estimated time to prepare new base image: %s", timeLimit.String())
		return timeLimit
	default:
		return defaultContainerSpinupTimeout
	}
}

func getSourceImage(opts *BuildOpts) string {
	var sourceImage string
	switch {
	case opts.BaseImageDigest != "":
		sourceImage = fmt.Sprintf("%s/%s@%s", opts.BaseImageRegistry, opts.BaseImageName, opts.BaseImageDigest)
	default:
		sourceImage = fmt.Sprintf("%s/%s:%s", opts.BaseImageRegistry, opts.BaseImageName, opts.BaseImageTag)
	}
	return sourceImage
}

func getImageTagOrDigest(digest string, tag string) string {
	if tag != "" {
		return tag
	}
	return digest
}

func getExitCodeMsg(exitCode int) string {
	msg, ok := types.WorkerContainerExitCodes[types.ContainerExitCode(exitCode)]
	if !ok {
		msg = types.WorkerContainerExitCodes[types.ContainerExitCodeUnknownError]
	}
	return msg
}
