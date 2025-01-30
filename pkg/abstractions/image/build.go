package image

import (
	"bytes"
	"context"
	"crypto/sha1"
	_ "embed"
	"encoding/hex"
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
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
)

type Builder struct {
	config        types.AppConfig
	scheduler     *scheduler.Scheduler
	registry      *common.ImageRegistry
	containerRepo repository.ContainerRepository
	tailscale     *network.Tailscale
	eventBus      *common.EventBus
	rdb           *common.RedisClient
	skopeoClient  common.SkopeoClient
}

type BuildStep struct {
	Command string
	Type    string
}

type BuildOpts struct {
	BaseImageRegistry  string
	BaseImageName      string
	BaseImageTag       string
	BaseImageDigest    string
	BaseImageCreds     string
	ExistingImageUri   string
	ExistingImageCreds map[string]string
	Dockerfile         string
	BuildCtxObject     string
	PythonVersion      string
	PythonPackages     []string
	Commands           []string
	BuildSteps         []BuildStep
	ForceRebuild       bool
	EnvVars            []string
	BuildSecrets       []string
	Gpu                string
}

func (o *BuildOpts) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "{")
	fmt.Fprintf(&b, "  \"BaseImageRegistry\": %q,", o.BaseImageRegistry)
	fmt.Fprintf(&b, "  \"BaseImageName\": %q,", o.BaseImageName)
	fmt.Fprintf(&b, "  \"BaseImageTag\": %q,", o.BaseImageTag)
	fmt.Fprintf(&b, "  \"BaseImageDigest\": %q,", o.BaseImageDigest)
	fmt.Fprintf(&b, "  \"BaseImageCreds\": %q,", o.BaseImageCreds)
	fmt.Fprintf(&b, "  \"ExistingImageUri\": %q,", o.ExistingImageUri)
	fmt.Fprintf(&b, "  \"ExistingImageCreds\": %#v,", o.ExistingImageCreds)
	fmt.Fprintf(&b, "  \"Dockerfile\": %q,", o.Dockerfile)
	fmt.Fprintf(&b, "  \"BuildCtxObject\": %q,", o.BuildCtxObject)
	fmt.Fprintf(&b, "  \"PythonVersion\": %q,", o.PythonVersion)
	fmt.Fprintf(&b, "  \"PythonPackages\": %#v,", o.PythonPackages)
	fmt.Fprintf(&b, "  \"Commands\": %#v,", o.Commands)
	fmt.Fprintf(&b, "  \"BuildSteps\": %#v,", o.BuildSteps)
	fmt.Fprintf(&b, "  \"ForceRebuild\": %v", o.ForceRebuild)
	fmt.Fprintf(&b, "  \"Gpu\": %q,", o.Gpu)
	fmt.Fprintf(&b, "}")
	return b.String()
}

func (o *BuildOpts) setCustomImageBuildOptions() error {
	baseImage, err := ExtractImageNameAndTag(o.ExistingImageUri)
	if err != nil {
		return err
	}

	if len(o.ExistingImageCreds) > 0 && o.ExistingImageUri != "" {
		token, err := GetRegistryToken(o)
		if err != nil {
			return err
		}
		o.BaseImageCreds = token
	}

	o.BaseImageRegistry = baseImage.Registry
	o.BaseImageName = baseImage.Repo
	o.BaseImageTag = baseImage.Tag
	o.BaseImageDigest = baseImage.Digest

	return nil
}

func (o *BuildOpts) addPythonRequirements() {
	// Override any specified python packages with base requirements (to ensure we have what need in the image)
	baseRequirementsSlice := strings.Split(strings.TrimSpace(basePythonRequirements), "\n")

	// Create a map to track package names in baseRequirementsSlice
	baseNames := make(map[string]bool)
	for _, basePkg := range baseRequirementsSlice {
		baseNames[extractPackageName(basePkg)] = true
	}

	// Filter out existing packages from opts.PythonPackages
	filteredPythonPackages := make([]string, 0)
	for _, optPkg := range o.PythonPackages {
		if !baseNames[extractPackageName(optPkg)] {
			filteredPythonPackages = append(filteredPythonPackages, optPkg)
		}
	}

	o.PythonPackages = append(filteredPythonPackages, baseRequirementsSlice...)
}

func NewBuilder(config types.AppConfig, registry *common.ImageRegistry, scheduler *scheduler.Scheduler, tailscale *network.Tailscale, containerRepo repository.ContainerRepository, rdb *common.RedisClient) (*Builder, error) {
	return &Builder{
		config:        config,
		scheduler:     scheduler,
		tailscale:     tailscale,
		registry:      registry,
		containerRepo: containerRepo,
		eventBus:      common.NewEventBus(rdb),
		rdb:           rdb,
		skopeoClient:  common.NewSkopeoClient(config),
	}, nil
}

var (
	//go:embed base_requirements.txt
	basePythonRequirements string
)

type ImageIdHash struct {
	BaseImageName   string
	BaseImageTag    string
	UserImageTag    string
	PythonVersion   string
	PythonPackages  []string
	ExitingImageUri string
	CommandListHash string
}

func (b *Builder) GetImageId(opts *BuildOpts) (string, error) {
	h := sha1.New()
	h.Write([]byte(strings.Join(opts.Commands, "-")))
	if len(opts.BuildSteps) > 0 {
		for _, step := range opts.BuildSteps {
			fmt.Fprintf(h, "%s-%s", step.Type, step.Command)
		}
	}
	if len(opts.EnvVars) > 0 {
		for _, envVar := range opts.EnvVars {
			fmt.Fprint(h, envVar)
		}
	}
	if opts.Dockerfile != "" {
		fmt.Fprint(h, opts.Dockerfile)
	}
	if opts.BuildCtxObject != "" {
		fmt.Fprint(h, opts.BuildCtxObject)
	}
	commandListHash := hex.EncodeToString(h.Sum(nil))

	bodyToHash := &ImageIdHash{
		BaseImageName:   opts.BaseImageName,
		BaseImageTag:    tagOrDigest(opts.BaseImageDigest, opts.BaseImageTag),
		PythonVersion:   opts.PythonVersion,
		PythonPackages:  opts.PythonPackages,
		ExitingImageUri: opts.ExistingImageUri,
		CommandListHash: commandListHash,
	}

	hash, err := hashstructure.Hash(bodyToHash, hashstructure.FormatV2, nil)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%016x", hash), nil
}

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

// Build user image
func (b *Builder) Build(ctx context.Context, opts *BuildOpts, outputChan chan common.OutputMsg) error {
	var (
		dockerfile             *string
		authInfo, _            = auth.AuthInfoFromContext(ctx)
		containerSpinupTimeout = defaultContainerSpinupTimeout
		success                = atomic.Bool{}
	)

	switch {
	case opts.Dockerfile != "":
		opts.addPythonRequirements()
		dockerfile = &opts.Dockerfile
		containerSpinupTimeout = dockerfileContainerSpinupTimeout
	case opts.ExistingImageUri != "":
		err := b.handleCustomBaseImage(opts, outputChan)
		if err != nil {
			return err
		}
		containerSpinupTimeout = b.calculateImageArchiveDuration(ctx, opts)
	}

	containerId := b.genContainerId()

	containerRequest, err := b.generateContainerRequest(opts, dockerfile, containerId, authInfo.Workspace)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: "Error occured while generating container request: " + err.Error()}
		return err
	}

	// If user cancels the build, send a stop-build event to the worker
	go func() {
		<-ctx.Done()
		if success.Load() {
			return
		}
		err := b.stopBuild(containerId)
		if err != nil {
			log.Error().Str("container_id", containerId).Err(err).Msg("failed to stop build")
		}
	}()

	err = b.scheduler.Run(containerRequest)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: err.Error() + "\n"}
		return err
	}

	hostname, err := b.containerRepo.GetWorkerAddress(ctx, containerId)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: "Failed to connect to build container.\n"}
		return err
	}

	err = b.rdb.Set(ctx, Keys.imageBuildContainerTTL(containerId), "1", time.Duration(imageContainerTtlS)*time.Second).Err()
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: "Failed to connect to build container.\n"}
		return err
	}

	go b.keepAlive(ctx, containerId, ctx.Done())

	conn, err := network.ConnectToHost(ctx, hostname, time.Second*30, b.tailscale, b.config.Tailscale)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: "Failed to connect to build container.\n"}
		return err
	}

	client, err := common.NewRunCClient(hostname, authInfo.Token.Key, conn)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: "Failed to connect to build container.\n"}
		return err
	}
	go client.StreamLogs(ctx, containerId, outputChan)

	go func() {
		<-ctx.Done() // If user cancels the build, kill the container
		client.Kill(containerId)
	}()
	defer client.Kill(containerId) // Kill and remove container after the build completes

	outputChan <- common.OutputMsg{Done: false, Success: success.Load(), Msg: "Waiting for build container to start...\n"}
	start := time.Now()
	buildContainerRunning := false

	for !buildContainerRunning {
		select {
		case <-ctx.Done():
			log.Info().Str("container_id", containerId).Msg("build was aborted")
			outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: "Build was aborted.\n"}
			return ctx.Err()

		case <-time.After(100 * time.Millisecond):
			r, err := client.Status(containerId)
			if err != nil {
				outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: "Error occurred while checking container status: " + err.Error()}
				return err
			}

			if r.Running {
				buildContainerRunning = true
				continue
			}

			exitCode, err := b.containerRepo.GetContainerExitCode(containerId)
			if err == nil && exitCode != 0 {
				msg, ok := types.WorkerContainerExitCodes[exitCode]
				if !ok {
					msg = types.WorkerContainerExitCodes[types.WorkerContainerExitCodeUnknownError]
				}
				// Wait for any final logs to get sent before returning
				time.Sleep(200 * time.Millisecond)
				outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: fmt.Sprintf("Container exited with error: %s\n", msg)}
				return errors.New(fmt.Sprintf("container exited with error: %s\n", msg))
			}

			if time.Since(start) > containerSpinupTimeout {
				err := b.stopBuild(containerId)
				if err != nil {
					log.Error().Str("container_id", containerId).Err(err).Msg("failed to stop build")
				}
				outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: fmt.Sprintf("Timeout: container not running after %s seconds.\n", containerSpinupTimeout)}
				return errors.New(fmt.Sprintf("timeout: container not running after %s seconds", containerSpinupTimeout))
			}
		}
	}

	imageId, err := b.GetImageId(opts)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: "Error occured while generating image id: " + err.Error()}
		return err
	}

	if !buildContainerRunning {
		outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: "Unable to connect to build container.\n"}
		return errors.New("container not running")
	}

	micromambaEnv := strings.Contains(opts.PythonVersion, "micromamba")
	if micromambaEnv {
		client.Exec(containerId, "micromamba config set use_lockfiles False")
	}

	var setupCommands []string
	// Detect if python3.x is installed in the container, if not install it
	checkPythonVersionCmd := fmt.Sprintf("%s --version", opts.PythonVersion)
	if resp, err := client.Exec(containerId, checkPythonVersionCmd); (err != nil || !resp.Ok) && !micromambaEnv {

		if opts.PythonVersion == b.config.ImageService.DefaultPythonVersion {
			opts.PythonVersion = types.Python310.String()
		}

		outputChan <- common.OutputMsg{Done: false, Success: success.Load(), Msg: fmt.Sprintf("%s not detected, installing it for you...\n", opts.PythonVersion)}
		installCmd, err := getPythonStandaloneInstallCommand(b.config.ImageService.Runner.PythonStandalone, opts.PythonVersion)
		if err != nil {
			outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: err.Error() + "\n"}
			return err
		}
		setupCommands = append(setupCommands, installCmd)
	}

	// Generate the pip install command and prepend it to the commands list
	if len(opts.PythonPackages) > 0 {
		pipInstallCmd := generatePipInstallCommand(opts.PythonPackages, opts.PythonVersion)
		setupCommands = append(setupCommands, pipInstallCmd)
	}

	opts.Commands = append(setupCommands, opts.Commands...)

	// Generate the commands to run in the container
	opts.Commands = append(opts.Commands, parseBuildSteps(opts.BuildSteps, opts.PythonVersion)...)

	log.Info().Str("container_id", containerId).Interface("options", opts).Msg("container building")
	startTime := time.Now()

	for _, cmd := range opts.Commands {
		if cmd == "" {
			continue
		}

		if r, err := client.Exec(containerId, cmd); err != nil || !r.Ok {
			log.Error().Str("container_id", containerId).Str("command", cmd).Err(err).Msg("failed to execute command for container")

			errMsg := ""
			if err != nil {
				errMsg = err.Error() + "\n"
			}

			time.Sleep(defaultImageBuildGracefulShutdownS) // Wait for logs to be passed through
			outputChan <- common.OutputMsg{Done: true, Success: success.Load(), Msg: errMsg}
			return err
		}
	}
	log.Info().Str("container_id", containerId).Dur("duration", time.Since(startTime)).Msg("container build took")

	err = client.Archive(ctx, containerId, imageId, outputChan)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Archiving: true, Success: success.Load(), Msg: err.Error() + "\n"}
		return err
	}

	success.Store(true)
	outputChan <- common.OutputMsg{Done: true, Archiving: true, Success: success.Load(), ImageId: imageId, PythonVersion: opts.PythonVersion}
	return nil
}

// generateContainerRequest generates a container request for the build container
func (b *Builder) generateContainerRequest(opts *BuildOpts, dockerfile *string, containerId string, workspace *types.Workspace) (*types.ContainerRequest, error) {
	baseImageId, err := b.GetImageId(&BuildOpts{
		BaseImageRegistry: opts.BaseImageRegistry,
		BaseImageName:     opts.BaseImageName,
		BaseImageTag:      opts.BaseImageTag,
		BaseImageDigest:   opts.BaseImageDigest,
		ExistingImageUri:  opts.ExistingImageUri,
		EnvVars:           opts.EnvVars,
		Dockerfile:        opts.Dockerfile,
		BuildCtxObject:    opts.BuildCtxObject,
	})
	if err != nil {
		return nil, err
	}

	sourceImage := getSourceImage(opts)

	// Allow config to override default build container settings
	cpu := defaultBuildContainerCpu
	memory := defaultBuildContainerMemory

	if b.config.ImageService.BuildContainerCpu > 0 {
		cpu = b.config.ImageService.BuildContainerCpu
	}

	if b.config.ImageService.BuildContainerMemory > 0 {
		memory = b.config.ImageService.BuildContainerMemory
	}

	containerRequest := &types.ContainerRequest{
		BuildOptions: types.BuildOptions{
			SourceImage:      &sourceImage,
			SourceImageCreds: opts.BaseImageCreds,
			Dockerfile:       dockerfile,
			BuildCtxObject:   &opts.BuildCtxObject,
			BuildSecrets:     opts.BuildSecrets,
		},
		ContainerId: containerId,
		Env:         opts.EnvVars,
		Cpu:         cpu,
		Memory:      memory,
		ImageId:     baseImageId,
		WorkspaceId: workspace.ExternalId,
		Workspace:   *workspace,
		EntryPoint:  []string{"tail", "-f", "/dev/null"},
	}

	if opts.Gpu != "" {
		containerRequest.GpuRequest = []string{opts.Gpu}
		containerRequest.GpuCount = 1
	} else {
		containerRequest.PoolSelector = b.config.ImageService.BuildContainerPoolSelector
	}

	return containerRequest, nil
}

func (b *Builder) genContainerId() string {
	return fmt.Sprintf("%s%s", types.BuildContainerPrefix, uuid.New().String()[:8])
}

// handleCustomBaseImage validates the custom base image and parses its details into build options
func (b *Builder) handleCustomBaseImage(opts *BuildOpts, outputChan chan common.OutputMsg) error {
	if outputChan != nil {
		outputChan <- common.OutputMsg{Done: false, Success: false, Msg: fmt.Sprintf("Using custom base image: %s\n", opts.ExistingImageUri)}
	}

	err := opts.setCustomImageBuildOptions()
	if err != nil {
		if outputChan != nil {
			outputChan <- common.OutputMsg{Done: true, Success: false, Msg: err.Error() + "\n"}
		}
		return err
	}

	opts.addPythonRequirements()
	return nil
}

// Check if an image already exists in the registry
func (b *Builder) Exists(ctx context.Context, imageId string) bool {
	return b.registry.Exists(ctx, imageId)
}

func (b *Builder) keepAlive(ctx context.Context, containerId string, done <-chan struct{}) {
	ticker := time.NewTicker(time.Duration(buildContainerKeepAliveIntervalS) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			b.rdb.Set(ctx, Keys.imageBuildContainerTTL(containerId), "1", time.Duration(imageContainerTtlS)*time.Second).Err()
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
		registry = "docker.io"
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

// PythonStandaloneTemplate is used to render the standalone python install script
type PythonStandaloneTemplate struct {
	PythonVersion string

	// Architecture, OS, and Vendor are determined at runtime
	Architecture string
	OS           string
	Vendor       string
}

func getPythonStandaloneInstallCommand(config types.PythonStandaloneConfig, pythonVersion string) (string, error) {
	var arch string
	switch runtime.GOARCH {
	case "amd64":
		arch = "x86_64"
	case "arm64":
		arch = "aarch64"
	default:
		return "", errors.New("unsupported architecture for python standalone install")
	}

	var vendor, os string
	switch runtime.GOOS {
	case "linux":
		vendor, os = "unknown", "linux"
	case "darwin":
		vendor, os = "apple", "darwin"
	default:
		return "", errors.New("unsupported OS for python standalone install")
	}

	tmpl, err := template.New("standalonePython").Parse(config.InstallScriptTemplate)
	if err != nil {
		return "", err
	}

	var output bytes.Buffer
	if err := tmpl.Execute(&output, PythonStandaloneTemplate{
		PythonVersion: config.Versions[pythonVersion],
		Architecture:  arch,
		OS:            os,
		Vendor:        vendor,
	}); err != nil {
		return "", err
	}

	return output.String(), nil
}

func generatePipInstallCommand(pythonPackages []string, pythonVersion string) string {
	flagLines, packages := parseFlagLinesAndPackages(pythonPackages)

	command := fmt.Sprintf("PIP_ROOT_USER_ACTION=ignore %s -m pip install", pythonVersion)
	if len(flagLines) > 0 {
		command += " " + strings.Join(flagLines, " ")
	}
	if len(packages) > 0 {
		command += " " + strings.Join(packages, " ")
	}

	return command
}

func generateMicromambaInstallCommand(pythonPackages []string) string {
	flagLines, packages := parseFlagLinesAndPackages(pythonPackages)

	command := fmt.Sprintf("%s install -y -n beta9", micromambaCommandType)
	if len(flagLines) > 0 {
		command += " " + strings.Join(flagLines, " ")
	}
	if len(packages) > 0 {
		command += " " + strings.Join(packages, " ")
	}

	return command
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}

func parseFlagLinesAndPackages(pythonPackages []string) ([]string, []string) {
	var flagLines []string
	var packages []string
	var flags = []string{"--", "-"}

	for _, pkg := range pythonPackages {
		if hasAnyPrefix(pkg, flags) {
			flagLines = append(flagLines, pkg)
		} else {
			packages = append(packages, fmt.Sprintf("%q", pkg))
		}
	}
	return flagLines, packages
}

// Generate the commands to run in the container. This function will coalesce pip and mamba commands
// into a single command if they are adjacent to each other.
func parseBuildSteps(buildSteps []BuildStep, pythonVersion string) []string {
	commands := []string{}
	var (
		mambaStart int = -1
		mambaGroup []string
		pipStart   int = -1
		pipGroup   []string
	)

	for _, step := range buildSteps {
		if step.Type == shellCommandType {
			commands = append(commands, step.Command)
		}

		flagCmd := containsFlag(step.Command)

		// Flush any pending pip or mamba groups
		if pipStart != -1 && (step.Type != pipCommandType || flagCmd) {
			pipStart, pipGroup = flushPipCommand(commands, pipStart, pipGroup, pythonVersion)
		}

		if mambaStart != -1 && (step.Type != micromambaCommandType || flagCmd) {
			mambaStart, mambaGroup = flushMambaCommand(commands, mambaStart, mambaGroup)
		}

		if step.Type == pipCommandType {
			if pipStart == -1 {
				pipStart = len(commands)
				commands = append(commands, "")
			}
			pipGroup = append(pipGroup, step.Command)

			if flagCmd {
				pipStart, pipGroup = flushPipCommand(commands, pipStart, pipGroup, pythonVersion)
			}
		}

		if step.Type == micromambaCommandType {
			if mambaStart == -1 {
				mambaStart = len(commands)
				commands = append(commands, "")
			}
			mambaGroup = append(mambaGroup, step.Command)

			if flagCmd {
				mambaStart, mambaGroup = flushMambaCommand(commands, mambaStart, mambaGroup)
			}
		}
	}

	if mambaStart != -1 {
		commands[mambaStart] = generateMicromambaInstallCommand(mambaGroup)
	}

	if pipStart != -1 {
		commands[pipStart] = generatePipInstallCommand(pipGroup, pythonVersion)
	}

	return commands
}

func flushMambaCommand(commands []string, mambaStart int, mambaGroup []string) (int, []string) {
	commands[mambaStart] = generateMicromambaInstallCommand(mambaGroup)
	return -1, nil
}

func flushPipCommand(commands []string, pipStart int, pipGroup []string, pythonVersion string) (int, []string) {
	commands[pipStart] = generatePipInstallCommand(pipGroup, pythonVersion)
	return -1, nil
}

func containsFlag(s string) bool {
	flags := []string{
		"--no-deps",
		"--only-binary",
		"--no-binary",
		"--prefer-binary",
		"--require-hashes",
		"--pre",
		"--ignore-requires-python",
		"--no-pin",
		"--force-reinstall",
		"--freeze-installed",
		"--update-deps",
		"--no-update-deps",
	}

	for _, flag := range flags {
		if strings.Contains(s, flag) {
			return true
		}
	}
	return false
}

func extractPackageName(pkg string) string {
	// For now we let this go through and let the pip install command fail if the package is not found
	if len(pkg) == 0 {
		return ""
	}

	// Handle Git URLs
	if strings.HasPrefix(pkg, "git+") || strings.HasPrefix(pkg, "-e git+") {
		if eggTag := strings.Split(pkg, "#egg="); len(eggTag) > 1 {
			return eggTag[1]
		}
	}

	// Handle packages with index URLs
	if strings.HasPrefix(pkg, "-i ") || strings.HasPrefix(pkg, "--index-url ") {
		return ""
	}

	// Handle regular packages
	return strings.FieldsFunc(pkg, func(c rune) bool { return c == '=' || c == '>' || c == '<' || c == '[' || c == ';' })[0]
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

func tagOrDigest(digest string, tag string) string {
	if tag != "" {
		return tag
	}
	return digest
}

func (b *Builder) calculateImageArchiveDuration(ctx context.Context, opts *BuildOpts) time.Duration {
	sourceImage := getSourceImage(opts)
	imageMetadata, err := b.skopeoClient.Inspect(ctx, sourceImage, opts.BaseImageCreds)
	if err != nil {
		log.Error().Err(err).Msg("failed to inspect image")
		return defaultContainerSpinupTimeout
	}

	imageSizeBytes := 0
	for _, layer := range imageMetadata.LayersData {
		imageSizeBytes += layer.Size
	}

	timePerByte := time.Duration(b.config.ImageService.ArchiveNanosecondsPerByte) * time.Nanosecond
	timeLimit := timePerByte * time.Duration(imageSizeBytes)
	log.Info().Int("image_size", imageSizeBytes).Msgf("estimated time to prepare new base image: %s", timeLimit.String())
	return timeLimit
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
