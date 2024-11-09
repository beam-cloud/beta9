package image

import (
	"context"
	"crypto/sha1"
	_ "embed"
	"encoding/hex"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/pkg/errors"

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
	defaultContainerSpinupTimeout      time.Duration = 180 * time.Second

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
}

type BuildStep struct {
	Command string
	Type    string
}

type BuildOpts struct {
	BaseImageRegistry  string
	BaseImageName      string
	BaseImageTag       string
	BaseImageCreds     string
	PythonVersion      string
	PythonPackages     []string
	Commands           []string
	BuildSteps         []BuildStep
	ExistingImageUri   string
	ExistingImageCreds map[string]string
	ForceRebuild       bool
	EnvVars            []string
}

func (o *BuildOpts) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "{")
	fmt.Fprintf(&b, "  \"BaseImageRegistry\": %q,", o.BaseImageRegistry)
	fmt.Fprintf(&b, "  \"BaseImageName\": %q,", o.BaseImageName)
	fmt.Fprintf(&b, "  \"BaseImageTag\": %q,", o.BaseImageTag)
	fmt.Fprintf(&b, "  \"BaseImageCreds\": %q,", o.BaseImageCreds)
	fmt.Fprintf(&b, "  \"PythonVersion\": %q,", o.PythonVersion)
	fmt.Fprintf(&b, "  \"PythonPackages\": %#v,", o.PythonPackages)
	fmt.Fprintf(&b, "  \"Commands\": %#v,", o.Commands)
	fmt.Fprintf(&b, "  \"BuildSteps\": %#v,", o.BuildSteps)
	fmt.Fprintf(&b, "  \"ExistingImageUri\": %q,", o.ExistingImageUri)
	fmt.Fprintf(&b, "  \"ExistingImageCreds\": %#v,", o.ExistingImageCreds)
	fmt.Fprintf(&b, "  \"ForceRebuild\": %v", o.ForceRebuild)
	fmt.Fprintf(&b, "}")
	return b.String()
}

func NewBuilder(config types.AppConfig, registry *common.ImageRegistry, scheduler *scheduler.Scheduler, tailscale *network.Tailscale, containerRepo repository.ContainerRepository) (*Builder, error) {
	return &Builder{
		config:        config,
		scheduler:     scheduler,
		tailscale:     tailscale,
		registry:      registry,
		containerRepo: containerRepo,
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
	commandListHash := hex.EncodeToString(h.Sum(nil))

	bodyToHash := &ImageIdHash{
		BaseImageName:   opts.BaseImageName,
		BaseImageTag:    opts.BaseImageTag,
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
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if opts.ExistingImageUri != "" {
		err := b.handleCustomBaseImage(opts, outputChan)
		if err != nil {
			outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Unknown error occurred.\n"}
			return err
		}
	}

	baseImageId, err := b.GetImageId(&BuildOpts{
		BaseImageRegistry: opts.BaseImageRegistry,
		BaseImageName:     opts.BaseImageName,
		BaseImageTag:      opts.BaseImageTag,
		ExistingImageUri:  opts.ExistingImageUri,
		EnvVars:           opts.EnvVars,
	})
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Unknown error occurred.\n"}
		return err
	}

	sourceImage := fmt.Sprintf("%s/%s:%s", opts.BaseImageRegistry, opts.BaseImageName, opts.BaseImageTag)
	containerId := b.genContainerId()

	// Allow config to override default build container settings
	cpu := defaultBuildContainerCpu
	memory := defaultBuildContainerMemory

	if b.config.ImageService.BuildContainerCpu > 0 {
		cpu = b.config.ImageService.BuildContainerCpu
	}

	if b.config.ImageService.BuildContainerMemory > 0 {
		memory = b.config.ImageService.BuildContainerMemory
	}

	err = b.scheduler.Run(&types.ContainerRequest{
		ContainerId:      containerId,
		Env:              opts.EnvVars,
		Cpu:              cpu,
		Memory:           memory,
		ImageId:          baseImageId,
		SourceImage:      &sourceImage,
		SourceImageCreds: opts.BaseImageCreds,
		WorkspaceId:      authInfo.Workspace.ExternalId,
		Workspace:        *authInfo.Workspace,
		EntryPoint:       []string{"tail", "-f", "/dev/null"},
		PoolSelector:     b.config.ImageService.BuildContainerPoolSelector,
	})
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: err.Error() + "\n"}
		return err
	}

	mctx, mcancel := context.WithCancel(ctx)
	go b.monitorContainerForPreloadErrors(mctx, containerId, outputChan)

	hostname, err := b.containerRepo.GetWorkerAddress(ctx, containerId)
	mcancel()
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Failed to connect to build container.\n"}
		return err
	}

	conn, err := network.ConnectToHost(ctx, hostname, time.Second*30, b.tailscale, b.config.Tailscale)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Failed to connect to build container.\n"}
		return err
	}

	client, err := common.NewRunCClient(hostname, authInfo.Token.Key, conn)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Failed to connect to build container.\n"}
		return err
	}

	go func() {
		<-ctx.Done() // If user cancels the build, kill the container
		client.Kill(containerId)
	}()
	defer client.Kill(containerId) // Kill and remove container after the build completes

	outputChan <- common.OutputMsg{Done: false, Success: false, Msg: "Waiting for build container to start...\n"}
	start := time.Now()
	buildContainerRunning := false
	for {
		r, err := client.Status(containerId)
		if err != nil {
			outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Unknown error occurred.\n"}
			return err
		}

		if r.Running {
			buildContainerRunning = true
			break
		}

		if time.Since(start) > defaultContainerSpinupTimeout {
			outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Timeout: container not running after 180 seconds.\n"}
			return errors.New("timeout: container not running after 180 seconds")
		}

		time.Sleep(100 * time.Millisecond)
	}

	imageId, err := b.GetImageId(opts)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Unknown error occurred.\n"}
		return err
	}

	if !buildContainerRunning {
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Unable to connect to build container.\n"}
		return errors.New("container not running")
	}

	go client.StreamLogs(ctx, containerId, outputChan)

	// Generate the pip install command and prepend it to the commands list
	if len(opts.PythonPackages) > 0 {
		pipInstallCmd := generatePipInstallCommand(opts.PythonPackages, opts.PythonVersion)
		opts.Commands = append([]string{pipInstallCmd}, opts.Commands...)
	}

	log.Printf("container <%v> building with options: %s\n", containerId, opts)
	startTime := time.Now()

	micromambaEnv := strings.Contains(opts.PythonVersion, "micromamba")
	if micromambaEnv {
		client.Exec(containerId, "micromamba config set use_lockfiles False")
	}

	// Detect if python3.x is installed in the container, if not install it
	checkPythonVersionCmd := fmt.Sprintf("%s --version", opts.PythonVersion)
	if resp, err := client.Exec(containerId, checkPythonVersionCmd); (err != nil || !resp.Ok) && !micromambaEnv {
		outputChan <- common.OutputMsg{Done: false, Success: false, Msg: fmt.Sprintf("%s not detected, installing it for you...\n", opts.PythonVersion)}
		installCmd := getPythonInstallCommand(opts.PythonVersion)
		opts.Commands = append([]string{installCmd}, opts.Commands...)
	}

	// Generate the commands to run in the container
	opts.Commands = append(opts.Commands, parseBuildSteps(opts.BuildSteps, opts.PythonVersion)...)

	for _, cmd := range opts.Commands {
		if cmd == "" {
			continue
		}

		if r, err := client.Exec(containerId, cmd); err != nil || !r.Ok {
			log.Printf("failed to execute command for container <%v>: \"%v\" - %v\n", containerId, cmd, err)

			errMsg := ""
			if err != nil {
				errMsg = err.Error() + "\n"
			}

			time.Sleep(defaultImageBuildGracefulShutdownS) // Wait for logs to be passed through
			outputChan <- common.OutputMsg{Done: true, Success: false, Msg: errMsg}
			return err
		}
	}
	log.Printf("container <%v> build took %v\n", containerId, time.Since(startTime))

	err = client.Archive(ctx, containerId, imageId, outputChan)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Archiving: true, Success: false, Msg: err.Error() + "\n"}
		return err
	}

	outputChan <- common.OutputMsg{Done: true, Archiving: true, Success: true, ImageId: imageId}
	return nil
}

func (b *Builder) monitorContainerForPreloadErrors(ctx context.Context, containerId string, outputChan chan common.OutputMsg) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
			if exitCode, err := b.containerRepo.GetContainerExitCode(containerId); err == nil {
				if exitCode != 0 {
					msg, ok := types.WorkerContainerExitCodes[exitCode]
					if !ok {
						msg = types.WorkerContainerExitCodes[types.WorkerContainerExitCodeUnknownError]
					}

					outputChan <- common.OutputMsg{Done: true, Success: false, Msg: fmt.Sprintf("Container exited with error: %s\n", msg)}
				}
				return
			}
		}
	}
}

func (b *Builder) genContainerId() string {
	return fmt.Sprintf("%s%s", types.BuildContainerPrefix, uuid.New().String()[:8])
}

func (b *Builder) extractPackageName(pkg string) string {
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

// handleCustomBaseImage validates the custom base image and parses its details into build options
func (b *Builder) handleCustomBaseImage(opts *BuildOpts, outputChan chan common.OutputMsg) error {
	if outputChan != nil {
		outputChan <- common.OutputMsg{Done: false, Success: false, Msg: fmt.Sprintf("Using custom base image: %s\n", opts.ExistingImageUri)}
	}

	baseImage, err := ExtractImageNameAndTag(opts.ExistingImageUri)
	if err != nil {
		if outputChan != nil {
			outputChan <- common.OutputMsg{Done: true, Success: false, Msg: err.Error() + "\n"}
		}
		return err
	}

	if len(opts.ExistingImageCreds) > 0 && opts.ExistingImageUri != "" {
		token, err := GetRegistryToken(opts)
		if err != nil {
			if outputChan != nil {
				outputChan <- common.OutputMsg{Done: true, Success: false, Msg: err.Error() + "\n"}
			}
			return err
		}
		opts.BaseImageCreds = token
	}

	opts.BaseImageRegistry = baseImage.Registry
	opts.BaseImageName = baseImage.Repo
	opts.BaseImageTag = baseImage.Tag

	// Override any specified python packages with base requirements (to ensure we have what need in the image)
	baseRequirementsSlice := strings.Split(strings.TrimSpace(basePythonRequirements), "\n")

	// Create a map to track package names in baseRequirementsSlice
	baseNames := make(map[string]bool)
	for _, basePkg := range baseRequirementsSlice {
		baseNames[b.extractPackageName(basePkg)] = true
	}

	// Filter out existing packages from opts.PythonPackages
	filteredPythonPackages := make([]string, 0)
	for _, optPkg := range opts.PythonPackages {
		if !baseNames[b.extractPackageName(optPkg)] {
			filteredPythonPackages = append(filteredPythonPackages, optPkg)
		}
	}

	opts.PythonPackages = append(filteredPythonPackages, baseRequirementsSlice...)

	if outputChan != nil {
		outputChan <- common.OutputMsg{Done: false, Success: false, Msg: "Custom base image is valid.\n"}
	}
	return nil
}

// Check if an image already exists in the registry
func (b *Builder) Exists(ctx context.Context, imageId string) bool {
	return b.registry.Exists(ctx, imageId)
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

func getPythonInstallCommand(pythonVersion string) string {
	baseCmd := "apt-get update -q && apt-get install -q -y software-properties-common gcc curl git"
	components := []string{
		"python3-future",
		pythonVersion,
		fmt.Sprintf("%s-distutils", pythonVersion),
		fmt.Sprintf("%s-dev", pythonVersion),
	}

	installCmd := strings.Join(components, " ")
	installPipCmd := fmt.Sprintf("curl -sS https://bootstrap.pypa.io/get-pip.py | %s", pythonVersion)
	postInstallCmd := fmt.Sprintf("rm -f /usr/bin/python && rm -f /usr/bin/python3 && ln -s /usr/bin/%s /usr/bin/python && ln -s /usr/bin/%s /usr/bin/python3 && %s", pythonVersion, pythonVersion, installPipCmd)

	return fmt.Sprintf("%s && add-apt-repository ppa:deadsnakes/ppa && apt-get update && apt-get install -q -y %s && %s", baseCmd, installCmd, postInstallCmd)
}

func generatePipInstallCommand(pythonPackages []string, pythonVersion string) string {
	flagLines, packages := parseFlagLinesAndPackages(pythonPackages)

	command := fmt.Sprintf("%s -m pip install --root-user-action=ignore", pythonVersion)
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

		if step.Type == pipCommandType {
			if pipStart == -1 {
				pipStart = len(commands)
				commands = append(commands, "")
			}
			pipGroup = append(pipGroup, step.Command)
		}

		if step.Type == micromambaCommandType {
			if mambaStart == -1 {
				mambaStart = len(commands)
				commands = append(commands, "")
			}
			mambaGroup = append(mambaGroup, step.Command)
		}

		// Flush any pending pip or mamba groups
		if pipStart != -1 && step.Type != pipCommandType {
			commands[pipStart] = generatePipInstallCommand(pipGroup, pythonVersion)
			pipStart = -1
			pipGroup = nil
		}

		if mambaStart != -1 && step.Type != micromambaCommandType {
			commands[mambaStart] = generateMicromambaInstallCommand(mambaGroup)
			mambaStart = -1
			mambaGroup = nil
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
