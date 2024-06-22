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

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"

	"github.com/google/uuid"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/pkg/errors"
)

const (
	defaultBuildContainerCpu      int64         = 1000
	defaultBuildContainerMemory   int64         = 1024
	defaultContainerSpinupTimeout time.Duration = 180 * time.Second
)

type Builder struct {
	config        types.AppConfig
	scheduler     *scheduler.Scheduler
	registry      *common.ImageRegistry
	containerRepo repository.ContainerRepository
	tailscale     *network.Tailscale
}

type BuildOpts struct {
	BaseImageRegistry  string
	BaseImageName      string
	BaseImageTag       string
	PythonVersion      string
	PythonPackages     []string
	Commands           []string
	ExistingImageUri   string
	ExistingImageCreds *string
	ForceRebuild       bool
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
	SourceRegistry string
	ImageName      string
	ImageTag       string
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
		ContainerId:  containerId,
		Env:          []string{},
		Cpu:          cpu,
		Memory:       memory,
		ImageId:      baseImageId,
		SourceImage:  &sourceImage,
		WorkspaceId:  authInfo.Workspace.ExternalId,
		EntryPoint:   []string{"tail", "-f", "/dev/null"},
		PoolSelector: b.config.ImageService.BuildContainerPoolSelector,
	})
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: err.Error() + "\n"}
		return err
	}

	hostname, err := b.containerRepo.GetWorkerAddress(containerId)
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
		pipInstallCmd := b.generatePipInstallCommand(opts)
		opts.Commands = append([]string{pipInstallCmd}, opts.Commands...)
	}

	log.Printf("container <%v> building with options: %+v\n", containerId, opts)
	startTime := time.Now()

	// Detect if python3.x is installed in the container, if not install it
	checkPythonVersionCmd := fmt.Sprintf("%s --version", opts.PythonVersion)
	if resp, err := client.Exec(containerId, checkPythonVersionCmd); err != nil || !resp.Ok {
		outputChan <- common.OutputMsg{Done: false, Success: false, Msg: fmt.Sprintf("%s not detected, installing it for you...\n", opts.PythonVersion)}
		installCmd := b.getPythonInstallCommand(opts.PythonVersion)
		opts.Commands = append([]string{installCmd}, opts.Commands...)
	}

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

			outputChan <- common.OutputMsg{Done: true, Success: false, Msg: errMsg}
			return err
		}
	}
	log.Printf("container <%v> build took %v\n", containerId, time.Since(startTime))

	outputChan <- common.OutputMsg{Done: false, Success: false, Msg: "\nSaving image, this may take a few minutes...\n"}
	err = client.Archive(ctx, containerId, imageId, outputChan)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: err.Error() + "\n"}
		return err
	}

	outputChan <- common.OutputMsg{Done: true, Success: true, ImageId: imageId}
	return nil
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

func (b *Builder) handleCustomBaseImage(opts *BuildOpts, outputChan chan common.OutputMsg) error {
	if outputChan != nil {
		outputChan <- common.OutputMsg{Done: false, Success: false, Msg: fmt.Sprintf("Using custom base image: %s\n", opts.ExistingImageUri)}
	}

	baseImage, err := b.extractImageNameAndTag(opts.ExistingImageUri)
	if err != nil {
		if outputChan != nil {
			outputChan <- common.OutputMsg{Done: true, Success: false, Msg: err.Error() + "\n"}
		}
		return err
	}

	opts.BaseImageRegistry = baseImage.SourceRegistry
	opts.BaseImageName = baseImage.ImageName
	opts.BaseImageTag = baseImage.ImageTag

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

// Extracts the image name and tag from a given Docker image URI.
// Returns an error if the URI is invalid.
func (b *Builder) extractImageNameAndTag(imageURI string) (BaseImage, error) {
	re := regexp.MustCompile(`^(([^/]+/[^/]+)/)?([^:]+):?(.*)$`)
	matches := re.FindStringSubmatch(imageURI)

	if matches == nil {
		return BaseImage{}, errors.New("invalid image URI format")
	}

	// Use default source registry if not specified
	sourceRegistry := "docker.io"
	if matches[2] != "" {
		sourceRegistry = matches[2]
	}

	imageName := matches[3]
	imageTag := "latest"

	if matches[4] != "" {
		imageTag = matches[4]
	}

	return BaseImage{
		SourceRegistry: sourceRegistry,
		ImageName:      imageName,
		ImageTag:       imageTag,
	}, nil
}

func (b *Builder) getPythonInstallCommand(pythonVersion string) string {
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

func (b *Builder) generatePipInstallCommand(opts *BuildOpts) string {
	// Escape each package name individually
	escapedPackages := make([]string, len(opts.PythonPackages))
	for i, pkg := range opts.PythonPackages {
		escapedPackages[i] = fmt.Sprintf("%q", pkg)
	}

	packages := strings.Join(escapedPackages, " ")
	return fmt.Sprintf("%s -m pip install --root-user-action=ignore %s", opts.PythonVersion, packages)
}
