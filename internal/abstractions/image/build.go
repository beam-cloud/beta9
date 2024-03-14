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

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/network"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/types"

	"github.com/google/uuid"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/pkg/errors"
)

const (
	buildContainerPrefix          string        = "build-"
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
	requirementsFilename string = "requirements.txt"
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
		err := b.handleCustomBaseImage(ctx, opts, outputChan)
		if err != nil {
			return err
		}
	}

	baseImageId, err := b.GetImageId(&BuildOpts{
		BaseImageRegistry: opts.BaseImageRegistry,
		BaseImageName:     opts.BaseImageName,
		BaseImageTag:      opts.BaseImageTag})
	if err != nil {
		return err
	}

	sourceImage := fmt.Sprintf("%s/%s:%s", opts.BaseImageRegistry, opts.BaseImageName, opts.BaseImageTag)
	containerId := b.genContainerId()

	err = b.scheduler.Run(&types.ContainerRequest{
		ContainerId: containerId,
		Env:         []string{},
		Cpu:         defaultBuildContainerCpu,
		Gpu:         "T4",
		Memory:      defaultBuildContainerMemory,
		ImageId:     baseImageId,
		SourceImage: &sourceImage,
		WorkspaceId: authInfo.Workspace.ExternalId,
		EntryPoint:  []string{"tail", "-f", "/dev/null"},
	})
	if err != nil {
		return err
	}

	hostname, err := b.containerRepo.GetContainerWorkerHostname(containerId)
	if err != nil {
		return err
	}

	conn, err := network.ConnectToHost(ctx, hostname, time.Second*30, b.tailscale, b.config.Tailscale)
	if err != nil {
		return err
	}

	// TODO: replace placeholder service token
	client, err := common.NewRunCClient(hostname, "", conn)
	if err != nil {
		return err
	}

	defer client.Kill(containerId) // Kill and remove container after the build completes

	outputChan <- common.OutputMsg{Done: false, Success: false, Msg: "Waiting for build container to start..."}
	start := time.Now()
	buildContainerRunning := false
	for {
		r, err := client.Status(containerId)
		if err != nil {
			return err
		}

		if r.Running {
			buildContainerRunning = true
			break
		}

		if time.Since(start) > defaultContainerSpinupTimeout {
			return errors.New("timeout: container not running after 180 seconds")
		}

		time.Sleep(100 * time.Millisecond)
	}

	imageId, err := b.GetImageId(opts)
	if err != nil {
		return err
	}

	if !buildContainerRunning {
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Unable to connect to build container."}
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
	if _, err := client.Exec(containerId, checkPythonVersionCmd); err != nil {
		outputChan <- common.OutputMsg{Done: false, Success: false, Msg: fmt.Sprintf("%s not detected, installing it for you...", opts.PythonVersion)}
		installCmd := b.getPythonInstallCommand(opts.PythonVersion)
		opts.Commands = append([]string{installCmd}, opts.Commands...)
	}

	for _, cmd := range opts.Commands {
		if cmd == "" {
			continue
		}

		if r, err := client.Exec(containerId, cmd); !r.Ok || err != nil {
			log.Printf("failed to execute command for container <%v>: \"%v\" - %v\n", containerId, cmd, err)

			errMsg := ""
			if err != nil {
				errMsg = err.Error()
			}

			outputChan <- common.OutputMsg{Done: true, Success: false, Msg: errMsg}
			return err
		}
	}
	log.Printf("container <%v> build took %v\n", containerId, time.Since(startTime))

	err = client.Archive(containerId, imageId)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: err.Error()}
		return err
	}

	outputChan <- common.OutputMsg{Done: true, Success: true, ImageId: imageId}
	return nil
}

func (b *Builder) genContainerId() string {
	return fmt.Sprintf("%s%s", buildContainerPrefix, uuid.New().String()[:8])
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

func (b *Builder) handleCustomBaseImage(ctx context.Context, opts *BuildOpts, outputChan chan common.OutputMsg) error {
	outputChan <- common.OutputMsg{Done: false, Success: false, Msg: fmt.Sprintf("Downloading custom base image: %s", opts.ExistingImageUri)}

	baseImage, err := b.extractImageNameAndTag(opts.ExistingImageUri)
	if err != nil {
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: err.Error()}
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

	outputChan <- common.OutputMsg{Done: false, Success: false, Msg: "Custom base image downloaded."}
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
	baseCmd := "apt-get update -q && apt-get install -q -y software-properties-common curl git"
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
	packages := strings.Join(opts.PythonPackages, " ")
	return fmt.Sprintf("%s -m pip install --root-user-action=ignore %s", opts.PythonVersion, packages)
}
