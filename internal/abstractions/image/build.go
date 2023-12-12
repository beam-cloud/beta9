package image

import (
	"context"
	"crypto/sha1"
	_ "embed"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/types"
	"github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"

	"github.com/google/uuid"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/opencontainers/umoci"
	"github.com/opencontainers/umoci/oci/cas/dir"
	"github.com/opencontainers/umoci/oci/casext"
	"github.com/opencontainers/umoci/oci/layer"
	"github.com/pkg/errors"
)

const (
	buildContainerPrefix        string = "build-"
	defaultBuildContainerCpu    int64  = 1000
	defaultBuildContainerMemory int64  = 1024
)

type Builder struct {
	baseImageCachePath string
	userImageBuildPath string
	scheduler          *scheduler.Scheduler
	cacheLock          sync.Mutex
	registry           *common.ImageRegistry
}

type BuildOpts struct {
	BaseImageName      string
	BaseImageTag       string
	UserImageTag       string
	PythonVersion      string
	PythonPackages     []string
	Commands           []string
	ExistingImageUri   string
	ExistingImageCreds *string
	ForceRebuild       bool
}

type BaseImageCacheOpt struct {
	SourceRegistry string
	ImageName      string
	ImageTag       string
	Copies         int
}

func NewBuilder(scheduler *scheduler.Scheduler) (*Builder, error) {
	userImageBuildPath := "test" // filepath.Join(ImageServiceConfig.UserImageBuildPath)
	baseImageCachePath := "test" // filepath.Join(ImageServiceConfig.BaseImageCachePath)

	os.MkdirAll(baseImageCachePath, os.ModePerm)
	os.MkdirAll(userImageBuildPath, os.ModePerm)

	storeName := common.Secrets().GetWithDefault("BEAM_IMAGESERVICE_IMAGE_REGISTRY_STORE", "s3")
	registry, err := common.NewImageRegistry(storeName)
	if err != nil {
		return nil, err
	}

	return &Builder{
		baseImageCachePath: baseImageCachePath,
		userImageBuildPath: userImageBuildPath,
		scheduler:          scheduler,
		cacheLock:          sync.Mutex{},
		registry:           registry,
	}, nil
}

var (
	defaultWorkingDirectory      string        = "/workspace"
	requiredContainerDirectories []string      = []string{"/workspace", "/volumes", "/snapshot", "/outputs", "/packages"}
	requirementsFilename         string        = "requirements.txt"
	monitorImageCacheInterval    time.Duration = time.Duration(10) * time.Second
	//go:embed base_requirements.txt
	basePythonRequirements string
)

type ImageTagHash struct {
	BaseImageName   string
	BaseImageTag    string
	UserImageTag    string
	PythonVersion   string
	PythonPackages  []string
	ExitingImageUri string
	CommandListHash string
}

func (b *Builder) GetImageTag(opts *BuildOpts) (string, error) {
	h := sha1.New()
	h.Write([]byte(strings.Join(opts.Commands, "-")))
	commandListHash := hex.EncodeToString(h.Sum(nil))

	bodyToHash := &ImageTagHash{
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

type CustomBaseImage struct {
	SourceRegistry string
	ImageName      string
	ImageTag       string
}

// Extracts the image name and tag from a given Docker image URI.
// Returns an error if the URI is invalid.
func (b *Builder) extractImageNameAndTag(imageURI string) (CustomBaseImage, error) {
	re := regexp.MustCompile(`^(([^/]+/[^/]+)/)?([^:]+):?(.*)$`)
	matches := re.FindStringSubmatch(imageURI)

	if matches == nil {
		return CustomBaseImage{}, errors.New("invalid image URI format")
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

	return CustomBaseImage{
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

// Build user image
func (b *Builder) Build(ctx context.Context, opts *BuildOpts, outputChan chan common.OutputMsg) error {
	if opts.ExistingImageUri != "" {
		err := b.handleCustomBaseImage(ctx, opts, outputChan)
		if err != nil {
			return err
		}
	}

	log.Printf("build opts: %+v\n", opts)

	// Step one - run an interactive container for the image build
	err := b.scheduler.Run(&types.ContainerRequest{
		ContainerId: b.genContainerId(),
		Env:         []string{},
		Cpu:         defaultBuildContainerCpu,
		Memory:      defaultBuildContainerMemory,
		ImageName:   opts.BaseImageName,
		ImageTag:    opts.BaseImageTag,
		Mode:        types.ContainerModeInteractive,
	})
	if err != nil {
		return err
	}

	// Step two - connect to the worker that is running the container...?
	// Poll for container to be up, and get container it is on

	// imgTag, err := b.GetImageTag(opts)
	// if err != nil {
	// 	return err
	// }

	// defer func() {
	// 	err := b.stopBuildContainer(ctx, containerId)
	// 	if err != nil {
	// 		log.Printf("unable to delete container<%s>: %v\n", containerId, err)
	// 	}

	// 	time.Sleep(time.Second * 1) // Give container a second to shut down
	// 	overlay.Cleanup()
	// }()

	// log.Printf("container <%v> start took %v", containerId, time.Since(startTime))

	// bundlePath := overlay.TopLayerPath()
	// err = b.generateRequirementsFile(bundlePath, opts)
	// if err != nil {
	// 	log.Printf("failed to generate python requirements for container <%v>: %v", containerId, err)
	// 	outputChan <- common.OutputMsg{Done: true, Success: false, Msg: err.Error()}
	// 	return err
	// }

	// pipInstallCmd := fmt.Sprintf("%s -m pip install -r %s", opts.PythonVersion, filepath.Join("/", requirementsFilename))
	// opts.Commands = append(opts.Commands, pipInstallCmd)

	// log.Printf("container <%v> building with options: %+v", containerId, opts)
	// startTime = time.Now()

	// // Detect if python3.x is installed in the container, if not install it
	// checkPythonVersionCmd := fmt.Sprintf("%s --version", opts.PythonVersion)
	// if err := b.execute(ctx, containerId, checkPythonVersionCmd, opts, nil); err != nil {
	// 	outputChan <- common.OutputMsg{Done: false, Success: false, Msg: fmt.Sprintf("%s not detected, installing it for you...", opts.PythonVersion)}
	// 	installCmd := b.getPythonInstallCommand(opts.PythonVersion)
	// 	opts.Commands = append([]string{installCmd}, opts.Commands...)
	// }

	// for _, cmd := range opts.Commands {
	// 	if cmd == "" {
	// 		continue
	// 	}

	// 	if err := b.execute(ctx, containerId, cmd, opts, outputChan); err != nil {
	// 		log.Printf("failed to execute command for container <%v>: \"%v\" - %v", containerId, cmd, err)
	// 		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: err.Error()}
	// 		return err
	// 	}
	// }
	// log.Printf("container <%v> build took %v", containerId, time.Since(startTime))

	// // Archive and push image to registry
	// err = b.archiveImage(ctx, bundlePath, containerId, opts, outputChan)
	// if err != nil {
	// 	return err
	// }

	// outputChan <- common.OutputMsg{Done: true, Success: true, Msg: "Build complete."}
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

	// dest := fmt.Sprintf("oci:%s:%s", baseImage.ImageName, baseImage.ImageTag)

	// creds := "" //fmt.Sprintf("%s:%s", common.Secrets().Get("DOCKERHUB_USERNAME"), common.Secrets().Get("DOCKERHUB_PASSWORD"))
	// if opts.ExistingImageCreds != nil {
	// 	creds = *opts.ExistingImageCreds
	// }

	// err = b.puller.Pull(ctx, fmt.Sprintf("docker://%s", opts.ExistingImageUri), dest, &creds)
	// if err != nil {
	// 	outputChan <- common.OutputMsg{Done: true, Success: false, Msg: err.Error()}
	// 	return err
	// }

	// err = b.unpackIntoCache(cacheDir, baseImage.ImageName, baseImage.ImageTag)
	// if err != nil {
	// 	log.Printf("unable to unpack image: %v", err)
	// 	return err
	// }

	opts.BaseImageName = baseImage.ImageName
	opts.BaseImageTag = baseImage.ImageTag

	// TODO: ensure required dependencies are always mounted at runtime
	// in worker so we don't have to override the packages here

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

// Clean up a build after completion (either failed or successful)
func (b *Builder) Clean(opts *BuildOpts) error {
	log.Printf("cleaning up: %+v", opts)
	imagePath := fmt.Sprintf("%s/%s", b.userImageBuildPath, opts.UserImageTag)
	return os.RemoveAll(imagePath)
}

// Check if an image already exists in the registry
func (b *Builder) Exists(ctx context.Context, imageTag string) bool {
	return b.registry.Exists(ctx, imageTag)
}

// Generate a python requirements file to install into the image
func (b *Builder) generateRequirementsFile(bundlePath string, opts *BuildOpts) error {
	requirementsFileBody := strings.Join(opts.PythonPackages, "\n")
	requirementsFilePath := filepath.Join(bundlePath, "rootfs", requirementsFilename)

	f, err := os.Create(requirementsFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteString(requirementsFileBody)
	if err != nil {
		return err
	}

	return nil
}

// Generate and upload archived version of the image for distribution
func (b *Builder) archiveImage(ctx context.Context, bundlePath string, containerId string, opts *BuildOpts, outputChan chan common.OutputMsg) error {
	startTime := time.Now()

	archiveName := fmt.Sprintf("%s.%s", opts.UserImageTag, b.registry.ImageFileExtension)
	archivePath := filepath.Join(filepath.Dir(bundlePath), archiveName)

	var err error = nil
	archiveStore := common.Secrets().GetWithDefault("BEAM_IMAGESERVICE_IMAGE_REGISTRY_STORE", "s3")
	switch archiveStore {
	case "s3":
		err = clip.CreateAndUploadArchive(clip.CreateOptions{
			InputPath:  bundlePath,
			OutputPath: archivePath,
		}, &clipCommon.S3StorageInfo{
			Bucket: common.Secrets().Get("BEAM_IMAGESERVICE_IMAGE_REGISTRY_S3_BUCKET"),
			Region: common.Secrets().Get("BEAM_IMAGESERVICE_IMAGE_REGISTRY_S3_REGION"),
			Key:    fmt.Sprintf("%s.clip", opts.UserImageTag),
		})
	case "local":
		err = clip.CreateArchive(clip.CreateOptions{
			InputPath:  bundlePath,
			OutputPath: archivePath,
		})
	}

	if err != nil {
		log.Printf("unable to create archive: %v\n", err)
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Unable to archive image."}
		return err
	}
	log.Printf("container <%v> archive took %v", containerId, time.Since(startTime))

	// Push the archive to a registry
	startTime = time.Now()
	err = b.registry.Push(ctx, archivePath, opts.UserImageTag)
	if err != nil {
		log.Printf("failed to push image for container <%v>: %v", containerId, err)
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Unable to push image."}
		return err
	}

	log.Printf("container <%v> push took %v", containerId, time.Since(startTime))
	log.Printf("container <%v> build completed successfully", containerId)
	return nil
}

// Kill and remove a runc container
// func (b *Builder) stopBuildContainer(ctx context.Context, containerId string) error {
// 	log.Printf("container <%v> being terminated and deleted", containerId)

// 	err := b.runcHandle.Kill(ctx, containerId, int(syscall.SIGTERM), &runc.KillOpts{All: true})
// 	if err != nil {
// 		return err
// 	}

// 	return b.runcHandle.Delete(ctx, containerId, &runc.DeleteOpts{
// 		Force: true,
// 	})
// }

func (b *Builder) unpackIntoCache(cacheDir string, imageName string, imageTag string) error {
	log.Printf("unpacking: %v:%v", imageName, imageTag)
	bundleId := b.uuid()
	err := b.unpack(imageName, imageTag, cacheDir, bundleId)
	return err
}

func (b *Builder) getCachedImagePath(cacheDir string) (string, error) {
	directories, _ := os.ReadDir(cacheDir)

	for _, dir := range directories {
		if strings.HasPrefix("_", dir.Name()) {
			continue
		}

		selectedImagePath := filepath.Join(cacheDir, dir.Name())
		return selectedImagePath, nil
	}

	return "", errors.New("image not found")
}

// Copy cached image to a location where it can be modified by an external user
// func (b *Builder) createContainerBundle(containerId, baseImageName, baseImageTag, userImageTag string) (*common.ContainerOverlay, error) {
// 	b.cacheLock.Lock()
// 	defer b.cacheLock.Unlock()

// 	cacheDir := b.getBaseImageCacheDir(baseImageName, baseImageTag)
// 	imagePath := filepath.Join(b.userImageBuildPath, userImageTag)

// 	selectedImagePath, err := b.getCachedImagePath(cacheDir)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Remove any old image overlay with the same path
// 	err = os.RemoveAll(imagePath)
// 	if err != nil {
// 		return nil, err
// 	}

// 	err = os.MkdirAll(imagePath, os.ModePerm)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Setup a new overlayfs to build the image in
// 	overlay := common.NewContainerOverlay(containerId, selectedImagePath, imagePath, selectedImagePath)
// 	err = overlay.Setup()
// 	if err != nil {
// 		return nil, err
// 	}

// 	tempConfig := b.baseConfigSpec
// 	tempConfig.Hooks.Prestart = nil
// 	tempConfig.Process.Terminal = false
// 	tempConfig.Process.Args = []string{"tail", "-f", "/dev/null"}
// 	tempConfig.Root.Readonly = false

// 	file, err := json.MarshalIndent(tempConfig, "", " ")
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Preserve initial config file in the bundle
// 	os.Rename(filepath.Join(overlay.TopLayerPath(), "config.json"), filepath.Join(overlay.TopLayerPath(), "initial_config.json"))

// 	configPath := filepath.Join(overlay.TopLayerPath(), "config.json")
// 	err = os.WriteFile(configPath, file, 0644)
// 	return overlay, err
// }

// Unpack an image from OCI format -> rootfs bundle
func (b *Builder) unpack(baseImageName string, baseImageTag string, cacheDir string, bundleId string) error {
	var unpackOptions layer.UnpackOptions
	var meta umoci.Meta
	meta.Version = umoci.MetaVersion

	unpackOptions.KeepDirlinks = true
	unpackOptions.MapOptions = meta.MapOptions

	// Get a reference to the CAS.
	baseImagePath := fmt.Sprintf("%s/%s", b.baseImageCachePath, baseImageName)
	engine, err := dir.Open(baseImagePath)
	if err != nil {
		return errors.Wrap(err, "open CAS")
	}
	defer engine.Close()

	engineExt := casext.NewEngine(engine)
	defer engineExt.Close()

	tmpBundlePath := filepath.Join(cacheDir, "_"+bundleId)
	bundlePath := filepath.Join(cacheDir, bundleId)
	err = umoci.Unpack(engineExt, baseImageTag, tmpBundlePath, unpackOptions)
	if err == nil {
		for _, dir := range requiredContainerDirectories {
			fullPath := filepath.Join(tmpBundlePath, "rootfs", dir)
			err := os.MkdirAll(fullPath, 0755)
			if err != nil {
				errors.Wrap(err, fmt.Sprintf("creating /%s directory", dir))
				return err
			}
		}

		return os.Rename(tmpBundlePath, bundlePath)
	}

	return err
}

// Start a new container using the selected base image
// func (b *Builder) startBuildContainer(ctx context.Context, opts *BuildOpts) (*common.ContainerOverlay, string, error) {
// 	containerId := b.uuid()

// 	overlay, err := b.createContainerBundle(containerId, opts.BaseImageName, opts.BaseImageTag, opts.UserImageTag)
// 	if err != nil {
// 		return nil, containerId, err
// 	}

// 	status, err := b.runcHandle.Run(ctx, containerId, overlay.TopLayerPath(), &runc.CreateOpts{
// 		Detach: true,
// 	})

// 	if err != nil {
// 		overlay.Cleanup()
// 		return nil, containerId, err
// 	}

// 	if status != 0 {
// 		overlay.Cleanup()
// 		return nil, "", fmt.Errorf("unable to start container: %d", status)
// 	}

// 	return overlay, containerId, err
// }

// Generate a unique identifier
func (b *Builder) uuid() string {
	splitUUID := strings.Split(uuid.New().String(), "-")
	return splitUUID[len(splitUUID)-1]
}
