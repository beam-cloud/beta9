package image

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type RuncClient interface {
	Exec(containerId string, command string, env []string) (*pb.RunCExecResponse, error)
	Archive(ctx context.Context, containerId string, imageId string, outputChan chan common.OutputMsg) error
	Kill(containerId string) (*pb.RunCKillResponse, error)
	Status(containerId string) (*pb.RunCStatusResponse, error)
	StreamLogs(ctx context.Context, containerId string, outputChan chan common.OutputMsg) error
}

// PythonStandaloneTemplate is used to render the standalone python install script
type PythonStandaloneTemplate struct {
	PythonVersion string

	// Architecture, OS, and Vendor are determined at runtime
	Architecture string
	OS           string
	Vendor       string
}

type BuildStep struct {
	Command string
	Type    string
}

type Build struct {
	ctx         context.Context
	config      types.AppConfig
	opts        *BuildOpts
	success     *atomic.Bool
	containerID string
	imageID     string
	outputChan  chan common.OutputMsg
	authInfo    *auth.AuthInfo
	runcClient  RuncClient
	commands    []string
	micromamba  bool
}

func NewBuild(ctx context.Context, opts *BuildOpts, outputChan chan common.OutputMsg, config types.AppConfig) (*Build, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if opts.IgnorePython && len(opts.PythonPackages) == 0 {
		opts.PythonVersion = ""
	}

	return &Build{
		ctx:         ctx,
		config:      config,
		opts:        opts,
		success:     &atomic.Bool{},
		containerID: genContainerId(),
		outputChan:  outputChan,
		authInfo:    authInfo,
		micromamba:  strings.Contains(opts.PythonVersion, "micromamba"),
	}, nil
}

func (b *Build) initializeBuildConfiguration() error {
	if err := b.opts.initializeBuildConfiguration(b.config, b.outputChan); err != nil {
		return err
	}
	return nil
}

// prepareCommands prepares a slice of strings representing commands that will be executed in the build container.
// Commands come from three sources:
//   - Python version requirement that needs to be installed
//   - Python packages and shell commands that are passed as parameters to the image in the sdk
//   - Build steps that are chained to the image in the sdk
func (b *Build) prepareCommands() error {
	if err := b.setupPythonEnv(); err != nil {
		return err
	}

	isVirtualEnv := b.checkForVirtualEnv()
	if isVirtualEnv && !b.micromamba {
		b.commands = append(b.commands, updatePyvenvCmd(b.opts.PythonVersion))
	}

	// Add pip install command from image's python package list
	if len(b.opts.PythonPackages) > 0 && b.opts.PythonVersion != "" {
		pipInstallCmd := generatePipInstallCommand(b.opts.PythonPackages, b.opts.PythonVersion, isVirtualEnv)
		b.commands = append(b.commands, pipInstallCmd)
	}

	// Add shell commands from image's command list
	b.commands = append(b.commands, b.opts.Commands...)

	// Add any additional build steps that were chained to the image
	b.commands = append(b.commands, parseBuildSteps(b.opts.BuildSteps, b.opts.PythonVersion, isVirtualEnv)...)

	return nil
}

func (b *Build) checkForVirtualEnv() bool {
	if b.opts.PythonVersion == "" {
		return false
	}

	// Conda type environments do not follow the same prefix != base_prefix convention
	if b.micromamba {
		return true
	}

	// Check whether the python version belongs to a virtual environment
	checkVenvCmd := fmt.Sprintf(`%s -c "import sys; exit(0 if sys.prefix != sys.base_prefix else 1)"`, b.opts.PythonVersion)
	if resp, err := b.runcClient.Exec(b.containerID, checkVenvCmd, buildEnv); err == nil && resp.Ok {
		return true
	}

	return false
}

// setupPythonEnv ensures that if a python version is requested, it is installed.
func (b *Build) setupPythonEnv() error {
	if b.micromamba {
		b.commands = append(b.commands, "micromamba config set use_lockfiles False")
		return nil
	}

	if b.opts.IgnorePython && len(b.opts.PythonPackages) == 0 {
		b.opts.PythonVersion = ""
		return nil
	}

	if b.opts.PythonVersion == types.Python3.String() {
		// The provided python version is python3 (default). If "python3 --version" is successful then there is no need to install
		// the current default python version.
		checkPythonVersionCmd := fmt.Sprintf("%s --version", b.opts.PythonVersion)
		if resp, err := b.runcClient.Exec(b.containerID, checkPythonVersionCmd, buildEnv); err == nil && resp.Ok {
			return nil
		}

		// Since no default python3 was found, specify the python version to be the default provided by the image config
		// and generate the install command for it.
		b.opts.PythonVersion = b.config.ImageService.PythonVersion
		installCmd, err := getPythonInstallCommand(b.config.ImageService.Runner.PythonStandalone, b.opts.PythonVersion)
		if err != nil {
			b.log(true, err.Error()+"\n")
			return err
		}
		b.commands = append(b.commands, installCmd)
		return nil
	}

	// Detect if python3.x is installed in the container, if not install it
	checkPythonVersionCmd := fmt.Sprintf("%s --version", b.opts.PythonVersion)
	if resp, err := b.runcClient.Exec(b.containerID, checkPythonVersionCmd, buildEnv); err != nil || !resp.Ok {
		// Warn the user if they are overriding an existing python3 environment
		checkPythonVersionCmd = fmt.Sprintf("%s --version", types.Python3.String())
		if resp, err := b.runcClient.Exec(b.containerID, checkPythonVersionCmd, buildEnv); err == nil && resp.Ok {
			b.logWarning(fmt.Sprintf("requested python version (%s) was not detected, but an existing python3 environment was detected. The requested python version will be installed, replacing the existing python environment.\n", b.opts.PythonVersion))
		}

		b.log(false, fmt.Sprintf("%s not detected, installing it for you...\n", b.opts.PythonVersion))
		installCmd, err := getPythonInstallCommand(b.config.ImageService.Runner.PythonStandalone, b.opts.PythonVersion)
		if err != nil {
			b.log(true, err.Error()+"\n")
			return err
		}

		b.commands = append(b.commands, installCmd)
	}

	return nil
}

func (b *Build) executeCommands() error {
	log.Info().Str("container_id", b.containerID).Interface("options", b.opts).Msg("container building")
	startTime := time.Now()

	for _, cmd := range b.commands {
		if cmd == "" {
			continue
		}

		if r, err := b.runcClient.Exec(b.containerID, cmd, buildEnv); err != nil || !r.Ok {
			log.Error().Str("container_id", b.containerID).Str("command", cmd).Err(err).Msg("failed to execute command for container")

			errMsg := ""
			if err != nil {
				errMsg = err.Error() + "\n"
			}

			time.Sleep(defaultImageBuildGracefulShutdownS) // Wait for logs to be passed through
			b.log(true, errMsg)
			return err
		}
	}
	log.Info().Str("container_id", b.containerID).Dur("duration", time.Since(startTime)).Msg("container build took")
	return nil
}

func (b *Build) archive() error {
	if err := b.runcClient.Archive(b.ctx, b.containerID, b.imageID, b.outputChan); err != nil {
		b.log(true, err.Error()+"\n")
		return err
	}
	return nil
}

func (b *Build) setSuccess(success bool) {
	b.success.Store(success)
}

func (b *Build) log(Done bool, Msg string) {
	b.outputChan <- common.OutputMsg{Done: Done, Success: b.success.Load(), Msg: Msg}
}

func (b *Build) logWarning(Msg string) {
	b.outputChan <- common.OutputMsg{Done: false, Success: b.success.Load(), Warning: true, Msg: Msg}
}

func (b *Build) logWithImageAndPythonVersion(Done bool, Msg string) {
	b.outputChan <- common.OutputMsg{Done: Done, Success: b.success.Load(), Archiving: true, Msg: Msg, ImageId: b.imageID, PythonVersion: b.opts.PythonVersion}
}

func (b *Build) connectToHost(hostname string, tailscale *network.Tailscale) error {
	conn, err := network.ConnectToHost(b.ctx, hostname, time.Second*30, tailscale, b.config.Tailscale)
	if err != nil {
		b.log(true, "Failed to connect to build container.\n")
		return err
	}

	client, err := common.NewRunCClient(hostname, b.authInfo.Token.Key, conn)
	if err != nil {
		b.log(true, "Failed to connect to build container.\n")
		return err
	}

	b.runcClient = client
	return nil
}

func (b *Build) streamLogs() {
	go b.runcClient.StreamLogs(b.ctx, b.containerID, b.outputChan)
}

func (b *Build) killContainer() error {
	if b.runcClient == nil {
		return nil
	}
	_, err := b.runcClient.Kill(b.containerID)
	return err
}

func (b *Build) getContainerStatus() (*pb.RunCStatusResponse, error) {
	if b.runcClient == nil {
		return nil, nil
	}
	return b.runcClient.Status(b.containerID)
}

// generateContainerRequest generates a container request for the build container
func (b *Build) generateContainerRequest() (*types.ContainerRequest, error) {
	baseImageID, err := getImageID(&BuildOpts{
		BaseImageRegistry: b.opts.BaseImageRegistry,
		BaseImageName:     b.opts.BaseImageName,
		BaseImageTag:      b.opts.BaseImageTag,
		BaseImageDigest:   b.opts.BaseImageDigest,
		ExistingImageUri:  b.opts.ExistingImageUri,
		EnvVars:           b.opts.EnvVars,
		Dockerfile:        b.opts.Dockerfile,
		BuildCtxObject:    b.opts.BuildCtxObject,
	})
	if err != nil {
		return nil, err
	}

	sourceImage := getSourceImage(b.opts)

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
			SourceImageCreds: b.opts.BaseImageCreds,
			Dockerfile:       &b.opts.Dockerfile,
			BuildCtxObject:   &b.opts.BuildCtxObject,
			BuildSecrets:     b.opts.BuildSecrets,
		},
		ContainerId: b.containerID,
		Env:         b.opts.EnvVars,
		Cpu:         cpu,
		Memory:      memory,
		ImageId:     baseImageID,
		WorkspaceId: b.authInfo.Workspace.ExternalId,
		Workspace:   *b.authInfo.Workspace,
		EntryPoint:  []string{"tail", "-f", "/dev/null"},
	}

	if b.opts.Gpu != "" {
		containerRequest.GpuRequest = []string{b.opts.Gpu}
		containerRequest.GpuCount = 1
	} else {
		containerRequest.PoolSelector = b.config.ImageService.BuildContainerPoolSelector
	}

	return containerRequest, nil
}

func genContainerId() string {
	return fmt.Sprintf("%s%s", types.BuildContainerPrefix, uuid.New().String()[:8])
}

func generatePipInstallCommand(pythonPackages []string, pythonVersion string, virtualEnv bool) string {
	flagLines, packages := parseFlagLinesAndPackages(pythonPackages)

	command := "uv-b9 pip install"
	if !virtualEnv {
		command += " --system"
	}

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
func parseBuildSteps(buildSteps []BuildStep, pythonVersion string, virtualEnv bool) []string {
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
			pipStart, pipGroup = flushPipCommand(commands, pipStart, pipGroup, pythonVersion, virtualEnv)
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
				pipStart, pipGroup = flushPipCommand(commands, pipStart, pipGroup, pythonVersion, virtualEnv)
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
		commands[pipStart] = generatePipInstallCommand(pipGroup, pythonVersion, virtualEnv)
	}

	return commands
}

func flushMambaCommand(commands []string, mambaStart int, mambaGroup []string) (int, []string) {
	commands[mambaStart] = generateMicromambaInstallCommand(mambaGroup)
	return -1, nil
}

func flushPipCommand(commands []string, pipStart int, pipGroup []string, pythonVersion string, virtualEnv bool) (int, []string) {
	commands[pipStart] = generatePipInstallCommand(pipGroup, pythonVersion, virtualEnv)
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

func getPythonInstallCommand(config types.PythonStandaloneConfig, pythonVersion string) (string, error) {
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

// updatePyvenvCmd modifies the pyvenv.cfg to include system site packages so that the mounted beam and beta9
// packages are available.
func updatePyvenvCmd(pythonVersion string) string {
	findVenvCmd := fmt.Sprintf(`%s -c "import sys; import os; venv_dir = os.path.dirname(os.path.dirname(sys.executable)); print(venv_dir)"`, pythonVersion)
	updatePyvenvCmd := fmt.Sprintf(`%s && echo "include-system-site-packages = true" >> $(dirname $(dirname $(%s -c "import sys; print(sys.executable)")))/pyvenv.cfg`, findVenvCmd, pythonVersion)
	return updatePyvenvCmd
}
