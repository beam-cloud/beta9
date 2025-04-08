package image

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type RuncClient interface {
	Exec(containerId string, command string, env []string) (*pb.RunCExecResponse, error)
	Archive(ctx context.Context, containerId string, imageId string, outputChan chan common.OutputMsg) error
	Kill(containerId string) (*pb.RunCKillResponse, error)
	Status(containerId string) (*pb.RunCStatusResponse, error)
	StreamLogs(ctx context.Context, containerId string, outputChan chan common.OutputMsg) error
}

type Build struct {
	ctx         context.Context
	config      types.AppConfig
	opts        *BuildOpts
	success     *atomic.Bool
	containerId string
	imageId     string
	outputChan  chan common.OutputMsg
	authInfo    *auth.AuthInfo
	runcClient  RuncClient
}

func NewBuild(ctx context.Context, opts *BuildOpts, outputChan chan common.OutputMsg, config types.AppConfig) (*Build, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	return &Build{ctx: ctx, opts: opts, success: &atomic.Bool{}, containerId: genContainerId(), outputChan: outputChan, authInfo: authInfo, config: config}, nil
}

func (b *Build) initializeBuildConfiguration() error {
	if err := b.opts.initializeBuildConfiguration(b.config, b.outputChan); err != nil {
		return err
	}
	return nil
}

func (b *Build) prepareSteps() error {
	var err error
	b.imageId, err = getImageId(b.opts)
	if err != nil {
		return err
	}
	log.Info().Str("image_id", b.imageId).Msg("image id")

	micromambaEnv := strings.Contains(b.opts.PythonVersion, "micromamba")
	if micromambaEnv {
		b.runcClient.Exec(b.containerId, "micromamba config set use_lockfiles False", buildEnv)
	}

	var setupCommands []string

	// Detect if python3.x is installed in the container, if not install it
	checkPythonVersionCmd := fmt.Sprintf("%s --version", b.opts.PythonVersion)
	if resp, err := b.runcClient.Exec(b.containerId, checkPythonVersionCmd, buildEnv); (err != nil || !resp.Ok) && !micromambaEnv && !b.opts.IgnorePython {

		if b.opts.PythonVersion == types.Python3.String() {
			b.opts.PythonVersion = b.config.ImageService.PythonVersion
		} else {
			// Check if any python version is installed and warn the user that they are overriding it
			checkPythonVersionCmd := fmt.Sprintf("%s --version", types.Python3.String())
			if resp, err := b.runcClient.Exec(b.containerId, checkPythonVersionCmd, buildEnv); err == nil && resp.Ok {
				b.outputChan <- common.OutputMsg{Done: false, Success: b.success.Load(), Msg: fmt.Sprintf("requested python version (%s) was not detected, but an existing python3 environment was detected. The requested python version will be installed, replacing the existing python environment.\n", b.opts.PythonVersion), Warning: true}
			}
		}

		b.outputChan <- common.OutputMsg{Done: false, Success: b.success.Load(), Msg: fmt.Sprintf("%s not detected, installing it for you...\n", b.opts.PythonVersion)}
		installCmd, err := getPythonStandaloneInstallCommand(b.config.ImageService.Runner.PythonStandalone, b.opts.PythonVersion)
		if err != nil {
			b.outputChan <- common.OutputMsg{Done: true, Success: b.success.Load(), Msg: err.Error() + "\n"}
			return err
		}

		setupCommands = append(setupCommands, installCmd)
	} else if b.opts.IgnorePython && resp != nil && !resp.Ok {
		b.opts.PythonVersion = ""
	}

	// Generate the pip install command and prepend it to the commands list
	if len(b.opts.PythonPackages) > 0 && b.opts.PythonVersion != "" {
		pipInstallCmd := generatePipInstallCommand(b.opts.PythonPackages, b.opts.PythonVersion)
		setupCommands = append(setupCommands, pipInstallCmd)
	}

	b.opts.Commands = append(setupCommands, b.opts.Commands...)

	// Generate the commands to run in the container
	b.opts.Commands = append(b.opts.Commands, parseBuildSteps(b.opts.BuildSteps, b.opts.PythonVersion)...)

	return nil
}

func (b *Build) executeSteps() error {
	log.Info().Str("container_id", b.containerId).Interface("options", b.opts).Msg("container building")
	startTime := time.Now()

	for _, cmd := range b.opts.Commands {
		if cmd == "" {
			continue
		}

		if r, err := b.runcClient.Exec(b.containerId, cmd, buildEnv); err != nil || !r.Ok {
			log.Error().Str("container_id", b.containerId).Str("command", cmd).Err(err).Msg("failed to execute command for container")

			errMsg := ""
			if err != nil {
				errMsg = err.Error() + "\n"
			}

			time.Sleep(defaultImageBuildGracefulShutdownS) // Wait for logs to be passed through
			b.Log(true, errMsg)
			return err
		}
	}
	log.Info().Str("container_id", b.containerId).Dur("duration", time.Since(startTime)).Msg("container build took")
	return nil
}

func (b *Build) archive() error {
	if err := b.runcClient.Archive(b.ctx, b.containerId, b.imageId, b.outputChan); err != nil {
		b.Log(true, err.Error()+"\n")
		return err
	}
	return nil
}

func (b *Build) SetSuccess(success bool) {
	b.success.Store(success)
}

func (b *Build) Log(Done bool, Msg string) {
	b.outputChan <- common.OutputMsg{Done: Done, Success: b.success.Load(), Msg: Msg}
}

func (b *Build) LogWithImageAndPythonVersion(Done bool, Msg string) {
	b.outputChan <- common.OutputMsg{Done: Done, Success: b.success.Load(), Archiving: true, Msg: Msg, ImageId: b.imageId, PythonVersion: b.opts.PythonVersion}
}

func (b *Build) connectToHost(hostname string, tailscale *network.Tailscale) error {
	conn, err := network.ConnectToHost(b.ctx, hostname, time.Second*30, tailscale, b.config.Tailscale)
	if err != nil {
		b.Log(true, "Failed to connect to build container.\n")
		return err
	}

	client, err := common.NewRunCClient(hostname, b.authInfo.Token.Key, conn)
	if err != nil {
		b.Log(true, "Failed to connect to build container.\n")
		return err
	}

	b.runcClient = client
	return nil
}

func (b *Build) streamLogs() {
	go b.runcClient.StreamLogs(b.ctx, b.containerId, b.outputChan)
}

func (b *Build) killContainer() error {
	if b.runcClient == nil {
		return nil
	}
	_, err := b.runcClient.Kill(b.containerId)
	return err
}

func (b *Build) getContainerStatus() (*pb.RunCStatusResponse, error) {
	if b.runcClient == nil {
		return nil, nil
	}
	return b.runcClient.Status(b.containerId)
}

// generateContainerRequest generates a container request for the build container
func (b *Build) generateContainerRequest() (*types.ContainerRequest, error) {
	baseImageId, err := getImageId(&BuildOpts{
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
		ContainerId: b.containerId,
		Env:         b.opts.EnvVars,
		Cpu:         cpu,
		Memory:      memory,
		ImageId:     baseImageId,
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

func generatePipInstallCommand(pythonPackages []string, pythonVersion string) string {
	flagLines, packages := parseFlagLinesAndPackages(pythonPackages)

	// DEBIAN_FRONTEND=noninteractive PIP_ROOT_USER_ACTION=ignore
	// command := "uv pip install --system"
	// if strings.Contains(pythonVersion, "micromamba") {
	command := fmt.Sprintf("PIP_ROOT_USER_ACTION=ignore %s -m pip install", pythonVersion)
	// }

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
