package image

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/image/mocks"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/assert"
)

func setupTestBuild(t *testing.T, opts *BuildOpts) (*Build, *mocks.RuncClient, chan common.OutputMsg) {
	mockRuncClient := mocks.NewRuncClient(t)
	outputChan := make(chan common.OutputMsg, 10) // Buffered channel

	if opts == nil {
		opts = &BuildOpts{
			BaseImageRegistry: "docker.io",
			BaseImageName:     "library/ubuntu",
			BaseImageTag:      "latest",
			PythonVersion:     "python3.10", // Default python version for tests
		}
	}

	config := types.AppConfig{
		ImageService: types.ImageServiceConfig{
			PythonVersion: "python3.10", // Match default python version
			Runner: types.RunnerConfig{
				PythonStandalone: types.PythonStandaloneConfig{
					InstallScriptTemplate: "echo installing python {{.PythonVersion}} {{.Architecture}} {{.OS}} {{.Vendor}}",
					Versions: map[string]string{
						"python3.10": "cpython-3.10.13+20230826",
						"python3.11": "cpython-3.11.5+20230826",
					},
				},
			},
		},
	}

	build, err := NewBuild(context.Background(), opts, outputChan, config)
	assert.NoError(t, err)
	build.runcClient = mockRuncClient // Inject the mock client

	// Mock image ID generation (simplified)
	build.imageID = "test-image-id"

	return build, mockRuncClient, outputChan
}

func TestBuild_prepareSteps_PythonExists(t *testing.T) {
	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "latest",
		PythonVersion:     "python3.10",
		PythonPackages:    []string{"requests", "numpy"},
		BuildSteps:        []BuildStep{{Command: "echo hello", Type: shellCommandType}},
	}
	build, mockRuncClient, _ := setupTestBuild(t, opts)

	// Mock python version check - python exists
	mockRuncClient.On("Exec", build.containerID, "python3.10 --version", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil)

	err := build.prepareCommands()
	assert.NoError(t, err)

	expectedCommands := []string{
		"uv-b9 pip install \"requests\" \"numpy\"",
		"echo hello",
	}
	assert.Equal(t, expectedCommands, build.commands)
	assert.NotEmpty(t, build.imageID)
	mockRuncClient.AssertExpectations(t)
}

func TestBuild_prepareSteps_PythonNeedsInstall(t *testing.T) {
	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "latest",
		PythonVersion:     "python3.11",
		PythonPackages:    []string{"pandas"},
	}
	build, mockRuncClient, outputChan := setupTestBuild(t, opts)

	// Mock python version check - specific version doesn't exist
	mockRuncClient.On("Exec", build.containerID, "python3.11 --version", buildEnv).Return(nil, errors.New("not found"))
	// Mock general python3 check - it exists (so we show a warning)
	mockRuncClient.On("Exec", build.containerID, "python3 --version", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil)

	err := build.prepareCommands()
	assert.NoError(t, err)

	// Expect installation command based on PythonStandaloneConfig
	expectedPipCmd := "uv-b9 pip install \"pandas\""

	// Installation command should contain arch, os, vendor derived from runtime and template
	assert.Contains(t, build.commands[0], "installing python cpython-3.11.5+20230826")
	assert.Equal(t, expectedPipCmd, build.commands[1])
	assert.NotEmpty(t, build.imageID)

	// Check for warning message
	select {
	case msg := <-outputChan:
		assert.True(t, msg.Warning)
		assert.Contains(t, msg.Msg, "requested python version (python3.11) was not detected")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected warning message not received")
	}
	// Check for installing message
	select {
	case msg := <-outputChan:
		assert.False(t, msg.Warning)
		assert.Contains(t, msg.Msg, "python3.11 not detected, installing it for you...")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected installing message not received")
	}

	mockRuncClient.AssertExpectations(t)
}

func TestBuild_prepareSteps_IgnorePython(t *testing.T) {
	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "latest",
		PythonVersion:     "python3.10",
		IgnorePython:      true,
		Commands:          []string{"apt update"},
	}
	build, mockRuncClient, _ := setupTestBuild(t, opts)

	err := build.prepareCommands()
	assert.NoError(t, err)

	// No python install or pip commands should be added
	assert.Equal(t, []string{"apt update"}, build.commands)
	assert.Equal(t, "", build.opts.PythonVersion, "PythonVersion should be cleared if IgnorePython is true and python is not found")
	assert.NotEmpty(t, build.imageID)
	mockRuncClient.AssertExpectations(t)
}

func TestBuild_prepareSteps_Micromamba(t *testing.T) {
	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "latest",
		PythonVersion:     "micromamba-1.5",
		PythonPackages:    []string{"conda-forge::numpy", "-c", "pytorch"},
		BuildSteps: []BuildStep{
			{Type: micromambaCommandType, Command: "scipy"},
			{Type: shellCommandType, Command: "echo done mamba"},
			{Type: pipCommandType, Command: "requests"},
			{Type: pipCommandType, Command: "beautifulsoup4"},
		},
	}
	build, mockRuncClient, _ := setupTestBuild(t, opts)

	err := build.prepareCommands()
	assert.NoError(t, err)

	expectedCommands := []string{
		"micromamba config set use_lockfiles False",
		"micromamba-1.5 -m pip install -c \"conda-forge::numpy\" \"pytorch\"", // From PythonPackages
		"micromamba install -y -n beta9 \"scipy\"",                            // From BuildSteps (mamba)
		"echo done mamba",
		"micromamba-1.5 -m pip install \"requests\" \"beautifulsoup4\"", // From BuildSteps (pip)
	}

	assert.Equal(t, expectedCommands, build.commands)
	assert.NotEmpty(t, build.imageID)
	mockRuncClient.AssertExpectations(t)
}

func TestBuild_executeSteps_Success(t *testing.T) {
	build, mockRuncClient, _ := setupTestBuild(t, nil)
	build.commands = []string{"cmd1", "cmd2"}

	mockRuncClient.On("Exec", build.containerID, "cmd1", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil).Once()
	mockRuncClient.On("Exec", build.containerID, "cmd2", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil).Once()

	err := build.executeCommands()
	assert.NoError(t, err)
	mockRuncClient.AssertExpectations(t)
}

func TestBuild_executeSteps_Failure(t *testing.T) {
	build, mockRuncClient, outputChan := setupTestBuild(t, nil)
	build.commands = []string{"cmd1", "cmd2-fails", "cmd3"}

	mockRuncClient.On("Exec", build.containerID, "cmd1", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil).Once()
	// Mock failure on the second command
	execErr := errors.New("command failed")
	mockRuncClient.On("Exec", build.containerID, "cmd2-fails", buildEnv).Return(nil, execErr).Once()
	// cmd3 should not be called

	err := build.executeCommands()
	assert.Error(t, err)
	assert.Equal(t, execErr, err)

	// Check for error log message
	select {
	case msg := <-outputChan:
		assert.True(t, msg.Done)
		assert.False(t, msg.Success)
		assert.Contains(t, msg.Msg, execErr.Error())
	case <-time.After(6 * time.Second): // Includes defaultImageBuildGracefulShutdownS
		t.Fatal("Expected error message not received")
	}

	mockRuncClient.AssertExpectations(t)
	// Ensure cmd3 was not called
	mockRuncClient.AssertNotCalled(t, "Exec", build.containerID, "cmd3", buildEnv)
}

func TestBuild_archive_Success(t *testing.T) {
	build, mockRuncClient, outputChan := setupTestBuild(t, nil)
	build.imageID = "final-image-id" // Ensure imageId is set

	mockRuncClient.On("Archive", build.ctx, build.containerID, build.imageID, outputChan).Return(nil).Once()

	err := build.archive()
	assert.NoError(t, err)
	mockRuncClient.AssertExpectations(t)
}

func TestBuild_archive_Failure(t *testing.T) {
	build, mockRuncClient, outputChan := setupTestBuild(t, nil)
	build.imageID = "final-image-id"
	archiveErr := errors.New("archiving failed")

	mockRuncClient.On("Archive", build.ctx, build.containerID, build.imageID, outputChan).Return(archiveErr).Once()

	err := build.archive()
	assert.Error(t, err)
	assert.Equal(t, archiveErr, err)

	// Check for error log message
	select {
	case msg := <-outputChan:
		assert.True(t, msg.Done)
		assert.False(t, msg.Success)
		assert.Contains(t, msg.Msg, archiveErr.Error())
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected error message not received")
	}

	mockRuncClient.AssertExpectations(t)
}

// Test parseBuildSteps specifically for command coalescing
func Test_parseBuildSteps(t *testing.T) {
	pythonVersion := "python3.9"
	steps := []BuildStep{
		{Type: shellCommandType, Command: "apt update"},
		{Type: pipCommandType, Command: "requests"},
		{Type: pipCommandType, Command: "numpy"},
		{Type: shellCommandType, Command: "echo 'installing libs'"},
		{Type: micromambaCommandType, Command: "conda-forge::pandas"},
		{Type: micromambaCommandType, Command: "-c pytorch"},
		{Type: micromambaCommandType, Command: "scipy"},
		{Type: shellCommandType, Command: "echo 'done'"},
		{Type: pipCommandType, Command: "--no-deps flask"}, // Flag forces split
		{Type: pipCommandType, Command: "gunicorn"},
	}

	expected := []string{
		"apt update",
		"uv-b9 pip install \"requests\" \"numpy\"", // Coalesced pip
		"echo 'installing libs'",
		"micromamba install -y -n beta9 -c pytorch \"conda-forge::pandas\" \"scipy\"", // Coalesced mamba (flags don't split mamba)
		"echo 'done'",
		"uv-b9 pip install --no-deps flask", // Flagged line isn't quoted
		"uv-b9 pip install \"gunicorn\"",    // Second pip group
	}

	result := parseBuildSteps(steps, pythonVersion)
	assert.Equal(t, expected, result)
}
