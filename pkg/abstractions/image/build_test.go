package image

import (
	"context"
	"errors"
	"fmt"
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
	build.imageId = "test-image-id"

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
	mockRuncClient.On("Exec", build.containerId, "python3.10 --version", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil)

	err := build.prepareSteps()
	assert.NoError(t, err)

	expectedCommands := []string{
		"PIP_ROOT_USER_ACTION=ignore python3.10 -m pip install \"requests\" \"numpy\"",
		"echo hello",
	}
	assert.Equal(t, expectedCommands, build.opts.Commands)
	assert.NotEmpty(t, build.imageId)
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
	mockRuncClient.On("Exec", build.containerId, "python3.11 --version", buildEnv).Return(nil, errors.New("not found"))
	// Mock general python3 check - it exists (so we show a warning)
	mockRuncClient.On("Exec", build.containerId, "python3 --version", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil)

	err := build.prepareSteps()
	assert.NoError(t, err)

	// Expect installation command based on PythonStandaloneConfig
	// expectedInstallCmd := "echo installing python cpython-3.11.5+20230826" // Simplified based on template - REMOVED as unused
	expectedPipCmd := "PIP_ROOT_USER_ACTION=ignore python3.11 -m pip install \"pandas\""

	// Installation command should contain arch, os, vendor derived from runtime and template
	assert.Contains(t, build.opts.Commands[0], "installing python cpython-3.11.5+20230826")
	assert.Equal(t, expectedPipCmd, build.opts.Commands[1])
	assert.NotEmpty(t, build.imageId)

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

	// Mock python version check - python doesn't exist
	mockRuncClient.On("Exec", build.containerId, "python3.10 --version", buildEnv).Return(&pb.RunCExecResponse{Ok: false}, nil) // Ok: false simulates not found

	err := build.prepareSteps()
	assert.NoError(t, err)

	// No python install or pip commands should be added
	assert.Equal(t, []string{"apt update"}, build.opts.Commands)
	assert.Equal(t, "", build.opts.PythonVersion, "PythonVersion should be cleared if IgnorePython is true and python is not found")
	assert.NotEmpty(t, build.imageId)
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

	// Mock the initial version check for micromamba (it proceeds even if not found)
	mockRuncClient.On("Exec", build.containerId, "micromamba-1.5 --version", buildEnv).Return(nil, errors.New("not found"))
	// Mock micromamba config set
	mockRuncClient.On("Exec", build.containerId, "micromamba config set use_lockfiles False", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil)

	err := build.prepareSteps()
	assert.NoError(t, err)

	expectedCommands := []string{
		"PIP_ROOT_USER_ACTION=ignore micromamba-1.5 -m pip install -c \"conda-forge::numpy\" \"pytorch\"", // From PythonPackages
		"micromamba install -y -n beta9 \"scipy\"",                                                        // From BuildSteps (mamba)
		"echo done mamba",
		"PIP_ROOT_USER_ACTION=ignore micromamba-1.5 -m pip install \"requests\" \"beautifulsoup4\"", // From BuildSteps (pip)
	}

	assert.Equal(t, expectedCommands, build.opts.Commands)
	assert.NotEmpty(t, build.imageId)
	mockRuncClient.AssertExpectations(t)
}

func TestBuild_executeSteps_Success(t *testing.T) {
	build, mockRuncClient, _ := setupTestBuild(t, nil)
	build.opts.Commands = []string{"cmd1", "cmd2"}

	mockRuncClient.On("Exec", build.containerId, "cmd1", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil).Once()
	mockRuncClient.On("Exec", build.containerId, "cmd2", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil).Once()

	err := build.executeSteps()
	assert.NoError(t, err)
	mockRuncClient.AssertExpectations(t)
}

func TestBuild_executeSteps_Failure(t *testing.T) {
	build, mockRuncClient, outputChan := setupTestBuild(t, nil)
	build.opts.Commands = []string{"cmd1", "cmd2-fails", "cmd3"}

	mockRuncClient.On("Exec", build.containerId, "cmd1", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil).Once()
	// Mock failure on the second command
	execErr := errors.New("command failed")
	mockRuncClient.On("Exec", build.containerId, "cmd2-fails", buildEnv).Return(nil, execErr).Once()
	// cmd3 should not be called

	err := build.executeSteps()
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
	mockRuncClient.AssertNotCalled(t, "Exec", build.containerId, "cmd3", buildEnv)
}

func TestBuild_archive_Success(t *testing.T) {
	build, mockRuncClient, outputChan := setupTestBuild(t, nil)
	build.imageId = "final-image-id" // Ensure imageId is set

	mockRuncClient.On("Archive", build.ctx, build.containerId, build.imageId, outputChan).Return(nil).Once()

	err := build.archive()
	assert.NoError(t, err)
	mockRuncClient.AssertExpectations(t)
}

func TestBuild_archive_Failure(t *testing.T) {
	build, mockRuncClient, outputChan := setupTestBuild(t, nil)
	build.imageId = "final-image-id"
	archiveErr := errors.New("archiving failed")

	mockRuncClient.On("Archive", build.ctx, build.containerId, build.imageId, outputChan).Return(archiveErr).Once()

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

// Helper to get image ID for testing purposes (simplified version of actual logic)
func test_getImageId(opts *BuildOpts) (string, error) {
	if opts.ExistingImageUri != "" {
		return "existing-" + opts.ExistingImageUri, nil
	}
	if opts.Dockerfile != "" {
		return "dockerfile-build", nil
	}
	return fmt.Sprintf("%s-%s-%s-%s", opts.BaseImageRegistry, opts.BaseImageName, opts.BaseImageTag, opts.BaseImageDigest), nil
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
		"PIP_ROOT_USER_ACTION=ignore python3.9 -m pip install \"requests\" \"numpy\"", // Coalesced pip
		"echo 'installing libs'",
		"micromamba install -y -n beta9 -c pytorch \"conda-forge::pandas\" \"scipy\"", // Coalesced mamba (flags don't split mamba)
		"echo 'done'",
		"PIP_ROOT_USER_ACTION=ignore python3.9 -m pip install --no-deps flask", // Flagged line isn't quoted
		"PIP_ROOT_USER_ACTION=ignore python3.9 -m pip install \"gunicorn\"",    // Second pip group
	}

	result := parseBuildSteps(steps, pythonVersion)
	assert.Equal(t, expected, result)
}
