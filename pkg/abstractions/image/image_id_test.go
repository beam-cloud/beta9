package image

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock renderer for testing
func mockRenderer(opts *BuildOpts) (string, error) {
	var sb strings.Builder
	sb.WriteString("FROM ")
	sb.WriteString(opts.BaseImageRegistry)
	sb.WriteString("/")
	sb.WriteString(opts.BaseImageName)
	sb.WriteString(":")
	sb.WriteString(opts.BaseImageTag)
	sb.WriteString("\n")
	for _, cmd := range opts.Commands {
		sb.WriteString("RUN ")
		sb.WriteString(cmd)
		sb.WriteString("\n")
	}
	return sb.String(), nil
}

// TestGetImageID_Consistency ensures the same inputs produce the same image ID
func TestGetImageID_Consistency(t *testing.T) {
	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		PythonVersion:     "python3.10",
		PythonPackages:    []string{"requests", "numpy"},
		Commands:          []string{"apt update", "apt install -y curl"},
	}

	id1, err := getImageID(opts)
	require.NoError(t, err)

	id2, err := getImageID(opts)
	require.NoError(t, err)

	assert.Equal(t, id1, id2, "Same inputs should produce the same image ID")
}

// TestGetImageID_Uniqueness ensures different inputs produce different image IDs
func TestGetImageID_Uniqueness(t *testing.T) {
	baseOpts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		PythonVersion:     "python3.10",
	}

	// Different commands
	opts1 := *baseOpts
	opts1.Commands = []string{"echo one"}
	id1, err := getImageID(&opts1)
	require.NoError(t, err)

	opts2 := *baseOpts
	opts2.Commands = []string{"echo two"}
	id2, err := getImageID(&opts2)
	require.NoError(t, err)

	assert.NotEqual(t, id1, id2, "Different commands should produce different image IDs")

	// Different python packages
	opts3 := *baseOpts
	opts3.PythonPackages = []string{"requests"}
	id3, err := getImageID(&opts3)
	require.NoError(t, err)

	opts4 := *baseOpts
	opts4.PythonPackages = []string{"numpy"}
	id4, err := getImageID(&opts4)
	require.NoError(t, err)

	assert.NotEqual(t, id3, id4, "Different packages should produce different image IDs")

	// Different base image tags
	opts5 := *baseOpts
	opts5.BaseImageTag = "20.04"
	id5, err := getImageID(&opts5)
	require.NoError(t, err)

	opts6 := *baseOpts
	opts6.BaseImageTag = "22.04"
	id6, err := getImageID(&opts6)
	require.NoError(t, err)

	assert.NotEqual(t, id5, id6, "Different base tags should produce different image IDs")
}

// TestGetImageID_DockerfileIncluded ensures Dockerfile content affects the image ID
func TestGetImageID_DockerfileIncluded(t *testing.T) {
	opts1 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Dockerfile:        "FROM ubuntu:22.04\nRUN echo hello",
	}

	opts2 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Dockerfile:        "FROM ubuntu:22.04\nRUN echo world",
	}

	id1, err := getImageID(opts1)
	require.NoError(t, err)

	id2, err := getImageID(opts2)
	require.NoError(t, err)

	assert.NotEqual(t, id1, id2, "Different Dockerfile content should produce different image IDs")
}

// TestGetBaseImageID ensures base image ID excludes build steps
func TestGetBaseImageID(t *testing.T) {
	optsWithSteps := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		PythonVersion:     "python3.10",
		PythonPackages:    []string{"requests"},
		Commands:          []string{"echo hello"},
		BuildSteps:        []BuildStep{{Type: shellCommandType, Command: "apt update"}},
	}

	// Get the base image ID (should exclude build steps, commands, packages)
	baseID, err := getBaseImageID(optsWithSteps)
	require.NoError(t, err)

	// Build from same base with different build content should have same base ID
	opts2 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		PythonVersion:     "python3.11",
		Commands:          []string{"echo different"},
		BuildSteps:        []BuildStep{{Type: pipCommandType, Command: "numpy"}},
	}

	baseID2, err := getBaseImageID(opts2)
	require.NoError(t, err)

	assert.Equal(t, baseID, baseID2, "Base image ID should be the same for different build steps")

	// But full image IDs should differ
	fullID1, err := getImageID(optsWithSteps)
	require.NoError(t, err)
	fullID2, err := getImageID(opts2)
	require.NoError(t, err)

	assert.NotEqual(t, fullID1, fullID2, "Full image IDs should differ with different build steps")
	assert.NotEqual(t, baseID, fullID1, "Base ID should differ from full ID")
}

// TestGetBaseImageID_WithDockerfile ensures Dockerfile in base opts is preserved
func TestGetBaseImageID_WithDockerfile(t *testing.T) {
	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Dockerfile:        "FROM ubuntu:22.04\nRUN echo base",
		Commands:          []string{"echo extra"},
	}

	baseID, err := getBaseImageID(opts)
	require.NoError(t, err)

	// Base ID should include the Dockerfile since it's part of base definition
	optsWithSameDockerfile := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Dockerfile:        "FROM ubuntu:22.04\nRUN echo base",
	}
	sameBaseID, err := getImageID(optsWithSameDockerfile)
	require.NoError(t, err)

	assert.Equal(t, baseID, sameBaseID, "Base ID should include Dockerfile")
}

// TestPrepareOptsForImageID_V1 ensures v1 builds don't modify opts
func TestPrepareOptsForImageID_V1(t *testing.T) {
	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Commands:          []string{"echo hello"},
	}

	originalDockerfile := opts.Dockerfile
	err := prepareOptsForImageID(opts, 1, mockRenderer)
	require.NoError(t, err)

	// For v1, Dockerfile should not be modified
	assert.Equal(t, originalDockerfile, opts.Dockerfile, "V1 should not render Dockerfile")
}

// TestPrepareOptsForImageID_V2_WithCommands ensures v2 renders Dockerfile when commands exist
func TestPrepareOptsForImageID_V2_WithCommands(t *testing.T) {
	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Commands:          []string{"echo hello", "apt update"},
	}

	assert.Empty(t, opts.Dockerfile, "Dockerfile should start empty")

	err := prepareOptsForImageID(opts, 2, mockRenderer)
	require.NoError(t, err)

	// For v2, Dockerfile should be rendered
	assert.NotEmpty(t, opts.Dockerfile, "V2 should render Dockerfile")
	assert.Contains(t, opts.Dockerfile, "FROM docker.io/library/ubuntu:22.04")
	assert.Contains(t, opts.Dockerfile, "RUN echo hello")
	assert.Contains(t, opts.Dockerfile, "RUN apt update")
}

// TestPrepareOptsForImageID_V2_Idempotent ensures calling prepare multiple times is safe
func TestPrepareOptsForImageID_V2_Idempotent(t *testing.T) {
	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Commands:          []string{"echo hello"},
	}

	err := prepareOptsForImageID(opts, 2, mockRenderer)
	require.NoError(t, err)
	firstDockerfile := opts.Dockerfile

	// Call again
	err = prepareOptsForImageID(opts, 2, mockRenderer)
	require.NoError(t, err)

	// Should not double-render (Dockerfile already set, so it won't render again)
	assert.Equal(t, firstDockerfile, opts.Dockerfile, "Multiple prepare calls should be idempotent")
}

// TestPrepareOptsForImageID_V2_NoCommandsNoDockerfile ensures v2 without commands doesn't crash
func TestPrepareOptsForImageID_V2_NoCommandsNoDockerfile(t *testing.T) {
	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		// No commands, no packages, no build steps
	}

	err := prepareOptsForImageID(opts, 2, mockRenderer)
	require.NoError(t, err)

	// Should not render Dockerfile if there's nothing to build
	assert.Empty(t, opts.Dockerfile, "V2 without build steps should not render Dockerfile")
}

// TestPrepareOptsForImageID_V2_WithExistingDockerfile ensures existing Dockerfile is preserved
func TestPrepareOptsForImageID_V2_WithExistingDockerfile(t *testing.T) {
	existingDockerfile := "FROM ubuntu:22.04\nRUN custom command"
	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Dockerfile:        existingDockerfile,
		Commands:          []string{"echo hello"},
	}

	err := prepareOptsForImageID(opts, 2, mockRenderer)
	require.NoError(t, err)

	// Should not overwrite existing Dockerfile
	assert.Equal(t, existingDockerfile, opts.Dockerfile, "Existing Dockerfile should be preserved")
}

// TestImageID_V1_V2_Consistency ensures v1 and v2 produce different IDs for same inputs
func TestImageID_V1_V2_Different(t *testing.T) {
	// For v1 (no Dockerfile rendered)
	optsV1 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Commands:          []string{"echo hello"},
	}

	idV1, err := getImageID(optsV1)
	require.NoError(t, err)

	// For v2 (Dockerfile rendered)
	optsV2 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Commands:          []string{"echo hello"},
	}
	err = prepareOptsForImageID(optsV2, 2, mockRenderer)
	require.NoError(t, err)

	idV2, err := getImageID(optsV2)
	require.NoError(t, err)

	// V1 and V2 should produce different IDs (v2 includes rendered Dockerfile)
	assert.NotEqual(t, idV1, idV2, "V1 and V2 should produce different image IDs")
}

// TestImageID_Format ensures image IDs are in expected format
func TestImageID_Format(t *testing.T) {
	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
	}

	id, err := getImageID(opts)
	require.NoError(t, err)

	// Should be 16 hex characters
	assert.Len(t, id, 16, "Image ID should be 16 characters")
	assert.Regexp(t, "^[0-9a-f]{16}$", id, "Image ID should be hexadecimal")
}

// TestImageID_EnvVarsAffectHash ensures environment variables affect the image ID
func TestImageID_EnvVarsAffectHash(t *testing.T) {
	opts1 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		EnvVars:           []string{"FOO=bar"},
	}

	opts2 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		EnvVars:           []string{"FOO=baz"},
	}

	id1, err := getImageID(opts1)
	require.NoError(t, err)

	id2, err := getImageID(opts2)
	require.NoError(t, err)

	assert.NotEqual(t, id1, id2, "Different environment variables should produce different image IDs")
}

// TestImageID_BuildStepsAffectHash ensures build steps affect the image ID
func TestImageID_BuildStepsAffectHash(t *testing.T) {
	opts1 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		BuildSteps: []BuildStep{
			{Type: shellCommandType, Command: "apt update"},
		},
	}

	opts2 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		BuildSteps: []BuildStep{
			{Type: pipCommandType, Command: "requests"},
		},
	}

	id1, err := getImageID(opts1)
	require.NoError(t, err)

	id2, err := getImageID(opts2)
	require.NoError(t, err)

	assert.NotEqual(t, id1, id2, "Different build steps should produce different image IDs")
}

// TestImageID_BuildCtxObjectAffectsHash ensures build context affects the image ID
func TestImageID_BuildCtxObjectAffectsHash(t *testing.T) {
	opts1 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		BuildCtxObject:    "object1",
	}

	opts2 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		BuildCtxObject:    "object2",
	}

	id1, err := getImageID(opts1)
	require.NoError(t, err)

	id2, err := getImageID(opts2)
	require.NoError(t, err)

	assert.NotEqual(t, id1, id2, "Different build contexts should produce different image IDs")
}
