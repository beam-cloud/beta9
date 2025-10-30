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

// TestBaseImageID_ExcludesBuildSteps ensures base image ID calculation works
func TestBaseImageID_ExcludesBuildSteps(t *testing.T) {
	// Two images with same base but different build steps
	base1 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Dockerfile:        "FROM ubuntu:22.04",
	}
	base2 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Dockerfile:        "FROM ubuntu:22.04",
	}

	baseID1, err := getImageID(base1)
	require.NoError(t, err)
	baseID2, err := getImageID(base2)
	require.NoError(t, err)

	assert.Equal(t, baseID1, baseID2, "Base image ID should be identical")

	// Full images with build steps should differ
	full1 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Dockerfile:        "FROM ubuntu:22.04",
		Commands:          []string{"echo hello"},
	}
	full2 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Dockerfile:        "FROM ubuntu:22.04",
		Commands:          []string{"echo different"},
	}

	fullID1, err := getImageID(full1)
	require.NoError(t, err)
	fullID2, err := getImageID(full2)
	require.NoError(t, err)

	assert.NotEqual(t, fullID1, fullID2, "Full image IDs should differ with different commands")
	assert.NotEqual(t, baseID1, fullID1, "Base ID should differ from full ID")
}

// TestImageID_V1_V2_Different ensures v1 and v2 produce different IDs
func TestImageID_V1_V2_Different(t *testing.T) {
	// V1 (no Dockerfile)
	optsV1 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Commands:          []string{"echo hello"},
	}

	idV1, err := getImageID(optsV1)
	require.NoError(t, err)

	// V2 (with rendered Dockerfile)
	optsV2 := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "22.04",
		Commands:          []string{"echo hello"},
		Dockerfile:        "FROM docker.io/library/ubuntu:22.04\nRUN echo hello\n",
	}

	idV2, err := getImageID(optsV2)
	require.NoError(t, err)

	// V1 and V2 should produce different IDs (v2 has Dockerfile field set)
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
