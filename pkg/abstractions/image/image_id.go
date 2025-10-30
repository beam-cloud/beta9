package image

import (
	"fmt"

	"github.com/mitchellh/hashstructure/v2"
)

// getImageID calculates a deterministic image ID from build options.
// This is the single source of truth for image identity.
func getImageID(opts *BuildOpts) (string, error) {
	// Hash all fields that affect the final image
	// Order matters for consistency, so we use a struct with defined field order
	hashInput := struct {
		BaseImageName   string
		BaseImageTag    string
		BaseImageDigest string
		ExistingImageUri string
		PythonVersion   string
		PythonPackages  []string
		Commands        []string
		BuildSteps      []BuildStep
		EnvVars         []string
		Dockerfile      string
		BuildCtxObject  string
	}{
		BaseImageName:    opts.BaseImageName,
		BaseImageTag:     opts.BaseImageTag,
		BaseImageDigest:  opts.BaseImageDigest,
		ExistingImageUri: opts.ExistingImageUri,
		PythonVersion:    opts.PythonVersion,
		PythonPackages:   opts.PythonPackages,
		Commands:         opts.Commands,
		BuildSteps:       opts.BuildSteps,
		EnvVars:          opts.EnvVars,
		Dockerfile:       opts.Dockerfile,
		BuildCtxObject:   opts.BuildCtxObject,
	}

	hash, err := hashstructure.Hash(hashInput, hashstructure.FormatV2, nil)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%016x", hash), nil
}
