package image

import (
	"fmt"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/mitchellh/hashstructure/v2"
)

// getImageID calculates a deterministic image ID from build options.
// This is the single source of truth for image identity.
//
// For V2 builds with Dockerfiles (custom or generated), the Dockerfile content
// contains all build instructions, so we only need to hash the Dockerfile and
// build context. For V1 builds and V2 builds without Dockerfiles, we hash all
// the individual build options that will be used to construct the image.
func getImageID(opts *BuildOpts) (string, error) {
	// For V2 builds with a Dockerfile, the Dockerfile contains all the build instructions
	// Base the image ID primarily on the Dockerfile content and build context
	if opts.ClipVersion == uint32(types.ClipVersion2) && opts.Dockerfile != "" {
		hashInput := struct {
			Dockerfile     string
			BuildCtxObject string
			EnvVars        []string
		}{
			Dockerfile:     opts.Dockerfile,
			BuildCtxObject: opts.BuildCtxObject,
			EnvVars:        opts.EnvVars,
		}

		hash, err := hashstructure.Hash(hashInput, hashstructure.FormatV2, nil)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%016x", hash), nil
	}

	// For V1 builds and V2 builds without Dockerfiles, hash all build options
	// Order matters for consistency, so we use a struct with defined field order
	hashInput := struct {
		BaseImageName     string
		BaseImageTag      string
		BaseImageDigest   string
		BaseImageRegistry string
		ExistingImageUri  string
		PythonVersion     string
		PythonPackages    []string
		Commands          []string
		BuildSteps        []BuildStep
		EnvVars           []string
		Dockerfile        string
		BuildCtxObject    string
	}{
		BaseImageName:     opts.BaseImageName,
		BaseImageTag:      opts.BaseImageTag,
		BaseImageDigest:   opts.BaseImageDigest,
		BaseImageRegistry: opts.BaseImageRegistry,
		ExistingImageUri:  opts.ExistingImageUri,
		PythonVersion:     opts.PythonVersion,
		PythonPackages:    opts.PythonPackages,
		Commands:          opts.Commands,
		BuildSteps:        opts.BuildSteps,
		EnvVars:           opts.EnvVars,
		Dockerfile:        opts.Dockerfile,
		BuildCtxObject:    opts.BuildCtxObject,
	}

	hash, err := hashstructure.Hash(hashInput, hashstructure.FormatV2, nil)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%016x", hash), nil
}
