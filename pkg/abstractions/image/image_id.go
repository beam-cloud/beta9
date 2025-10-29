package image

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/mitchellh/hashstructure/v2"
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

// prepareOptsForImageID ensures BuildOpts is ready for image ID calculation.
// For v2 builds, this renders the Dockerfile if needed. For v1, opts is used as-is.
// This function is idempotent - calling it multiple times has no side effects.
func prepareOptsForImageID(opts *BuildOpts, clipVersion uint32, renderer func(*BuildOpts) (string, error)) error {
	// For v2, ensure Dockerfile is rendered and set in opts before calculating ID
	if clipVersion == 2 && opts.Dockerfile == "" {
		// Only render if we have build steps or commands that need to be in the Dockerfile
		if len(opts.Commands) > 0 || len(opts.BuildSteps) > 0 || len(opts.PythonPackages) > 0 {
			if renderer == nil {
				return fmt.Errorf("v2 build requires a Dockerfile renderer")
			}
			df, err := renderer(opts)
			if err != nil {
				return err
			}
			opts.Dockerfile = df
		}
	}
	return nil
}

// GetImageID calculates a deterministic image ID from build options.
// For v2 builds, the Dockerfile must be rendered and set in opts before calling this.
// For v1 builds, commands and build steps are hashed directly.
func getImageID(opts *BuildOpts) (string, error) {
	h := sha1.New()
	h.Write([]byte(strings.Join(opts.Commands, "-")))
	if len(opts.BuildSteps) > 0 {
		for _, step := range opts.BuildSteps {
			fmt.Fprintf(h, "%s-%s", step.Type, step.Command)
		}
	}
	if len(opts.EnvVars) > 0 {
		for _, envVar := range opts.EnvVars {
			fmt.Fprint(h, envVar)
		}
	}
	if opts.Dockerfile != "" {
		fmt.Fprint(h, opts.Dockerfile)
	}
	if opts.BuildCtxObject != "" {
		fmt.Fprint(h, opts.BuildCtxObject)
	}
	commandListHash := hex.EncodeToString(h.Sum(nil))

	bodyToHash := &ImageIdHash{
		BaseImageName:   opts.BaseImageName,
		BaseImageTag:    getImageTagOrDigest(opts.BaseImageDigest, opts.BaseImageTag),
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

// GetBaseImageID calculates the image ID for just the base image (without build steps).
// This is used in v1 builds to determine which base image to start the container with.
func getBaseImageID(opts *BuildOpts) (string, error) {
	baseOpts := &BuildOpts{
		BaseImageRegistry: opts.BaseImageRegistry,
		BaseImageName:     opts.BaseImageName,
		BaseImageTag:      opts.BaseImageTag,
		BaseImageDigest:   opts.BaseImageDigest,
		ExistingImageUri:  opts.ExistingImageUri,
		EnvVars:           opts.EnvVars,
		Dockerfile:        opts.Dockerfile,
		BuildCtxObject:    opts.BuildCtxObject,
	}
	return getImageID(baseOpts)
}
