package image

import (
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type BuildOpts struct {
	BaseImageRegistry  string
	BaseImageName      string
	BaseImageTag       string
	BaseImageDigest    string
	BaseImageCreds     string
	ExistingImageUri   string
	ExistingImageCreds map[string]string
	Dockerfile         string
	BuildCtxObject     string
	PythonVersion      string
	PythonPackages     []string
	Commands           []string
	BuildSteps         []BuildStep
	ForceRebuild       bool
	EnvVars            []string
	BuildSecrets       []string
	Gpu                string
	IgnorePython       bool
	ClipVersion        uint32
}

func (o *BuildOpts) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "{")
	fmt.Fprintf(&b, "  \"BaseImageRegistry\": %q,", o.BaseImageRegistry)
	fmt.Fprintf(&b, "  \"BaseImageName\": %q,", o.BaseImageName)
	fmt.Fprintf(&b, "  \"BaseImageTag\": %q,", o.BaseImageTag)
	fmt.Fprintf(&b, "  \"BaseImageDigest\": %q,", o.BaseImageDigest)
	fmt.Fprintf(&b, "  \"BaseImageCreds\": %q,", o.BaseImageCreds)
	fmt.Fprintf(&b, "  \"ExistingImageUri\": %q,", o.ExistingImageUri)
	fmt.Fprintf(&b, "  \"ExistingImageCreds\": %#v,", o.ExistingImageCreds)
	fmt.Fprintf(&b, "  \"Dockerfile\": %q,", o.Dockerfile)
	fmt.Fprintf(&b, "  \"BuildCtxObject\": %q,", o.BuildCtxObject)
	fmt.Fprintf(&b, "  \"PythonVersion\": %q,", o.PythonVersion)
	fmt.Fprintf(&b, "  \"PythonPackages\": %#v,", o.PythonPackages)
	fmt.Fprintf(&b, "  \"Commands\": %#v,", o.Commands)
	fmt.Fprintf(&b, "  \"BuildSteps\": %#v,", o.BuildSteps)
	fmt.Fprintf(&b, "  \"ForceRebuild\": %v", o.ForceRebuild)
	fmt.Fprintf(&b, "  \"Gpu\": %q,", o.Gpu)
	fmt.Fprintf(&b, "  \"IgnorePython\": %v,", o.IgnorePython)
	fmt.Fprintf(&b, "}")
	return b.String()
}

// setCustomImageBuildOptions extracts and sets base image details from an existing image URI
// and handles registry credentials if provided
func (o *BuildOpts) setCustomImageBuildOptions() error {
	baseImage, err := ExtractImageNameAndTag(o.ExistingImageUri)
	if err != nil {
		return err
	}

	if len(o.ExistingImageCreds) > 0 && o.ExistingImageUri != "" {
		token, err := GetRegistryToken(o)
		if err != nil {
			return err
		}
		o.BaseImageCreds = token
	}

	o.BaseImageRegistry = baseImage.Registry
	o.BaseImageName = baseImage.Repo
	o.BaseImageTag = baseImage.Tag
	o.BaseImageDigest = baseImage.Digest

	return nil
}

// addPythonRequirements merges base Python requirements with user-specified packages,
// ensuring no duplicates and base requirements take precedence
func (o *BuildOpts) addPythonRequirements() {
	// Override any specified python packages with base requirements (to ensure we have what need in the image)
	baseRequirementsSlice := strings.Split(strings.TrimSpace(basePythonRequirements), "\n")

	// Create a map to track package names in baseRequirementsSlice
	baseNames := make(map[string]bool)
	for _, basePkg := range baseRequirementsSlice {
		baseNames[extractPackageName(basePkg)] = true
	}

	// Filter out existing packages from opts.PythonPackages
	filteredPythonPackages := make([]string, 0)
	for _, optPkg := range o.PythonPackages {
		if !baseNames[extractPackageName(optPkg)] {
			filteredPythonPackages = append(filteredPythonPackages, optPkg)
		}
	}

	o.PythonPackages = append(filteredPythonPackages, baseRequirementsSlice...)
}

// handleCustomBaseImage processes a custom base image by validating it, setting build options,
// and adding required Python packages
func (o *BuildOpts) handleCustomBaseImage(outputChan chan common.OutputMsg) error {
	if outputChan != nil {
		outputChan <- common.OutputMsg{Done: false, Success: false, Msg: fmt.Sprintf("Using custom base image: %s\n", o.ExistingImageUri)}
	}

	err := o.setCustomImageBuildOptions()
	if err != nil {
		if outputChan != nil {
			outputChan <- common.OutputMsg{Done: true, Success: false, Msg: err.Error() + "\n"}
		}
		return err
	}

	o.addPythonRequirements()
	return nil
}

// initializeBuildConfiguration prepares the build options by:
// - Processing Dockerfile if specified
// - Handling custom base images
// - Setting up Docker credentials
// Returns error if configuration fails
func (o *BuildOpts) initializeBuildConfiguration(cfg types.AppConfig, outputChan chan common.OutputMsg) error {
	switch {
	case o.Dockerfile != "":
		o.addPythonRequirements()
	case o.ExistingImageUri != "":
		if err := o.handleCustomBaseImage(outputChan); err != nil {
			return err
		}
	}

	o.setDefaultDockerCreds(cfg)

	return nil
}

// setDefaultDockerCreds checks if Docker Hub credentials should be used from config
// when base image is from Docker Hub and no credentials are explicitly set
func (o *BuildOpts) setDefaultDockerCreds(cfg types.AppConfig) {
	isDockerHub := o.BaseImageRegistry == dockerHubRegistry
	credsNotSet := o.BaseImageCreds == ""

	if isDockerHub && credsNotSet {
		username := cfg.ImageService.Registries.Docker.Username
		password := cfg.ImageService.Registries.Docker.Password
		if username != "" && password != "" {
			o.BaseImageCreds = fmt.Sprintf("%s:%s", username, password)
		}
	}
}
