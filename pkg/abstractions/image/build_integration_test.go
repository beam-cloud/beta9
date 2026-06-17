package image

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

const imageBuildIntegrationEnv = "BETA9_IMAGE_BUILD_INTEGRATION"
const imageBuildPlatformEnv = "BETA9_IMAGE_BUILD_PLATFORM"

type imageBuildContainerCheck struct {
	Modules map[string]bool   `json:"modules"`
	Files   map[string]string `json:"files"`
}

func TestV2PreparedDockerfilesBuildAndRun(t *testing.T) {
	if os.Getenv(imageBuildIntegrationEnv) != "1" {
		t.Skipf("set %s=1 to run Docker-backed image build integration tests", imageBuildIntegrationEnv)
	}
	requireDocker(t)

	baseRef := dockerPythonBaseRef(t)
	customDockerfile := func(marker string) string {
		return fmt.Sprintf(`FROM %s
RUN mkdir -p /opt/beta9-test && echo %s > /opt/beta9-test/mode
`, baseRef, marker)
	}

	tests := []struct {
		name        string
		req         *pb.VerifyImageBuildRequest
		wantModules map[string]bool
		wantFiles   map[string]string
	}{
		{
			name: "registry image injects runtime requirements",
			req: &pb.VerifyImageBuildRequest{
				PythonVersion:    "python3.10",
				ExistingImageUri: baseRef,
			},
			wantModules: map[string]bool{
				"rich":        true,
				"cloudpickle": true,
				"pyfiglet":    false,
			},
		},
		{
			name: "registry image ignore python injects nothing",
			req: &pb.VerifyImageBuildRequest{
				PythonVersion:    "python3.10",
				ExistingImageUri: baseRef,
				IgnorePython:     true,
			},
			wantModules: map[string]bool{
				"rich":        false,
				"cloudpickle": false,
				"pyfiglet":    false,
			},
		},
		{
			name: "registry image ignore python keeps user packages only",
			req: &pb.VerifyImageBuildRequest{
				PythonVersion:    "python3.10",
				PythonPackages:   []string{"pyfiglet==1.0.2"},
				ExistingImageUri: baseRef,
				IgnorePython:     true,
			},
			wantModules: map[string]bool{
				"rich":        false,
				"cloudpickle": false,
				"pyfiglet":    true,
			},
		},
		{
			name: "custom dockerfile injects runtime requirements",
			req: &pb.VerifyImageBuildRequest{
				PythonVersion: "python3.10",
				Dockerfile:    customDockerfile("custom-runtime"),
			},
			wantModules: map[string]bool{
				"rich":        true,
				"cloudpickle": true,
				"pyfiglet":    false,
			},
			wantFiles: map[string]string{
				"/opt/beta9-test/mode": "custom-runtime",
			},
		},
		{
			name: "custom dockerfile ignore python injects nothing",
			req: &pb.VerifyImageBuildRequest{
				PythonVersion: "python3.10",
				Dockerfile:    customDockerfile("custom-no-python"),
				IgnorePython:  true,
			},
			wantModules: map[string]bool{
				"rich":        false,
				"cloudpickle": false,
				"pyfiglet":    false,
			},
			wantFiles: map[string]string{
				"/opt/beta9-test/mode": "custom-no-python",
			},
		},
		{
			name: "custom dockerfile ignore python keeps user packages only",
			req: &pb.VerifyImageBuildRequest{
				PythonVersion:  "python3.10",
				PythonPackages: []string{"pyfiglet==1.0.2"},
				Dockerfile:     customDockerfile("custom-user-package"),
				IgnorePython:   true,
			},
			wantModules: map[string]bool{
				"rich":        false,
				"cloudpickle": false,
				"pyfiglet":    true,
			},
			wantFiles: map[string]string{
				"/opt/beta9-test/mode": "custom-user-package",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dockerfile := prepareV2DockerfileForIntegration(t, tt.req)
			imageRef := baseRef
			if dockerfile != "" {
				imageRef = dockerBuildFromDockerfile(t, dockerfile)
			}

			check := dockerRunImageChecks(t, imageRef, moduleNames(tt.wantModules), fileNames(tt.wantFiles))
			for moduleName, want := range tt.wantModules {
				require.Equalf(t, want, check.Modules[moduleName], "module %s availability", moduleName)
			}
			for path, want := range tt.wantFiles {
				require.Equalf(t, want, check.Files[path], "file %s content", path)
			}
		})
	}
}

func prepareV2DockerfileForIntegration(t *testing.T, req *pb.VerifyImageBuildRequest) string {
	t.Helper()

	cfg := types.AppConfig{
		ImageService: types.ImageServiceConfig{
			PythonVersion: "python3.10",
			ClipVersion:   uint32(types.ClipVersion2),
			Runner: types.RunnerConfig{
				BaseImageName:     "beta9-runner",
				BaseImageRegistry: "registry.localhost:5000",
				Tags: map[string]string{
					"python3.10": "py310-latest",
				},
				PythonStandalone: types.PythonStandaloneConfig{
					Versions: map[string]string{
						"python3.10": "3.10.15",
					},
					InstallScriptTemplate: "true",
				},
			},
		},
	}
	is := &ContainerImageService{
		config: cfg,
		builder: &Builder{
			config: cfg,
		},
		baseImageDigests: newBaseImageDigestCache(),
	}

	opts, err := is.buildOptionsFromVerifyRequest(context.Background(), req)
	require.NoError(t, err)
	require.NoError(t, is.prepareBuildOptionsForImageID(context.Background(), req, opts))
	return opts.Dockerfile
}

func requireDocker(t *testing.T) {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	cmd := exec.Command("docker", "version", "--format", "{{.Server.Version}}")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("docker daemon is not available: %v\n%s", err, output)
	}
}

func dockerPythonBaseRef(t *testing.T) string {
	t.Helper()

	baseImage := os.Getenv("BETA9_IMAGE_BUILD_BASE")
	if baseImage == "" {
		baseImage = "python:3.10-slim"
	}

	platform := dockerBuildPlatform()
	runDocker(t, "pull", "--platform", platform, baseImage)
	output := runDocker(t, "image", "inspect", "--format", "{{json .RepoDigests}}", baseImage)

	var repoDigests []string
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(output)), &repoDigests))
	require.NotEmpty(t, repoDigests, "base image must expose a repo digest")

	ref := repoDigests[0]
	if strings.HasPrefix(ref, "python@") {
		return "docker.io/library/" + ref
	}
	if !strings.Contains(strings.Split(ref, "@")[0], "/") {
		return "docker.io/library/" + ref
	}
	return ref
}

func dockerBuildFromDockerfile(t *testing.T, dockerfile string) string {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0o644))

	tag := "beta9-image-build-it:" + stableDockerTag(t.Name(), dockerfile)
	runDocker(t, "build", "--platform", dockerBuildPlatform(), "--quiet", "--tag", tag, dir)
	t.Cleanup(func() {
		_ = exec.Command("docker", "image", "rm", "--force", tag).Run()
	})
	return tag
}

func dockerRunImageChecks(t *testing.T, imageRef string, modules []string, files []string) imageBuildContainerCheck {
	t.Helper()

	modulesJSON, err := json.Marshal(modules)
	require.NoError(t, err)
	filesJSON, err := json.Marshal(files)
	require.NoError(t, err)

	script := fmt.Sprintf(`
import importlib.util
import json
from pathlib import Path

modules = %s
files = %s

print(json.dumps({
    "modules": {name: importlib.util.find_spec(name) is not None for name in modules},
    "files": {
        path: Path(path).read_text().strip() if Path(path).exists() else ""
        for path in files
    },
}))
`, modulesJSON, filesJSON)

	output := runDocker(t, "run", "--platform", dockerBuildPlatform(), "--rm", imageRef, "python3.10", "-c", script)

	var check imageBuildContainerCheck
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(output)), &check))
	return check
}

func runDocker(t *testing.T, args ...string) string {
	t.Helper()

	cmd := exec.Command("docker", args...)
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "docker %s failed\n%s", strings.Join(args, " "), output)
	return string(output)
}

func moduleNames(modules map[string]bool) []string {
	names := make([]string, 0, len(modules))
	for name := range modules {
		names = append(names, name)
	}
	return names
}

func fileNames(files map[string]string) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	return names
}

func dockerBuildPlatform() string {
	if platform := os.Getenv(imageBuildPlatformEnv); platform != "" {
		return platform
	}
	return "linux/amd64"
}

func stableDockerTag(name string, dockerfile string) string {
	cleanName := regexp.MustCompile(`[^a-zA-Z0-9_.-]+`).ReplaceAllString(strings.ToLower(name), "-")
	cleanName = strings.Trim(cleanName, "-")
	if cleanName == "" {
		cleanName = "case"
	}
	if len(cleanName) > 80 {
		cleanName = cleanName[:80]
	}

	sum := sha1.Sum([]byte(name + "\x00" + dockerfile))
	return cleanName + "-" + hex.EncodeToString(sum[:])[:12]
}
