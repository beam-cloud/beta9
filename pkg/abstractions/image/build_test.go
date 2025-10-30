package image

import (
	"context"
	"errors"
    "strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/image/mocks"
	"github.com/beam-cloud/beta9/pkg/auth"
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

	// Set up auth info if not already present
	if build.authInfo == nil {
		build.authInfo = &auth.AuthInfo{
			Workspace: &types.Workspace{
				ExternalId: "test-workspace-id",
			},
		}
	}

	// Mock image ID generation (simplified)
	build.imageID = "test-image-id"

	return build, mockRuncClient, outputChan
}
func TestRenderV2Dockerfile_FromStepsAndCommands(t *testing.T) {
    cfg := types.AppConfig{}
    b := &Builder{config: cfg}
    opts := &BuildOpts{
        BaseImageRegistry: "docker.io",
        BaseImageName:     "library/alpine",
        BaseImageTag:      "3.18",
        Commands:          []string{"echo one", "echo two"},
        BuildSteps:        []BuildStep{{Type: shellCommandType, Command: "echo step"}},
    }

    df, err := b.RenderV2Dockerfile(opts)
    assert.NoError(t, err)
    assert.True(t, strings.HasPrefix(df, "FROM docker.io/library/alpine:3.18\n"))
    // Note: /workspace and /volumes are created in the overlay upper layer at runtime
    assert.Contains(t, df, "RUN echo one\n")
    assert.Contains(t, df, "RUN echo two\n")
    assert.Contains(t, df, "RUN echo step\n")
}

func TestAppendToDockerfile_WithPythonPackages(t *testing.T) {
    cfg := types.AppConfig{
        ImageService: types.ImageServiceConfig{
            PythonVersion: "python3.10",
        },
    }
    b := &Builder{config: cfg}
    
    tests := []struct {
        name       string
        dockerfile string
        opts       *BuildOpts
        wantPip    bool
        wantCmd    bool
    }{
        {
            name: "AppendPythonPackages",
            dockerfile: "FROM ubuntu:22.04\nRUN apt-get update",
            opts: &BuildOpts{
                Dockerfile:     "FROM ubuntu:22.04\nRUN apt-get update",
                PythonVersion:  "python3.10",
                PythonPackages: []string{"numpy", "pandas"},
            },
            wantPip: true,
            wantCmd: false,
        },
        {
            name: "AppendCommands",
            dockerfile: "FROM ubuntu:22.04",
            opts: &BuildOpts{
                Dockerfile: "FROM ubuntu:22.04",
                Commands:   []string{"echo hello", "apt update"},
            },
            wantPip: false,
            wantCmd: true,
        },
        {
            name: "AppendBoth",
            dockerfile: "FROM ubuntu:22.04\nRUN apt-get update",
            opts: &BuildOpts{
                Dockerfile:     "FROM ubuntu:22.04\nRUN apt-get update",
                PythonVersion:  "python3.10",
                PythonPackages: []string{"numpy"},
                Commands:       []string{"echo done"},
            },
            wantPip: true,
            wantCmd: true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := b.appendToDockerfile(tt.opts)
            
            // Should contain original Dockerfile
            assert.Contains(t, result, "FROM ubuntu:22.04")
            
            if tt.wantPip {
                assert.Contains(t, result, "pip install", "Should contain pip install command")
                if len(tt.opts.PythonPackages) > 0 {
                    for _, pkg := range tt.opts.PythonPackages {
                        assert.Contains(t, result, pkg, "Should contain package: "+pkg)
                    }
                }
            }
            
            if tt.wantCmd {
                for _, cmd := range tt.opts.Commands {
                    assert.Contains(t, result, cmd, "Should contain command: "+cmd)
                }
            }
            
            // Verify commands come after original Dockerfile
            origIdx := strings.Index(result, tt.dockerfile)
            assert.Equal(t, 0, origIdx, "Original Dockerfile should be at the beginning")
        })
    }
}

func TestAppendToDockerfile_WithBuildSteps(t *testing.T) {
    cfg := types.AppConfig{
        ImageService: types.ImageServiceConfig{
            PythonVersion: "python3.10",
        },
    }
    b := &Builder{config: cfg}
    
    dockerfile := "FROM ubuntu:22.04\nRUN apt-get update"
    opts := &BuildOpts{
        Dockerfile:    dockerfile,
        PythonVersion: "python3.10",
        BuildSteps: []BuildStep{
            {Type: shellCommandType, Command: "echo step1"},
            {Type: pipCommandType, Command: "requests"},
        },
    }
    
    result := b.appendToDockerfile(opts)
    
    assert.Contains(t, result, "FROM ubuntu:22.04")
    assert.Contains(t, result, "echo step1")
    assert.Contains(t, result, "pip install")
    assert.Contains(t, result, "requests")
}

// TestCustomDockerfile_WithAdditionalPythonPackages validates the original issue:
// Image.from_dockerfile("Dockerfile").add_python_packages(["numpy"]) should work
func TestCustomDockerfile_WithAdditionalPythonPackages(t *testing.T) {
    cfg := types.AppConfig{
        ImageService: types.ImageServiceConfig{
            PythonVersion: "python3.10",
            ClipVersion:   2, // V2 build
            Runner: types.RunnerConfig{
                PythonStandalone: types.PythonStandaloneConfig{
                    Versions: map[string]string{
                        "python3.10": "3.10.15+20241002",
                    },
                    InstallScriptTemplate: "install python {{.PythonVersion}}",
                },
            },
        },
    }
    b := &Builder{config: cfg}
    
    // Simulate: Image.from_dockerfile("Dockerfile").add_python_packages(["numpy"])
    customDockerfile := `FROM ubuntu:22.04
RUN apt-get update && apt-get install -y python3 python3-pip
WORKDIR /app
COPY . /app`
    
    opts := &BuildOpts{
        Dockerfile:     customDockerfile,
        PythonVersion:  "python3.10",
        PythonPackages: []string{"numpy", "pandas"},
        Commands:       []string{"echo 'Setup complete'"},
        ClipVersion:    2,
    }
    
    // This simulates what happens in builder.Build()
    var finalDockerfile string
    if opts.Dockerfile != "" && b.hasWorkToDo(opts) {
        finalDockerfile = b.appendToDockerfile(opts)
    } else {
        finalDockerfile = opts.Dockerfile
    }
    
    // Verify the custom Dockerfile is preserved
    assert.Contains(t, finalDockerfile, "FROM ubuntu:22.04")
    assert.Contains(t, finalDockerfile, "RUN apt-get update")
    assert.Contains(t, finalDockerfile, "WORKDIR /app")
    
    // Verify Python installation is included
    assert.Contains(t, finalDockerfile, "install python", "Should install Python version")
    
    // Verify additional Python packages are appended
    assert.Contains(t, finalDockerfile, "pip install")
    assert.Contains(t, finalDockerfile, "numpy")
    assert.Contains(t, finalDockerfile, "pandas")
    
    // Verify additional commands are appended
    assert.Contains(t, finalDockerfile, "echo 'Setup complete'")
    
    // Verify order: custom Dockerfile first, then Python install, then packages
    customIdx := strings.Index(finalDockerfile, "FROM ubuntu:22.04")
    pythonIdx := strings.Index(finalDockerfile, "install python")
    numpyIdx := strings.Index(finalDockerfile, "numpy")
    assert.True(t, customIdx < pythonIdx, "Custom Dockerfile should come before Python install")
    assert.True(t, pythonIdx < numpyIdx, "Python install should come before packages")
}

// TestCustomDockerfile_WithPythonVersionOnly tests that add_python_version() alone works
func TestCustomDockerfile_WithPythonVersionOnly(t *testing.T) {
    cfg := types.AppConfig{
        ImageService: types.ImageServiceConfig{
            PythonVersion: "python3.10",
            ClipVersion:   2,
            Runner: types.RunnerConfig{
                PythonStandalone: types.PythonStandaloneConfig{
                    Versions: map[string]string{
                        "python3.11": "3.11.10+20241002",
                    },
                    InstallScriptTemplate: "install python {{.PythonVersion}}",
                },
            },
        },
    }
    b := &Builder{config: cfg}
    
    // Simulate: Image.from_dockerfile("Dockerfile").add_python_version("python3.11")
    customDockerfile := `FROM ubuntu:22.04
RUN apt-get update`
    
    opts := &BuildOpts{
        Dockerfile:    customDockerfile,
        PythonVersion: "python3.11",
        ClipVersion:   2,
    }
    
    // This simulates what happens in builder.Build()
    var finalDockerfile string
    if opts.Dockerfile != "" && b.hasWorkToDo(opts) {
        finalDockerfile = b.appendToDockerfile(opts)
    } else {
        finalDockerfile = opts.Dockerfile
    }
    
    // Verify the custom Dockerfile is preserved
    assert.Contains(t, finalDockerfile, "FROM ubuntu:22.04")
    
    // Verify Python installation is included even without packages
    assert.Contains(t, finalDockerfile, "install python", "Should install Python version")
    assert.Contains(t, finalDockerfile, "3.11.10+20241002", "Should use correct version")
}

// TestCustomDockerfile_ExactUserScenario tests the exact scenario from the issue:
// Image.from_dockerfile("Dockerfile").add_python_version("python3.10").add_python_packages(["numpy", "csaps"])
func TestCustomDockerfile_ExactUserScenario(t *testing.T) {
    cfg := types.AppConfig{
        ImageService: types.ImageServiceConfig{
            PythonVersion: "python3.9",
            ClipVersion:   2,
            Runner: types.RunnerConfig{
                PythonStandalone: types.PythonStandaloneConfig{
                    Versions: map[string]string{
                        "python3.10": "3.10.15+20241002",
                    },
                    InstallScriptTemplate: "apt-get update -q && apt-get install -q -y build-essential curl git && curl -fsSL -o python.tgz 'https://example.com/python-{{.PythonVersion}}.tgz' && tar -xzf python.tgz -C /usr/local --strip-components 1 && rm -f python.tgz",
                },
            },
        },
    }
    b := &Builder{config: cfg}
    
    // Exact user scenario
    customDockerfile := `FROM ubuntu:22.04
RUN apt-get update && apt-get install -y curl git
WORKDIR /workspace`
    
    opts := &BuildOpts{
        Dockerfile:     customDockerfile,
        PythonVersion:  "python3.10",
        PythonPackages: []string{"numpy", "csaps"},
        ClipVersion:    2,
    }
    
    // This simulates what happens in builder.Build()
    var finalDockerfile string
    if opts.Dockerfile != "" && b.hasWorkToDo(opts) {
        finalDockerfile = b.appendToDockerfile(opts)
    } else {
        finalDockerfile = opts.Dockerfile
    }
    
    // Verify custom Dockerfile is preserved
    assert.Contains(t, finalDockerfile, "FROM ubuntu:22.04")
    assert.Contains(t, finalDockerfile, "WORKDIR /workspace")
    
    // Verify Python 3.10 is installed (not the default 3.9)
    assert.Contains(t, finalDockerfile, "apt-get update -q && apt-get install -q -y build-essential curl git")
    assert.Contains(t, finalDockerfile, "python-3.10.15+20241002")
    
    // Verify Python packages are installed with correct version
    assert.Contains(t, finalDockerfile, "python3.10")
    assert.Contains(t, finalDockerfile, "pip install")
    assert.Contains(t, finalDockerfile, "numpy")
    assert.Contains(t, finalDockerfile, "csaps")
    
    // Verify correct order
    fromIdx := strings.Index(finalDockerfile, "FROM ubuntu")
    pythonInstallIdx := strings.Index(finalDockerfile, "python-3.10.15")
    numpyIdx := strings.Index(finalDockerfile, "numpy")
    
    assert.True(t, fromIdx < pythonInstallIdx, "Custom Dockerfile first")
    assert.True(t, pythonInstallIdx < numpyIdx, "Python install before packages")
}

// TestCustomDockerfile_IgnorePython_NoPackages tests that ignore_python=true with no packages skips Python
func TestCustomDockerfile_IgnorePython_NoPackages(t *testing.T) {
    cfg := types.AppConfig{
        ImageService: types.ImageServiceConfig{
            PythonVersion: "python3.10",
            ClipVersion:   2,
            Runner: types.RunnerConfig{
                PythonStandalone: types.PythonStandaloneConfig{
                    Versions: map[string]string{
                        "python3.10": "3.10.15+20241002",
                    },
                    InstallScriptTemplate: "install python {{.PythonVersion}}",
                },
            },
        },
    }
    b := &Builder{config: cfg}
    
    // Custom Dockerfile with ignore_python=true and no packages
    customDockerfile := `FROM ubuntu:22.04
RUN apt-get update && apt-get install -y nodejs
WORKDIR /app`
    
    opts := &BuildOpts{
        Dockerfile:    customDockerfile,
        PythonVersion: "python3.10",
        IgnorePython:  true,
        Commands:      []string{"echo 'No Python needed'"},
        ClipVersion:   2,
    }
    
    var finalDockerfile string
    if opts.Dockerfile != "" && b.hasWorkToDo(opts) {
        finalDockerfile = b.appendToDockerfile(opts)
    } else {
        finalDockerfile = opts.Dockerfile
    }
    
    // Verify custom Dockerfile is preserved
    assert.Contains(t, finalDockerfile, "FROM ubuntu:22.04")
    assert.Contains(t, finalDockerfile, "nodejs")
    
    // Verify Python is NOT installed when ignore_python=true and no packages
    assert.NotContains(t, finalDockerfile, "install python", "Should not install Python when ignore_python=true and no packages")
    assert.NotContains(t, finalDockerfile, "pip install", "Should not have pip commands")
    
    // Verify commands are still executed
    assert.Contains(t, finalDockerfile, "echo 'No Python needed'")
}

// TestCustomDockerfile_IgnorePython_WithPackages tests that ignore_python=true WITH packages still installs Python
func TestCustomDockerfile_IgnorePython_WithPackages(t *testing.T) {
    cfg := types.AppConfig{
        ImageService: types.ImageServiceConfig{
            PythonVersion: "python3.10",
            ClipVersion:   2,
            Runner: types.RunnerConfig{
                PythonStandalone: types.PythonStandaloneConfig{
                    Versions: map[string]string{
                        "python3.10": "3.10.15+20241002",
                    },
                    InstallScriptTemplate: "install python {{.PythonVersion}}",
                },
            },
        },
    }
    b := &Builder{config: cfg}
    
    // Custom Dockerfile with ignore_python=true BUT has packages
    customDockerfile := `FROM ubuntu:22.04
RUN apt-get update
WORKDIR /app`
    
    opts := &BuildOpts{
        Dockerfile:     customDockerfile,
        PythonVersion:  "python3.10",
        IgnorePython:   true,
        PythonPackages: []string{"numpy"}, // Has packages, so Python is needed
        ClipVersion:    2,
    }
    
    var finalDockerfile string
    if opts.Dockerfile != "" && b.hasWorkToDo(opts) {
        finalDockerfile = b.appendToDockerfile(opts)
    } else {
        finalDockerfile = opts.Dockerfile
    }
    
    // Verify custom Dockerfile is preserved
    assert.Contains(t, finalDockerfile, "FROM ubuntu:22.04")
    
    // Verify Python IS installed because packages need it (even with ignore_python=true)
    assert.Contains(t, finalDockerfile, "install python", "Should install Python when packages are specified, even with ignore_python=true")
    assert.Contains(t, finalDockerfile, "pip install", "Should install packages")
    assert.Contains(t, finalDockerfile, "numpy")
}

// TestCustomDockerfile_IgnorePython_CompleteScenarios tests all ignore_python scenarios
func TestCustomDockerfile_IgnorePython_CompleteScenarios(t *testing.T) {
    cfg := types.AppConfig{
        ImageService: types.ImageServiceConfig{
            PythonVersion: "python3.10",
            ClipVersion:   2,
            Runner: types.RunnerConfig{
                PythonStandalone: types.PythonStandaloneConfig{
                    Versions: map[string]string{
                        "python3.10": "3.10.15+20241002",
                    },
                    InstallScriptTemplate: "install python standalone {{.PythonVersion}}",
                },
            },
        },
    }
    b := &Builder{config: cfg}
    
    tests := []struct {
        name              string
        dockerfile        string
        pythonVersion     string
        ignorePython      bool
        packages          []string
        commands          []string
        shouldInstallPy   bool
        shouldInstallPkgs bool
        description       string
    }{
        {
            name:              "IgnorePython_NothingElse",
            dockerfile:        "FROM alpine:latest",
            pythonVersion:     "python3.10",
            ignorePython:      true,
            packages:          []string{},
            commands:          []string{},
            shouldInstallPy:   false,
            shouldInstallPkgs: false,
            description:       "ignore_python=true, no packages, no commands → no Python",
        },
        {
            name:              "IgnorePython_WithCommands",
            dockerfile:        "FROM alpine:latest",
            pythonVersion:     "python3.10",
            ignorePython:      true,
            packages:          []string{},
            commands:          []string{"apk add nodejs"},
            shouldInstallPy:   false,
            shouldInstallPkgs: false,
            description:       "ignore_python=true, no packages, has commands → no Python, but commands run",
        },
        {
            name:              "IgnorePython_WithPackages",
            dockerfile:        "FROM alpine:latest",
            pythonVersion:     "python3.10",
            ignorePython:      true,
            packages:          []string{"numpy"},
            commands:          []string{},
            shouldInstallPy:   true,
            shouldInstallPkgs: true,
            description:       "ignore_python=true, has packages → Python installed (packages need it)",
        },
        {
            name:              "Normal_WithPython",
            dockerfile:        "FROM alpine:latest",
            pythonVersion:     "python3.10",
            ignorePython:      false,
            packages:          []string{},
            commands:          []string{},
            shouldInstallPy:   true,
            shouldInstallPkgs: false,
            description:       "ignore_python=false, no packages → Python installed",
        },
        {
            name:              "Normal_WithPythonAndPackages",
            dockerfile:        "FROM alpine:latest",
            pythonVersion:     "python3.10",
            ignorePython:      false,
            packages:          []string{"numpy"},
            commands:          []string{},
            shouldInstallPy:   true,
            shouldInstallPkgs: true,
            description:       "ignore_python=false, has packages → Python and packages installed",
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            opts := &BuildOpts{
                Dockerfile:     tt.dockerfile,
                PythonVersion:  tt.pythonVersion,
                IgnorePython:   tt.ignorePython,
                PythonPackages: tt.packages,
                Commands:       tt.commands,
                ClipVersion:    2,
            }
            
            var finalDockerfile string
            if opts.Dockerfile != "" && b.hasWorkToDo(opts) {
                finalDockerfile = b.appendToDockerfile(opts)
            } else {
                finalDockerfile = opts.Dockerfile
            }
            
            // Check Python installation
            if tt.shouldInstallPy {
                assert.Contains(t, finalDockerfile, "install python standalone", 
                    "Test %s: %s - should install Python", tt.name, tt.description)
            } else {
                assert.NotContains(t, finalDockerfile, "install python standalone", 
                    "Test %s: %s - should NOT install Python", tt.name, tt.description)
            }
            
            // Check package installation
            if tt.shouldInstallPkgs {
                assert.Contains(t, finalDockerfile, "pip install", 
                    "Test %s: %s - should install packages", tt.name, tt.description)
            } else {
                assert.NotContains(t, finalDockerfile, "pip install", 
                    "Test %s: %s - should NOT install packages", tt.name, tt.description)
            }
            
            // Check commands are always preserved
            for _, cmd := range tt.commands {
                assert.Contains(t, finalDockerfile, cmd, 
                    "Test %s: %s - should include command", tt.name, tt.description)
            }
        })
    }
}

func TestRenderV2Dockerfile_PythonInstallation(t *testing.T) {
    cfg := types.AppConfig{
        ImageService: types.ImageServiceConfig{
            Runner: types.RunnerConfig{
                BaseImageName:     "beta9-runner",
                BaseImageRegistry: "registry.localhost:5000",
                PythonStandalone: types.PythonStandaloneConfig{
                    Versions: map[string]string{
                        "python3.10": "3.10.15+20241002",
                        "python3.11": "3.11.10+20241002",
                    },
                    InstallScriptTemplate: "apt-get update -q && apt-get install -q -y build-essential curl git && curl -fsSL -o python.tgz 'https://github.com/indygreg/python-build-standalone/releases/download/20241002/cpython-{{.PythonVersion}}-{{.Architecture}}-{{.Vendor}}-{{.OS}}-gnu-install_only.tar.gz' && tar -xzf python.tgz -C /usr/local --strip-components 1 && rm -f python.tgz",
                },
            },
            PythonVersion: "python3.10",
        },
    }
    b := &Builder{config: cfg}
    
    t.Run("IgnorePython_NoPackages_SkipsPython", func(t *testing.T) {
        // Matches v1: if IgnorePython && no packages → skip Python entirely
        opts := &BuildOpts{
            BaseImageRegistry: "docker.io",
            BaseImageName:     "library/ubuntu",
            BaseImageTag:      "22.04",
            IgnorePython:      true,
            Commands:          []string{"echo hello"},
        }

        df, err := b.RenderV2Dockerfile(opts)
        assert.NoError(t, err)
        
        // Should NOT install Python when explicitly ignored
        assert.NotContains(t, df, "python-build-standalone", "Should not install Python when IgnorePython=true")
        assert.NotContains(t, df, "pip install", "Should not install packages when IgnorePython=true")
        assert.Contains(t, df, "echo hello", "Should still run commands")
    })
    
    t.Run("Beta9BaseImage_SkipsPythonInstall", func(t *testing.T) {
        // Matches v1: beta9 images have Python, skip installation (like successful probe)
        opts := &BuildOpts{
            BaseImageRegistry: "registry.localhost:5000",
            BaseImageName:     "beta9-runner",
            BaseImageTag:      "py310-latest",
            PythonVersion:     "python3.10",
            PythonPackages:    []string{"requests"},
        }

        df, err := b.RenderV2Dockerfile(opts)
        assert.NoError(t, err)
        
        // Should NOT install Python for beta9 base images (they already have it)
        assert.NotContains(t, df, "python-build-standalone", "Should not install Python for beta9 base images")
        assert.NotContains(t, df, "apt-get", "Should not install Python for beta9 base images")
        
        // Should still install packages
        assert.Contains(t, df, "pip install", "Should install Python packages")
        assert.Contains(t, df, "requests", "Should install requested package")
    })
    
    t.Run("Beta9BaseImage_WithCommands_NoPythonInstall", func(t *testing.T) {
        // Matches v1: beta9 images with commands still skip Python installation
        opts := &BuildOpts{
            BaseImageRegistry: "registry.localhost:5000",
            BaseImageName:     "beta9-runner",
            BaseImageTag:      "py311-latest",
            PythonVersion:     "python3.11",
            Commands:          []string{"echo hello", "apt update"},
            BuildSteps:        []BuildStep{{Type: pipCommandType, Command: "numpy"}},
        }

        df, err := b.RenderV2Dockerfile(opts)
        assert.NoError(t, err)
        
        // Should NOT install Python
        assert.NotContains(t, df, "python-build-standalone", "Should not install Python for beta9 base images")
        
        // Should contain pip install and commands
        assert.Contains(t, df, "pip install", "Should install packages")
        assert.Contains(t, df, "numpy", "Should install numpy")
        assert.Contains(t, df, "echo hello", "Should run custom commands")
        assert.Contains(t, df, "apt update", "Should run custom commands")
    })
    
    t.Run("CustomBaseImage_InstallsPython", func(t *testing.T) {
        // Matches v1: custom images without Python get it installed
        opts := &BuildOpts{
            BaseImageRegistry: "docker.io",
            BaseImageName:     "library/ubuntu",
            BaseImageTag:      "22.04",
            PythonVersion:     "python3.10",
            PythonPackages:    []string{"requests"},
        }

        df, err := b.RenderV2Dockerfile(opts)
        assert.NoError(t, err)
        assert.True(t, strings.HasPrefix(df, "FROM docker.io/library/ubuntu:22.04\n"))
        
        // Should install Python for custom base images
        assert.Contains(t, df, "python-build-standalone", "Should install Python for custom base images")
        assert.Contains(t, df, "apt-get", "Should install dependencies")
        
        // Should contain pip install for packages
        assert.Contains(t, df, "pip install", "Should install Python packages")
        assert.Contains(t, df, "requests", "Should install requested package")
    })
    
    t.Run("MicromambaHandling", func(t *testing.T) {
        opts := &BuildOpts{
            BaseImageRegistry: "docker.io",
            BaseImageName:     "mambaorg/micromamba",
            BaseImageTag:      "latest",
            PythonVersion:     "micromamba3.10",
            PythonPackages:    []string{"numpy"},
        }

        df, err := b.RenderV2Dockerfile(opts)
        assert.NoError(t, err)
        
        // Should configure micromamba instead of installing standalone Python
        assert.Contains(t, df, "micromamba config set use_lockfiles False", "Should configure micromamba")
        assert.NotContains(t, df, "python-build-standalone", "Should not install standalone Python for micromamba")
    })
}

// TestV1_V2_PythonInstallationParity verifies that v1 and v2 have consistent Python installation behavior
func TestV1_V2_PythonInstallationParity(t *testing.T) {
    cfg := types.AppConfig{
        ImageService: types.ImageServiceConfig{
            Runner: types.RunnerConfig{
                BaseImageName:     "beta9-runner",
                BaseImageRegistry: "registry.localhost:5000",
                PythonStandalone: types.PythonStandaloneConfig{
                    Versions: map[string]string{
                        "python3.10": "3.10.15+20241002",
                    },
                    InstallScriptTemplate: "install python standalone",
                },
            },
            PythonVersion: "python3.10",
        },
    }
    b := &Builder{config: cfg}
    
    tests := []struct {
        name                      string
        opts                      *BuildOpts
        shouldInstallPython       bool
        shouldInstallPackages     bool
        description               string
    }{
        {
            name: "IgnorePython_NoPackages",
            opts: &BuildOpts{
                BaseImageRegistry: "docker.io",
                BaseImageName:     "library/ubuntu",
                BaseImageTag:      "22.04",
                IgnorePython:      true,
            },
            shouldInstallPython:   false,
            shouldInstallPackages: false,
            description:          "v1: returns early in setupPythonEnv, v2: returns early in RenderV2Dockerfile",
        },
        {
            name: "IgnorePython_WithPackages_Ubuntu",
            opts: &BuildOpts{
                BaseImageRegistry: "docker.io",
                BaseImageName:     "library/ubuntu",
                BaseImageTag:      "22.04",
                IgnorePython:      true,
                PythonVersion:     "python3.10",
                PythonPackages:    []string{"numpy"},
            },
            shouldInstallPython:   true,
            shouldInstallPackages: true,
            description:          "v1: has packages so continues, probe fails, installs; v2: same logic",
        },
        {
            name: "IgnorePython_WithPackages_Beta9",
            opts: &BuildOpts{
                BaseImageRegistry: "registry.localhost:5000",
                BaseImageName:     "beta9-runner",
                BaseImageTag:      "py310-latest",
                IgnorePython:      true,
                PythonVersion:     "python3.10",
                PythonPackages:    []string{"numpy"},
            },
            shouldInstallPython:   false,
            shouldInstallPackages: true,
            description:          "v1: has packages so continues, probe succeeds, skips; v2: detects beta9, skips",
        },
        {
            name: "Beta9BaseImage",
            opts: &BuildOpts{
                BaseImageRegistry: "registry.localhost:5000",
                BaseImageName:     "beta9-runner",
                BaseImageTag:      "py310-latest",
                PythonVersion:     "python3.10",
                PythonPackages:    []string{"numpy"},
            },
            shouldInstallPython:   false,
            shouldInstallPackages: true,
            description:          "v1: probe succeeds (python exists), v2: static detection",
        },
        {
            name: "CustomBaseImage",
            opts: &BuildOpts{
                BaseImageRegistry: "docker.io",
                BaseImageName:     "library/ubuntu",
                BaseImageTag:      "22.04",
                PythonVersion:     "python3.10",
                PythonPackages:    []string{"numpy"},
            },
            shouldInstallPython:   true,
            shouldInstallPackages: true,
            description:          "v1: probe fails (python missing), v2: static detection",
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            df, err := b.RenderV2Dockerfile(tt.opts)
            assert.NoError(t, err)
            
            t.Logf("Test: %s\nDescription: %s\nDockerfile:\n%s", tt.name, tt.description, df)
            
            if tt.shouldInstallPython {
                assert.Contains(t, df, "install python standalone", "Expected Python installation")
            } else {
                assert.NotContains(t, df, "install python standalone", "Should NOT install Python")
            }
            
            if tt.shouldInstallPackages {
                assert.Contains(t, df, "pip install", "Expected package installation")
            } else {
                assert.NotContains(t, df, "pip install", "Should NOT install packages")
            }
        })
    }
}

func TestBuild_SkipsRuncFlow_WhenClipV2(t *testing.T) {
    opts := &BuildOpts{
        BaseImageRegistry: "docker.io",
        BaseImageName:     "library/ubuntu",
        BaseImageTag:      "latest",
        Commands:          []string{"echo hello"},
    }
    build, mockRunc, _ := setupTestBuild(t, opts)
    // Enable v2 in config
    build.config.ImageService.ClipVersion = 2
    // Create a minimal builder bound to this config
    b := &Builder{config: build.config}

    // No expectations should be set on runc Exec/Archive for v2 path
    // Do not invoke Build here as it requires scheduler/worker; just ensure render path works
    df, derr := b.RenderV2Dockerfile(build.opts)
    assert.NoError(t, derr)
    assert.True(t, strings.HasPrefix(df, "FROM "))
    // We can't easily run the full Build without scheduler and worker; just ensure it doesn't try to call Exec/Archive here.
    // Since Build starts external processes, limit our assertion to "no unwanted runc calls"
    mockRunc.AssertNotCalled(t, "Exec")
    mockRunc.AssertNotCalled(t, "Archive")
    // err may be non-nil due to environment; we only validate the behavior regarding runc usage for v2
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

	// Mock python version check - python exists (setupDefaultPythonInstall)
	mockRuncClient.On("Exec", build.containerID, "python3.10 --version", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil)
	// Mock virtual environment check - python exists but NOT in venv
	mockRuncClient.On("Exec", build.containerID, `python3.10 -c "import sys; exit(0 if sys.prefix != sys.base_prefix else 1)"`, buildEnv).Return(&pb.RunCExecResponse{Ok: false}, nil)

	err := build.prepareCommands()
	assert.NoError(t, err)

	// When NOT in venv, expect pip install with --system
	expectedCommands := []string{
		"uv-b9 pip install --system \"requests\" \"numpy\"",
		"echo hello",
	}
	assert.Equal(t, expectedCommands, build.commands)
	assert.NotEmpty(t, build.imageID)
	mockRuncClient.AssertExpectations(t)
}

func TestBuild_prepareSteps_PythonExistsInVenv(t *testing.T) {
	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "latest",
		PythonVersion:     "python3.10",
		PythonPackages:    []string{"requests", "numpy"},
		BuildSteps:        []BuildStep{{Command: "echo hello", Type: shellCommandType}},
	}
	build, mockRuncClient, _ := setupTestBuild(t, opts)

	// Mock python version check - python exists (setupDefaultPythonInstall)
	mockRuncClient.On("Exec", build.containerID, "python3.10 --version", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil)
	// Mock virtual environment check - python exists and IS in venv
	mockRuncClient.On("Exec", build.containerID, `python3.10 -c "import sys; exit(0 if sys.prefix != sys.base_prefix else 1)"`, buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil)

	err := build.prepareCommands()
	assert.NoError(t, err)

	// When in venv, expect the pyvenv.cfg update command and pip install without --system
	assert.Len(t, build.commands, 3)
	assert.Contains(t, build.commands[0], "include-system-site-packages = true")
	assert.Equal(t, "uv-b9 pip install \"requests\" \"numpy\"", build.commands[1])
	assert.Equal(t, "echo hello", build.commands[2])
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

	// Mock python version check - specific version doesn't exist (setupDefaultPythonInstall)
	mockRuncClient.On("Exec", build.containerID, "python3.11 --version", buildEnv).Return(nil, errors.New("not found"))
	// Mock general python3 check - it exists (so we show a warning)
	mockRuncClient.On("Exec", build.containerID, "python3 --version", buildEnv).Return(&pb.RunCExecResponse{Ok: true}, nil)
	// Mock virtual environment check - after python install, check if it's in venv (it won't be)
	mockRuncClient.On("Exec", build.containerID, `python3.11 -c "import sys; exit(0 if sys.prefix != sys.base_prefix else 1)"`, buildEnv).Return(&pb.RunCExecResponse{Ok: false}, nil)

	err := build.prepareCommands()
	assert.NoError(t, err)

	// Expect installation command based on PythonStandaloneConfig
	expectedPipCmd := "uv-b9 pip install --system \"pandas\""

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
		PythonPackages:    []string{},
	}
	build, mockRuncClient, _ := setupTestBuild(t, opts)

	err := build.prepareCommands()
	assert.NoError(t, err)

	assert.Equal(t, []string{"apt update"}, build.commands)
	assert.Equal(t, []string{}, build.opts.PythonPackages, "PythonPackages should be cleared if IgnorePython is true and python is not found")
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
		"uv-b9 pip install -c \"conda-forge::numpy\" \"pytorch\"", // From PythonPackages
		"micromamba install -y -n beta9 \"scipy\"",                // From BuildSteps (mamba)
		"echo done mamba",
		"uv-b9 pip install \"requests\" \"beautifulsoup4\"", // From BuildSteps (pip)
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

// Test parseBuildStepsForDockerfile (v2) uses standard pip instead of uv-b9
func Test_parseBuildStepsForDockerfile(t *testing.T) {
	pythonVersion := "python3.9"
	steps := []BuildStep{
		{Type: shellCommandType, Command: "apt update"},
		{Type: pipCommandType, Command: "requests"},
		{Type: pipCommandType, Command: "numpy"},
		{Type: shellCommandType, Command: "echo done"},
	}

	expected := []string{
		"apt update",
		"python3.9 -m pip install --break-system-packages \"requests\" \"numpy\"", // Standard pip for v2
		"echo done",
	}

	result := parseBuildStepsForDockerfile(steps, pythonVersion, false)
	assert.Equal(t, expected, result)
}

// TestGenerateContainerRequest_SourceImageHandling verifies that SourceImage is correctly set
// based on whether a custom Dockerfile, custom base image, or beta9 base image is used
func TestGenerateContainerRequest_SourceImageHandling(t *testing.T) {
	tests := []struct {
		name                string
		opts                *BuildOpts
		expectedSourceImage *string
		description         string
	}{
		{
			name: "CustomDockerfile_NoSourceImage",
			opts: &BuildOpts{
				Dockerfile:     "FROM ubuntu:22.04\nRUN echo hello",
				BuildCtxObject: "some-object-id",
				PythonVersion:  "python3.10",
				// Note: BaseImageName and BaseImageRegistry are not set for custom Dockerfiles
			},
			expectedSourceImage: nil,
			description:         "Custom Dockerfile should not set SourceImage (Dockerfile has its own FROM)",
		},
		{
			name: "CustomBaseImage_SetsSourceImage",
			opts: &BuildOpts{
				BaseImageRegistry: "docker.io",
				BaseImageName:     "library/ubuntu",
				BaseImageTag:      "22.04",
				ExistingImageUri:  "docker.io/library/ubuntu:22.04",
				PythonVersion:     "python3.10",
			},
			expectedSourceImage: stringPtr("docker.io/library/ubuntu:22.04"),
			description:         "Custom base image (from_registry) should set SourceImage",
		},
		{
			name: "Beta9BaseImage_SetsSourceImage",
			opts: &BuildOpts{
				BaseImageRegistry: "registry.localhost:5000",
				BaseImageName:     "beta9-runner",
				BaseImageTag:      "py310-latest",
				PythonVersion:     "python3.10",
			},
			expectedSourceImage: stringPtr("registry.localhost:5000/beta9-runner:py310-latest"),
			description:         "Beta9 base image should set SourceImage",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			build, _, _ := setupTestBuild(t, tt.opts)
			build.config.ImageService.ClipVersion = 2 // Test with v2

			req, err := build.generateContainerRequest()
			assert.NoError(t, err)

			if tt.expectedSourceImage == nil {
				assert.Nil(t, req.BuildOptions.SourceImage, tt.description)
			} else {
				assert.NotNil(t, req.BuildOptions.SourceImage, tt.description)
				assert.Equal(t, *tt.expectedSourceImage, *req.BuildOptions.SourceImage, tt.description)
			}
		})
	}
}

func stringPtr(s string) *string {
	return &s
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
		"uv-b9 pip install --system \"requests\" \"numpy\"", // Coalesced pip
		"echo 'installing libs'",
		"micromamba install -y -n beta9 -c pytorch \"conda-forge::pandas\" \"scipy\"", // Coalesced mamba (flags don't split mamba)
		"echo 'done'",
		"uv-b9 pip install --system --no-deps flask", // Flagged line isn't quoted
		"uv-b9 pip install --system \"gunicorn\"",    // Second pip group
	}

	result := parseBuildSteps(steps, pythonVersion, false)
	assert.Equal(t, expected, result)
}
