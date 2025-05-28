package image

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractImageNameAndTag(t *testing.T) {
	tests := []struct {
		ref          string
		wantTag      string
		wantDigest   string
		wantRepo     string
		wantRegistry string
	}{
		{
			ref: "",
		},
		{
			ref:          "nginx",
			wantTag:      "latest",
			wantRepo:     "nginx",
			wantRegistry: "docker.io",
		},
		{
			ref:          "docker.io/nginx",
			wantTag:      "latest",
			wantRepo:     "nginx",
			wantRegistry: "docker.io",
		},
		{
			ref:          "docker.io/nginx:1.25.3",
			wantTag:      "1.25.3",
			wantRepo:     "nginx",
			wantRegistry: "docker.io",
		},
		{
			ref:          "docker.io/nginx:latest",
			wantTag:      "latest",
			wantRepo:     "nginx",
			wantRegistry: "docker.io",
		},
		{
			ref:          "docker.io/nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04",
			wantTag:      "12.4.1-cudnn-runtime-ubuntu22.04",
			wantRepo:     "nvidia/cuda",
			wantRegistry: "docker.io",
		},
		{
			ref:          "nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04",
			wantTag:      "12.4.1-cudnn-runtime-ubuntu22.04",
			wantRepo:     "nvidia/cuda",
			wantRegistry: "docker.io",
		},
		{
			ref:          "nvidia/cuda@sha256:2fcc4280646484290cc50dce5e65f388dd04352b07cbe89a635703bd1f9aedb6",
			wantDigest:   "sha256:2fcc4280646484290cc50dce5e65f388dd04352b07cbe89a635703bd1f9aedb6",
			wantRepo:     "nvidia/cuda",
			wantRegistry: "docker.io",
		},
		{
			ref:          "registry.localhost:5000/beta9-runner:py311-latest",
			wantTag:      "py311-latest",
			wantRepo:     "beta9-runner",
			wantRegistry: "registry.localhost:5000",
		},
		{
			ref:          "111111111111.dkr.ecr.us-east-1.amazonaws.com/myapp:latest",
			wantTag:      "latest",
			wantRepo:     "myapp",
			wantRegistry: "111111111111.dkr.ecr.us-east-1.amazonaws.com",
		},
		{
			ref:          "111111111111.dkr.ecr.us-east-1.amazonaws.com/myapp/service:latest",
			wantTag:      "latest",
			wantRepo:     "myapp/service",
			wantRegistry: "111111111111.dkr.ecr.us-east-1.amazonaws.com",
		},
		{
			ref:          "nvcr.io/nim/meta/llama-3.1-8b-instruct:1.1.0",
			wantTag:      "1.1.0",
			wantRepo:     "nim/meta/llama-3.1-8b-instruct",
			wantRegistry: "nvcr.io",
		},
		{
			ref:          "ghcr.io/gis-ops/docker-valhalla/valhalla:latest",
			wantTag:      "latest",
			wantRepo:     "gis-ops/docker-valhalla/valhalla",
			wantRegistry: "ghcr.io",
		},
		{
			ref:          "us-east1-docker.pkg.dev/test/ds-us-east1/test-test-test@sha256:c31c45b6fdc3d01c131a6dcae1daed008e3df4001bb43e555e49d82ac8d779e4",
			wantDigest:   "sha256:c31c45b6fdc3d01c131a6dcae1daed008e3df4001bb43e555e49d82ac8d779e4",
			wantRepo:     "test/ds-us-east1/test-test-test",
			wantRegistry: "us-east1-docker.pkg.dev",
		},
	}

	for _, test := range tests {
		t.Run(test.ref, func(t *testing.T) {
			image, err := ExtractImageNameAndTag(test.ref)
			if test.ref == "" {
				assert.Error(t, err)
				return
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, test.wantTag, image.Tag)
			assert.Equal(t, test.wantDigest, image.Digest)
			assert.Equal(t, test.wantRepo, image.Repo)
			assert.Equal(t, test.wantRegistry, image.Registry)
		})
	}
}

func TestExtractPackageName(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{"numpy==1.18", "numpy"},
		{"scipy>1.4", "scipy"},
		{"pandas>=1.0,<2.0", "pandas"},
		{"matplotlib<=2.2", "matplotlib"},
		{"seaborn", "seaborn"},
		{"tensorflow>=2.0; python_version > '3.0'", "tensorflow"},
		{"Flask[extra]>=1.0", "Flask"},
		{"git+https://github.com/django/django.git#egg=django", "django"},
		{"-e git+https://github.com/django/django.git#egg=django", "django"},
		{"-i https://pypi.org/simple", ""},
		{"--index-url https://pypi.org/simple", ""},
	}

	for _, tc := range testCases {
		output := extractPackageName(tc.input)
		if output != tc.expected {
			t.Errorf("extractPackageName(%q) = %q; expected %q", tc.input, output, tc.expected)
		}
	}
}

func TestGeneratePipInstallCommand(t *testing.T) {
	testCases := []struct {
		opts *BuildOpts
		want string
	}{
		{
			opts: &BuildOpts{
				PythonPackages: []string{"--extra-index-url https://download.pytorch.org/whl/cu121", "numpy==1.18", "scipy>1.4", "pandas>=1.0,<2.0", "matplotlib<=2.2", "seaborn"},
			},
			want: `uv-b9 pip install --extra-index-url https://download.pytorch.org/whl/cu121 "numpy==1.18" "scipy>1.4" "pandas>=1.0,<2.0" "matplotlib<=2.2" "seaborn"`,
		},
	}

	for _, tc := range testCases {
		cmd := generatePipInstallCommand(tc.opts.PythonPackages, tc.opts.PythonVersion, false)
		assert.Equal(t, tc.want, cmd)
	}
}

func TestParseBuildSteps(t *testing.T) {
	testCases := []struct {
		steps []BuildStep
		want  []string
	}{
		{
			steps: []BuildStep{},
			want:  []string{},
		},
		{
			steps: []BuildStep{
				{Type: shellCommandType, Command: "echo 'hello'"},
				{Type: shellCommandType, Command: "echo 'world'"},
			},
			want: []string{"echo 'hello'", "echo 'world'"},
		},
		{
			steps: []BuildStep{
				{Type: pipCommandType, Command: "numpy"},
				{Type: pipCommandType, Command: "pandas"},
			},
			want: []string{"micromamba3.10 -m pip install \"numpy\" \"pandas\""},
		},
		{
			steps: []BuildStep{
				{Type: micromambaCommandType, Command: "torch"},
				{Type: micromambaCommandType, Command: "vllm"},
			},
			want: []string{"micromamba install -y -n beta9 \"torch\" \"vllm\""},
		},
		{
			steps: []BuildStep{
				{Type: shellCommandType, Command: "echo 'start'"},
				{Type: pipCommandType, Command: "numpy"},
				{Type: micromambaCommandType, Command: "torch"},
				{Type: shellCommandType, Command: "echo 'end'"},
			},
			want: []string{"echo 'start'", "micromamba3.10 -m pip install \"numpy\"", "micromamba install -y -n beta9 \"torch\"", "echo 'end'"},
		},
		{
			steps: []BuildStep{
				{Type: shellCommandType, Command: "echo 'hello'"},
				{Type: pipCommandType, Command: "numpy"},
				{Type: pipCommandType, Command: "pandas"},
				{Type: micromambaCommandType, Command: "torch"},
				{Type: micromambaCommandType, Command: "vllm"},
			},
			want: []string{"echo 'hello'", "micromamba3.10 -m pip install \"numpy\" \"pandas\"", "micromamba install -y -n beta9 \"torch\" \"vllm\""},
		},
		{
			steps: []BuildStep{
				{Type: shellCommandType, Command: "echo 'hello'"},
				{Type: pipCommandType, Command: "numpy"},
				{Type: pipCommandType, Command: "pandas"},
				{Type: micromambaCommandType, Command: "torch"},
				{Type: micromambaCommandType, Command: "vllm"},
				{Type: shellCommandType, Command: "apt install -y ffmpeg"},
				{Type: micromambaCommandType, Command: "ffmpeg"},
			},
			want: []string{"echo 'hello'", "micromamba3.10 -m pip install \"numpy\" \"pandas\"", "micromamba install -y -n beta9 \"torch\" \"vllm\"", "apt install -y ffmpeg", "micromamba install -y -n beta9 \"ffmpeg\""},
		},
		{
			steps: []BuildStep{
				{Type: micromambaCommandType, Command: "torch"},
				{Type: pipCommandType, Command: "unsloth[colab-new] @ git+https://github.com/unslothai/unsloth.git"},
				{Type: pipCommandType, Command: "--no-deps trl peft accelerate bitsandbytes"},
			},
			want: []string{
				"micromamba install -y -n beta9 \"torch\"",
				"micromamba3.10 -m pip install \"unsloth[colab-new] @ git+https://github.com/unslothai/unsloth.git\"",
				"micromamba3.10 -m pip install --no-deps trl peft accelerate bitsandbytes",
			},
		},
		{
			steps: []BuildStep{
				{Type: micromambaCommandType, Command: "torch"},
				{Type: pipCommandType, Command: "--no-deps trl peft accelerate bitsandbytes"},
				{Type: pipCommandType, Command: "unsloth[colab-new] @ git+https://github.com/unslothai/unsloth.git"},
			},
			want: []string{
				"micromamba install -y -n beta9 \"torch\"",
				"micromamba3.10 -m pip install --no-deps trl peft accelerate bitsandbytes",
				"micromamba3.10 -m pip install \"unsloth[colab-new] @ git+https://github.com/unslothai/unsloth.git\"",
			},
		},
		{
			steps: []BuildStep{
				{Type: micromambaCommandType, Command: "pytorch-cuda=12.1"},
				{Type: micromambaCommandType, Command: "-c pytorch"},
				{Type: pipCommandType, Command: "--no-deps trl peft accelerate bitsandbytes"},
				{Type: pipCommandType, Command: "unsloth[colab-new] @ git+https://github.com/unslothai/unsloth.git"},
			},
			want: []string{
				"micromamba install -y -n beta9 -c pytorch \"pytorch-cuda=12.1\"",
				"micromamba3.10 -m pip install --no-deps trl peft accelerate bitsandbytes",
				"micromamba3.10 -m pip install \"unsloth[colab-new] @ git+https://github.com/unslothai/unsloth.git\"",
			},
		},
	}

	for _, tc := range testCases {
		got := parseBuildSteps(tc.steps, "micromamba3.10", false)
		assert.Equal(t, tc.want, got)
	}
}
