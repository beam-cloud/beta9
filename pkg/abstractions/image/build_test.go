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
	b := &Builder{}

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
		output := b.extractPackageName(tc.input)
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
			want: ` -m pip install --root-user-action=ignore --extra-index-url https://download.pytorch.org/whl/cu121 "numpy==1.18" "scipy>1.4" "pandas>=1.0,<2.0" "matplotlib<=2.2" "seaborn"`,
		},
	}

	for _, tc := range testCases {
		b := &Builder{}
		cmd := b.generatePipInstallCommand(tc.opts.PythonPackages, tc.opts.PythonVersion)
		assert.Equal(t, tc.want, cmd)
	}
}
