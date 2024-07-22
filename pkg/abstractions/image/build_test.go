package image

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractImageNameAndTag(t *testing.T) {
	tests := []struct {
		image        string
		wantTag      string
		wantName     string
		wantRegistry string
	}{
		{
			image:        "nginx",
			wantTag:      "latest",
			wantName:     "nginx",
			wantRegistry: "docker.io",
		},
		{
			image:        "docker.io/nginx",
			wantTag:      "latest",
			wantName:     "nginx",
			wantRegistry: "docker.io",
		},
		{
			image:        "docker.io/nginx:1.25.3",
			wantTag:      "1.25.3",
			wantName:     "nginx",
			wantRegistry: "docker.io",
		},
		{
			image:        "docker.io/nginx:latest",
			wantTag:      "latest",
			wantName:     "nginx",
			wantRegistry: "docker.io",
		},
		{
			image:        "registry.localhost:5000/beta9-runner:py311-latest",
			wantTag:      "py311-latest",
			wantName:     "beta9-runner",
			wantRegistry: "registry.localhost:5000",
		},
		{
			image:        "111111111111.dkr.ecr.us-east-1.amazonaws.com/myapp:latest",
			wantTag:      "latest",
			wantName:     "myapp",
			wantRegistry: "111111111111.dkr.ecr.us-east-1.amazonaws.com",
		},
	}

	for _, test := range tests {
		t.Run(test.image, func(t *testing.T) {
			image, err := ExtractImageNameAndTag(test.image)
			assert.NoError(t, err)

			assert.Equal(t, test.wantTag, image.ImageTag)
			assert.Equal(t, test.wantName, image.ImageName)
			assert.Equal(t, test.wantRegistry, image.SourceRegistry)
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
