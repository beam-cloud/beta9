package image

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestExtractImageNameAndTag(t *testing.T) {
	b := &Builder{}

	tests := []struct {
		imageURI      string
		expected      CustomBaseImage
		expectedError error
	}{
		{
			imageURI: "docker.io/registry1/docker-image:v1",
			expected: CustomBaseImage{
				SourceRegistry: "docker.io/registry1",
				ImageName:      "docker-image",
				ImageTag:       "v1",
			},
			expectedError: nil,
		},
		{
			imageURI: "docker.io/registry1/docker-image",
			expected: CustomBaseImage{
				SourceRegistry: "docker.io/registry1",
				ImageName:      "docker-image",
				ImageTag:       "latest",
			},
			expectedError: nil,
		},
		{
			imageURI:      "",
			expected:      CustomBaseImage{},
			expectedError: errors.New("invalid image URI format"),
		},
		{
			imageURI: "ubuntu:22.04",
			expected: CustomBaseImage{
				SourceRegistry: "docker.io",
				ImageName:      "ubuntu",
				ImageTag:       "22.04",
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		result, err := b.extractImageNameAndTag(tt.imageURI)
		assert.Equal(t, tt.expected, result)

		if tt.expectedError != nil {
			assert.NotNil(t, err)
			assert.Equal(t, tt.expectedError.Error(), err.Error())
		} else {
			assert.Nil(t, err)
		}
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
