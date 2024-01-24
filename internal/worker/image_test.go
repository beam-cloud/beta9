package worker

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
	}

	for _, test := range tests {
		t.Run(test.image, func(t *testing.T) {
			image, err := extractImageNameAndTag(test.image)
			assert.NoError(t, err)

			assert.Equal(t, test.wantTag, image.ImageTag)
			assert.Equal(t, test.wantName, image.ImageName)
			assert.Equal(t, test.wantRegistry, image.SourceRegistry)
		})
	}
}
