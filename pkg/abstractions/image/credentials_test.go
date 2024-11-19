package image

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetDockerHubToken(t *testing.T) {
	tests := []struct {
		name    string
		opts    *BuildOpts
		want    string
		wantErr bool
	}{
		{
			name: "empty image creds with full image uri",
			opts: &BuildOpts{
				ExistingImageUri:   "docker.io/debian:bullseye",
				ExistingImageCreds: map[string]string{},
			},
			want: "",
		},
		{
			name: "empty image creds",
			opts: &BuildOpts{
				ExistingImageUri:   "debian:bullseye",
				ExistingImageCreds: map[string]string{},
			},
			want: "",
		},
		{
			name: "with username and password with full image uri",
			opts: &BuildOpts{
				ExistingImageUri: "docker.io/debian:bullseye",
				ExistingImageCreds: map[string]string{
					"DOCKERHUB_USERNAME": "user123",
					"DOCKERHUB_PASSWORD": "pass123",
				},
			},
			want: "user123:pass123",
		},
		{
			name: "with username and password",
			opts: &BuildOpts{
				ExistingImageUri: "debian:bullseye",
				ExistingImageCreds: map[string]string{
					"DOCKERHUB_USERNAME": "user123",
					"DOCKERHUB_PASSWORD": "pass123",
				},
			},
			want: "user123:pass123",
		},
		{
			name: "with username and empty password",
			opts: &BuildOpts{
				ExistingImageUri: "debian:bullseye",
				ExistingImageCreds: map[string]string{
					"DOCKERHUB_USERNAME": "user123",
					"DOCKERHUB_PASSWORD": "",
				},
			},
			wantErr: true,
		},
		{
			name: "with empty username and password",
			opts: &BuildOpts{
				ExistingImageUri: "debian:bullseye",
				ExistingImageCreds: map[string]string{
					"DOCKERHUB_USERNAME": "",
					"DOCKERHUB_PASSWORD": "pass123",
				},
			},
			wantErr: true,
		},
		{
			name: "with empty username and empty password",
			opts: &BuildOpts{
				ExistingImageUri: "debian:bullseye",
				ExistingImageCreds: map[string]string{
					"DOCKERHUB_USERNAME": "",
					"DOCKERHUB_PASSWORD": "",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := GetDockerHubToken(tt.opts)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want, token)
		})
	}
}
