package image

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRegistryToken(t *testing.T) {
	tests := []struct {
		name    string
		opts    *BuildOpts
		want    string
		wantErr bool
	}{
		// TODO: Mock ECR test so we can test for a valid test token
		{
			name: "amazon ecr registry",
			opts: &BuildOpts{
				ExistingImageUri: "123456789012.dkr.ecr.us-west-2.amazonaws.com/my-repo:latest",
				ExistingImageCreds: map[string]string{
					"AWS_ACCESS_KEY_ID":     "accessKey123",
					"AWS_SECRET_ACCESS_KEY": "secretKey123",
					"AWS_REGION":            "us-west-2",
				},
			},
			wantErr: true,
		},
		{
			name: "google cloud artifact registry",
			opts: &BuildOpts{
				ExistingImageUri: "us-east4-docker.pkg.dev/project-abcd/test-repo/test-image:0.1.0",
				ExistingImageCreds: map[string]string{
					"GCP_ACCESS_TOKEN": "token123",
				},
			},
			want: "oauth2accesstoken:token123",
		},
		{
			name: "nvidia gpu cloud registry",
			opts: &BuildOpts{
				ExistingImageUri: "nvcr.io/nvidia/cuda:11.0-base",
				ExistingImageCreds: map[string]string{
					"NGC_API_KEY": "key123",
				},
			},
			want: "$oauthtoken:key123",
		},
		{
			name: "dockerhub registry",
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
			name: "github container registry",
			opts: &BuildOpts{
				ExistingImageUri: "ghcr.io/user123/python:3.9",
				ExistingImageCreds: map[string]string{
					"GITHUB_USERNAME": "user123",
					"GITHUB_TOKEN":    "token123",
				},
			},
			want: "user123:token123",
		},
		{
			name: "unknown registry",
			opts: &BuildOpts{
				ExistingImageUri:   "unknown.registry.com/image:latest",
				ExistingImageCreds: map[string]string{},
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := GetRegistryToken(tt.opts)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want, token)
		})
	}
}

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

func TestGetGARToken(t *testing.T) {
	tests := []struct {
		name    string
		opts    *BuildOpts
		want    string
		wantErr bool
	}{
		{
			name: "with access token",
			opts: &BuildOpts{
				ExistingImageUri: "us-east4-docker.pkg.dev/project-abcd/test-repo/test-image:0.1.0",
				ExistingImageCreds: map[string]string{
					"GCP_ACCESS_TOKEN": "token123",
				},
			},
			want: "oauth2accesstoken:token123",
		},
		{
			name: "with empty access token",
			opts: &BuildOpts{
				ExistingImageUri: "us-east4-docker.pkg.dev/project-abcd/test-repo/test-image:0.1.0",
				ExistingImageCreds: map[string]string{
					"GCP_ACCESS_TOKEN": "",
				},
			},
			wantErr: true,
		},
		{
			name: "with no access token",
			opts: &BuildOpts{
				ExistingImageUri:   "us-east4-docker.pkg.dev/project-abcd/test-repo/test-image:0.1.0",
				ExistingImageCreds: map[string]string{},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := GetGARToken(tt.opts)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want, token)
		})
	}
}
