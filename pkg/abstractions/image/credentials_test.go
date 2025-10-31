package image

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetECRCredentials(t *testing.T) {
	t.Run("valid credentials", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageUri: "123456789.dkr.ecr.us-east-1.amazonaws.com/my-repo:latest",
			ExistingImageCreds: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIAIOSFODNN7EXAMPLE",
				"AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"AWS_REGION":            "us-east-1",
				"AWS_SESSION_TOKEN":     "session-token",
			},
		}

		creds, err := GetECRCredentials(opts)
		require.NoError(t, err)
		require.NotNil(t, creds)
		assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", creds["AWS_ACCESS_KEY_ID"])
		assert.Equal(t, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", creds["AWS_SECRET_ACCESS_KEY"])
		assert.Equal(t, "us-east-1", creds["AWS_REGION"])
		assert.Equal(t, "session-token", creds["AWS_SESSION_TOKEN"])
	})

	t.Run("missing access key", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageCreds: map[string]string{
				"AWS_SECRET_ACCESS_KEY": "secret",
				"AWS_REGION":            "us-east-1",
			},
		}

		creds, err := GetECRCredentials(opts)
		assert.Error(t, err)
		assert.Nil(t, creds)
		assert.Contains(t, err.Error(), "AWS_ACCESS_KEY_ID")
	})

	t.Run("missing region", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageCreds: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIAIOSFODNN7EXAMPLE",
				"AWS_SECRET_ACCESS_KEY": "secret",
			},
		}

		creds, err := GetECRCredentials(opts)
		assert.Error(t, err)
		assert.Nil(t, creds)
		assert.Contains(t, err.Error(), "AWS_REGION")
	})
}

func TestGetGARCredentials(t *testing.T) {
	t.Run("valid token", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageUri: "us-docker.pkg.dev/my-project/my-repo/my-image:latest",
			ExistingImageCreds: map[string]string{
				"GCP_ACCESS_TOKEN": "ya29.a0AfH6SMBx...",
			},
		}

		creds, err := GetGARCredentials(opts)
		require.NoError(t, err)
		require.NotNil(t, creds)
		assert.Equal(t, "ya29.a0AfH6SMBx...", creds["GCP_ACCESS_TOKEN"])
	})

	t.Run("missing token", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageCreds: map[string]string{},
		}

		creds, err := GetGARCredentials(opts)
		assert.Error(t, err)
		assert.Nil(t, creds)
		assert.Contains(t, err.Error(), "GCP_ACCESS_TOKEN")
	})
}

func TestGetNGCCredentials(t *testing.T) {
	t.Run("valid API key", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageUri: "nvcr.io/nvidia/pytorch:latest",
			ExistingImageCreds: map[string]string{
				"NGC_API_KEY": "my-api-key",
			},
		}

		creds, err := GetNGCCredentials(opts)
		require.NoError(t, err)
		require.NotNil(t, creds)
		assert.Equal(t, "my-api-key", creds["NGC_API_KEY"])
	})

	t.Run("missing API key", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageCreds: map[string]string{},
		}

		creds, err := GetNGCCredentials(opts)
		assert.Error(t, err)
		assert.Nil(t, creds)
		assert.Contains(t, err.Error(), "NGC_API_KEY")
	})
}

func TestGetGHCRCredentials(t *testing.T) {
	t.Run("valid credentials", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageUri: "ghcr.io/my-org/my-repo:latest",
			ExistingImageCreds: map[string]string{
				"GITHUB_USERNAME": "myuser",
				"GITHUB_TOKEN":    "ghp_abc123",
			},
		}

		creds, err := GetGHCRCredentials(opts)
		require.NoError(t, err)
		require.NotNil(t, creds)
		assert.Equal(t, "myuser", creds["GITHUB_USERNAME"])
		assert.Equal(t, "ghp_abc123", creds["GITHUB_TOKEN"])
	})

	t.Run("public access", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageUri:   "ghcr.io/my-org/my-repo:latest",
			ExistingImageCreds: map[string]string{},
		}

		creds, err := GetGHCRCredentials(opts)
		require.NoError(t, err)
		assert.Nil(t, creds)
	})

	t.Run("only username provided", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageCreds: map[string]string{
				"GITHUB_USERNAME": "myuser",
			},
		}

		creds, err := GetGHCRCredentials(opts)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})
}

func TestGetDockerHubCredentials(t *testing.T) {
	t.Run("valid credentials", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageUri: "docker.io/library/ubuntu:latest",
			ExistingImageCreds: map[string]string{
				"DOCKERHUB_USERNAME": "myuser",
				"DOCKERHUB_PASSWORD": "mypassword",
			},
		}

		creds, err := GetDockerHubCredentials(opts)
		require.NoError(t, err)
		require.NotNil(t, creds)
		assert.Equal(t, "myuser", creds["DOCKERHUB_USERNAME"])
		assert.Equal(t, "mypassword", creds["DOCKERHUB_PASSWORD"])
	})

	t.Run("public access", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageUri:   "ubuntu:latest",
			ExistingImageCreds: map[string]string{},
		}

		creds, err := GetDockerHubCredentials(opts)
		require.NoError(t, err)
		assert.Nil(t, creds)
	})
}

func TestGetRegistryCredentials(t *testing.T) {
	t.Run("ECR registry", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageUri: "123456789.dkr.ecr.us-east-1.amazonaws.com/my-repo:latest",
			ExistingImageCreds: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIAIOSFODNN7EXAMPLE",
				"AWS_SECRET_ACCESS_KEY": "secret",
				"AWS_REGION":            "us-east-1",
			},
		}

		registry, creds, err := GetRegistryCredentials(opts)
		require.NoError(t, err)
		assert.Equal(t, "123456789.dkr.ecr.us-east-1.amazonaws.com", registry)
		require.NotNil(t, creds)
		assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", creds["AWS_ACCESS_KEY_ID"])
	})

	t.Run("GCR registry", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageUri: "gcr.io/my-project/my-image:latest",
			ExistingImageCreds: map[string]string{
				"GCP_ACCESS_TOKEN": "ya29.a0AfH6SMBx...",
			},
		}

		registry, creds, err := GetRegistryCredentials(opts)
		require.NoError(t, err)
		assert.Equal(t, "gcr.io", registry)
		require.NotNil(t, creds)
		assert.Equal(t, "ya29.a0AfH6SMBx...", creds["GCP_ACCESS_TOKEN"])
	})

	t.Run("NGC registry", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageUri: "nvcr.io/nvidia/pytorch:latest",
			ExistingImageCreds: map[string]string{
				"NGC_API_KEY": "my-api-key",
			},
		}

		registry, creds, err := GetRegistryCredentials(opts)
		require.NoError(t, err)
		assert.Equal(t, "nvcr.io", registry)
		require.NotNil(t, creds)
		assert.Equal(t, "my-api-key", creds["NGC_API_KEY"])
	})

	t.Run("GHCR registry", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageUri: "ghcr.io/my-org/my-repo:latest",
			ExistingImageCreds: map[string]string{
				"GITHUB_USERNAME": "myuser",
				"GITHUB_TOKEN":    "ghp_abc123",
			},
		}

		registry, creds, err := GetRegistryCredentials(opts)
		require.NoError(t, err)
		assert.Equal(t, "ghcr.io", registry)
		require.NotNil(t, creds)
		assert.Equal(t, "myuser", creds["GITHUB_USERNAME"])
	})

	t.Run("Docker Hub", func(t *testing.T) {
		opts := &BuildOpts{
			ExistingImageUri: "ubuntu:latest",
			ExistingImageCreds: map[string]string{
				"DOCKERHUB_USERNAME": "myuser",
				"DOCKERHUB_PASSWORD": "mypassword",
			},
		}

		registry, creds, err := GetRegistryCredentials(opts)
		require.NoError(t, err)
		assert.Equal(t, "docker.io", registry)
		require.NotNil(t, creds)
		assert.Equal(t, "myuser", creds["DOCKERHUB_USERNAME"])
	})
}

func TestParseCredentialsFromJSON(t *testing.T) {
	t.Run("valid JSON", func(t *testing.T) {
		credStr := `{"AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE", "AWS_SECRET_ACCESS_KEY": "secret", "AWS_REGION": "us-east-1"}`
		creds, err := ParseCredentialsFromJSON(credStr)
		require.NoError(t, err)
		require.NotNil(t, creds)
		assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", creds["AWS_ACCESS_KEY_ID"])
		assert.Equal(t, "secret", creds["AWS_SECRET_ACCESS_KEY"])
		assert.Equal(t, "us-east-1", creds["AWS_REGION"])
	})

	t.Run("legacy username:password format", func(t *testing.T) {
		credStr := "myuser:mypassword"
		creds, err := ParseCredentialsFromJSON(credStr)
		require.NoError(t, err)
		require.NotNil(t, creds)
		assert.Equal(t, "myuser", creds["USERNAME"])
		assert.Equal(t, "mypassword", creds["PASSWORD"])
	})

	t.Run("password with colon", func(t *testing.T) {
		credStr := "myuser:my:pass:word"
		creds, err := ParseCredentialsFromJSON(credStr)
		require.NoError(t, err)
		require.NotNil(t, creds)
		assert.Equal(t, "myuser", creds["USERNAME"])
		assert.Equal(t, "my:pass:word", creds["PASSWORD"])
	})

	t.Run("empty string", func(t *testing.T) {
		creds, err := ParseCredentialsFromJSON("")
		require.NoError(t, err)
		assert.Nil(t, creds)
	})

	t.Run("invalid format", func(t *testing.T) {
		credStr := "invalid"
		creds, err := ParseCredentialsFromJSON(credStr)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})

	t.Run("JSON with unknown keys", func(t *testing.T) {
		credStr := `{"UNKNOWN_KEY": "value", "AWS_ACCESS_KEY_ID": "key"}`
		creds, err := ParseCredentialsFromJSON(credStr)
		require.NoError(t, err)
		require.NotNil(t, creds)
		// Only known keys should be returned
		assert.Equal(t, "key", creds["AWS_ACCESS_KEY_ID"])
		assert.NotContains(t, creds, "UNKNOWN_KEY")
	})
}

func TestMarshalCredentialsToJSON(t *testing.T) {
	t.Run("ECR credentials", func(t *testing.T) {
		registry := "123456789.dkr.ecr.us-east-1.amazonaws.com"
		creds := map[string]string{
			"AWS_ACCESS_KEY_ID":     "AKIAIOSFODNN7EXAMPLE",
			"AWS_SECRET_ACCESS_KEY": "secret",
			"AWS_REGION":            "us-east-1",
		}

		jsonStr, err := MarshalCredentialsToJSON(registry, creds)
		require.NoError(t, err)
		assert.NotEmpty(t, jsonStr)
		assert.Contains(t, jsonStr, "aws")
		assert.Contains(t, jsonStr, registry)
	})

	t.Run("basic auth credentials", func(t *testing.T) {
		registry := "docker.io"
		creds := map[string]string{
			"USERNAME": "myuser",
			"PASSWORD": "mypassword",
		}

		jsonStr, err := MarshalCredentialsToJSON(registry, creds)
		require.NoError(t, err)
		assert.NotEmpty(t, jsonStr)
		assert.Contains(t, jsonStr, registry)
	})

	t.Run("empty credentials", func(t *testing.T) {
		jsonStr, err := MarshalCredentialsToJSON("docker.io", map[string]string{})
		require.NoError(t, err)
		assert.Empty(t, jsonStr)
	})
}

func TestValidateRequiredCredential(t *testing.T) {
	t.Run("valid credential", func(t *testing.T) {
		creds := map[string]string{
			"KEY": "value",
		}
		value, err := validateRequiredCredential(creds, "KEY")
		require.NoError(t, err)
		assert.Equal(t, "value", value)
	})

	t.Run("missing credential", func(t *testing.T) {
		creds := map[string]string{}
		value, err := validateRequiredCredential(creds, "KEY")
		assert.Error(t, err)
		assert.Empty(t, value)
		assert.Contains(t, err.Error(), "KEY")
	})

	t.Run("empty credential", func(t *testing.T) {
		creds := map[string]string{
			"KEY": "",
		}
		value, err := validateRequiredCredential(creds, "KEY")
		assert.Error(t, err)
		assert.Empty(t, value)
	})
}

func TestValidateOptionalCredentials(t *testing.T) {
	t.Run("both credentials provided", func(t *testing.T) {
		creds := map[string]string{
			"USERNAME": "user",
			"PASSWORD": "pass",
		}
		username, password, hasAuth, err := validateOptionalCredentials(creds, "USERNAME", "PASSWORD")
		require.NoError(t, err)
		assert.True(t, hasAuth)
		assert.Equal(t, "user", username)
		assert.Equal(t, "pass", password)
	})

	t.Run("no credentials provided", func(t *testing.T) {
		creds := map[string]string{}
		username, password, hasAuth, err := validateOptionalCredentials(creds, "USERNAME", "PASSWORD")
		require.NoError(t, err)
		assert.False(t, hasAuth)
		assert.Empty(t, username)
		assert.Empty(t, password)
	})

	t.Run("only username provided", func(t *testing.T) {
		creds := map[string]string{
			"USERNAME": "user",
		}
		username, password, hasAuth, err := validateOptionalCredentials(creds, "USERNAME", "PASSWORD")
		assert.Error(t, err)
		assert.False(t, hasAuth)
		assert.Empty(t, username)
		assert.Empty(t, password)
	})

	t.Run("only password provided", func(t *testing.T) {
		creds := map[string]string{
			"PASSWORD": "pass",
		}
		username, password, hasAuth, err := validateOptionalCredentials(creds, "USERNAME", "PASSWORD")
		assert.Error(t, err)
		assert.False(t, hasAuth)
		assert.Empty(t, username)
		assert.Empty(t, password)
	})

	t.Run("empty username", func(t *testing.T) {
		creds := map[string]string{
			"USERNAME": "",
			"PASSWORD": "pass",
		}
		username, password, hasAuth, err := validateOptionalCredentials(creds, "USERNAME", "PASSWORD")
		assert.Error(t, err)
		assert.False(t, hasAuth)
		assert.Empty(t, username)
		assert.Empty(t, password)
	})
}

func TestContainsAny(t *testing.T) {
	t.Run("contains substring", func(t *testing.T) {
		assert.True(t, containsAny("hello world", "world"))
		assert.True(t, containsAny("123456789.dkr.ecr.us-east-1.amazonaws.com", "amazonaws.com"))
		assert.True(t, containsAny("gcr.io/project/image", "gcr.io"))
	})

	t.Run("does not contain substring", func(t *testing.T) {
		assert.False(t, containsAny("hello world", "foo"))
		assert.False(t, containsAny("docker.io/image", "amazonaws.com"))
	})

	t.Run("multiple substrings", func(t *testing.T) {
		assert.True(t, containsAny("gcr.io/project", "gcr.io", "pkg.dev"))
		assert.True(t, containsAny("us-docker.pkg.dev/project", "gcr.io", "pkg.dev"))
	})
}
