package image

import (
	reg "github.com/beam-cloud/beta9/pkg/registry"
)

// GetRegistryCredentials returns structured credentials for the registry
// Returns both the registry name and credentials in a format compatible with CLIP
// This is a thin wrapper around registry.GetRegistryCredentialsForImage
func GetRegistryCredentials(opts *BuildOpts) (registry string, creds map[string]string, err error) {
	return reg.GetRegistryCredentialsForImage(opts.ExistingImageUri, opts.ExistingImageCreds)
}

// GetRegistryToken returns credentials as username:password format (legacy)
// Deprecated: Use GetRegistryCredentials for structured credentials
// This is a thin wrapper around registry.GetRegistryTokenForImage
func GetRegistryToken(opts *BuildOpts) (string, error) {
	return reg.GetRegistryTokenForImage(opts.ExistingImageUri, opts.ExistingImageCreds)
}

// MarshalCredentialsToJSON converts credentials to JSON format for CLIP
// This is a thin wrapper around registry.MarshalCredentials
func MarshalCredentialsToJSON(registry string, creds map[string]string) (string, error) {
	if len(creds) == 0 {
		return "", nil
	}

	credType := reg.DetectCredentialType(registry, creds)
	return reg.MarshalCredentials(registry, credType, creds)
}

// ParseCredentialsFromJSON parses credentials from JSON string or username:password format
// Returns structured credentials compatible with CLIP
// This is a thin wrapper around registry.ParseCredentialsFromJSON
func ParseCredentialsFromJSON(credStr string) (map[string]string, error) {
	return reg.ParseCredentialsFromJSON(credStr)
}
