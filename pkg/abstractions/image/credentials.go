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

// GetECRCredentials returns structured AWS ECR credentials
// Wrapper for testing compatibility
func GetECRCredentials(opts *BuildOpts) (map[string]string, error) {
	return reg.GetECRCredentials(opts.ExistingImageCreds)
}

// GetGARCredentials returns structured GCP GAR credentials
// Wrapper for testing compatibility
func GetGARCredentials(opts *BuildOpts) (map[string]string, error) {
	return reg.GetGARCredentials(opts.ExistingImageCreds)
}

// GetNGCCredentials returns structured NGC credentials
// Wrapper for testing compatibility
func GetNGCCredentials(opts *BuildOpts) (map[string]string, error) {
	return reg.GetNGCCredentials(opts.ExistingImageCreds)
}

// GetGHCRCredentials returns structured GHCR credentials
// Wrapper for testing compatibility
func GetGHCRCredentials(opts *BuildOpts) (map[string]string, error) {
	return reg.GetGHCRCredentials(opts.ExistingImageCreds)
}

// GetDockerHubCredentials returns structured Docker Hub credentials
// Wrapper for testing compatibility
func GetDockerHubCredentials(opts *BuildOpts) (map[string]string, error) {
	return reg.GetDockerHubCredentials(opts.ExistingImageCreds)
}

// validateRequiredCredential validates that a required credential key exists and is non-empty
// Wrapper for testing compatibility
func validateRequiredCredential(creds map[string]string, key string) (string, error) {
	if value := creds[key]; value != "" {
		return value, nil
	}
	return "", reg.ErrMissingCredential(key)
}

// validateOptionalCredentials validates optional username/password credentials
// Wrapper for testing compatibility
func validateOptionalCredentials(creds map[string]string, usernameKey, passwordKey string) (string, string, bool, error) {
	username, usernameExists := creds[usernameKey]
	password, passwordExists := creds[passwordKey]

	// If neither key exists, credentials were not provided (ok for public registries)
	if !usernameExists && !passwordExists {
		return "", "", false, nil
	}

	// If one or both keys exist but are empty, this is an error
	if (usernameExists && username == "") || (passwordExists && password == "") {
		return "", "", false, reg.ErrBothCredentialsRequired(usernameKey, passwordKey)
	}

	// If only one credential is provided (non-empty), both are required
	if (username == "" && password != "") || (username != "" && password == "") {
		return "", "", false, reg.ErrBothCredentialsRequired(usernameKey, passwordKey)
	}

	return username, password, username != "" && password != "", nil
}

// containsAny checks if string s contains any of the given substrings
// Wrapper for testing compatibility
func containsAny(s string, substrs ...string) bool {
	return reg.ContainsAny(s, substrs...)
}
