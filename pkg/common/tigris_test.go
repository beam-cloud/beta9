package common

import "testing"

func TestIsTigrisEndpoint(t *testing.T) {
	for _, endpoint := range []string{
		"https://t3.storage.dev", "https://fly.storage.tigris.dev",
		"https://bucket.t3.storage.dev", "https://bucket.fly.storage.tigris.dev",
		"https://T3.STORAGE.DEV.:443/path",
	} {
		if !IsTigrisEndpoint(endpoint) {
			t.Errorf("IsTigrisEndpoint(%q) = false", endpoint)
		}
	}
	for _, endpoint := range []string{
		"", "https://s3.amazonaws.com", "http://localstack:4566",
		"https://nott3.storage.dev", "https://t3.storage.dev.example.com",
		"https://fly.storage.tigris.dev.example.com", "t3.storage.dev", "://invalid",
	} {
		if IsTigrisEndpoint(endpoint) {
			t.Errorf("IsTigrisEndpoint(%q) = true", endpoint)
		}
	}
}
