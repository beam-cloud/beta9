package common

import "testing"

func TestIsStrongTigrisBucketLocation(t *testing.T) {
	for _, location := range []string{"usa", "eur", "sjc", "ord", "iad", "ams", "fra", "lhr", "sin", "nrt", "syd", "gru", "jnb", " USA "} {
		if !IsStrongTigrisBucketLocation(location) {
			t.Errorf("IsStrongTigrisBucketLocation(%q) = false", location)
		}
	}
	for _, location := range []string{"", "auto", "global", "iad,sjc", "fra,ams", "single", "multi", "strong", "iad1", "us-east-1", "unknown"} {
		if IsStrongTigrisBucketLocation(location) {
			t.Errorf("IsStrongTigrisBucketLocation(%q) = true", location)
		}
	}
}

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
