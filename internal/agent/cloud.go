package agent

import (
	"fmt"
	"net/http"
	"time"
)

type CloudProvider struct {
	Name            string
	MetadataURL     string
	MetadataHeaders map[string]string
}

var cloudProviders map[string]CloudProvider

func init() {
	cloudProviders = map[string]CloudProvider{
		"aws": {
			Name:        "Amazon Web Services",
			MetadataURL: "http://169.254.169.254/latest/meta-data/",
		},
		"gcp": {
			Name:            "Google Cloud",
			MetadataURL:     "http://metadata.google.internal",
			MetadataHeaders: map[string]string{"Metadata-Flavor": "Google"},
		},
		"azure": {
			Name:            "Microsoft Azure",
			MetadataURL:     "http://169.254.169.254/metadata/instance",
			MetadataHeaders: map[string]string{"Metadata": "true"},
		},
	}
}

// Detects the cloud provider.
// Returns an "Unknown" CloudProvider if an error is returned.
func getCloudProviderInfo(timeout time.Duration) *CloudProvider {
	unknown := &CloudProvider{
		Name: "Unknown",
	}

	for _, cloud := range cloudProviders {
		client := http.Client{
			Timeout: timeout,
		}

		req, err := http.NewRequest("GET", cloud.MetadataURL, nil)
		if err != nil {
			fmt.Printf("failed to make new request: %v", err)
			continue
		}

		if len(cloud.MetadataHeaders) > 0 {
			for k, v := range cloud.MetadataHeaders {
				req.Header.Set(k, v)
			}
		}

		res, err := client.Do(req)
		if err != nil {
			fmt.Printf("failed to send request: %v", err)
			continue
		}
		defer res.Body.Close()

		if res.StatusCode == http.StatusOK {
			return &cloud
		}
	}

	return unknown
}
