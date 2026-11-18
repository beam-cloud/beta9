package watch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEveryProvidersRegionsArePutInTheSamePartsOfTheWorld(t *testing.T) {
	for region, geo := range map[string]string{
		// AWS, GCP and Nebius name a region for where it is.
		"us-east-1": "us-east", "us-east-2": "us-east", "us-west-2": "us-west", "ca-central-1": "ca-east",
		"eu-west-3": "eu-west", "eu-central-1": "eu-central", "eu-north-1": "eu-north", "eu-south-1": "eu-central",
		"ap-southeast-2": "ap-southeast", "ap-northeast-1": "ap-northeast", "ap-south-1": "ap-south",
		"me-central-1": "me-west", "sa-east-1": "other", "af-south-1": "other",
		"us-central1": "us-central", "us-east4": "us-east", "us-south1": "us-central",
		"europe-west4": "eu-west", "europe-west3": "eu-central", "asia-northeast1": "ap-northeast",
		"australia-southeast1": "ap-southeast", "southamerica-east1": "other",
		"eu-north1": "eu-north", "eu-west1": "eu-west",
		// Hetzner names a data centre.
		"ash": "us-east", "hil": "us-west", "fsn1": "eu-central", "nbg1": "eu-central", "hel1": "eu-north", "sin": "ap-southeast",
		// Shadeform names a city and its country, Verda a country, and Vast
		// two letters.
		"newyork-usa-1": "us-east", "dallas-usa-2": "us-central", "frankfurt-deu-1": "eu-central",
		"FIN-01": "eu-north", "ICE-01": "eu-north",
		"US": "us-central", "CA": "ca-east", "DE": "eu-central", "JP": "ap-northeast",
		// A part of the world is itself.
		"us-west": "us-west", "eu-north": "eu-north",
		"": "other", "mars-1": "other",
	} {
		assert.Equal(t, geo, Geo(region), "region %q", region)
	}
}
