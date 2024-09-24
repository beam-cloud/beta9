package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateSubdomain(t *testing.T) {
	tests := []struct {
		name              string
		deploymentName    string
		stubType          string
		workspaceId       uint
		expectedSubdomain string
		expectedDuplicate bool
	}{
		{
			"my-app in workspace 1 as endpoint is unique",
			"my-app",
			"endpoint/deployment",
			1,
			"my-app-a8fc5c8",
			false,
		},
		{
			"my_app in workspace 1 as endpoint is unique",
			"my_app",
			"endpoint/deployment",
			1,
			"my-app-0305796",
			false,
		},
		{
			"my_app in workspace 1 as taskqueue is unique",
			"my_app",
			"taskqueue/deployment",
			1,
			"my-app-e50a2fc",
			false,
		},
		{
			"my-app in workspace 2 as endpoint is unique",
			"my-app",
			"endpoint/deployment",
			2,
			"my-app-0fee434",
			false,
		},
		{
			"my-app in workspace 2 as endpoint is a duplicate",
			"my-app",
			"endpoint/deployment",
			2,
			"my-app-0fee434",
			true,
		},
	}

	subdomains := make(map[string]bool)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			subdomain := generateSubdomain(test.deploymentName, test.stubType, test.workspaceId)
			assert.Equal(t, test.expectedSubdomain, subdomain)

			if _, exists := subdomains[subdomain]; exists {
				assert.True(t, test.expectedDuplicate)
				return
			}

			assert.False(t, test.expectedDuplicate)
			subdomains[subdomain] = true
		})
	}
}
