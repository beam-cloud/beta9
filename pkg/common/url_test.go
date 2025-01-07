package common

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

func TestBuildDeploymentURL(t *testing.T) {
	externalUrl := "http://app.example.com"

	tests := []struct {
		name        string
		stub        *types.StubWithRelated
		deployment  *types.Deployment
		invokeType  string
		expectedURL string
	}{
		{
			name: "returns host-based URL",
			stub: &types.StubWithRelated{},
			deployment: &types.Deployment{
				Subdomain: "my-app-1234567",
				Version:   8,
			},
			invokeType:  InvokeUrlTypeHost,
			expectedURL: "http://my-app-1234567-v8.app.example.com",
		},
		{
			name: "returns path-based URL",
			stub: &types.StubWithRelated{Stub: types.Stub{
				Type: types.StubType(types.StubTypeEndpointDeployment),
			}},
			deployment: &types.Deployment{
				Name:    "my-app",
				Version: 8,
			},
			invokeType:  InvokeUrlTypePath,
			expectedURL: "http://app.example.com/endpoint/my-app/v8",
		},
		{
			name: "returns public path-based URL",
			stub: &types.StubWithRelated{Stub: types.Stub{
				Type:       types.StubType(types.StubTypeEndpointDeployment),
				Config:     `{"authorized": false}`,
				ExternalId: "e9c29586-c465-4a67-9c9b-25293d1ce77b",
			}},
			invokeType:  InvokeUrlTypePath,
			expectedURL: "http://app.example.com/endpoint/public/e9c29586-c465-4a67-9c9b-25293d1ce77b",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := BuildDeploymentURL(externalUrl, test.invokeType, test.stub, test.deployment)
			assert.Equal(t, test.expectedURL, got)
		})
	}
}

func TestBuildServeURL(t *testing.T) {
	externalUrl := "http://app.example.com"

	tests := []struct {
		name        string
		stub        *types.StubWithRelated
		invokeType  string
		expectedUrl string
	}{
		{
			name: "returns host-based URL",
			stub: &types.StubWithRelated{Stub: types.Stub{
				ExternalId: "e9c29586-c465-4a67-9c9b-25293d1ce77b",
			}},
			invokeType:  InvokeUrlTypeHost,
			expectedUrl: "http://e9c29586-c465-4a67-9c9b-25293d1ce77b.app.example.com",
		},
		{
			name: "returns host-based URL",
			stub: &types.StubWithRelated{Stub: types.Stub{
				ExternalId: "fbedeff-c465-4a67-9c9b-25293d1ce77b",
				Type:       types.StubType(types.StubTypeShell),
			}},
			invokeType:  InvokeUrlTypeHost,
			expectedUrl: "http://fbedeff-c465-4a67-9c9b-25293d1ce77b.app.example.com",
		},
		{
			name: "returns path-based URL",
			stub: &types.StubWithRelated{Stub: types.Stub{
				ExternalId: "e9c29586-c465-4a67-9c9b-25293d1ce77b",
				Type:       types.StubType(types.StubTypeEndpointDeployment),
			}},
			invokeType:  InvokeUrlTypePath,
			expectedUrl: "http://app.example.com/endpoint/id/e9c29586-c465-4a67-9c9b-25293d1ce77b",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := BuildStubURL(externalUrl, test.invokeType, test.stub)
			assert.Equal(t, test.expectedUrl, got)
		})
	}
}
