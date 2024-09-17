package common

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

func TestBuildInvokeURL(t *testing.T) {
	tests := []struct {
		name        string
		externalUrl string
		stub        *types.StubWithRelated
		deployment  *types.Deployment
		invokeType  string
		want        string
	}{
		{
			name:        "should return non-public url when stubConfig is invalid",
			externalUrl: "http://app.example.com",
			stub: &types.StubWithRelated{
				Stub: types.Stub{
					ExternalId: "c19515e6-13c7-423f-9d91-227082151538",
					Config:     `{`,
					Type:       types.StubType(types.StubTypeScheduledJobDeployment),
				},
			},
			deployment: &types.Deployment{
				Name:    "app1",
				Version: uint(11),
			},
			invokeType: "path",
			want:       "http://app.example.com/schedule/app1/v11",
		},
		{
			name:        "should return path based public url",
			externalUrl: "https://app.example.com",
			stub: &types.StubWithRelated{
				Stub: types.Stub{
					ExternalId: "64741d05-2273-422a-8424-2e0760e539d3",
					Config:     `{"authorized": false}`,
					Type:       types.StubType(types.StubTypeEndpointDeployment),
				},
			},
			deployment: &types.Deployment{
				Name:    "app2",
				Version: uint(22),
			},
			invokeType: "path",
			want:       "https://app.example.com/public/64741d05-2273-422a-8424-2e0760e539d3",
		},
		{
			name:        "should return path based private url",
			externalUrl: "https://app.example.com",
			stub: &types.StubWithRelated{
				Stub: types.Stub{
					ExternalId: "20a3f632-a5f8-4013-a2ac-4ab5c80912c7",
					Config:     `{"authorized": true}`,
					Type:       types.StubType(types.StubTypeEndpointDeployment),
				},
			},
			deployment: &types.Deployment{
				Name:    "app2",
				Version: uint(23),
			},
			invokeType: "path",
			want:       "https://app.example.com/endpoint/app2/v23",
		},
		{
			name:        "should return domain based public url",
			externalUrl: "https://app.example.com",
			stub: &types.StubWithRelated{
				Stub: types.Stub{
					ExternalId: "20a3f632-a5f8-4013-a2ac-4ab5c80912c7",
					Config:     `{"authorized": false}`,
					Type:       types.StubType(types.StubTypeEndpointDeployment),
					Group:      "app2-fffffff",
				},
			},
			deployment: &types.Deployment{
				Name:    "app2",
				Version: uint(23),
			},
			invokeType: "domain",
			want:       "https://app2-fffffff-20a3f632-a5f8-4013-a2ac-4ab5c80912c7.app.example.com",
		},
		{
			name:        "should return domain based private url",
			externalUrl: "https://app.example.com",
			stub: &types.StubWithRelated{
				Stub: types.Stub{
					ExternalId: "44ad2b42-69be-49f8-806d-e0ee644f9341",
					Config:     `{"authorized": true}`,
					Type:       types.StubType(types.StubTypeASGIDeployment),
					Group:      "app2-fffffff",
				},
			},
			deployment: &types.Deployment{
				Name:    "app2",
				Version: uint(24),
			},
			invokeType: "domain",
			want:       "https://app2-fffffff-v24.app.example.com",
		},
		{
			name:        "should return path based serve url",
			externalUrl: "https://app.example.com",
			stub: &types.StubWithRelated{
				Stub: types.Stub{
					ExternalId: "aff86f02-c968-47a9-9132-0bde826b0aca",
					Config:     `{}`,
					Type:       types.StubType(types.StubTypeASGIServe),
				},
			},
			deployment: &types.Deployment{
				Name:    "app2",
				Version: uint(28),
			},
			invokeType: "path",
			want:       "https://app.example.com/asgi/id/aff86f02-c968-47a9-9132-0bde826b0aca",
		},
		{
			name:        "should return domain based serve url",
			externalUrl: "https://app.example.com",
			stub: &types.StubWithRelated{
				Stub: types.Stub{
					ExternalId: "aff86f02-c968-47a9-9132-0bde826b0aca",
					Config:     `{}`,
					Type:       types.StubType(types.StubTypeASGIServe),
					Group:      "app2-eeeeeee",
				},
			},
			deployment: &types.Deployment{
				Name:    "app2",
				Version: uint(25),
			},
			invokeType: "domain",
			want:       "https://app2-eeeeeee-aff86f02-c968-47a9-9132-0bde826b0aca.app.example.com",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got string
			if test.stub.Type.IsServe() {
				got = BuildServeURL(test.externalUrl, test.invokeType, test.stub)
			} else {
				got = BuildDeploymentURL(test.externalUrl, test.invokeType, test.stub, test.deployment)
			}
			assert.Equal(t, test.want, got)
		})
	}
}
