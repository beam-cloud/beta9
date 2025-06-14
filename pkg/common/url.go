package common

import (
	"fmt"
	"net/url"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	InvokeUrlTypePath string = "path"
	InvokeUrlTypeHost string = "host"
)

func BuildDeploymentURL(externalUrl, urlType string, stub *types.StubWithRelated, deployment *types.Deployment) string {
	parsedUrl, err := url.Parse(externalUrl)
	if err != nil {
		return ""
	}

	if urlType == InvokeUrlTypeHost {
		return fmt.Sprintf("%s://%s-v%d.%s", parsedUrl.Scheme, deployment.Subdomain, deployment.Version, parsedUrl.Host)
	}

	stubConfig, err := stub.UnmarshalConfig()
	isPublic := err == nil && !stubConfig.Authorized

	if isPublic {
		return fmt.Sprintf("%s://%s/%s/public/%s", parsedUrl.Scheme, parsedUrl.Host, stub.Type.Kind(), stub.ExternalId)
	}
	return fmt.Sprintf("%s://%s/%s/%s/v%d", parsedUrl.Scheme, parsedUrl.Host, stub.Type.Kind(), deployment.Name, deployment.Version)
}

func BuildStubURL(externalUrl, urlType string, stub *types.StubWithRelated) string {
	parsedUrl, err := url.Parse(externalUrl)
	if err != nil {
		return ""
	}

	if urlType == InvokeUrlTypeHost {
		return fmt.Sprintf("%s://%s.%s", parsedUrl.Scheme, stub.ExternalId, parsedUrl.Host)
	}
	return fmt.Sprintf("%s://%s/%s/id/%s", parsedUrl.Scheme, parsedUrl.Host, stub.Type.Kind(), stub.ExternalId)
}

func BuildPodURL(externalUrl, urlType string, stub *types.StubWithRelated, stubConfig *types.StubConfigV1) string {
	parsedUrl, err := url.Parse(externalUrl)
	if err != nil {
		return ""
	}

	url := ""

	portPlaceholder := "<PORT>"
	if len(stubConfig.Ports) == 1 {
		portPlaceholder = fmt.Sprintf("%d", stubConfig.Ports[0])
	}

	if urlType == InvokeUrlTypeHost {
		url = fmt.Sprintf("%s://%s-%s.%s", parsedUrl.Scheme, stub.ExternalId, portPlaceholder, parsedUrl.Host)
	} else {
		url = fmt.Sprintf("%s://%s/%s/id/%s/%s", parsedUrl.Scheme, parsedUrl.Host, stub.Type.Kind(), stub.ExternalId, portPlaceholder)
	}

	return url
}

func BuildSandboxURL(externalUrl, urlType string, stub *types.StubWithRelated, port int32) string {
	parsedUrl, err := url.Parse(externalUrl)
	if err != nil {
		return ""
	}

	url := ""

	if urlType == InvokeUrlTypeHost {
		url = fmt.Sprintf("%s://%s-%d.%s", parsedUrl.Scheme, stub.ExternalId, port, parsedUrl.Host)
	} else {
		url = fmt.Sprintf("%s://%s/%s/id/%s/%d", parsedUrl.Scheme, parsedUrl.Host, stub.Type.Kind(), stub.ExternalId, port)
	}

	return url
}
