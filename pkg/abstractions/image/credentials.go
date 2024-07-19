package image

import (
	"context"
	_ "embed"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
)

// Check which registry is passed in
func GetRegistryToken(opts *BuildOpts) (string, error) {
	if strings.Contains(opts.ExistingImageUri, "amazonaws.com") {
		if token, err := GetECRToken(opts); err == nil {
			return token, nil
		} else {
			return "", err
		}
	}

	// Add additional registry checks here as needed.
	return "", fmt.Errorf("unsupported registry or missing implementation")
}

// Retrieve an authorization token from Amazon ECR
func GetECRToken(opts *BuildOpts) (string, error) {
	creds := opts.ExistingImageCreds

	accessKey, ok := creds["AWS_ACCESS_KEY_ID"]
	if !ok {
		return "", fmt.Errorf("AWS_ACCESS_KEY_ID missing or not a string")
	}
	secretKey, ok := creds["AWS_SECRET_ACCESS_KEY"]
	if !ok {
		return "", fmt.Errorf("AWS_SECRET_ACCESS_KEY missing or not a string")
	}
	sessionToken, ok := creds["AWS_SESSION_TOKEN"]
	if !ok {
		sessionToken = ""
		// return "", fmt.Errorf("AWS_SESSION_TOKEN missing or not a string")
	}
	region, ok := creds["AWS_REGION"]
	if !ok {
		return "", fmt.Errorf("AWS_REGION missing or not a string")
	}

	credentials := credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken)
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region), config.WithCredentialsProvider(credentials))
	if err != nil {
		return "", fmt.Errorf("error loading AWS configuration: %v", err)
	}

	// Create an ECR client with the custom configuration
	client := ecr.NewFromConfig(cfg)

	// Get authorization token
	output, err := client.GetAuthorizationToken(context.TODO(), &ecr.GetAuthorizationTokenInput{})
	if err != nil {
		return "", fmt.Errorf("error getting ECR authorization token: %v", err)
	}

	if len(output.AuthorizationData) == 0 || output.AuthorizationData[0].AuthorizationToken == nil {
		return "", fmt.Errorf("no authorization data returned")
	}

	// Decode the authorization token to get the username and password
	base64Token := aws.ToString(output.AuthorizationData[0].AuthorizationToken)
	decodedToken, err := base64.StdEncoding.DecodeString(base64Token)

	if err != nil {
		return "", fmt.Errorf("error decoding token: %v", err)
	}

	parts := strings.SplitN(string(decodedToken), ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("decoded token format invalid, expected username:password")
	}

	// Return the username and password
	username := parts[0]
	password := parts[1]

	token := fmt.Sprintf("%s:%s", username, password)
	return token, nil
}
