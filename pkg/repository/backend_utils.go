package repository

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

var (
	invalidCharRegexp     = regexp.MustCompile(`[^a-zA-Z0-9-]+`)
	multipleHyphensRegexp = regexp.MustCompile(`-{2,}`)
)

// sanitizeDeploymentName cleans up the deployment name according to DNS rules
func sanitizeDeploymentName(name string) string {
	s := invalidCharRegexp.ReplaceAllString(name, "-")
	s = multipleHyphensRegexp.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")

	return strings.ToLower(s)
}

func generateHash(hashLength int, inputs ...any) string {
	inputStrings := make([]string, len(inputs))
	for i, input := range inputs {
		inputStrings[i] = fmt.Sprintf("%v", input)
	}

	data := strings.Join(inputStrings, "")
	hashBytes := md5.Sum([]byte(data))
	hashString := hex.EncodeToString(hashBytes[:])

	return hashString[:hashLength]
}

func generateSubdomain(deploymentName, stubType string, workspaceId uint) string {
	const maxSubdomainLength = 63
	const hashLength = 7
	const hyphenLength = 1

	sanitized := sanitizeDeploymentName(deploymentName)
	if sanitized == "" {
		return ""
	}

	// Adjust the sanitized length to ensure the final subdomain does not exceed 63 characters
	maxSanitizedLength := maxSubdomainLength - hashLength - hyphenLength
	if len(sanitized) > maxSanitizedLength {
		sanitized = sanitized[:maxSanitizedLength]
	}

	hashPart := generateHash(hashLength, deploymentName, stubType, workspaceId)
	subdomain := fmt.Sprintf("%s-%s", sanitized, hashPart)

	return subdomain
}
