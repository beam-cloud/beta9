package agent

import (
	"regexp"
	"time"

	corev1 "k8s.io/api/core/v1"
)

type Config struct {
	AgentAddress                      string
	AgentToken                        string
	AgentVersion                      string
	AgentIsLocal                      bool
	WorkerEventSubscriptionRetryDelay time.Duration
	WorkerNamespace                   string
	WorkerImagePullSecrets            []string
	WorkerContainerEnvVars            []corev1.EnvVar
}

var (
	agentVersion   string         = "dev"
	addressPattern *regexp.Regexp = regexp.MustCompile(`^[a-zA-Z0-9-\.]+:[0-9]{1,5}$`)
	tokenPattern   *regexp.Regexp = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

// func LoadConfig() (*Config, error) {
// 	secrets := common.Secrets()

// 	c := &Config{
// 		AgentAddress:                      secrets.Get("BEAM_AGENT_URL"),
// 		AgentToken:                        secrets.Get("BEAM_AGENT_TOKEN"),
// 		AgentVersion:                      agentVersion,
// 		AgentIsLocal:                      secrets.Get("BEAM_AGENT_IS_LOCAL") == "true",
// 		WorkerNamespace:                   secrets.Get("BEAM_AGENT_WORKER_NAMESPACE"),
// 		WorkerEventSubscriptionRetryDelay: time.Second * 10,
// 	}

// 	c.WorkerContainerEnvVars = []corev1.EnvVar{
// 		{
// 			Name:  "AWS_ACCESS_KEY_ID",
// 			Value: secrets.Get("BEAM_AGENT_WORKER_AWS_ACCESS_KEY_ID"),
// 		},
// 		{
// 			Name:  "AWS_SECRET_ACCESS_KEY",
// 			Value: secrets.Get("BEAM_AGENT_WORKER_AWS_SECRET_ACCESS_KEY"),
// 		},
// 		{
// 			Name:  "AWS_REGION",
// 			Value: secrets.Get("BEAM_AGENT_WORKER_AWS_REGION"),
// 		},
// 	}

// 	if !addressPattern.MatchString(c.AgentAddress) {
// 		return nil, fmt.Errorf("BEAM_AGENT_URL is invalid. It must be in the format HOST:PORT. Provided value: %v", c.AgentAddress)
// 	}

// 	if !tokenPattern.MatchString(c.AgentToken) {
// 		return nil, fmt.Errorf("BEAM_AGENT_TOKEN is invalid. It must be a valid UUIDv4. Provided value: %v", c.AgentToken)
// 	}

// 	if c.WorkerNamespace == "" {
// 		return nil, errors.New("BEAM_AGENT_WORKER_NAMESPACE is invalid. It must be a non-empty string.")
// 	}

// 	return c, nil
// }
