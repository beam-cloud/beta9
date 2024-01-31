package agent

import (
	"regexp"
)

type Config struct {
	AgentAddress    string
	AgentToken      string
	AgentVersion    string
	WorkerNamespace string
}

var (
	agentVersion   string         = "dev"
	addressPattern *regexp.Regexp = regexp.MustCompile(`^[a-zA-Z0-9-\.]+:[0-9]{1,5}$`)
	tokenPattern   *regexp.Regexp = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)
