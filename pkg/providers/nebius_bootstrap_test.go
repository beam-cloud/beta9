package providers

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// Nebius wraps this field as a cloud-config part before delivering it to
// cloud-init. Decode it as the YAML mapping the real guest requires; the old
// raw shell body is a scalar and fails here, just as the production serial
// console reported: "Yaml load allows ... dict ... but got str instead".
func nebiusStartupCommand(t *testing.T, userData string) []string {
	t.Helper()
	require.True(t, strings.HasPrefix(userData, "#cloud-config\n"))
	var config struct {
		RunCmd [][]string `yaml:"runcmd"`
	}
	require.NoError(t, yaml.UnmarshalStrict([]byte(userData), &config))
	require.Len(t, config.RunCmd, 1, "install once in the final stage, rather than every boot or twice")
	require.Len(t, config.RunCmd[0], 3)
	require.Equal(t, []string{"/bin/bash", "-c"}, config.RunCmd[0][:2])
	return config.RunCmd[0]
}

func TestNebiusStartupIsExecutableCloudConfig(t *testing.T) {
	for name, commands := range map[string]string{
		"installer": "curl -fsSL 'https://gateway.example/install' | sh -s -- --join-token 'synthetic-token'",
		"quoting":   "printf '%s\\n' \"double: 'single' # comment \\ tab\t <>& UTF-8: λ\"\n",
		"heredoc":   "cat <<'CONFIG'\n#cloud-config\nruncmd:\n  - [echo, do-not-run-as-another-command]\nCONFIG\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			api := newNebiusAPI(t)
			_, err := api.client(testOwner).Create(context.Background(), CreateRequest{
				Offer: nebiusH200, Name: "beam-cloud-config-test", StartupScript: commands,
			})
			require.NoError(t, err)
			userData := api.lastCreate()["spec"].(map[string]any)["cloudInitUserData"].(string)
			argv := nebiusStartupCommand(t, userData)
			require.Equal(t, "#!/usr/bin/env bash\nset -euo pipefail\n"+commands+"\n", argv[2], "the complete shell program must survive the provider's YAML parsing unchanged")
		})
	}
}

func TestNebiusCloudConfigRunsTheOriginalCommandOnceAndKeepsPipefail(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	output := filepath.Join(t.TempDir(), "result")
	commands := `printf '%s\n' "single ' quote: # \\" >> "$SMOKE_OUTPUT"` + "\n"
	_, err := client.Create(context.Background(), CreateRequest{
		Offer: nebiusH200, Name: "beam-cloud-config-test", StartupScript: commands,
	})
	require.NoError(t, err)
	argv := nebiusStartupCommand(t, api.lastCreate()["spec"].(map[string]any)["cloudInitUserData"].(string))
	command := exec.Command(argv[0], argv[1:]...)
	command.Env = append(os.Environ(), "SMOKE_OUTPUT="+output)
	log, err := command.CombinedOutput()
	require.NoError(t, err, "%s", log)
	contents, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, "single ' quote: # \\\n", string(contents), "the one runcmd entry must execute the preserved shell program exactly once")

	_, err = client.Create(context.Background(), CreateRequest{
		Offer: nebiusH200, Name: "beam-cloud-config-failure-test", StartupScript: "false | true\nprintf unexpected >> \"$SMOKE_OUTPUT\"",
	})
	require.NoError(t, err)
	argv = nebiusStartupCommand(t, api.lastCreate()["spec"].(map[string]any)["cloudInitUserData"].(string))
	command = exec.Command(argv[0], argv[1:]...)
	command.Env = append(os.Environ(), "SMOKE_OUTPUT="+output)
	log, err = command.CombinedOutput()
	require.Error(t, err, "%s", log)
	contents, err = os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, "single ' quote: # \\\n", string(contents), "a failed bootstrap pipeline must stop before later commands")
}
