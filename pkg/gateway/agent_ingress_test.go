package gateway

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Run explicitly in a disposable Linux network namespace/container with
// CAP_NET_ADMIN and CAP_SYS_ADMIN. The normal test suite never edits networking.
func TestManagedIngressLinuxNetwork(t *testing.T) {
	if os.Getenv("BETA9_TEST_MANAGED_INGRESS_NETWORK") != "1" || runtime.GOOS != "linux" {
		t.Skip("requires an explicitly enabled isolated Linux network test")
	}
	script := filepath.Join(t.TempDir(), "ingress.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nset -eu\nINTERFACES=public0\nTCP_PORTS='22 8443'\nUDP_PORTS=41641\n"+managedIngressApplyScript), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("python3", "testdata/managed_ingress_network.py", script)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("network regression: %v\n%s", err, out)
	} else {
		t.Log(string(out))
	}
}

func TestManagedIngressPreservesOnlyExplicitIngressAndOutboundReplies(t *testing.T) {
	bin := t.TempDir()
	logDir := t.TempDir()
	restore := `#!/bin/sh
set -eu
[ "$*" = "--wait 5 --noflush" ]
cat > "$INGRESS_TEST_LOG/$(basename "$0").rules"
`
	hook := `#!/bin/sh
set -eu
case "$*" in
  *"-t mangle -C PREROUTING"*)
    test -f "$INGRESS_TEST_LOG/$(basename "$0").hook" ;;
  *"-t mangle -I PREROUTING 1 -i public0 -j BEAM-INGRESS")
    echo installed >> "$INGRESS_TEST_LOG/$(basename "$0").hook" ;;
  *) echo "unexpected firewall command: $*" >&2; exit 1 ;;
esac
`
	for _, name := range []string{"iptables", "ip6tables", "iptables-restore", "ip6tables-restore"} {
		script := hook
		if strings.HasSuffix(name, "-restore") {
			script = restore
		}
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("INGRESS_TEST_LOG", logDir)
	for i := 0; i < 2; i++ {
		cmd := exec.Command("sh", "-eu")
		cmd.Stdin = strings.NewReader("INTERFACES=public0\nTCP_PORTS='22 8443'\nUDP_PORTS=41641\n" + managedIngressApplyScript)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("apply %d: %v\n%s", i, err, out)
		}
	}
	for _, family := range []string{"iptables", "ip6tables"} {
		data, err := os.ReadFile(filepath.Join(logDir, family+"-restore.rules"))
		if err != nil {
			t.Fatal(err)
		}
		rules := string(data)
		for _, rule := range []string{
			"--ctstate ESTABLISHED,RELATED --ctdir REPLY -j RETURN",
			"-p tcp --dport 22 -j RETURN", "-p tcp --dport 8443 -j RETURN",
			"-p udp --dport 41641 -j RETURN", "-A BEAM-INGRESS -j DROP\nCOMMIT",
		} {
			if !strings.Contains(rules, rule) {
				t.Errorf("%s missing %q", family, rule)
			}
		}
		// Preserve filter/NAT policies and apply the port list to established
		// inbound connections as well as new connections.
		for _, forbidden := range []string{"*filter", "*nat", "-P ", "--dport 18081", "--ctstate ESTABLISHED,RELATED -j RETURN"} {
			if strings.Contains(rules, forbidden) {
				t.Errorf("%s contains %q", family, forbidden)
			}
		}
		hooks, err := os.ReadFile(filepath.Join(logDir, family+".hook"))
		if err != nil || string(hooks) != "installed\n" {
			t.Fatalf("%s installed duplicate hooks: %q, %v", family, hooks, err)
		}
	}
}

func TestManagedIngressStopsOnFirewallFailure(t *testing.T) {
	bin := t.TempDir()
	for _, name := range []string{"iptables-restore", "ip6tables-restore", "iptables", "ip6tables"} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\nexit 42\n"), 0755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	cmd := exec.Command("sh", "-eu")
	cmd.Stdin = strings.NewReader("INTERFACES=public0\nTCP_PORTS=22\nUDP_PORTS=41641\n" + managedIngressApplyScript + "\necho AGENT_STARTED\n")
	out, err := cmd.CombinedOutput()
	if err == nil || strings.Contains(string(out), "AGENT_STARTED") {
		t.Fatalf("continued after firewall failure: %v, %s", err, out)
	}
}

func TestManagedIngressIsOptInAndRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, vars string
		wantError  bool
	}{
		{"manual join", "MANAGED_INGRESS=0\nOS=darwin", false},
		{"non-linux", "MANAGED_INGRESS=1\nOS=darwin", true},
		{"invalid interface", "MANAGED_INGRESS=1\nOS=linux\nBEAM_AGENT_INGRESS_INTERFACES='eth0;echo BAD'", true},
		{"invalid port", "MANAGED_INGRESS=1\nOS=linux\nBEAM_AGENT_INGRESS_INTERFACES=eth0\nBEAM_AGENT_PUBLIC_TCP_PORTS=65536", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Stubs stop valid installs before any machine filesystem changes.
			bin := t.TempDir()
			for _, name := range []string{"ip", "iptables", "ip6tables", "iptables-restore", "ip6tables-restore", "systemctl", "id"} {
				if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\nprintf '0\\n'\n"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command("sh", "-eu")
			cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			cmd.Stdin = strings.NewReader(managedIngressInstallScript + "\nfail() { echo \"$1\" >&2; exit 1; }\n" + tc.vars + "\nensure_linux_managed_ingress\n")
			out, err := cmd.CombinedOutput()
			if (err != nil) != tc.wantError {
				t.Fatalf("error=%v, wantError=%v; %s", err, tc.wantError, out)
			}
		})
	}
}
