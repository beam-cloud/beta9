//go:build linux

package worker

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/coreos/go-iptables/iptables"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/dns/dnsmessage"
)

func TestRuntimeNetworkHelper(t *testing.T) {
	switch os.Getenv("BETA9_NETWORK_HELPER") {
	case "server":
		_ = http.ListenAndServe(":19082", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "ok") }))
		os.Exit(1)
	case "client":
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		request, _ := http.NewRequestWithContext(ctx, "GET", os.Getenv("BETA9_NETWORK_URL"), nil)
		client := http.Client{Transport: &http.Transport{Proxy: nil}}
		response, err := client.Do(request)
		if err != nil {
			os.Exit(1)
		}
		response.Body.Close()
		if response.StatusCode != 200 {
			os.Exit(1)
		}
		os.Exit(0)
	default:
		t.Skip("namespace helper")
	}
}

func TestRuntimeNetworkIntegration(t *testing.T) {
	if os.Getenv("BETA9_LINUX_INTEGRATION") != "1" {
		t.Skip("requires an isolated privileged Linux test container")
	}
	require.Zero(t, os.Geteuid())
	run := func(args ...string) {
		t.Helper()
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		require.NoError(t, err, "%v: %s", args, out)
	}
	for _, program := range []string{"iptables", "ip6tables"} {
		saved, err := exec.Command(program + "-save").Output()
		require.NoError(t, err)
		t.Cleanup(func() { cmd := exec.Command(program + "-restore"); cmd.Stdin = bytes.NewReader(saved); _ = cmd.Run() })
	}
	t.Cleanup(func() {
		_ = exec.Command("ip", "link", "del", containerBridgeLinkName).Run()
		_ = exec.Command("ip", "link", "del", "cni-podman0").Run()
	})
	run("ip", "link", "add", containerBridgeLinkName, "type", "bridge")
	run("ip", "addr", "add", "192.168.0.1/24", "dev", containerBridgeLinkName)
	run("ip", "-6", "addr", "add", "fd00:abcd::1/64", "dev", containerBridgeLinkName, "nodad")
	run("ip", "link", "set", containerBridgeLinkName, "up")
	run("ip", "link", "add", "cni-podman0", "type", "bridge")
	run("ip", "addr", "add", "10.88.0.1/24", "dev", "cni-podman0")
	run("ip", "-6", "addr", "add", "fd00:8888::1/64", "dev", "cni-podman0", "nodad")
	run("ip", "link", "set", "cni-podman0", "up")
	for _, address := range []string{"1.1.1.1/32", "169.254.169.254/32", "10.55.0.7/32", "198.18.0.2/32", "2606:4700:4700::1111/128", "fd00:ec2::254/128", "2001:db8:feed::7/128"} {
		run("ip", "addr", "add", address, "dev", "lo")
		t.Cleanup(func() { _ = exec.Command("ip", "addr", "del", address, "dev", "lo").Run() })
	}
	require.NoError(t, os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1\n"), 0644))
	require.NoError(t, os.WriteFile("/proc/sys/net/ipv6/conf/all/forwarding", []byte("1\n"), 0644))
	original, err := os.ReadFile("/etc/resolv.conf")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.WriteFile("/etc/resolv.conf", original, 0644) })
	require.NoError(t, os.WriteFile("/etc/resolv.conf", []byte("nameserver 192.168.0.1\noptions timeout:1 attempts:1\n"), 0644))
	dns, err := net.ListenPacket("udp4", "192.168.0.1:53")
	require.NoError(t, err)
	t.Cleanup(func() { dns.Close() })
	go func() {
		for {
			buffer := make([]byte, 4096)
			n, peer, err := dns.ReadFrom(buffer)
			if err != nil {
				return
			}
			var parser dnsmessage.Parser
			header, err := parser.Start(buffer[:n])
			if err != nil {
				continue
			}
			question, err := parser.Question()
			if err != nil {
				continue
			}
			builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: header.ID, Response: true, Authoritative: true, RecursionAvailable: true})
			builder.EnableCompression()
			_ = builder.StartQuestions()
			_ = builder.Question(question)
			_ = builder.StartAnswers()
			rh := dnsmessage.ResourceHeader{Name: question.Name, Class: dnsmessage.ClassINET, TTL: 1}
			switch question.Type {
			case dnsmessage.TypeA:
				_ = builder.AResource(rh, dnsmessage.AResource{A: [4]byte{1, 1, 1, 1}})
			case dnsmessage.TypeAAAA:
				var address [16]byte
				copy(address[:], net.ParseIP("2606:4700:4700::1111").To16())
				_ = builder.AAAAResource(rh, dnsmessage.AAAAResource{AAAA: address})
			}
			answer, err := builder.Finish()
			if err == nil {
				_, _ = dns.WriteTo(answer, peer)
			}
		}
	}()
	listen := func() int {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "ok") })}
		go server.Serve(listener)
		t.Cleanup(func() { server.Close() })
		return listener.Addr().(*net.TCPAddr).Port
	}
	publicPort, proxyPort := listen(), listen()
	binary, err := os.Executable()
	require.NoError(t, err)
	create := func(name, bridge, ipv4, gateway, ipv6, gateway6 string) {
		run("ip", "netns", "add", name)
		t.Cleanup(func() { _ = exec.Command("ip", "netns", "del", name).Run() })
		host, guest := "h"+name, "n"+name
		run("ip", "link", "add", host, "type", "veth", "peer", "name", guest)
		run("ip", "link", "set", host, "master", bridge)
		run("ip", "link", "set", host, "up")
		run("ip", "link", "set", guest, "netns", name)
		run("ip", "netns", "exec", name, "ip", "link", "set", "lo", "up")
		run("ip", "netns", "exec", name, "ip", "link", "set", guest, "up")
		run("ip", "netns", "exec", name, "ip", "addr", "add", ipv4+"/24", "dev", guest)
		run("ip", "netns", "exec", name, "ip", "route", "add", "default", "via", gateway)
		run("ip", "netns", "exec", name, "ip", "-6", "addr", "add", ipv6+"/64", "dev", guest, "nodad")
		run("ip", "netns", "exec", name, "ip", "-6", "route", "add", "default", "via", gateway6)
	}
	create("ordinary", containerBridgeLinkName, "192.168.0.2", "192.168.0.1", "fd00:abcd::2", "fd00:abcd::1")
	create("guest", containerBridgeLinkName, "192.168.0.3", "192.168.0.1", "fd00:abcd::3", "fd00:abcd::1")
	create("buildah", "cni-podman0", "10.88.0.2", "10.88.0.1", "fd00:8888::2", "fd00:8888::1")
	server := exec.Command("ip", "netns", "exec", "guest", binary, "-test.run=^TestRuntimeNetworkHelper$")
	server.Env = append(os.Environ(), "BETA9_NETWORK_HELPER=server")
	require.NoError(t, server.Start())
	t.Cleanup(func() { _ = server.Process.Kill(); _ = server.Wait() })
	client := func(namespace, url string) error {
		cmd := exec.Command("ip", "netns", "exec", namespace, binary, "-test.run=^TestRuntimeNetworkHelper$")
		cmd.Env = append(os.Environ(), "BETA9_NETWORK_HELPER=client", "BETA9_NETWORK_URL="+url)
		return cmd.Run()
	}
	require.Eventually(t, func() bool { return client("guest", "http://127.0.0.1:19082") == nil }, 5*time.Second, 50*time.Millisecond)
	ipt, err := iptables.New(iptables.IPFamily(iptables.ProtocolIPv4))
	require.NoError(t, err)
	ipt6, err := iptables.New(iptables.IPFamily(iptables.ProtocolIPv6))
	require.NoError(t, err)
	manager := &ContainerNetworkManager{ipt: ipt, ipt6: ipt6, config: types.AppConfig{Worker: types.WorkerConfig{UseHostResolvConf: true, Egress: types.EgressConfig{HTTPPort: proxyPort, DeniedCIDRs: []string{"2001:db8:feed::/64"}}}}}
	require.NoError(t, manager.ensureNetworkIsolation())
	// A later CNI insertion must still consult the administrative restrictions.
	for _, table := range []*iptables.IPTables{ipt, ipt6} {
		require.NoError(t, table.NewChain("filter", "CNI-FORWARD"))
		require.NoError(t, table.Append("filter", "CNI-FORWARD", "-j", "CNI-ADMIN"))
		require.NoError(t, table.Append("filter", "CNI-FORWARD", "-j", "ACCEPT"))
		require.NoError(t, table.Insert("filter", "FORWARD", 1, "-j", "CNI-FORWARD"))
	}
	for _, namespace := range []string{"ordinary", "buildah"} {
		t.Run(namespace, func(t *testing.T) {
			for _, host := range []string{"1.1.1.1", "public.test", "[2606:4700:4700::1111]"} {
				require.NoError(t, client(namespace, "http://"+host+":"+strconv.Itoa(publicPort)), "public/DNS %s", host)
			}
			for _, host := range []string{"169.254.169.254", "10.55.0.7", "[fd00:ec2::254]", "[2001:db8:feed::7]", "192.168.0.1"} {
				require.Error(t, client(namespace, "http://"+host+":"+strconv.Itoa(publicPort)), "restricted %s", host)
			}
			require.Error(t, client(namespace, "http://192.168.0.3:19082"))
			require.Error(t, client(namespace, "http://[fd00:abcd::3]:19082"))
			require.NoError(t, client(namespace, "http://198.18.0.2:"+strconv.Itoa(proxyPort)))
			require.Error(t, client(namespace, "http://198.18.0.2:"+strconv.Itoa(publicPort)))
		})
	}
	// A second worker adds listeners without disrupting the first worker.
	manager.config.Worker.Egress.HTTPPort = listen()
	require.NoError(t, manager.ensureNetworkIsolation())
	require.NoError(t, client("ordinary", "http://198.18.0.2:"+strconv.Itoa(proxyPort)))
	if _, err := os.Stat("/fixtures/alpine.tar"); err == nil {
		t.Run("build-runtime", func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.WithValue(context.Background(), buildNetworkContextKey{}, "/var/run/netns/ordinary")
			base := []string{"--root", filepath.Join(dir, "store"), "--runroot", filepath.Join(dir, "run"), "--storage-driver=vfs"}
			command := func(args ...string) (string, error) {
				var output bytes.Buffer
				commandCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
				defer cancel()
				err := newBuildahCommand(commandCtx, append(append([]string(nil), base...), args...), os.Environ(), &output, &output).Run()
				return output.String(), err
			}
			output, err := command("pull", "docker-archive:/fixtures/alpine.tar")
			require.NoError(t, err, output)
			output, err = command("from", "--name", "runtime-check", "alpine:3.20")
			require.NoError(t, err, output)
			// Nested Docker does not delegate a writable cgroup tree. Only this
			// integration fixture disables runsc's cgroup manager.
			isolation := append(buildahIsolationArgs(), "--runtime-flag=ignore-cgroups=true")
			args := append([]string{"run"}, isolation...)
			args = append(args, "runtime-check", "--", "/bin/sh", "-c", "uname -r | grep gvisor; echo persisted > /runtime-marker")
			output, err = command(args...)
			require.NoError(t, err, output)
			args = append(append([]string{"run"}, isolation...), "runtime-check", "--", "/bin/sh", "-c", "test \"$(cat /runtime-marker)\" = persisted")
			output, err = command(args...)
			require.NoError(t, err, output)
			dockerfile := filepath.Join(dir, "Dockerfile")
			require.NoError(t, os.WriteFile(dockerfile, []byte(fmt.Sprintf("FROM alpine:3.20\nRUN uname -r | grep gvisor && echo persisted > /runtime-marker\nRUN test \"$(cat /runtime-marker)\" = persisted\nADD http://1.1.1.1:%d /download\n", publicPort)), 0600))
			args = append([]string{"bud"}, isolation...)
			args = append(args, "-f", dockerfile, "-t", "runtime-built", dir)
			output, err = command(args...)
			require.NoError(t, err, output)
			for _, destination := range []string{"169.254.169.254", "10.55.0.7", "192.168.0.3"} {
				require.NoError(t, os.WriteFile(dockerfile, []byte(fmt.Sprintf("FROM alpine:3.20\nADD http://%s:%d /download\n", destination, publicPort)), 0600))
				_, err = command(args...)
				require.Error(t, err, "parent download %s", destination)
			}
			// An unavailable sandbox runtime must terminate the build.
			output, err = command("run", "--isolation=oci", "--runtime=/missing/runsc", "runtime-check", "--", "/bin/true")
			require.Error(t, err, output)
		})
	}
}
