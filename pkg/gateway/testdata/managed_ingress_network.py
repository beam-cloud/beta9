"""Packet-level regression; run only in a disposable Linux network namespace."""
import atexit
import json
import socket
import subprocess
import sys
import threading
import time

PROCESSES = []


@atexit.register
def cleanup():
    for process in PROCESSES:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def ns(name, *args):
    return run("ip", "netns", "exec", name, *args)


def echo_server(address, port, udp=False):
    family = socket.AF_INET6 if ":" in address else socket.AF_INET
    s = socket.socket(family, socket.SOCK_DGRAM if udp else socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((address, port))
    if udp:
        while True:
            data, addr = s.recvfrom(4096)
            s.sendto(data, addr)
    else:
        s.listen()
        while True:
            c, _ = s.accept()
            threading.Thread(target=echo_connection, args=(c,), daemon=True).start()


def echo_connection(c):
    with c:
        while data := c.recv(4096):
            c.sendall(data)


if sys.argv[1] == "server":
    for addr in sys.argv[2:]:
        for port in (22, 8080, 18081):
            threading.Thread(target=echo_server, args=(addr, port), daemon=True).start()
        for port in (41641, 9999):
            threading.Thread(target=echo_server, args=(addr, port, True), daemon=True).start()
    print("READY", flush=True)
    while True:
        time.sleep(1)

CLIENT = """
import socket,sys
addr,port,proto=sys.argv[1],int(sys.argv[2]),sys.argv[3]
s=socket.socket(socket.AF_INET6 if ':' in addr else socket.AF_INET,
                socket.SOCK_DGRAM if proto=='udp' else socket.SOCK_STREAM)
s.settimeout(2)
try:
 s.connect((addr,port));s.send(b'check');assert s.recv(100)==b'check'
 print('OPEN')
except (OSError,AssertionError):print('BLOCKED')
"""
PERSISTENT = """
import socket,sys
s=socket.create_connection((sys.argv[1],int(sys.argv[2])),timeout=.7)
for line in sys.stdin:
 try:s.send(line.encode());print('OPEN' if s.recv(100)==line.encode() else 'BLOCKED',flush=True)
 except OSError:print('BLOCKED',flush=True)
"""


def check(where, address, port, expected="OPEN", proto="tcp"):
    result = ns(where, "python3", "-c", CLIENT, address, str(port), proto)
    if result != expected:
        print(run("ip", "-6", "route", "show"), flush=True)
        print(run("ip", "-6", "neigh", "show"), flush=True)
        print(run("ip6tables", "-t", "nat", "-L", "-n", "-v"), flush=True)
        print(run("ip6tables", "-L", "-n", "-v"), flush=True)
        print(run("sysctl", "net.ipv6.conf.public0.forwarding", "net.ipv6.conf.private0.forwarding"), flush=True)
        for namespace in ("outside", "backend"):
            print(namespace, ns(namespace, "ip", "-6", "route", "show"), flush=True)
            print(ns(namespace, "ip", "-6", "addr", "show"), flush=True)
    assert result == expected, (where, address, port, proto, result, expected)


def persistent(address, port):
    p = subprocess.Popen(
        ["ip", "netns", "exec", "outside", "python3", "-c", PERSISTENT, address, str(port)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    )
    PROCESSES.append(p)
    p.stdin.write("before\n")
    p.stdin.flush()
    assert p.stdout.readline().strip() == "OPEN"
    return p


def main(script):
    # No host network or host filesystem mount is needed. All interfaces,
    # routing, sysctls and rules below belong to this disposable container.
    interfaces = json.loads(run("ip", "-j", "addr", "show"))
    # Docker Desktop also exposes these unconfigured kernel tunnel devices in
    # a network=none namespace. Reject any configured or unexpected interface.
    tunnels = {"tunl0", "gre0", "gretap0", "erspan0", "ip_vti0", "ip6_vti0",
               "sit0", "ip6tnl0", "ip6gre0"}
    assert all(i["ifname"] == "lo" or
               (i["ifname"] in tunnels and "UP" not in i["flags"] and not i["addr_info"])
               for i in interfaces), "requires --network none; refusing configured network"
    assert not json.loads(run("ip", "-j", "route", "show"))
    assert not json.loads(run("ip", "-j", "-6", "route", "show", "default"))
    for name in ("outside", "backend"):
        run("ip", "netns", "add", name)
        ns(name, "ip", "link", "set", "lo", "up")
    for host, peer, namespace, subnet in (
        ("public0", "peer0", "outside", 1), ("private0", "peer1", "backend", 2)
    ):
        run("ip", "link", "add", host, "type", "veth", "peer", "name", peer)
        run("ip", "link", "set", peer, "netns", namespace)
        run("ip", "link", "set", host, "up")
        ns(namespace, "ip", "link", "set", peer, "up")
        run("ip", "addr", "add", f"198.18.{subnet}.1/24", "dev", host)
        run("ip", "-6", "addr", "add", f"fd00:{subnet}::1/64", "dev", host, "nodad")
        ns(namespace, "ip", "addr", "add", f"198.18.{subnet}.2/24", "dev", peer)
        ns(namespace, "ip", "-6", "addr", "add", f"fd00:{subnet}::2/64", "dev", peer, "nodad")
        ns(namespace, "ip", "route", "add", "default", "via", f"198.18.{subnet}.1")
        ns(namespace, "ip", "-6", "route", "add", "default", "via", f"fd00:{subnet}::1")
        # Give this routed/DNAT fixture explicit L2 neighbors. Some container
        # kernels fail neighbor discovery for the forwarded IPv6 test packet
        # before any policy exists; neighbor discovery is not under test here.
        host_mac = json.loads(run("ip", "-j", "link", "show", host))[0]["address"]
        peer_mac = json.loads(ns(namespace, "ip", "-j", "link", "show", peer))[0]["address"]
        run("ip", "-6", "neigh", "replace", f"fd00:{subnet}::2", "lladdr", peer_mac,
            "nud", "permanent", "dev", host)
        ns(namespace, "ip", "-6", "neigh", "replace", f"fd00:{subnet}::1", "lladdr", host_mac,
           "nud", "permanent", "dev", peer)
    assert run("sysctl", "-n", "net.ipv4.ip_forward") == "1"
    assert run("sysctl", "-n", "net.ipv6.conf.all.forwarding") == "1"
    for name, addresses in ((None, ("198.18.1.1", "fd00:1::1", "127.0.0.1")),
                            ("outside", ("198.18.1.2", "fd00:1::2")),
                            ("backend", ("198.18.2.2", "fd00:2::2"))):
        command = ["python3", __file__, "server", *addresses]
        if name:
            command = ["ip", "netns", "exec", name, *command]
        p = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
        assert p.stdout.readline().strip() == "READY"
        PROCESSES.append(p)
    time.sleep(.15)
    for family, backend in (("iptables", "198.18.2.2:8080"), ("ip6tables", "[fd00:2::2]:8080")):
        run(family, "-t", "mangle", "-N", "OTHER-COMPONENT")
        run(family, "-t", "mangle", "-A", "PREROUTING", "-j", "OTHER-COMPONENT")
        for port in (5555, 8443):
            run(family, "-t", "nat", "-A", "PREROUTING", "-i", "public0", "-p", "tcp",
                "--dport", str(port), "-j", "DNAT", "--to-destination", backend)
    sessions = [(persistent(a, p), p) for a in ("198.18.1.1", "fd00:1::1") for p in (22, 18081)]
    for address in ("198.18.1.1", "fd00:1::1"):
        check("outside", address, 5555)
    run("sh", script)
    run("sh", script)  # Reapply without duplicate hooks or lost component rules.
    for session, port in sessions:
        session.stdin.write("after\n")
        session.stdin.flush()
        assert session.stdout.readline().strip() == ("OPEN" if port == 22 else "BLOCKED")
        session.terminate()
    for address in ("198.18.1.1", "fd00:1::1"):
        check("outside", address, 22)
        check("outside", address, 18081, "BLOCKED")
        check("outside", address, 5555, "BLOCKED")
        check("outside", address, 8443)
        check("outside", address, 41641, proto="udp")
        check("outside", address, 9999, "BLOCKED", "udp")
        check("backend", address, 18081)  # Private/loopback service access remains available.
    for address in ("198.18.1.2", "fd00:1::2", "127.0.0.1"):
        result = run("python3", "-c", CLIENT, address, "18081", "tcp")
        assert result == "OPEN", ("outbound/loopback", address, result)
    for family in ("iptables", "ip6tables"):
        rules = run(family, "-t", "mangle", "-S")
        assert rules.count("-A PREROUTING -i public0 -j BEAM-INGRESS") == 1
        assert "-A PREROUTING -j OTHER-COMPONENT" in rules
        assert "--dport 5555 -j DNAT" in run(family, "-t", "nat", "-S")
    print("PASS: IPv4/IPv6 SSH sessions, encrypted UDP, outbound replies, private/loopback access,")
    print("unlisted TCP sessions and new connections, blocked Docker DNAT, explicit app port,")
    print("idempotent hooks and preserved third-party rules.")


main(sys.argv[1])
