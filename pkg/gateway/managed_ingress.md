# Managed node ingress

Provider reservation bootstrap commands opt into `--managed-ingress`. Manual
joins, BYOC machines, and local development keep their existing network policy.
Releasing this change affects newly provisioned reservations; it does not modify
existing machines automatically. Release the gateway and its bundled agent
binaries together.

The installer creates `beam-managed-ingress.service` and an owned `BEAM-INGRESS`
chain in IPv4 and IPv6 mangle PREROUTING. The chain runs before Docker DNAT and
filter rules, covering host-network listeners and Docker-published ports. It
does not flush other chains or change default policies. Failed setup prevents
agent startup; the managed agent also requires the ingress service at boot.

On the configured interfaces, the default policy permits TCP 22, encrypted Beam
transport on UDP 41641, ICMP/ICMPv6, DHCP replies, and conntrack replies to outbound
connections. Other inbound traffic is dropped, including existing connections
to unlisted ports. Loopback and traffic arriving on other interfaces are
unaffected. The managed agent prefers UDP 41641 for tsnet; outbound connectivity
and relay fallback remain available.

By default, the configured interfaces are those carrying IPv4 or IPv6 default
routes. Operators must include any additional public interfaces in the
space-separated `BEAM_AGENT_INGRESS_INTERFACES` override. Additional intentional
public application ports can be supplied in `BEAM_AGENT_PUBLIC_TCP_PORTS` before
installation. Both settings are validated and stored in the root-owned helper
`/usr/local/sbin/beam-managed-ingress`.

The policy runs inside the guest and is managed by root. Provider-level network
configuration remains independent.

For an existing node, first inventory public interfaces, required ports,
existing SSH sessions, and the current agent's UDP listeners. Older agents use
dynamic UDP ports; preserve those ports without restarting active workloads.
Schedule a rollback that removes only this policy's interface jumps, owned
chain, helper, and service. Verify fresh SSH access, public blocking, private
workload access, unchanged workload start times, and fresh control-plane
heartbeats before cancelling rollback. Never restore a whole saved firewall
ruleset over concurrent Docker or worker changes.

The optional `TestManagedIngressLinuxNetwork` regression runs only when
`BETA9_TEST_MANAGED_INGRESS_NETWORK=1` in an isolated Linux container. It requires
`--network none`, NET_ADMIN, SYS_ADMIN, IPv4 forwarding, and IPv6 all/default
forwarding. It checks existing SSH and unlisted-port sessions, host listeners,
Docker DNAT, encrypted UDP, outbound replies, private/loopback access, and
preservation of other components' rules in both address families.
