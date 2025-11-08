# Network Security Improvements for Untrusted Tenants

This document describes the security improvements implemented in `network.go` to protect against untrusted tenant workloads.

## Overview

The Beta9 worker now implements a comprehensive security model for container networking that assumes untrusted tenants. These improvements prevent lateral movement, infrastructure access, and various attack vectors while maintaining clean, modular code.

## Security Improvements Implemented

### 1. Strict Container Bridge Firewall (B9-FORWARD Chain)

**Purpose**: Enforce strong tenant isolation and prevent unauthorized access.

**Implementation**: A dedicated iptables chain (`B9-FORWARD`) that all bridge traffic must traverse.

**Key Rules**:
- ✅ **Stateful firewall**: Allow established/related connections
- ✅ **Container-to-container isolation**: Drop all traffic between containers on the same bridge
- ✅ **Anti-spoofing**: Drop packets from bridge with source IP outside the container subnet
- ✅ **Metadata service blocking**: Block access to AWS/GCP/Azure metadata (169.254.169.254)
- ✅ **Private network blocking**: Block access to RFC1918 ranges (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
- ✅ **Localhost blocking**: Prevent access to 127.0.0.0/8
- ✅ **Link-local blocking**: Block 169.254.0.0/16 range
- ✅ **Default egress only**: Only allow outbound traffic via the default network interface
- ✅ **Default deny**: Drop all other traffic

**Code Location**: `setupSecurityChain()` method in `network.go`

### 2. Bridge Netfilter Configuration

**Purpose**: Ensure bridge traffic goes through iptables rules (critical for security).

**Implementation**: Enable kernel sysctls for bridge netfilter:
- `net.bridge.bridge-nf-call-iptables=1`
- `net.bridge.bridge-nf-call-ip6tables=1`

**Behavior**: Gracefully handles missing br_netfilter module by attempting to load it, with fallback to logging warnings.

**Code Location**: `enableBridgeNetfilter()` method in `network.go`

### 3. Tightened ExposePort Security

**Purpose**: Prevent tenants from pivoting through exposed ports to access other containers.

**Before**: Generic FORWARD rule allowed any traffic to exposed container ports.

**After**: ExposePort now:
- Routes through the B9-FORWARD chain
- Only accepts traffic from the node interface (not from other containers)
- Specifies both input and output interfaces
- Uses connection tracking (NEW,ESTABLISHED states)

**Code Location**: Updated `ExposePort()` method in `network.go`

### 4. IPv6 Support

All security rules are implemented for both IPv4 and IPv6, including:
- IPv6-specific private ranges (fc00::/7 ULA)
- IPv6 link-local blocking (fe80::/10)
- Graceful fallback when IPv6 is not available

## Testing

Comprehensive tests have been added to `network_test.go`:

1. **TestEnableBridgeNetfilter**: Verifies bridge netfilter setup handles errors gracefully
2. **TestSecurityChainConstants**: Validates security constants are properly defined
3. **TestSecurityRulesCoverage**: Documents all critical IP ranges that must be blocked
4. **TestContainerIsolationRules**: Verifies container-to-container isolation logic
5. **TestAntiSpoofingLogic**: Tests IP spoofing prevention rules
6. **TestExposePortSecurity**: Validates ExposePort security design
7. **TestPrivateNetworkBlocking**: Ensures private ranges are properly identified
8. **TestMetadataServiceBlocking**: Validates metadata service is blocked

All tests pass successfully.

## Security Properties

With these improvements, the network layer now provides:

### ✅ Strong Tenant Isolation
- Containers cannot communicate with each other on the bridge
- Each container is isolated in its own network sandbox

### ✅ Infrastructure Protection  
- Containers cannot access:
  - Cloud metadata services
  - Node management interfaces
  - Internal cluster services
  - Private infrastructure ranges

### ✅ Anti-Spoofing
- Containers cannot forge source IP addresses
- Only legitimate subnet addresses are accepted from the bridge

### ✅ Defense in Depth
- Even if gVisor is bypassed, network rules provide protection
- Stateful firewall with connection tracking
- Default-deny policy

### ✅ Clean Egress
- Containers can still access the internet via NAT
- Legitimate outbound connections work normally

## Design Principles

1. **Modular**: Security functions are separate, well-documented methods
2. **Idempotent**: Rules can be safely applied multiple times
3. **Fail-safe**: Missing modules or permissions log warnings but don't crash
4. **Testable**: Logic is tested independently of iptables
5. **Maintainable**: Clear comments explain the "why" behind each rule

## Migration Path

The changes are backward compatible:
- Existing containers continue to work
- Security is enabled automatically on bridge setup
- No configuration changes required

## Performance Impact

Minimal:
- Rules are evaluated once per connection (stateful firewall)
- Established connections bypass security checks
- No significant CPU or latency overhead

## Future Enhancements

Potential improvements for even stronger security:

1. **Per-container veth rules**: Pin specific (MAC, IP) pairs per veth
2. **ebtables integration**: Additional L2 filtering
3. **Rate limiting**: Protect against DoS from containers
4. **Traffic logging**: Audit blocked connection attempts
5. **Dynamic rule updates**: Add/remove rules without bridge restart

## References

- iptables man page: `man iptables`
- Netfilter documentation: https://netfilter.org/
- Container networking: https://docs.docker.com/network/
- RFC 1918 (Private Address Space): https://tools.ietf.org/html/rfc1918
