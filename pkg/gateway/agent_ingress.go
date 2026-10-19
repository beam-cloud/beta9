package gateway

// Only provider-reserved hosts opt in. Keep manually joined/BYOC machines'
// network policy under their operator's control.
const managedIngressInstallScript = `
ensure_linux_managed_ingress() {
  [ "$MANAGED_INGRESS" = "1" ] || return 0
  [ "$OS" = "linux" ] && [ "$(id -u)" = "0" ] || fail "Managed ingress requires a native Linux root install" 1
  if ! command -v ip >/dev/null 2>&1 || ! command -v iptables >/dev/null 2>&1 || ! command -v ip6tables >/dev/null 2>&1; then
    # Install tools only: firewall persistence packages may replace other
    # components' policies. Our own systemd unit persists this chain.
    if command -v apt-get >/dev/null 2>&1; then
      apt-get update
      DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends iproute2 iptables
    elif command -v dnf >/dev/null 2>&1; then
      dnf install -y iproute iptables
    elif command -v yum >/dev/null 2>&1; then
      yum install -y iproute iptables
    fi
  fi
  for tool in ip iptables ip6tables iptables-restore ip6tables-restore systemctl; do
    command -v "$tool" >/dev/null 2>&1 || fail "Managed ingress requires $tool; agent startup requires ingress setup" 1
  done

  INGRESS_INTERFACES="${BEAM_AGENT_INGRESS_INTERFACES:-$( { ip -o -4 route show default; ip -o -6 route show default; } | awk '{for(i=1;i<NF;i++) if($i=="dev") print $(i+1)}' | sort -u | tr '\n' ' ')}"
  [ -n "$(printf '%s' "$INGRESS_INTERFACES" | tr -d ' ')" ] || fail "Managed ingress could not determine the public interfaces" 1
  for iface in $INGRESS_INTERFACES; do
    case "$iface" in
      lo|*[!a-zA-Z0-9_.:-]*) fail "Invalid managed ingress interface: $iface" 1 ;;
    esac
    ip link show dev "$iface" >/dev/null 2>&1 || fail "Managed ingress interface does not exist: $iface" 1
  done
  INGRESS_TCP_PORTS="22 ${BEAM_AGENT_PUBLIC_TCP_PORTS:-}"
  for port in $INGRESS_TCP_PORTS; do
    case "$port" in ''|*[!0-9]*) fail "Invalid public TCP port: $port" 1 ;; esac
    [ "${#port}" -le 5 ] && [ "$port" -ge 1 ] && [ "$port" -le 65535 ] || fail "Invalid public TCP port: $port" 1
  done

  install -d -m 0755 /usr/local/sbin /etc/systemd/system
  INGRESS_TMP="$(mktemp)"
  {
    printf '#!/bin/sh\nset -eu\nINTERFACES="%s"\nTCP_PORTS="%s"\nUDP_PORTS="41641"\n' "$INGRESS_INTERFACES" "$INGRESS_TCP_PORTS"
    cat <<'BEAM_INGRESS_SCRIPT'
` + managedIngressApplyScript + `
BEAM_INGRESS_SCRIPT
  } > "$INGRESS_TMP"
  install -m 0755 "$INGRESS_TMP" /usr/local/sbin/beam-managed-ingress
  rm -f "$INGRESS_TMP"
  cat > /etc/systemd/system/beam-managed-ingress.service <<'BEAM_INGRESS_UNIT'
[Unit]
Description=Beam managed node public ingress
DefaultDependencies=no
After=systemd-modules-load.service
Before=network-pre.target
Wants=network-pre.target

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/beam-managed-ingress
ExecReload=/usr/local/sbin/beam-managed-ingress
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
BEAM_INGRESS_UNIT
  systemctl daemon-reload
  # Reapply on installer retries even when the oneshot is already active.
  systemctl enable beam-managed-ingress.service
  systemctl restart beam-managed-ingress.service
  # A fixed encrypted transport port avoids allowing arbitrary inbound UDP.
  export BEAM_AGENT_MANAGED_INGRESS=true
}
`

const managedIngressApplyScript = `# Mangle PREROUTING runs after conntrack and before Docker's DNAT/filter
# processing, applying the policy to host listeners and Docker-published ports.
# Never flush the ruleset or change another component's chains/policies.
for family in iptables ip6tables; do
  {
    printf '*mangle\n:BEAM-INGRESS - [0:0]\n-F BEAM-INGRESS\n'
    printf '%s\n' '-A BEAM-INGRESS -m conntrack --ctstate ESTABLISHED,RELATED --ctdir REPLY -j RETURN'
    if [ "$family" = iptables ]; then
      printf '%s\n' '-A BEAM-INGRESS -p icmp -j RETURN'
      printf '%s\n' '-A BEAM-INGRESS -p udp --sport 67 --dport 68 -j RETURN'
    else
      printf '%s\n' '-A BEAM-INGRESS -p ipv6-icmp -j RETURN'
      printf '%s\n' '-A BEAM-INGRESS -p udp --sport 547 --dport 546 -j RETURN'
    fi
    for port in $TCP_PORTS; do
      printf '%s\n' "-A BEAM-INGRESS -p tcp --dport $port -j RETURN"
    done
    for port in $UDP_PORTS; do
      printf '%s\n' "-A BEAM-INGRESS -p udp --dport $port -j RETURN"
    done
    printf '%s\n' '-A BEAM-INGRESS -j DROP' 'COMMIT'
  } | "$family-restore" --wait 5 --noflush
  for iface in $INTERFACES; do
    "$family" --wait 5 -t mangle -C PREROUTING -i "$iface" -j BEAM-INGRESS 2>/dev/null ||
      "$family" --wait 5 -t mangle -I PREROUTING 1 -i "$iface" -j BEAM-INGRESS
  done
done
`
