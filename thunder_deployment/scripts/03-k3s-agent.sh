#!/usr/bin/env bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"
parse_common_args "$@"

if is_master; then
  log_skip "k3s agent checks are skipped on master nodes"
  exit 0
fi

build_agent_args() {
  gpu_type="${GPU_TYPE:-A6000}"
  gpu_count="${GPU_COUNT:-8}"
  install_args="agent --node-label beta9.node-role=cpu"
  if is_gpu; then
    install_args="agent --node-label beta9.node-role=gpu --node-label beta9.gpu=true --node-label beta9.gpu-type=$gpu_type --node-label beta9.gpu-count=$gpu_count"
  fi
  if [ -n "${K3S_NODE_NAME:-}" ]; then
    install_args="$install_args --node-name $K3S_NODE_NAME"
  fi
  if [ -n "${K3S_NODE_IP:-}" ]; then
    install_args="$install_args --node-ip $K3S_NODE_IP"
  fi
  if [ -n "${K3S_FLANNEL_IFACE:-}" ]; then
    install_args="$install_args --flannel-iface $K3S_FLANNEL_IFACE"
  fi
  AGENT_ARGS="$install_args"
}

reconcile_agent_dropin() {
  build_agent_args
  if [ -z "${K3S_NODE_IP:-}${K3S_FLANNEL_IFACE:-}${K3S_NODE_NAME:-}${GPU_TYPE:-}${GPU_COUNT:-}" ]; then
    return 0
  fi

  tmp="$(mktemp)"
  {
    printf '%s\n' '[Service]'
    printf '%s\n' 'ExecStart='
    printf 'ExecStart=/usr/local/bin/k3s %s\n' "$AGENT_ARGS"
  } > "$tmp"

  dropin_dir=/etc/systemd/system/k3s-agent.service.d
  dropin="$dropin_dir/10-beam-network.conf"
  if ! run_sudo test -f "$dropin" || ! run_sudo cmp -s "$tmp" "$dropin"; then
    run_sudo mkdir -p "$dropin_dir"
    run_sudo cp "$tmp" "$dropin"
    run_sudo systemctl daemon-reload
    if systemctl is-active --quiet k3s-agent 2>/dev/null; then
      run_sudo systemctl restart k3s-agent
    fi
    log_pass "k3s agent systemd drop-in reconciled"
  else
    log_pass "k3s agent systemd drop-in already matches"
  fi
  rm -f "$tmp"
}

cleanup_stale_cni_state() {
  [ "$MODE" = "install" ] || return 0

  if run_sudo test -d /etc/cni/net.d; then
    stale="$(run_sudo find /etc/cni/net.d -maxdepth 1 -type f \( -name '*cilium*' -o -name '05-cilium.conflist' \) -print 2>/dev/null || true)"
    if [ -n "$stale" ]; then
      backup_dir="/etc/cni/net.d/beam-backup-$(date -u +%Y%m%d%H%M%S)"
      run_sudo mkdir -p "$backup_dir"
      printf '%s\n' "$stale" | while IFS= read -r file; do
        [ -n "$file" ] && run_sudo mv "$file" "$backup_dir/"
      done
      log_pass "stale Cilium CNI configs moved to $backup_dir"
    fi
  fi

  flannel_conf=/var/lib/rancher/k3s/agent/etc/cni/net.d/10-flannel.conflist
  if run_sudo test -f "$flannel_conf"; then
    run_sudo mkdir -p /etc/cni/net.d
    run_sudo cp "$flannel_conf" /etc/cni/net.d/10-flannel.conflist
    log_pass "k3s flannel CNI config installed in /etc/cni/net.d"
  fi

  cni_dir=/var/lib/rancher/k3s/data/cni
  if run_sudo test -d "$cni_dir"; then
    run_sudo mkdir -p /opt/cni/bin
    for plugin in "$cni_dir"/*; do
      [ -e "$plugin" ] || continue
      name="$(basename "$plugin")"
      if ! run_sudo test -e "/opt/cni/bin/$name"; then
        run_sudo ln -s "$plugin" "/opt/cni/bin/$name"
      fi
    done
    log_pass "k3s CNI plugin binaries are available under /opt/cni/bin"
  fi

  if [ "${K3S_FLANNEL_BACKEND:-}" = "wireguard-native" ] || [ "${K3S_FLANNEL_BACKEND:-}" = "wireguard" ]; then
    run_sudo ip link delete flannel.1 2>/dev/null || true
    iface="${K3S_FLANNEL_IFACE:-tailscale0}"
    ip route show | awk -v iface="$iface" '$1 ~ /^10[.]42[.]/ && $0 ~ ("dev " iface) {print}' | while IFS= read -r route; do
      [ -n "$route" ] && run_sudo ip route del $route 2>/dev/null || true
    done
    log_pass "stale flannel vxlan/host-gw routes cleaned for wireguard backend"
  fi
}

if ! have_cmd k3s; then
  if is_install; then
    if [ -z "${K3S_URL:-}" ] || [ -z "${K3S_TOKEN:-}" ]; then
      log_fail "K3S_URL and K3S_TOKEN are required to install/join k3s agent"
      exit 1
    fi

    build_agent_args
    curl -sfL https://get.k3s.io | K3S_URL="$K3S_URL" K3S_TOKEN="$K3S_TOKEN" sh -s - $AGENT_ARGS
  else
    log_fail "k3s agent is not installed"
    exit 1
  fi
fi

if is_install; then
  cleanup_stale_cni_state
  reconcile_agent_dropin
fi

log_pass "k3s command available"

if systemctl is-active --quiet k3s-agent 2>/dev/null; then
  log_pass "k3s-agent service is active"
else
  log_fail "k3s-agent service is not active"
fi

if [ -f /etc/systemd/system/k3s-agent.service.env ]; then
  log_pass "k3s agent environment file exists"
else
  log_warn "k3s agent environment file was not found"
fi
