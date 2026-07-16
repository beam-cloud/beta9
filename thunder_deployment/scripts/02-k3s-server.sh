#!/usr/bin/env bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"
parse_common_args "$@"
use_k3s_kubeconfig

if ! is_master; then
  log_skip "k3s server checks apply only to master nodes"
  exit 0
fi

build_server_args() {
  tls_san_args=""
  if [ -n "${K3S_TLS_SAN:-}" ]; then
    tls_san_args="--tls-san $K3S_TLS_SAN"
  fi

  node_name_args=""
  if [ -n "${K3S_NODE_NAME:-}" ]; then
    node_name_args="--node-name $K3S_NODE_NAME"
  fi

  network_args=""
  if [ -n "${K3S_NODE_IP:-}" ]; then
    network_args="$network_args --node-ip $K3S_NODE_IP"
  fi
  if [ -n "${K3S_FLANNEL_IFACE:-}" ]; then
    network_args="$network_args --flannel-iface $K3S_FLANNEL_IFACE"
  fi
  if [ -n "${K3S_FLANNEL_BACKEND:-}" ]; then
    network_args="$network_args --flannel-backend $K3S_FLANNEL_BACKEND"
  fi

  SERVER_ARGS="server --write-kubeconfig-mode 0644 --node-label beta9.node-role=master $tls_san_args $node_name_args $network_args"
}

install_or_reconcile_dropin() {
  build_server_args
  if [ -z "${K3S_NODE_IP:-}${K3S_FLANNEL_IFACE:-}${K3S_FLANNEL_BACKEND:-}${K3S_TLS_SAN:-}${K3S_NODE_NAME:-}" ]; then
    return 0
  fi

  tmp="$(mktemp)"
  {
    printf '%s\n' '[Service]'
    printf '%s\n' 'ExecStart='
    printf 'ExecStart=/usr/local/bin/k3s %s\n' "$SERVER_ARGS"
  } > "$tmp"

  dropin_dir=/etc/systemd/system/k3s.service.d
  dropin="$dropin_dir/10-beam-network.conf"
  if ! run_sudo test -f "$dropin" || ! run_sudo cmp -s "$tmp" "$dropin"; then
    run_sudo mkdir -p "$dropin_dir"
    run_sudo cp "$tmp" "$dropin"
    run_sudo systemctl daemon-reload
    if systemctl is-active --quiet k3s 2>/dev/null; then
      if [ "${K3S_FLANNEL_BACKEND:-}" = "wireguard-native" ] || [ "${K3S_FLANNEL_BACKEND:-}" = "wireguard" ]; then
        run_sudo ip link delete flannel.1 2>/dev/null || true
        iface="${K3S_FLANNEL_IFACE:-tailscale0}"
        ip route show | awk -v iface="$iface" '$1 ~ /^10[.]42[.]/ && $0 ~ ("dev " iface) {print}' | while IFS= read -r route; do
          [ -n "$route" ] && run_sudo ip route del $route 2>/dev/null || true
        done
      fi
      run_sudo systemctl restart k3s
    fi
    log_pass "k3s server systemd drop-in reconciled"
  else
    log_pass "k3s server systemd drop-in already matches"
  fi
  rm -f "$tmp"
}

if ! have_cmd k3s; then
  if require_install_mode "k3s server"; then
    build_server_args
    curl -sfL https://get.k3s.io | sh -s - $SERVER_ARGS
  fi
fi

if is_install; then
  install_or_reconcile_dropin
fi

have_cmd k3s || { log_fail "k3s is not installed"; exit 1; }
log_pass "k3s command available"

if systemctl is-active --quiet k3s 2>/dev/null; then
  log_pass "k3s server service is active"
else
  log_fail "k3s server service is not active"
fi

if [ -r /etc/rancher/k3s/k3s.yaml ]; then
  record_fact kubeconfig /etc/rancher/k3s/k3s.yaml
  log_pass "k3s kubeconfig exists"
else
  log_fail "missing /etc/rancher/k3s/k3s.yaml"
fi

if run_sudo test -r /var/lib/rancher/k3s/server/node-token; then
  record_fact k3s_node_token_path /var/lib/rancher/k3s/server/node-token
  log_pass "k3s node token exists"
else
  log_fail "missing k3s node token"
fi

if have_cmd kubectl; then
  if kubectl get nodes >/dev/null 2>&1; then
    log_pass "kubectl can reach the cluster"
  else
    log_fail "kubectl cannot reach the cluster"
  fi

  node="${K3S_NODE_NAME:-$(hostname -s)}"
  if [ -n "${K3S_NODE_IP:-}" ]; then
    internal_ip="$(kubectl get node "$node" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true)"
    [ "$internal_ip" = "$K3S_NODE_IP" ] && log_pass "node advertises requested InternalIP: $K3S_NODE_IP" || log_warn "node InternalIP is ${internal_ip:-unknown}, expected $K3S_NODE_IP"
  fi
  if [ -n "${K3S_FLANNEL_BACKEND:-}" ]; then
    backend="$(kubectl get node "$node" -o jsonpath='{.metadata.annotations.flannel\.alpha\.coreos\.com/backend-type}' 2>/dev/null || true)"
    case "$K3S_FLANNEL_BACKEND:$backend" in
      wireguard-native:wireguard|wireguard:wireguard|host-gw:host-gw|vxlan:vxlan) log_pass "flannel backend is $backend" ;;
      *) log_warn "flannel backend is ${backend:-unknown}, expected $K3S_FLANNEL_BACKEND" ;;
    esac
  fi
else
  log_warn "kubectl is not installed as a standalone command; k3s kubectl may still work"
fi
