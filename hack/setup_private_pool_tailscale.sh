#!/usr/bin/env bash
set -euo pipefail

TAILNET="${TAILNET:--}"
CONFIG_PATH="${CONFIG_PATH:-config.yaml}"
KEY_EXPIRY_SECONDS="${KEY_EXPIRY_SECONDS:-7776000}"
STRICT_TAILSCALE_POLICY="${STRICT_TAILSCALE_POLICY:-0}"
TAILSCALE_API_BASE="${TAILSCALE_API_BASE:-https://api.tailscale.com}"

if [[ -z "${TS_API_KEY:-}" ]]; then
  echo "TS_API_KEY is required" >&2
  echo "Create a Tailscale API access token, then run:" >&2
  echo "  TS_API_KEY=tskey-api-... $0" >&2
  exit 2
fi

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required" >&2
    exit 1
  fi
}

require curl
require jq

curl_auth=()
if [[ "$TS_API_KEY" == tskey-api-* ]]; then
  curl_auth=(-u "${TS_API_KEY}:")
else
  curl_auth=(-H "Authorization: Bearer ${TS_API_KEY}")
fi

api_url() {
  printf "%s/api/v2/tailnet/%s/%s" "${TAILSCALE_API_BASE%/}" "$TAILNET" "$1"
}

curl_status() {
  local body="$1"
  local headers="$2"
  shift 2

  curl -sS \
    -o "$body" \
    -D "$headers" \
    -w "%{http_code}" \
    "${curl_auth[@]}" \
    "$@"
}

require_success() {
  local status="$1"
  local body="$2"
  local message="$3"

  if [[ "$status" =~ ^2 ]]; then
    return 0
  fi

  echo "${message} failed with HTTP ${status}" >&2
  if [[ -s "$body" ]]; then
    jq -r '.message // .error // .detail // .' "$body" 2>/dev/null >&2 || cat "$body" >&2
  fi
  exit 1
}

merge_tailnet_policy() {
  local current="$1"
  local merged="$2"

  jq --argjson strict "$STRICT_TAILSCALE_POLICY" '
    def uniq_strings: map(select(type == "string")) | unique;
    def has_beam_grant:
      any(.grants[]?;
        ((.src // []) | index("tag:beam-gateway")) and
        ((.dst // []) | index("tag:beam-agent")) and
        ((.ip // []) | index("tcp:29443"))
      );
    def has_test_src($src):
      any(.tests[]?; .src == $src);
    def default_allow_all_acl:
      (.action // "") == "accept" and
      ((.src // []) == ["*"]) and
      ((.dst // []) == ["*:*"]);

    .tagOwners = (.tagOwners // {}) |
    .tagOwners["tag:beam-gateway"] = (((.tagOwners["tag:beam-gateway"] // []) + ["autogroup:admin"]) | uniq_strings) |
    .tagOwners["tag:beam-agent"] = (((.tagOwners["tag:beam-agent"] // []) + ["autogroup:admin"]) | uniq_strings) |

    .grants = (.grants // []) |
    if has_beam_grant then . else
      .grants += [{
        src: ["tag:beam-gateway"],
        dst: ["tag:beam-agent"],
        ip: ["tcp:29443"]
      }]
    end |

    if $strict == 1 then
      .acls = ((.acls // []) | map(select(default_allow_all_acl | not))) |
      .tests = (.tests // []) |
      if has_test_src("tag:beam-gateway") then . else
        .tests += [{
          src: "tag:beam-gateway",
          accept: ["tag:beam-agent:29443"],
          deny: ["tag:beam-agent:22"]
        }]
      end |
      if has_test_src("tag:beam-agent") then . else
        .tests += [{
          src: "tag:beam-agent",
          deny: ["tag:beam-gateway:29443", "tag:beam-agent:29443"]
        }]
      end
    else
      .
    end
  ' "$current" > "$merged"
}

ensure_tailnet_policy() {
  local tmpdir current headers merged status etag
  tmpdir="$(mktemp -d)"
  current="${tmpdir}/policy.json"
  headers="${tmpdir}/headers"
  merged="${tmpdir}/policy.merged.json"

  status="$(curl_status "$current" "$headers" -H "Accept: application/json" "$(api_url acl)")"
  require_success "$status" "$current" "fetching Tailscale policy"

  merge_tailnet_policy "$current" "$merged"
  if cmp -s "$current" "$merged"; then
    echo "tailscale policy already contains Beam private tags/grant"
    rm -rf "$tmpdir"
    return
  fi

  etag="$(awk 'tolower($1) == "etag:" {print $2}' "$headers" | tr -d '\r' | tail -1)"
  if [[ -n "$etag" ]]; then
    status="$(curl_status "${tmpdir}/update.json" "${tmpdir}/update.headers" \
      -H "Accept: application/json" \
      -H "Content-Type: application/hujson" \
      -H "If-Match: ${etag}" \
      -X POST \
      --data-binary "@${merged}" \
      "$(api_url acl)")"
  else
    status="$(curl_status "${tmpdir}/update.json" "${tmpdir}/update.headers" \
      -H "Accept: application/json" \
      -H "Content-Type: application/hujson" \
      -X POST \
      --data-binary "@${merged}" \
      "$(api_url acl)")"
  fi
  require_success "$status" "${tmpdir}/update.json" "updating Tailscale policy"

  echo "updated tailscale policy with Beam private tags/grant"
  rm -rf "$tmpdir"
}

create_key() {
  local tag="$1"
  local description="$2"
  local tmpdir body headers status
  tmpdir="$(mktemp -d)"
  body="${tmpdir}/body.json"
  headers="${tmpdir}/headers"

  jq -n \
    --arg tag "$tag" \
    --argjson expirySeconds "$KEY_EXPIRY_SECONDS" \
    '{
      capabilities: {
        devices: {
          create: {
            reusable: true,
            ephemeral: true,
            preauthorized: true,
            tags: [$tag]
          }
        }
      },
      expirySeconds: $expirySeconds
    }' > "${tmpdir}/request.json"

  status="$(curl_status "$body" "$headers" \
      -H "Accept: application/json" \
      -H "Content-Type: application/json" \
      -X POST \
      --data-binary "@${tmpdir}/request.json" \
      "$(api_url keys)")"
  require_success "$status" "$body" "creating ${description} auth key"
  jq -r '.key' "$body"
  rm -rf "$tmpdir"
}

ensure_tailnet_policy

gateway_key="$(create_key "tag:beam-gateway" "Beam local gateway")"
agent_key="$(create_key "tag:beam-agent" "Beam local agent")"

cat > "$CONFIG_PATH" <<YAML
tailscale:
  enabled: true
  controlUrl: ""
  authKey: "${gateway_key}"
  agentAuthKey: "${agent_key}"
YAML

echo "wrote ${CONFIG_PATH}"
echo "gateway auth key: ${gateway_key:0:18}..."
echo "agent auth key:  ${agent_key:0:18}..."
echo
echo "Next:"
echo "  make worker"
echo "  make start"
echo "  uv run --project ./sdk beam pool join private-dev"
