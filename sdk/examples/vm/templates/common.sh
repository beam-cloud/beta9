# Shared steps for the template recipes. Each <slug>/setup.sh sets RECIPE to its
# own directory and sources this file. Recipes run as root on a fresh VM.
set -eu
export DEBIAN_FRONTEND=noninteractive

log() { printf '\n==> %s\n' "$*"; }

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

# Images without a desktop ship without apt's package lists.
apt_install() {
  apt-get update -q
  apt-get install -yq --no-install-recommends "$@"
}

install_uv() {
  command -v uv >/dev/null 2>&1 && return 0
  log "Installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh
}

# The VM starts dockerd itself, a few seconds after boot, when Docker is enabled.
wait_docker() {
  end=$(($(date +%s) + 120))
  until docker info >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$end" ] || fail "Docker isn't running; create the VM with Docker enabled"
    sleep 1
  done
}

# Claude Code and Codex come with every Beam desktop image; templates ship the
# latest of both.
update_coding_agents() {
  log "Updating Claude Code and Codex"
  npm install --global --no-fund --no-audit @anthropic-ai/claude-code@latest @openai/codex@latest
}

# Installs agent skills from a skills.sh source, with any --skill NAME
# arguments, for Claude Code and Codex: for root, who runs the web terminal and
# `beam vm exec`, and for the desktop user.
add_agent_skills() {
  log "Installing agent skills from $1"
  command="cd /tmp && npx --yes skills add $* -g -a claude-code -a codex -y"
  sh -c "$command"
  su - "$(cat /opt/beam-vm/desktop-user)" -c "$command"
}

# The account that OpenClaw and Hermes run as: both refuse root, and images
# without a desktop come with this user.
APP_USER=ubuntu

# Keeps APP_USER's systemd user services, where both agents install their
# gateways, running from boot without anyone signed in.
prepare_app_user() {
  id "$APP_USER" >/dev/null 2>&1 || fail "the image has no $APP_USER user"
  apt_install dbus-user-session
  loginctl enable-linger "$APP_USER"
  # systemctl --user needs it, and neither `su` nor `runuser` sets it.
  line='export XDG_RUNTIME_DIR=/run/user/$(id -u)'
  grep -qsF "$line" "/home/$APP_USER/.bashrc" || echo "$line" >>"/home/$APP_USER/.bashrc"
}

# Installs /usr/local/sbin/NAME, which root's PATH finds first: it runs
# COMMAND, an absolute path, as APP_USER with their user session, so the web
# terminal and `beam vm exec` can use NAME directly.
install_user_command() {
  name=$1
  command=$2
  uid=$(id -u "$APP_USER")
  home=/home/$APP_USER
  cat >"/usr/local/sbin/$name" <<SCRIPT
#!/bin/sh
# Runs $name as $APP_USER, who owns its configuration and services.
case \$(id -u) in
  $uid) exec $command "\$@" ;;
  0) ;;
  *) echo "$name: run it as root or $APP_USER" >&2; exit 1 ;;
esac
cd $home
exec runuser -u $APP_USER -- env -i HOME=$home USER=$APP_USER LOGNAME=$APP_USER SHELL=/bin/bash \\
  PATH=$home/.local/bin:/usr/local/bin:/usr/bin:/bin LANG=C.UTF-8 TERM="\${TERM:-xterm-256color}" \\
  XDG_RUNTIME_DIR=/run/user/$uid DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$uid/bus \\
  $command "\$@"
SCRIPT
  chmod 755 "/usr/local/sbin/$name"
}
