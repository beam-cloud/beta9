#!/bin/sh
# A desktop VM for coding agents: Claude Code, Codex and the Cursor CLI, plus
# uv, the GitHub CLI and tmux, and claude-remote to drive Claude Code from a
# phone. Node.js 22, Python 3.12, Docker and Chrome come with every Beam
# desktop image.
RECIPE=$(cd "$(dirname "$0")" && pwd)
. "$RECIPE/../common.sh"

DESKTOP_USER=$(cat /opt/beam-vm/desktop-user)

install_uv

log "Installing the GitHub CLI and tmux"
install -d -m 755 /etc/apt/keyrings
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
  -o /etc/apt/keyrings/githubcli-archive-keyring.gpg
chmod 644 /etc/apt/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
  >/etc/apt/sources.list.d/github-cli.list
apt_install gh tmux

update_coding_agents

# Remote Control serves a session to claude.ai and the Claude app for as long
# as its process runs, and the web terminal ends its processes when the tab
# closes; tmux keeps it running.
cat >/usr/local/bin/claude-remote <<'SCRIPT'
#!/bin/sh
# Runs Claude Code Remote Control in /workspace, in the tmux session
# claude-remote, or reattaches to it. Ctrl-b d leaves it running. When it
# exits, for example signed out, the session stays open to show why.
exec tmux new-session -A -s claude-remote -c /workspace sh -c '
  claude remote-control "$@"
  printf "\nclaude remote-control exited with status %s. Press Enter to close.\n" "$?"
  read -r _' claude-remote "$@"
SCRIPT
chmod 755 /usr/local/bin/claude-remote

# The Cursor CLI installs per user: for root, who runs the web terminal and
# `beam vm exec`, and for the desktop user.
log "Installing the Cursor CLI"
curl -fsS https://cursor.com/install | bash
ln -sf /root/.local/bin/cursor-agent /usr/local/bin/cursor-agent
su - "$DESKTOP_USER" -c 'curl -fsS https://cursor.com/install | bash'
for home in /root "/home/$DESKTOP_USER"; do
  grep -qs 'HOME/.local/bin' "$home/.bashrc" || echo 'export PATH="$HOME/.local/bin:$PATH"' >>"$home/.bashrc"
done

log "Checking the tools"
claude --version
codex --version
cursor-agent --version
su - "$DESKTOP_USER" -c '$HOME/.local/bin/cursor-agent --version'
gh --version
tmux -V
uv --version
python3 --version
node --version
wait_docker
docker run --rm hello-world >/dev/null
docker image rm hello-world >/dev/null
