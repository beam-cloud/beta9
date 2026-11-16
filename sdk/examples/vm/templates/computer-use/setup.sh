#!/bin/sh
# A desktop that coding agents operate: Claude Code and Codex with
# agent-desktop, which sees and drives desktop apps through accessibility, and
# agent-browser, which drives Chrome, plus their agent skills. Google Chrome
# comes with every Beam desktop image.
RECIPE=$(cd "$(dirname "$0")" && pwd)
. "$RECIPE/../common.sh"

update_coding_agents

log "Installing agent-desktop and agent-browser"
npm install --global --no-fund --no-audit agent-desktop@latest agent-browser@latest

add_agent_skills vercel-labs/agent-desktop --skill agent-desktop --skill agent-desktop-linux
add_agent_skills vercel-labs/agent-browser --skill agent-browser

# agent-desktop reaches apps over the desktop session's accessibility bus, which
# the web terminal and `beam vm exec` aren't part of.
cat >/usr/local/bin/on-desktop <<'SCRIPT'
#!/bin/sh
# Runs a command, or a shell, as the desktop user inside the desktop session,
# where agent-desktop can see and operate apps.
set -eu
user=$(cat /opt/beam-vm/desktop-user)
pid=$(pgrep -o -u "$user" -x gnome-shell) || { echo "on-desktop: the desktop isn't running" >&2; exit 1; }
session=$(tr '\0' '\n' <"/proc/$pid/environ" |
  grep -E '^(DISPLAY|DBUS_SESSION_BUS_ADDRESS|XAUTHORITY|XDG_RUNTIME_DIR|XDG_SESSION_TYPE|XDG_CURRENT_DESKTOP)=')
[ $# -gt 0 ] || set -- bash -l
[ "$(id -u)" -eq 0 ] || exec env $session "$@"
runuser -u "$user" -- test -x "$PWD" || cd "/home/$user"
exec runuser -u "$user" -- env -i HOME="/home/$user" USER="$user" LOGNAME="$user" SHELL=/bin/bash \
  PATH="/home/$user/.local/bin:/usr/local/bin:/usr/bin:/bin" LANG=C.UTF-8 TERM="${TERM:-xterm-256color}" \
  $session "$@"
SCRIPT
chmod 755 /usr/local/bin/on-desktop

DESKTOP_USER=$(cat /opt/beam-vm/desktop-user)
for home in /root "/home/$DESKTOP_USER"; do
  for file in "$home/.claude/CLAUDE.md" "$home/.codex/AGENTS.md"; do
    install -d "$(dirname "$file")"
    cat >>"$file" <<'GUIDE'
# This VM's desktop

This VM has a Linux desktop (GNOME on X11) that the user watches at
`beam vm desktop`. To see and operate its apps, run agent-desktop through
`on-desktop`, which works from any shell: `on-desktop agent-desktop snapshot`.
For websites, use agent-browser.
GUIDE
  done
  chown -R "$(stat -c %U "$home"):" "$home/.claude" "$home/.codex"
done

log "Checking the tools"
claude --version
codex --version
agent-desktop --version
agent-browser --version
