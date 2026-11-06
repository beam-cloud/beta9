#!/bin/sh
# OpenClaw, a personal agent you message on WhatsApp, Telegram, Discord, Slack
# and more. It runs as the ubuntu user, who keeps its configuration and the
# gateway's systemd user service; `openclaw` in root's shell switches to them.
RECIPE=$(cd "$(dirname "$0")" && pwd)
. "$RECIPE/../common.sh"

# OpenClaw needs Node.js 24 or later, and the image has 22 from NodeSource.
log "Installing Node.js 24"
curl -fsSL https://deb.nodesource.com/setup_24.x | bash -
apt_install nodejs

log "Installing OpenClaw"
npm install --global --no-fund --no-audit --allow-scripts=openclaw openclaw@latest

prepare_app_user
install_user_command openclaw /usr/bin/openclaw

log "Checking OpenClaw"
node --version
openclaw --version
