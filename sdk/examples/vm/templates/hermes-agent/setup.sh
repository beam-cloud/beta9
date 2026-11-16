#!/bin/sh
# Hermes Agent from Nous Research: a personal agent with a messaging gateway for
# Telegram, Discord, Slack, WhatsApp and more. It runs commands for you, so it
# runs as the ubuntu user, under ~/.hermes; `hermes` in root's shell switches to
# them.
RECIPE=$(cd "$(dirname "$0")" && pwd)
. "$RECIPE/../common.sh"

apt_install git libatomic1
prepare_app_user

# The installer brings its own Python, Node.js, ripgrep, FFmpeg and browser. The
# VM has no display for its computer-use driver.
log "Installing Hermes Agent"
runuser -u "$APP_USER" -- env -i HOME="/home/$APP_USER" USER="$APP_USER" PATH=/usr/local/bin:/usr/bin:/bin \
  sh -c 'cd && curl -fsSL https://hermes-agent.nousresearch.com/install.sh | bash -s -- --non-interactive --skip-computer-use'
install_user_command hermes "/home/$APP_USER/.local/bin/hermes"

log "Checking Hermes Agent"
hermes --version

# Each VM starts its own logs, telemetry and process records.
log "Removing the install's records"
cd "/home/$APP_USER/.hermes"
rm -rf logs backups cache/scratch spawn-ledger.json spawn-ledger.json.lock .hermes-update-in-progress.lock \
  telemetry/shared_metrics/pending_installs
