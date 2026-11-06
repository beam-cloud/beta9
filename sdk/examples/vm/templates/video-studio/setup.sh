#!/bin/sh
# A desktop for making videos with coding agents: Claude Code and Codex with
# HyperFrames and Remotion, their agent skills, and a starter project for each
# in /workspace. FFmpeg and Google Chrome come with every Beam desktop image.
RECIPE=$(cd "$(dirname "$0")" && pwd)
. "$RECIPE/../common.sh"

DESKTOP_USER=$(cat /opt/beam-vm/desktop-user)

update_coding_agents

log "Installing HyperFrames"
npm install --global --no-fund --no-audit hyperframes@latest
# HyperFrames installs its skills for every agent it finds.
hyperframes skills update
su - "$DESKTOP_USER" -c 'hyperframes skills update'

add_agent_skills remotion-dev/skills

log "Creating the starter projects"
cd /workspace
hyperframes init hyperframes --non-interactive --skip-skills
npx --yes create-video@latest --yes --blank remotion
# create-video starts a repository but can't commit without a git identity.
rm -rf remotion/.git
(cd remotion && npm install --no-fund --no-audit && npx remotion browser ensure)

log "Checking the renders"
(cd hyperframes && hyperframes render -o /tmp/hyperframes.mp4 --quality draft)
(cd remotion && npx remotion render MyComp /tmp/remotion.mp4)
for video in /tmp/hyperframes.mp4 /tmp/remotion.mp4; do
  ffprobe -v error -show_entries format=duration -of csv=p=0 "$video"
done
rm -f /tmp/hyperframes.mp4 /tmp/remotion.mp4

# HyperFrames keeps an anonymous telemetry ID there; each VM makes its own.
rm -rf /root/.hyperframes "/home/$DESKTOP_USER/.hyperframes" /root/.cache/hyperframes/history /root/.local/state/hyperframes

# The starters belong to the desktop user, so apps on the desktop can open and
# save them; root can work in them too.
chown -R "$DESKTOP_USER:" /workspace/hyperframes /workspace/remotion
