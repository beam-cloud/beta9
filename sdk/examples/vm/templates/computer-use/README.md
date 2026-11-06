# Computer use

A Linux desktop that Claude Code and Codex operate while you watch:
[agent-desktop](https://github.com/vercel-labs/agent-desktop) sees and drives
desktop apps through their accessibility tree, and
[agent-browser](https://github.com/vercel-labs/agent-browser) drives Google
Chrome. Both come with their agent skills, so the agents know how to use them.

```sh
beam vm new computer-use --template TEMPLATE_ID    # the ID is on beam.cloud/templates
beam vm desktop computer-use                        # watch the desktop
beam vm terminal computer-use                       # sign in to claude or codex, then ask
```

The template ships signed out of `claude` and `codex`. Give them tasks that
need apps, such as "open Files and make a folder called reports" or "find the
pricing on example.com in Chrome", and watch them work on the desktop. The
desktop has Google Chrome, Files, a terminal and GNOME's settings; install
other apps with `apt`.

agent-desktop reaches apps over the desktop session's accessibility bus. The web
terminal and `beam vm exec` run as root, outside that session, so commands go
through `on-desktop`, which runs them as the desktop user inside it; the
agents' instructions in `~/.claude/CLAUDE.md` and `~/.codex/AGENTS.md` say so:

```sh
beam vm exec computer-use -- on-desktop agent-desktop list-apps
```

The desktop's own terminal is already in the session, and agents started there
work the same way.

Rebuild the template with `python sdk/examples/vm/templates/publish.py computer-use`.
