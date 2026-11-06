# Agent devbox

A desktop VM for coding agents that keep working after your laptop closes:
Claude Code, Codex and the Cursor CLI, with uv, the GitHub CLI and tmux next to
the Node.js 22, Python 3.12, Docker and Google Chrome every Beam desktop has.

```sh
beam vm new agent-devbox --template TEMPLATE_ID    # the ID is on beam.cloud/templates
beam vm terminal agent-devbox
```

Sign in to each tool on the VM (`claude`, `codex`, `cursor-agent`, `gh auth login`);
the template ships signed out. The web terminal and `beam vm exec` run as root;
the desktop's terminal runs as the desktop user, who has their own copy of the
Cursor CLI. `beam vm desktop agent-devbox` opens the desktop, with Chrome for
agents that test what they build.

## Claude Code from your phone

After signing in to `claude` with a Claude subscription, run `claude-remote` in
the web terminal. It starts
[Remote Control](https://code.claude.com/docs/en/remote-control) in
`/workspace`, inside tmux, and prints a link (the spacebar shows it as a QR
code): open it on claude.ai/code or in the Claude app and keep working from
there. Closing the tab
leaves it running; `claude-remote` again reattaches. Options after it go to
`claude remote-control`, such as `--name` and `--permission-mode`.

Other long sessions survive disconnects too: start them in `tmux`, or with
`beam vm exec --detach agent-devbox -- COMMAND` and follow them with
`beam vm logs agent-devbox --pid PID -f`.

Rebuild the template with `python sdk/examples/vm/templates/publish.py agent-devbox`.
