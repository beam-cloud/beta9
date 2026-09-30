# Setup

## Sign in

{{login_hint}}. Interactive:

```bash
{{cli}} login              # opens the browser; falls back to a link + code on SSH/CI
{{cli}} login --browserless  # always print the link + code
```

Through MCP, call `login`, show the user `verification_uri_complete`, then
call `login_status` (with `wait_seconds` up to 55) until it reports
`signed_in`. The workspace tools appear after that. The user is never asked
for a token; if there is no account yet the sign-in page creates one.

Check who you are with `whoami` (MCP) or `{{cli}} whoami --json`.

## Contexts

Credentials live in a config file as named contexts; `default` is used unless
`--context <name>` is given. `{{cli}} config list` shows them, `{{cli}} config
select <name>` switches. Sign in to a second workspace with
`{{cli}} login --name <context>`; signing in under an existing name renews it
where it already points. CLIs built for several clusters name them:
`{{cli}} login --environment <name>` signs in there (saved as `<name>` unless
`--name` says otherwise), and every command takes `--context <name>` after that.

## Unattended (CI, scripts)

No sign-in step exists without a human, so provide a token from the
environment instead: set `BETA9_TOKEN` (or the product's `*_TOKEN` variable,
e.g. `BEAM_TOKEN`). The CLI and MCP server pick it up when no context file
exists. Create tokens in the dashboard; treat them as secrets.

## MCP registration

`{{cli}} setup agent` installs this skill and registers the MCP server with
every supported agent on the machine (Cursor, Claude Code, Codex, Windsurf,
Claude Desktop). Per client: `{{cli}} mcp install --client <name>`; add
`--project` for repo-scoped config where supported; `--print` shows the
snippet. `{{cli}} mcp status` lists what is wired. The registered command is
`{{cli}} mcp`, a stdio server that uses the context's credentials, so nothing
secret is written into agent config and a rotated token needs no reinstall.

## Self-hosted installs

Point the CLI at another gateway with
`{{cli}} config create <name> --gateway-host <host> --gateway-port <port>`;
everything else is the same.
