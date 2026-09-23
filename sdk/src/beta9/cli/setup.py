from pathlib import Path
from typing import Optional

import click

from .. import terminal
from ..config import get_settings
from . import extraclick
from .extraclick import ClickCommonGroup
from .mcp import CLIENTS, install as mcp_install
from .skills import install as skills_install

AGENTS_MARKER = "<!-- beta9:agents -->"


def agents_briefing() -> str:
    name, cli = get_settings().name, terminal.cli_name()
    return f"""{AGENTS_MARKER}
## Working with {name}

This project deploys to {name} (serverless GPU/CPU apps).

- Check the target first: `{cli} whoami --format json` and `{cli} status --format json`.
- Deploy from the project root: `{cli} deploy app.py:handler --name <name> --json`
  (or `{cli} deploy --name <name>` for Dockerfile/entrypoint apps). Then
  `{cli} deployment wait <deployment_id>` and `{cli} logs --deployment-id <id>`.
- Every command accepts `--json`; errors come back as `{{"error", "code"}}`
  (`NOT_AUTHENTICATED`, `NEEDS_CONFIRMATION`, `CAPACITY`, `INVALID_CONFIG`).
- Destructive commands prompt; pass `--yes` only when the user asked for it.
- Secrets: `{cli} secret create NAME value`, then `secrets=["NAME"]` or
  `env={{"KEY": "${{{{secret.NAME}}}}"}}` in the app. Databases:
  `{cli} db postgres create <name>` and reference `${{{{db.<name>.DATABASE_URL}}}}`.
- Anything else: `{cli} api search <term>` then `{cli} api call METHOD PATH`.
- Set `BETA9_AGENT_SESSION=<id>` so your actions are grouped in the activity log.
"""


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.group(name="setup", help="One-time setup helpers.")
def setup():
    pass


@setup.command(name="agent", help="Prepare this machine and project for a coding agent.")
@click.option(
    "--client",
    type=click.Choice(CLIENTS),
    default=None,
    help="Register the MCP server with this client.",
)
@click.option("--project", is_flag=True, help="Project-scoped MCP config where supported.")
@click.option(
    "--agents-md/--no-agents-md",
    default=True,
    show_default=True,
    help="Append a short briefing to ./AGENTS.md (created if missing).",
)
@extraclick.config_context_option
@click.pass_context
def agent(
    ctx: click.Context,
    client: Optional[str],
    project: bool,
    agents_md: bool,
    context: Optional[str],
):
    if client:
        ctx.invoke(mcp_install, client=client, project=project, print_only=False, context=context)
        skill_target = {
            "cursor": "cursor",
            "claude-code": "claude",
            "claude-desktop": "claude",
            "codex": "codex",
        }.get(client, "agents")
        ctx.invoke(
            skills_install, target=(skill_target,), project=project, source=None, force=False
        )

    written = None
    if agents_md:
        path = Path.cwd() / "AGENTS.md"
        existing = path.read_text() if path.exists() else ""
        if AGENTS_MARKER in existing:
            terminal.detail("AGENTS.md already has the briefing.")
        else:
            path.write_text(
                (existing.rstrip("\n") + "\n\n" if existing else "") + agents_briefing()
            )
            written = str(path)

    if terminal.json_output():
        terminal.print_json({"client": client, "agents_md": written})
        return
    if written:
        terminal.success(f"Added the briefing to {written}")
    if not client:
        terminal.detail(
            "Register the MCP server with `--client cursor|claude-code|claude-desktop|codex|windsurf`."
        )
