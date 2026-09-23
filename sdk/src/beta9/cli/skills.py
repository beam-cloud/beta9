"""
`beam skills`: install agent skills (SKILL.md folders) for Cursor, Claude
Code, Codex and the shared ~/.agents directory.

Skills are embedded so they ship in the wheel; `--from <git-url>` installs
from beam-cloud/beam-skills or any repo instead.
"""

import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Optional

import click

from .. import terminal
from .extraclick import ClickCommonGroup

USE_BEAM_SKILL = """---
name: use-beam
description: Deploy and operate apps on Beam (serverless GPU/CPU platform). Use when the user mentions Beam, beam.cloud, `beam deploy`, Beam endpoints/task queues/pods, or wants to run code on GPUs without managing infrastructure.
---

# Use Beam

Beam runs Python functions, HTTP endpoints, task queues, scheduled jobs and
containers (pods) on serverless GPUs and CPUs. The `beam` CLI is the primary
interface; every command accepts `--json` and returns errors as
`{"error": "...", "code": "..."}`.

## Before you act

1. `beam whoami --format json` - confirm the context and workspace you are
   about to change. `beam --context <name> ...` targets another one.
2. `beam status --format json` - latest deployments and running containers.
3. If a command returns `NOT_AUTHENTICATED`, stop and ask the user to run
   `beam configure` (or set `BETA9_TOKEN`); never guess tokens.

## Writing an app

```python
from beam import endpoint, Image

@endpoint(
    name="my-api",
    cpu=1,
    memory="2Gi",
    gpu="A10G",                      # omit for CPU-only
    image=Image(python_version="python3.11", python_packages=["torch"]),
    secrets=["OPENAI_API_KEY"],      # workspace secrets, injected as env vars
    env={"DB_URL": "${{db.app-db.DATABASE_URL}}"},   # references, see below
    keep_warm_seconds=60,
    autoscaler=...,                  # optional
)
def handler(**inputs):
    return {"ok": True}
```

Decorators: `@endpoint` (sync HTTP), `@asgi` (FastAPI/etc), `@task_queue`
(async jobs), `@function` (run-once), `@schedule(when="0 * * * *")`. Pods run
an image with an entrypoint (`Pod(...)`) for non-Python services.

References in `env` values are resolved by the gateway at deploy time:

- `${{secret.NAME}}` binds a workspace secret to that env var.
- `${{db.<name>.DATABASE_URL}}` (or `REDIS_URL`, `USERNAME`, `PASSWORD`,
  `DATABASE`) binds a managed database's credential.
- `${{secret(32)}}` generates and stores a secret once; `${{randomInt(1,100)}}`
  and `${{app.<name>.URL}}` are inlined.

Secret-bearing references must be the whole value; the plaintext never lands
in the stub config.

## Deploying

```bash
beam deploy app.py:handler --name my-api --json    # Python entrypoint
beam deploy --name my-service --json              # Dockerfile / entrypoint app
beam deployment wait <deployment_id> --timeout 300
beam logs --deployment-id <deployment_id> --tail 200
curl -X POST "$INVOKE_URL" -H "Authorization: Bearer $BETA9_TOKEN" -d '{}'
```

`beam deploy --json` prints one object with `deployment_id`, `version`,
`invoke_url` and `status`. Deploying again creates a new version; the previous
one is stopped after the new one is healthy. Roll back by redeploying an
earlier stub from the dashboard's Versions tab or
`beam api call POST /api/v1/gateway/stubs/deploy -d '{"stub_id": "...", "name": "..."}'`.

## Operating

```bash
beam deployment list --format json --filter name=my-api
beam deployment stop|start|delete <id>          # delete asks; --yes skips
beam deployment scale <id> --containers 2       # pod deployments
beam api call PATCH "/api/v1/stub/{ws}/<stub_id>/config" \\
  -d '{"fields": {"keep_warm_seconds": 120, "autoscaler.max_containers": 3}}'
beam task list --format json --filter stub_id=<stub_id>
beam container list --format json
```

Config PATCH takes effect on running instances without a redeploy.

## Secrets and databases

```bash
beam secret create OPENAI_API_KEY sk-...
beam secret list --format json
beam db postgres create app-db --format json      # or: beam db redis create cache
beam db postgres credentials app-db --format json
beam db postgres rotate app-db                    # restarts with the new password
```

Database credentials are stored as secrets named
`BETA9_<KIND>_<NAME>_{URL,USERNAME,PASSWORD,DATABASE}`; reference them with
`${{db.<name>.DATABASE_URL}}` instead of copying values.

## Anything else

`beam api search <term>` lists REST routes; `beam api call METHOD PATH
[-d JSON] [-q key=value]` calls one. `{ws}` in the path is replaced with the
workspace id. Workspace limits (CPU, memory, replicas, GPU types) come from
`GET /api/v1/workspace/{ws}/limits`; do not hard-code them.

## More surfaces

- `beam mcp install --client cursor|claude-code|codex` registers the MCP server
  (same tools as this CLI, plus `stage_config`/`accept_deploy` for reviewed
  changes, `connect_services` to wire one app to another with a `${{...}}`
  reference, and `http_requests`/`http_error_rate`/`http_response_time`).
- `get_workspace_graph` (MCP) is the map of apps and the `${{...}}` references
  between them; read it before proposing changes.
- The dashboard's Stacks page is the same graph, one named stack at a time:
  drag a card's right handle onto an app to stage a reference, review the diff,
  deploy.
- `beam template plan|deploy <name|path>` deploys multi-service manifests
  (databases first, ordered by references); `beam template import
  docker-compose.yml`; `beam template export <apps>` saves apps as a manifest.
- `beam infra init|plan|apply` keeps `.beam/beam.yaml` in sync with the
  workspace; `plan --detailed-exit-code` exits 2 when there is drift; deletes
  need `--confirm-destructive`.
- `beam workspace duplicate|sync|diff <src> <dst>` treats workspaces as
  environments (staging → prod).
- `beam code [--agent claude|codex]` opens a persistent dev VM with agents
  preinstalled; `beam code sleep|wake|destroy`.
- Webhooks for workspace events: `POST /api/v1/webhook/{ws}` (HMAC-signed
  CloudEvents; Settings → Webhooks in the dashboard).

## Rules

- Confirm destructive actions with the user before passing `--yes` or
  `confirm=true`.
- Do not paste secret values into code or chat; create a secret and reference it.
- Export `BEAM_AGENT_SESSION=<id>` for a task so your actions group together in
  the workspace activity log, and leave `BEAM_CALLER` alone unless asked.
- Prefer `--json` output and read `code` on errors: `NEEDS_CONFIRMATION`,
  `NOT_FOUND`, `CAPACITY`, `INVALID_CONFIG`, `TIMEOUT`, `GATEWAY_UNAVAILABLE`.
"""

SKILLS: Dict[str, str] = {"use-beam": USE_BEAM_SKILL}

TARGETS = ("cursor", "claude", "codex", "agents")


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.group(name="skills", help="Install agent skills (SKILL.md) for coding agents.")
def skills():
    pass


def _target_dir(target: str, project: bool) -> Path:
    root = Path.cwd() if project else Path.home()
    return {
        "cursor": root / ".cursor" / "skills",
        "claude": root / ".claude" / "skills",
        "codex": root / ".codex" / "skills",
        "agents": root / ".agents" / "skills",
    }[target]


def _fetch_repo_skills(source: str) -> Dict[str, str]:
    """Clone a skills repo and read every <name>/SKILL.md at its top level."""
    tmp = Path(tempfile.mkdtemp(prefix="beam-skills-"))
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", source, str(tmp / "repo")],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        shutil.rmtree(tmp, ignore_errors=True)
        detail = exc.stderr.strip() if isinstance(exc, subprocess.CalledProcessError) else str(exc)
        raise click.ClickException(f"Failed to clone {source}: {detail}")
    found: Dict[str, str] = {}
    for skill_file in sorted((tmp / "repo").glob("*/SKILL.md")):
        found[skill_file.parent.name] = skill_file.read_text()
    shutil.rmtree(tmp, ignore_errors=True)
    if not found:
        raise click.ClickException(f"No <name>/SKILL.md folders found in {source}")
    return found


@skills.command(name="list", help="Show the skills this CLI can install.")
def list_skills():
    terminal.print_json(
        [{"name": name, "lines": content.count("\n")} for name, content in SKILLS.items()]
    )


@skills.command(name="show", help="Print a skill.")
@click.argument("name", default="use-beam")
def show(name: str):
    if name not in SKILLS:
        raise click.ClickException(f"Unknown skill {name!r}. Available: {', '.join(SKILLS)}")
    click.echo(SKILLS[name])


@skills.command(name="install", help="Write skills into an agent's skills directory.")
@click.option(
    "--target", type=click.Choice(TARGETS), multiple=True, default=("agents",), show_default=True
)
@click.option(
    "--project", is_flag=True, help="Install under the current directory instead of $HOME."
)
@click.option(
    "--from",
    "source",
    default=None,
    help="Git URL of a skills repo (defaults to the embedded skills).",
)
@click.option("--force", is_flag=True, help="Overwrite existing SKILL.md files.")
def install(target: List[str], project: bool, source: Optional[str], force: bool):
    skills_to_install = _fetch_repo_skills(source) if source else SKILLS

    written: List[str] = []
    skipped: List[str] = []
    for t in target:
        base = _target_dir(t, project)
        for name, content in skills_to_install.items():
            path = base / name / "SKILL.md"
            if path.exists() and not force:
                skipped.append(str(path))
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
            written.append(str(path))

    if terminal.json_output():
        terminal.print_json({"written": written, "skipped": skipped})
        return
    for path in written:
        terminal.success(f"Installed {path}")
    for path in skipped:
        terminal.detail(f"Exists, skipped (use --force): {path}")
