"""
`skills`: install agent skills (SKILL.md folders) for Cursor, Claude Code,
Codex and the shared ~/.agents directory, from the configured skills repo or
any git URL passed with `--from`.
"""

import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Optional

import click

from .. import terminal
from ..config import get_settings
from .extraclick import ClickCommonGroup

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
    tmp = Path(tempfile.mkdtemp(prefix="beta9-skills-"))
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


def _skills_source(source: Optional[str]) -> str:
    repo = source or get_settings().skills_repo
    if not repo:
        raise click.ClickException("No skills repo configured; pass --from <git-url>.")
    return repo


@skills.command(name="list", help="Show the skills the configured repo provides.")
@click.option("--from", "source", default=None, help="Git URL of a skills repo.")
def list_skills(source: Optional[str]):
    found = _fetch_repo_skills(_skills_source(source))
    terminal.print_json(
        [{"name": name, "lines": content.count("\n")} for name, content in found.items()]
    )


@skills.command(name="show", help="Print a skill.")
@click.argument("name")
@click.option("--from", "source", default=None, help="Git URL of a skills repo.")
def show(name: str, source: Optional[str]):
    found = _fetch_repo_skills(_skills_source(source))
    if name not in found:
        raise click.ClickException(f"Unknown skill {name!r}. Available: {', '.join(found)}")
    click.echo(found[name])


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
    help="Git URL of a skills repo (defaults to the configured one).",
)
@click.option("--force", is_flag=True, help="Overwrite existing SKILL.md files.")
def install(target: List[str], project: bool, source: Optional[str], force: bool):
    skills_to_install = _fetch_repo_skills(_skills_source(source))

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
