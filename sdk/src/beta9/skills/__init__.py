"""
The agent skill shipped with the CLI: how to operate the platform, written for
coding agents and installed into their skill directories by `setup agent`.
Files are Markdown with a few placeholders so one copy serves every CLI name.
"""

import shutil
from pathlib import Path
from typing import Dict

from ..config import get_settings

SKILL_SOURCE = Path(__file__).parent / "agent"


def skill_name() -> str:
    return f"use-{get_settings().name.lower()}"


def placeholders() -> Dict[str, str]:
    settings = get_settings()
    dashboard = settings.app_url_template.split("/app/")[0] if settings.app_url_template else ""
    return {
        "{{cli}}": settings.name.lower(),
        "{{product}}": settings.name,
        "{{skill}}": skill_name(),
        "{{dashboard_url}}": dashboard,
        "{{docs_url}}": settings.docs_url,
        "{{login_hint}}": (
            f"`{settings.name.lower()} login` (browser sign-in; creates the account if needed)"
            if settings.auth_url
            else f"`{settings.name.lower()} config create` with a token from the dashboard"
        ),
    }


def render(text: str) -> str:
    for key, value in placeholders().items():
        text = text.replace(key, value)
    return text


def install_skill(skills_dir: Path) -> Path:
    """Copy the rendered skill into <skills_dir>/<skill_name>, replacing any older copy."""
    target = skills_dir / skill_name()
    if target.exists():
        shutil.rmtree(target)
    for source in SKILL_SOURCE.rglob("*.md"):
        destination = target / source.relative_to(SKILL_SOURCE)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(render(source.read_text()))
    return target
