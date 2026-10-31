from typing import Optional

import click

from .. import feedback as reports
from .. import terminal
from ..channel import GatewayHTTPError, ServiceClient
from . import extraclick
from .extraclick import ClickCommonGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    name="feedback",
    help="Report a bug, a confusing error or doc, a missing feature, or something that worked well.",
    epilog="""
    Coding agents: send one report when the platform itself seems at fault, then
    continue the task. Never include secrets, environment values or the user's data.
    BETA9_NO_FEEDBACK=1 turns feedback off.

    \b
    Examples:
      {cli_name} feedback -m "deploy ignored --port" --kind bug --last-error
      {cli_name} feedback -m "no way to choose a GPU region" --kind missing_capability
    """,
)
@click.option("--message", "-m", help="One sentence: what happened, or what would help.")
@click.option(
    "--kind",
    type=click.Choice(reports.KINDS),
    default="other",
    metavar="KIND",
    help=f"{', '.join(reports.KINDS[:-1])} or {reports.KINDS[-1]}.",
)
@click.option("--blocked", is_flag=True, help="It stopped what you were doing.")
@click.option("--details", help="What you tried and what you expected.")
@click.option("--suggest", help="What would have helped.")
@click.option("--command", "command_name", help="The command or tool involved.")
@click.option("--last-error", is_flag=True, help="Attach the last error this CLI reported.")
@click.option("--model", help="The model sending this, when a coding agent does.")
@extraclick.format_option
@extraclick.pass_service_client
def feedback(
    service: ServiceClient,
    message: Optional[str],
    kind: str,
    blocked: bool,
    details: Optional[str],
    suggest: Optional[str],
    command_name: Optional[str],
    last_error: bool,
    model: Optional[str],
    format: str,
):
    # A failed report must not replace the error it may be about.
    reports.record_errors(False)
    if reports.feedback_disabled():
        terminal.error(reports.DISABLED_MESSAGE, code="FEEDBACK_DISABLED")

    summary = (message or "").strip()
    if not summary and terminal.is_interactive():
        summary = (terminal.prompt(text="What should the team know? One sentence") or "").strip()
    if not summary:
        terminal.error('Say what happened in one sentence: -m "<summary>".', code="INPUT_REQUIRED")
    body = {
        "kind": kind,
        "summary": summary,
        "blocked": blocked,
        "command": command_name or "",
        "details": details or "",
        "suggested_improvement": suggest or "",
        "model": model or "",
        "environment": reports.environment(),
    }
    if last_error:
        record = reports.last_error()
        if record is None:
            terminal.warn("No error from the last hour to attach.")
        else:
            body["command"] = body["command"] or record.get("command", "")
            body["error_code"] = record.get("code", "")
            body["error_message"] = "\n".join(
                part for part in (record.get("error"), record.get("details")) if part
            )

    try:
        reply = service.http.json("POST", "/api/v1/feedback/{ws}", json=body, timeout=30)
    except GatewayHTTPError as exc:
        terminal.error(exc.message, code="INVALID_ARGS" if exc.status in (400, 413) else exc.code)

    if terminal.json_output(format):
        terminal.print_json(reply)
        return
    if reply.get("duplicate"):
        terminal.success(f"Already sent ({reply['id']}). Thanks.")
    else:
        terminal.success(f"Feedback sent ({reply['id']}). Thanks.")
