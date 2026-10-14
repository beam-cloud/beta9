from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from jsonschema import Draft202012Validator

from beta9.mcp.vm import definitions


def test_build_error_after_unterminated_progress_is_reported():
    from beta9.mcp.tools import DeployJob

    job = DeployJob(id="test", name="image", directory=".", command=[], operation="command")
    job.lines = [
        "Reading package lists...{",
        '  "error": "Build connection interrupted",',
        '  "code": "ERROR"',
        "}",
    ]
    from beta9.mcp.tools import _json_objects

    errors, _ = _json_objects(job.lines)
    assert job._failure(errors, 1) == "Build connection interrupted"


def local(tmp_path):
    return SimpleNamespace(
        cwd=str(tmp_path),
        context_name="staging",
        start_command=MagicMock(return_value={"job_id": "job"}),
    )


def test_vm_local_tools_preserve_context_argv_and_confirmation(tmp_path, monkeypatch):
    monkeypatch.setattr("beta9.mcp.vm._cli_command", lambda: ["beam"])
    tools = local(tmp_path)
    catalog = {d["name"]: (d, h) for d, h in definitions(tools)}
    for definition, _ in catalog.values():
        Draft202012Validator.check_schema(definition["inputSchema"])
    definition, handler = catalog["vm_ssh"]
    args = {
        "name": "vm",
        "command": ["printf", "two words; $(literal)"],
        "request_key": "same",
        "confirm": False,
    }
    assert handler(args)["isError"]
    tools.start_command.assert_not_called()
    args["confirm"] = True
    assert handler(args)["job_id"] == "job"
    assert tools.start_command.call_args.args[2] == [
        "beam",
        "--context",
        "staging",
        "vm",
        "ssh",
        "vm",
        "--",
        "printf",
        "two words; $(literal)",
    ]
    assert tools.start_command.call_args.kwargs["operation"] == "command"


def test_image_build_cannot_upload_home_or_escape_context(tmp_path):
    tools = local(tmp_path)
    handler = next(h for d, h in definitions(tools) if d["name"] == "vm_build_image")
    for directory, dockerfile in [
        (str(Path.home()), "Dockerfile"),
        (str(tmp_path), "../Dockerfile"),
    ]:
        assert handler(
            {
                "directory": directory,
                "dockerfile": dockerfile,
                "confirm": True,
                "request_key": "key",
            }
        )["isError"]
    tools.start_command.assert_not_called()
