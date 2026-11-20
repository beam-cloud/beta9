import io
from unittest import mock
from unittest.mock import MagicMock, PropertyMock

import cloudpickle
import pytest

from beta9 import Image, pending, terminal
from beta9.abstractions.base.container import Container
from beta9.abstractions.function import Function
from beta9.clients.function import FunctionInvokePending, FunctionInvokeResponse
from beta9.clients.gateway import AttachToContainerResponse, ContainerPending


def waits(phase, reason="", waited_seconds=0):
    return FunctionInvokePending(phase=phase, reason=reason, waited_seconds=waited_seconds)


@pytest.fixture
def output():
    buffer = io.StringIO()
    with terminal.redirect_terminal_to_buffer(buffer):
        yield buffer


@pytest.fixture
def spinner(monkeypatch):
    """A spinner on screen, whose text is kept each time it is set."""
    texts = []
    monkeypatch.setattr(terminal, "spinner_visible", lambda: True)
    monkeypatch.setattr(
        terminal, "update_progress", lambda text: texts.append(text() if callable(text) else text)
    )
    return texts


@pytest.mark.parametrize(
    "phase,reason,said",
    [
        ("provisioning", "worker_starting", "no warm machine, starting one"),
        ("provisioning", "backup_worker_starting", "backup capacity"),
        ("provisioning", "a reason from a newer gateway", "starting a machine"),
        ("waiting_for_capacity", "no_capacity", "no capacity available right now"),
        ("waiting_for_capacity", "machine_busy", "pinned to is full"),
        ("waiting_for_capacity", "no_spare_capacity", "Spot tier"),
        ("waiting_for_capacity", "", "waiting for capacity"),
    ],
)
def test_describe_says_what_a_wait_is_for(phase, reason, said):
    assert said in pending.describe(phase, reason)


@pytest.mark.parametrize("phase", ["queued", "starting", "", "a phase from a newer gateway"])
def test_describe_has_nothing_to_say_about_a_container_that_is_not_held_up(phase):
    assert pending.describe(phase) == ""


def test_a_warm_start_says_nothing(output):
    status = pending.PendingStatus()
    for response in (waits(""), waits("queued"), waits("starting")):
        status.show(response)

    assert output.getvalue() == ""


def test_each_change_is_one_line_where_there_is_no_spinner(output):
    status = pending.PendingStatus("Running transcribe")
    for response in (
        waits("queued"),
        waits("waiting_for_capacity", "no_capacity"),
        waits("waiting_for_capacity", "no_capacity", waited_seconds=30),
        waits("provisioning", "worker_starting"),
        waits("provisioning", "worker_starting", waited_seconds=60),
        waits("starting"),
    ):
        status.show(response)

    lines = [line.strip() for line in output.getvalue().splitlines()]
    assert lines == [
        "No capacity available right now, waiting for some",
        "No warm machine, starting one",
        "Placed on a machine after 0:00, starting the container",
    ]


def test_the_spinner_says_what_the_call_waits_for_and_counts_the_time(spinner, output):
    status = pending.PendingStatus("Running transcribe")

    # The gateway says how long it has waited already: a client that joins
    # late counts from there.
    status.show(waits("provisioning", "worker_starting", waited_seconds=75))
    assert spinner == ["Running transcribe · no warm machine, starting one (1:15)"]

    status.show(waits("starting"))
    assert spinner[-1] == (
        "Running transcribe · placed on a machine after 1:15, starting the container"
    )

    # Once the container has output the spinner is the step's own again.
    status.clear()
    assert spinner[-1] == "Running transcribe"
    assert output.getvalue() == "", "nothing is printed while the spinner says it"


def test_a_spinner_that_never_said_anything_is_left_alone(spinner):
    status = pending.PendingStatus("Running transcribe")
    status.show(waits("queued"))
    status.show(waits("starting"))
    status.clear()

    assert spinner == []


def test_a_batch_counts_its_calls_that_wait(spinner):
    tally = pending.PendingTally("Completed 0/3 calls")
    first, second, third = tally.status(), tally.status(), tally.status()

    first.show(waits("provisioning", "worker_starting"))
    second.show(waits("provisioning", "worker_starting"))
    third.show(waits("waiting_for_capacity", "no_capacity"))
    assert spinner[-1] == (
        "Completed 0/3 calls · 2 waiting for a machine to start · 1 waiting for capacity"
    )

    first.show(waits("starting"))
    tally.retitle("Completed 1/3 calls")
    assert spinner[-1] == (
        "Completed 1/3 calls · 1 waiting for a machine to start · 1 waiting for capacity"
    )

    second.clear()
    third.clear()
    assert spinner[-1] == "Completed 1/3 calls"


def test_a_batch_without_a_spinner_says_each_kind_of_wait_once(output):
    tally = pending.PendingTally("Completed 0/3 calls")
    for status in (tally.status(), tally.status(), tally.status()):
        status.show(waits("provisioning", "worker_starting"))

    assert output.getvalue().count("Some calls are waiting: no warm machine, starting one") == 1


def function_answering(responses):
    function = Function(cpu=1, memory=128, image=Image(python_version="python3.8"))(lambda: 1998)
    function.parent.syncer = MagicMock()
    function.parent.prepare_runtime = MagicMock(return_value=True)
    function.parent.function_stub.function_invoke.return_value = responses
    return function


def test_a_function_call_says_what_it_waits_for_and_returns_its_result(output):
    with mock.patch(
        "beta9.abstractions.function.Function.function_stub",
        new_callable=PropertyMock,
        return_value=MagicMock(),
    ):
        function = function_answering(
            [
                FunctionInvokeResponse(task_id="task-1", container_id="container-1"),
                FunctionInvokeResponse(task_id="task-1", pending=waits("queued")),
                FunctionInvokeResponse(
                    task_id="task-1", pending=waits("provisioning", "backup_worker_starting")
                ),
                FunctionInvokeResponse(task_id="task-1", pending=waits("starting")),
                FunctionInvokeResponse(task_id="task-1", output="hello from the function\n"),
                FunctionInvokeResponse(
                    task_id="task-1", done=True, exit_code=0, result=cloudpickle.dumps(1998)
                ),
            ]
        )

        assert function() == 1998

    said = output.getvalue()
    assert "Preferred capacity is unavailable, starting a machine on backup capacity" in said
    assert "Placed on a machine after 0:00, starting the container" in said
    assert said.index("Placed on a machine") < said.index("hello from the function")


def test_a_gateway_that_says_nothing_about_waiting_changes_nothing(output):
    with mock.patch(
        "beta9.abstractions.function.Function.function_stub",
        new_callable=PropertyMock,
        return_value=MagicMock(),
    ):
        function = function_answering(
            [
                FunctionInvokeResponse(task_id="task-1", container_id="container-1"),
                FunctionInvokeResponse(task_id="task-1", output="hello\n"),
                FunctionInvokeResponse(
                    task_id="task-1", done=True, exit_code=0, result=cloudpickle.dumps(1998)
                ),
            ]
        )

        assert function() == 1998

    assert "waiting" not in output.getvalue()
    assert "machine" not in output.getvalue()


def test_map_says_that_calls_wait(output):
    with mock.patch(
        "beta9.abstractions.function.Function.function_stub",
        new_callable=PropertyMock,
        return_value=MagicMock(),
    ):
        function = function_answering(
            [
                FunctionInvokeResponse(
                    task_id="task-1", pending=waits("waiting_for_capacity", "no_capacity")
                ),
                FunctionInvokeResponse(
                    task_id="task-1", done=True, exit_code=0, result=cloudpickle.dumps(1998)
                ),
            ]
        )

        assert list(function.map([1])) == [1998]

    assert "Some calls are waiting: no capacity available right now" in output.getvalue()


def attached(responses, **kwargs):
    container = Container(container_id="container-1")
    stream = MagicMock()
    stream.__iter__.return_value = iter(responses)
    container.gateway_stub = MagicMock()
    container.gateway_stub.attach_to_container.return_value = stream
    return container.attach(container_id="container-1", **kwargs)


def test_attaching_to_a_container_says_what_it_waits_for(output):
    result = attached(
        [
            AttachToContainerResponse(
                pending=ContainerPending(phase="provisioning", reason="worker_starting")
            ),
            AttachToContainerResponse(output=""),
            AttachToContainerResponse(pending=ContainerPending(phase="starting")),
            AttachToContainerResponse(output="serving\n"),
            AttachToContainerResponse(done=True, exit_code=0, output="exited"),
        ]
    )

    assert result.done
    said = output.getvalue()
    assert "No warm machine, starting one" in said
    assert said.index("Placed on a machine") < said.index("serving")


def test_attaching_without_logs_says_nothing(output):
    attached(
        [
            AttachToContainerResponse(
                pending=ContainerPending(phase="provisioning", reason="worker_starting")
            ),
            AttachToContainerResponse(done=True, exit_code=0),
        ],
        hide_logs=True,
    )

    assert "machine" not in output.getvalue()


def list_containers(monkeypatch, containers, *args):
    from types import SimpleNamespace

    from click.testing import CliRunner

    from beta9.cli import extraclick
    from beta9.cli.container import management
    from beta9.clients.gateway import ListContainersResponse

    class FakeServiceClient:
        def __init__(self, _config):
            self.channel = object()
            self.gateway = self

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def list_containers(self, _request):
            return ListContainersResponse(ok=True, containers=containers)

    monkeypatch.setattr(extraclick, "ServiceClient", FakeServiceClient)
    monkeypatch.setattr(extraclick, "get_config_context", lambda _context: SimpleNamespace())
    result = CliRunner().invoke(management, ["list", *args])
    assert result.exit_code == 0, result.output
    return result.output


def test_container_list_says_what_a_pending_container_waits_for(monkeypatch):
    import json

    from beta9.clients.types import Container as ListedContainer

    containers = [
        ListedContainer(
            container_id="function-waiting",
            status="PENDING",
            pending_phase="waiting_for_capacity",
            pending_reason="no_capacity",
            pending_since=1_700_000_000,
        ),
        ListedContainer(container_id="function-running", status="RUNNING"),
    ]

    table = list_containers(monkeypatch, containers)
    assert "PENDING (waiting for capacity)" in table
    assert "RUNNING (" not in table

    listed = json.loads(list_containers(monkeypatch, containers, "--format", "json"))
    assert listed[0]["pending_phase"] == "waiting_for_capacity"
    assert listed[0]["pending_reason"] == "no_capacity"
    assert "pending_phase" not in listed[1]
