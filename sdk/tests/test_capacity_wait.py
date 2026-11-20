"""What a call does about a machine that does not come."""

import io
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import cloudpickle
import pytest

from beta9 import Image, NoCapacityError, pending, terminal
from beta9.abstractions.base import capacity
from beta9.abstractions.function import Function
from beta9.clients.function import FunctionInvokePending, FunctionInvokeResponse
from beta9.clients.gateway import (
    GetOrCreateStubRequest,
    GetOrCreateStubResponse,
    ListPoolOffersResponse,
    ListTasksResponse,
    PoolOffer,
    StopTasksRequest,
    Task,
)

LIMIT = capacity.wait_limit_name()


def waits(phase, reason="", waited_seconds=0):
    return FunctionInvokePending(phase=phase, reason=reason, waited_seconds=waited_seconds)


@pytest.fixture
def output():
    buffer = io.StringIO()
    with terminal.redirect_terminal_to_buffer(buffer):
        yield buffer


@pytest.fixture
def clock(monkeypatch):
    """The clock that waits are measured on, moved by hand."""
    now = SimpleNamespace(at=1000.0)
    monkeypatch.setattr(pending, "time", SimpleNamespace(monotonic=lambda: now.at))
    return now


# How long a call has waited


def test_a_wait_counts_from_when_the_gateway_does(clock):
    wait = pending.Wait(limit=120)
    assert wait.seconds() == 0 and not wait.over_limit()

    # A client that joins late hears how long the container has waited.
    wait.told(waits("queued", waited_seconds=30))
    assert wait.seconds() == 30

    clock.at += 89
    wait.told(waits("provisioning", "worker_starting", waited_seconds=80))
    assert wait.seconds() == 119 and not wait.over_limit()

    clock.at += 1
    assert wait.over_limit()


def test_a_call_that_a_machine_has_is_not_over_its_limit(clock):
    wait = pending.Wait(limit=120)
    wait.told(waits("provisioning", "worker_starting"))
    clock.at += 100
    wait.told(waits("starting"))
    clock.at += 100

    assert not wait.over_limit()

    # Another attempt, after the machine was lost, waits from the start.
    wait.told(waits("provisioning", "worker_starting"))
    clock.at += 119
    assert not wait.over_limit()
    clock.at += 1
    assert wait.over_limit()


def test_time_without_capacity_adds_up_and_is_asked_about_once(clock):
    wait = pending.Wait(ask_after=60)

    wait.told(waits("waiting_for_capacity", "no_capacity"))
    clock.at += 40
    wait.told(waits("waiting_for_capacity", "no_capacity", waited_seconds=40))
    assert wait.without_capacity() == 40 and not wait.ask()

    # A machine that is tried for and does not come is not time without capacity.
    wait.told(waits("provisioning", "worker_starting"))
    clock.at += 300
    assert wait.without_capacity() == 40 and not wait.ask()

    wait.told(waits("waiting_for_capacity", "no_capacity"))
    clock.at += 19
    assert not wait.ask()
    clock.at += 1
    assert wait.without_capacity() == 60
    assert wait.ask()
    assert not wait.ask(), "asked once"


def test_a_wait_is_as_long_as_the_gateway_says(clock):
    wait = pending.Wait(limit=120, ask_after=60)

    wait.told(waits("waiting_for_capacity", "no_capacity", waited_seconds=20))
    assert wait.seconds() == 20 and not wait.over_limit()
    # The client's clock stood still: a laptop that slept.
    wait.told(waits("waiting_for_capacity", "no_capacity", waited_seconds=130))

    assert wait.without_capacity() == 130
    assert wait.ask()
    assert wait.seconds() == 130
    assert wait.over_limit()


def test_nothing_is_asked_about_a_machine_that_is_starting(clock):
    wait = pending.Wait(ask_after=60)
    wait.told(waits("provisioning", "worker_starting"))
    clock.at += 600

    assert not wait.ask()


@pytest.mark.parametrize(
    "value,seconds",
    [("", 0), ("0", 0), ("90", 90), ("90s", 90), ("5m", 300), ("1h", 3600), (" 2M ", 120)],
)
def test_the_limit_is_read_from_the_environment(monkeypatch, value, seconds):
    monkeypatch.setenv(LIMIT, value)

    assert capacity.wait_limit() == seconds


@pytest.mark.parametrize("value", ["soon", "-5", "5 minutes", "nan", "m"])
def test_a_limit_that_cannot_be_read_is_an_error(monkeypatch, capsys, value):
    monkeypatch.setenv(LIMIT, value)
    monkeypatch.setattr(terminal, "json_output", lambda: True)

    with pytest.raises(SystemExit):
        capacity.wait_limit()

    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "INVALID_CONFIG"
    assert LIMIT in payload["error"]


# A function call

ACCEPTED = FunctionInvokeResponse(task_id="task-1", container_id="container-1")
RESULT = FunctionInvokeResponse(
    task_id="task-1", done=True, exit_code=0, result=cloudpickle.dumps(1998)
)


def says(phase, reason="", waited_seconds=0):
    return FunctionInvokeResponse(task_id="task-1", pending=waits(phase, reason, waited_seconds))


NO_CAPACITY = says("waiting_for_capacity", "no_capacity", waited_seconds=130)


def function_on(gateway_says, gpu="A10G", **kwargs):
    """A function on a gateway that answers each invocation with the next list of responses."""
    function = Function(
        cpu=1, memory=128, gpu=gpu, image=Image(python_version="python3.8"), **kwargs
    )(lambda: 1998)
    parent = function.parent
    parent.syncer = MagicMock()
    parent.prepare_runtime = MagicMock(return_value=True)
    parent.stub_id = "stub-1"
    parent.stub_request = GetOrCreateStubRequest(gpu=gpu if isinstance(gpu, str) else ",".join(gpu))
    parent.gateway_stub = MagicMock()
    parent.gateway_stub.list_pool_offers.return_value = ListPoolOffersResponse(
        ok=True,
        offers=[
            PoolOffer(id="dear", gpu="A10G", provider="p", hourly_cost_micros=900_000),
            PoolOffer(id="cheap", gpu="A10G", provider="p", hourly_cost_micros=600_000),
        ],
    )
    parent.gateway_stub.list_tasks.return_value = ListTasksResponse(
        ok=True, tasks=[Task(id="task-1", status="PENDING")]
    )
    parent.function_stub = MagicMock()
    parent.function_stub.function_invoke.side_effect = [iter(answer) for answer in gateway_says]
    return function


def stopped(function):
    """The tasks the function asked the gateway to stop."""
    return [call.args[0] for call in function.parent.gateway_stub.stop_tasks.call_args_list]


def invoked_on(function):
    """The stub of each invocation the function made."""
    calls = function.parent.function_stub.function_invoke.call_args_list
    return [call.args[0].stub_id for call in calls]


def test_a_call_gives_up_when_it_has_waited_as_long_as_it_may(monkeypatch, output):
    monkeypatch.setenv(LIMIT, "2m")
    function = function_on([[ACCEPTED, NO_CAPACITY, RESULT]])

    with pytest.raises(NoCapacityError) as gave_up:
        function()

    error = gave_up.value
    assert error.message == (
        "No A10G capacity became available in 2 minutes. The call was cancelled."
    )
    assert error.code == 1, "a script that does not catch it exits with status 1"
    assert "`beta9 machine reserve --gpu A10G` (from $0.60/hr)" in error.hint
    assert f"{LIMIT} limits the wait to 2 minutes." in error.hint
    # A call that is given up on does not run later.
    assert stopped(function) == [StopTasksRequest(task_ids=["task-1"])]

    said = output.getvalue()
    assert "✗ No A10G capacity became available in 2 minutes. The call was cancelled." in said
    assert "hint: " in said


def test_giving_up_has_a_code_a_script_can_branch_on(monkeypatch, capsys):
    monkeypatch.setenv(LIMIT, "120")
    monkeypatch.setattr(terminal, "json_output", lambda: True)
    function = function_on([[ACCEPTED, NO_CAPACITY, RESULT]], gpu=["A10G", "T4"])

    with pytest.raises(NoCapacityError):
        function()

    lines = capsys.readouterr().out.splitlines()
    payload = json.loads("\n".join(lines[lines.index("{") :]))
    assert payload["code"] == "CAPACITY"
    assert payload["error"] == (
        "No A10G or T4 capacity became available in 2 minutes. The call was cancelled."
    )
    assert "machine list" in payload["hint"]


@pytest.mark.parametrize(
    "said,message",
    [
        (
            says("provisioning", "worker_starting", waited_seconds=400),
            "A machine for this call was still starting after 7 minutes.",
        ),
        (
            says("waiting_for_capacity", "no_spare_capacity", waited_seconds=125),
            "The Spot tier had no spare capacity for this call in 2 minutes.",
        ),
        (
            says("waiting_for_capacity", "machine_busy", waited_seconds=125),
            "The machine this call is pinned to had no room for it in 2 minutes.",
        ),
        (says("queued", waited_seconds=125), "This call was not placed on a machine in 2 minutes."),
    ],
)
def test_giving_up_says_what_the_call_waited_for(monkeypatch, output, said, message):
    monkeypatch.setenv(LIMIT, "120")
    function = function_on([[ACCEPTED, said, RESULT]])

    with pytest.raises(NoCapacityError) as gave_up:
        function()

    assert gave_up.value.message == f"{message} The call was cancelled."


def test_a_call_within_its_limit_runs(monkeypatch, output):
    monkeypatch.setenv(LIMIT, "10m")
    function = function_on([[ACCEPTED, NO_CAPACITY, says("starting"), RESULT]])

    assert function() == 1998
    assert stopped(function) == []


def test_a_call_with_no_limit_keeps_waiting_and_says_once_what_can_be_done(monkeypatch, output):
    monkeypatch.delenv(LIMIT, raising=False)
    function = function_on(
        [
            [
                ACCEPTED,
                says("waiting_for_capacity", "no_capacity", waited_seconds=20),
                NO_CAPACITY,
                says("waiting_for_capacity", "no_capacity", waited_seconds=140),
                says("starting"),
                RESULT,
            ]
        ]
    )

    assert function() == 1998

    said = " ".join(line.strip() for line in output.getvalue().splitlines())
    assert said.count("Still no A10G capacity after 2:10.") == 1
    assert f"The call keeps waiting; {LIMIT}=<seconds> gives up sooner." in said
    assert "`beta9 machine reserve --gpu A10G`" in said
    assert stopped(function) == []
    # Nobody is asked where nobody can answer.
    function.parent.gateway_stub.list_pool_offers.assert_not_called()


def test_a_call_on_the_spot_tier_is_told_what_does_not_wait(monkeypatch, output):
    monkeypatch.delenv(LIMIT, raising=False)
    function = function_on(
        [
            [
                ACCEPTED,
                says("waiting_for_capacity", "no_spare_capacity", waited_seconds=70),
                says("starting"),
                RESULT,
            ]
        ],
        gpu="",
        cpu_tier="spot",
    )

    assert function() == 1998

    said = " ".join(line.strip() for line in output.getvalue().splitlines())
    assert "Still no spare capacity for the Spot tier after 1:10." in said
    assert 'cpu_tier="shared" or "performance" does not wait for it.' in said


def test_a_batch_says_once_what_can_be_done_and_reports_each_call_given_up_on(monkeypatch, output):
    monkeypatch.delenv(LIMIT, raising=False)
    function = function_on([[ACCEPTED, NO_CAPACITY, says("starting"), RESULT]] * 3)

    assert list(function.map([1, 2, 3])) == [1998, 1998, 1998]
    assert output.getvalue().count("Still no A10G capacity") == 1

    monkeypatch.setenv(LIMIT, "2m")
    function = function_on([[ACCEPTED, NO_CAPACITY, RESULT]] * 3)

    assert list(function.map([1, 2, 3])) == [None, None, None]
    said = output.getvalue()
    for index in (1, 2, 3):
        assert f"Input {index} failed: No A10G capacity became available in 2 minutes." in said
    assert len(stopped(function)) == 3
    # The price of a machine is not looked up for each call of a batch.
    function.parent.gateway_stub.list_pool_offers.assert_not_called()


# A caller at a terminal


@pytest.fixture
def at_a_terminal(monkeypatch):
    """A caller who can be asked, and who answers each question with the next answer."""
    answers = []
    asked = []

    def select(title, options, default_index=0):
        asked.append((title, [option.label for option in options]))
        answer = answers.pop(0)
        if callable(answer):
            return answer(options)
        return answer

    monkeypatch.delenv(LIMIT, raising=False)
    # A spinner that is on screen stands in for the terminal's output, so
    # nobody can be asked anything until it is taken off.
    monkeypatch.setattr(terminal, "is_interactive", lambda: terminal._current_status is None)
    monkeypatch.setattr(terminal, "select", select)
    monkeypatch.setattr(terminal, "confirm", lambda *_args, **_kwargs: True)
    return SimpleNamespace(answers=answers, asked=asked)


QUESTION = "How do you want to proceed?"


def test_a_caller_at_a_terminal_is_asked_and_can_keep_waiting(at_a_terminal, output):
    at_a_terminal.answers.append(capacity.WAIT)
    function = function_on([[ACCEPTED, NO_CAPACITY, NO_CAPACITY, says("starting"), RESULT]])

    assert function() == 1998

    assert at_a_terminal.asked == [(QUESTION, ["Keep waiting", "Reserve a machine", "Cancel"])]
    said = " ".join(line.strip() for line in output.getvalue().splitlines())
    assert "No A10G capacity has been available for 2:10." in said
    assert "This is a platform limit, not your code." in said
    assert stopped(function) == []


def test_a_caller_who_cancels_stops_the_call(at_a_terminal, output):
    at_a_terminal.answers.append(capacity.CANCEL)
    function = function_on([[ACCEPTED, NO_CAPACITY, RESULT]])

    with pytest.raises(SystemExit) as exit:
        function()

    assert exit.value.code == 1
    assert not isinstance(exit.value, NoCapacityError)
    assert stopped(function) == [StopTasksRequest(task_ids=["task-1"])]
    assert "Cancelled. The function did not run." in output.getvalue()


def test_a_caller_can_reserve_a_machine_and_the_call_runs_on_it(at_a_terminal, monkeypatch, output):
    at_a_terminal.answers.extend(
        [
            capacity.RESERVE,
            lambda offers: offers[0].value,  # the cheapest offer
            "2h",  # for how long
        ]
    )
    ready = MagicMock(return_value=True)
    monkeypatch.setattr(capacity, "wait_for_pool_ready", ready)
    function = function_on([[ACCEPTED, NO_CAPACITY, RESULT], [ACCEPTED, RESULT]])
    gateway = function.parent.gateway_stub
    gateway.get_or_create_stub.return_value = GetOrCreateStubResponse(ok=True, stub_id="stub-2")

    assert function() == 1998

    # The stub was created again, on the offer that was chosen.
    pool = gateway.get_or_create_stub.call_args.args[0].pool
    assert (pool.offer_id, pool.ttl, pool.pricing) == ("cheap", "2h", "")
    assert function.parent.pool_config is pool
    ready.assert_called_once_with(gateway, pool.name)
    # The call that waited was given up, and made again on the machine.
    assert stopped(function) == [StopTasksRequest(task_ids=["task-1"])]
    assert invoked_on(function) == ["stub-1", "stub-2"]
    # The machine is reserved before the call that waits is given up.
    order = [
        call[0] for call in gateway.method_calls if call[0] in ("get_or_create_stub", "stop_tasks")
    ]
    assert order == ["get_or_create_stub", "stop_tasks"]


def test_a_call_that_got_a_machine_while_its_caller_chose_is_left_to_run(at_a_terminal, output):
    at_a_terminal.answers.append(capacity.RESERVE)
    function = function_on([[ACCEPTED, NO_CAPACITY, says("starting"), RESULT]])
    gateway = function.parent.gateway_stub
    gateway.list_tasks.return_value = ListTasksResponse(
        ok=True, tasks=[Task(id="task-1", status="RUNNING")]
    )

    assert function() == 1998

    assert "A machine took the call while you were choosing." in output.getvalue()
    gateway.get_or_create_stub.assert_not_called()
    assert stopped(function) == []


def test_a_caller_who_reserves_nothing_keeps_waiting(at_a_terminal, output):
    at_a_terminal.answers.append(capacity.RESERVE)
    function = function_on([[ACCEPTED, NO_CAPACITY, says("starting"), RESULT]])
    gateway = function.parent.gateway_stub
    gateway.list_pool_offers.return_value = ListPoolOffersResponse(ok=True, offers=[])

    assert function() == 1998

    said = output.getvalue()
    assert "No on-demand offers currently available for A10G." in said
    assert "No machine was reserved. Still waiting for capacity." in said
    assert stopped(function) == []
    assert invoked_on(function) == ["stub-1"]


def test_a_machine_that_is_reserved_and_not_ready_ends_the_call(at_a_terminal, monkeypatch, output):
    at_a_terminal.answers.extend([capacity.RESERVE, lambda offers: offers[0].value, "1h"])
    monkeypatch.setattr(capacity, "wait_for_pool_ready", lambda *_args: False)
    function = function_on([[ACCEPTED, NO_CAPACITY, RESULT]])
    gateway = function.parent.gateway_stub
    gateway.get_or_create_stub.return_value = GetOrCreateStubResponse(ok=True, stub_id="stub-2")

    with pytest.raises(SystemExit) as exit:
        function()

    assert exit.value.code == 1
    # The call does not run on other capacity once a machine is paid for.
    assert stopped(function) == [StopTasksRequest(task_ids=["task-1"])]
    assert invoked_on(function) == ["stub-1"]


@pytest.mark.parametrize(
    "said,kwargs,warning",
    [
        (
            says("waiting_for_capacity", "no_spare_capacity", waited_seconds=70),
            {"gpu": "", "cpu_tier": "spot"},
            "The Spot tier has had no spare capacity for 1:10.",
        ),
        (
            says("waiting_for_capacity", "no_capacity", waited_seconds=70),
            {"gpu": ""},
            "No capacity has been available for 1:10.",
        ),
        (
            says("waiting_for_capacity", "no_capacity", waited_seconds=70),
            {"gpu": "A10G", "pool": "my-pool"},
            "No A10G capacity has been available for 1:10.",
        ),
    ],
)
def test_a_machine_is_offered_only_where_one_would_help(
    at_a_terminal, output, said, kwargs, warning
):
    at_a_terminal.answers.append(capacity.WAIT)
    function = function_on([[ACCEPTED, said, says("starting"), RESULT]], **kwargs)

    assert function() == 1998

    assert at_a_terminal.asked == [(QUESTION, ["Keep waiting", "Cancel"])]
    assert warning in output.getvalue()


def test_a_batch_asks_nobody(at_a_terminal, output):
    function = function_on([[ACCEPTED, NO_CAPACITY, says("starting"), RESULT]] * 2)

    assert list(function.map([1, 2])) == [1998, 1998]

    assert at_a_terminal.asked == []
    assert output.getvalue().count("Still no A10G capacity") == 1


def test_a_function_that_runs_in_the_background_asks_nobody(at_a_terminal, output):
    function = function_on([[ACCEPTED, NO_CAPACITY, says("starting"), RESULT]], headless=True)

    assert function() == 1998

    assert at_a_terminal.asked == []
    assert "Still no A10G capacity" in output.getvalue()


# The spinner


def test_a_paused_spinner_is_back_as_it_was(monkeypatch):
    with terminal.redirect_terminal_to_buffer(io.StringIO()):
        with terminal.progress("Running transcribe") as running:
            with terminal.progress_paused():
                assert not terminal.spinner_visible()
                # What runs meanwhile has a spinner of its own.
                with terminal.progress("Finding available hardware...") as finding:
                    assert finding is not running
            with terminal.progress("still the first") as again:
                assert again is running
