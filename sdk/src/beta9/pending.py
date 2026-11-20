"""Says what a container that has not started is waiting for.

A container is pending from the moment it is asked for until it runs. That is
a second when a machine has room for it, minutes when a machine has to start,
and longer when there is no capacity to be had. The gateway says which on the
streams that wait with the container, and this shows it: on the spinner in a
terminal, and as a line of output anywhere else. Without it all three look
like a spinner that does not move.
"""

import threading
import time
from typing import Any, Callable, Dict, Optional, Set, Tuple

from . import terminal

# The phases a gateway reports.
QUEUED = "queued"
PROVISIONING = "provisioning"
WAITING_FOR_CAPACITY = "waiting_for_capacity"
STARTING = "starting"

_PROVISIONING = {
    "worker_starting": "no warm machine, starting one",
    "backup_worker_starting": "preferred capacity is unavailable, starting a machine on backup capacity",
}
_WAITING_FOR_CAPACITY = {
    "no_capacity": "no capacity available right now, waiting for some",
    "machine_busy": "the machine it is pinned to is full, waiting for room",
    "no_spare_capacity": "the Spot tier has no spare capacity right now, waiting for some",
}


def describe(phase: str, reason: str = "") -> str:
    """What a container in this phase waits for, or "" when there is nothing to say.

    A container that is queued is about to be placed, and one that a machine
    has is starting: neither is a wait worth a word. Nor is a phase this
    client does not know, from a newer gateway.
    """
    if phase == PROVISIONING:
        return _PROVISIONING.get(reason, "starting a machine")
    if phase == WAITING_FOR_CAPACITY:
        return _WAITING_FOR_CAPACITY.get(reason, "waiting for capacity")
    return ""


def clock(seconds: float) -> str:
    """A length of time as a spinner counts it: 1:15."""
    minutes, seconds = divmod(max(int(seconds), 0), 60)
    return f"{minutes}:{seconds:02d}"


def _sentence(text: str) -> str:
    return text[0].upper() + text[1:]


class PendingStatus:
    """Shows what one container waits for until it runs.

    Given the text of the spinner that is on screen, the spinner says it and
    counts the time, and gets its own text back once the container runs.
    Otherwise each change is one line of output.
    """

    def __init__(self, title: Optional[str] = None) -> None:
        self._title = title
        # What was shown last, as (phase, reason).
        self._shown: Optional[Tuple[str, str]] = None
        # When the container began to wait, on the monotonic clock.
        self._began = 0.0

    def show(self, pending: Any) -> None:
        """Take what a response says the container waits for, if it says anything."""
        phase, reason = pending.phase, pending.reason
        if not phase or (phase, reason) == self._shown:
            return

        now = time.monotonic()
        if phase == STARTING:
            # Only a container that was said to wait is said to have stopped.
            if self._shown is None:
                return
            text = f"placed on a machine after {clock(now - self._began)}, starting the container"
            since = None
        else:
            text = describe(phase, reason)
            if not text:
                return
            since = now - pending.waited_seconds
            if self._shown is None:
                self._began = since
        self._shown = (phase, reason)

        if not self._on_spinner():
            terminal.note(_sentence(text))
        elif since is None:
            terminal.update_progress(f"{self._title} · {text}")
        else:
            terminal.update_progress(
                lambda: f"{self._title} · {text} ({clock(time.monotonic() - since)})"
            )

    def advise(self, text: Callable[[], str]) -> None:
        """Say what can be done about a wait: one line of output."""
        terminal.note(text())

    def clear(self) -> None:
        """The container runs, or the call is over: the spinner gets its own text back."""
        if self._shown is not None and self._on_spinner():
            terminal.update_progress(self._title)
        self._shown = None

    def _on_spinner(self) -> bool:
        return self._title is not None and terminal.spinner_visible()


class PendingTally:
    """Shows how many of a batch of containers wait, on the batch's spinner.

    Each container reports to a status of its own (status). The spinner then
    reads, say, "Completed 3/20 calls · 12 waiting for a machine to start".
    Where there is no spinner, the first container to wait for a machine and
    the first to wait for capacity are each one line of output.
    """

    def __init__(self, title: str) -> None:
        self._lock = threading.Lock()
        self._title = title
        self._waiting: Dict["_TallyStatus", str] = {}
        self._said: Set[str] = set()

    def retitle(self, title: str) -> None:
        with self._lock:
            self._title = title
            self._draw()

    def status(self) -> "_TallyStatus":
        return _TallyStatus(self)

    def _set(self, status: "_TallyStatus", phase: str, reason: str) -> None:
        with self._lock:
            if phase in (PROVISIONING, WAITING_FOR_CAPACITY):
                self._waiting[status] = phase
                if phase not in self._said and not terminal.spinner_visible():
                    self._said.add(phase)
                    terminal.note(f"Some calls are waiting: {describe(phase, reason)}")
            elif phase in (QUEUED, STARTING):
                self._waiting.pop(status, None)
            else:
                return
            self._draw()

    def _advise(self, text: Callable[[], str]) -> None:
        with self._lock:
            if "advice" in self._said:
                return
            self._said.add("advice")
        terminal.note(text())

    def _drop(self, status: "_TallyStatus") -> None:
        with self._lock:
            if self._waiting.pop(status, None) is not None:
                self._draw()

    def _draw(self) -> None:
        parts = [self._title]
        phases = list(self._waiting.values())
        if machines := phases.count(PROVISIONING):
            parts.append(f"{machines} waiting for a machine to start")
        if capacity := phases.count(WAITING_FOR_CAPACITY):
            parts.append(f"{capacity} waiting for capacity")
        terminal.update_progress(" · ".join(parts))


class _TallyStatus:
    """One container's place in a PendingTally."""

    def __init__(self, tally: PendingTally) -> None:
        self._tally = tally

    def show(self, pending: Any) -> None:
        if pending.phase:
            self._tally._set(self, pending.phase, pending.reason)

    def advise(self, text: Callable[[], str]) -> None:
        """Say what can be done about a wait, once for the batch."""
        self._tally._advise(text)

    def clear(self) -> None:
        self._tally._drop(self)


class Wait:
    """How long one call has gone without a machine, by what the gateway says.

    The gateway says what a call waits for when that changes, and again every
    few seconds while it does not. So a call that waits is heard from often
    enough to be given up on, or asked about, close to the time.

    limit is the longest the call may go without a machine, and ask_after how
    long it may go without capacity before its caller is asked what to do
    about it. Both are in seconds, and 0 is never.
    """

    def __init__(self, limit: float = 0.0, ask_after: float = 0.0) -> None:
        self.limit = limit
        self.phase = ""
        self.reason = ""
        self._ask_after = ask_after
        self._asked = False
        # When the call began to wait for a machine, on the monotonic clock.
        self._began: Optional[float] = None
        # The time it has had no capacity to wait for: the stretches that are
        # over, and when the one under way began.
        self._without = 0.0
        self._without_since: Optional[float] = None

    def told(self, pending: Any) -> None:
        """Take what a response says the call waits for, if it says anything."""
        phase = pending.phase
        if not phase:
            return
        if phase == STARTING:
            self.placed()
            return

        # The gateway says how long the container has been in its phase, and
        # the wait counts from there: a client that joins late, or whose
        # clock stood still for a while, has waited as long as the gateway
        # says.
        now = time.monotonic()
        since = now - pending.waited_seconds
        self._began = since if self._began is None else min(self._began, since)
        if phase != WAITING_FOR_CAPACITY:
            self._without = self.without_capacity()
            self._without_since = None
        elif self._without_since is None:
            self._without_since = since
        else:
            self._without_since = min(self._without_since, since)
        self.phase, self.reason = phase, pending.reason

    def placed(self) -> None:
        """A machine has the call. If it waits again, for another attempt, it starts over."""
        self.phase = self.reason = ""
        self._began = self._without_since = None
        self._without = 0.0

    def seconds(self) -> float:
        """How long the call has gone without a machine."""
        return 0.0 if self._began is None else time.monotonic() - self._began

    def without_capacity(self) -> float:
        """How long the call has had no capacity to wait for."""
        if self._without_since is None:
            return self._without
        return self._without + time.monotonic() - self._without_since

    def over_limit(self) -> bool:
        return self.limit > 0 and self._began is not None and self.seconds() >= self.limit

    def ask(self) -> bool:
        """True once, when the call has gone long enough without capacity to ask about."""
        if self._asked or self._ask_after <= 0 or self.phase != WAITING_FOR_CAPACITY:
            return False
        if self.without_capacity() < self._ask_after:
            return False
        self._asked = True
        return True
