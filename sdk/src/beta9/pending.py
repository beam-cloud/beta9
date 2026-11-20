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
from typing import Any, Dict, Optional, Set, Tuple

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


def _clock(seconds: float) -> str:
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
            text = f"placed on a machine after {_clock(now - self._began)}, starting the container"
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
                lambda: f"{self._title} · {text} ({_clock(time.monotonic() - since)})"
            )

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

    def clear(self) -> None:
        self._tally._drop(self)
