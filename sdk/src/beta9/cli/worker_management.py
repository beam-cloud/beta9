from typing import List, Sequence

import click

from .. import terminal
from ..channel import ServiceClient
from ..clients.gateway import (
    CordonWorkerRequest,
    DrainWorkerRequest,
    ListWorkersRequest,
    UncordonWorkerRequest,
)


def worker_ids_from_args(worker_ids: Sequence[str]) -> List[str]:
    ids = [worker_id.strip() for worker_id in worker_ids if worker_id.strip()]
    if "-" in ids:
        if len(ids) != 1:
            terminal.error("Use '-' by itself when reading worker IDs from stdin.")
        ids = click.get_text_stream("stdin").read().split()
    ids = list(dict.fromkeys(ids))
    if not ids:
        terminal.error("Provide at least one worker ID.")
    return ids


def worker_ids_for_machine(service: ServiceClient, machine_id: str) -> List[str]:
    res = service.gateway.list_workers(ListWorkersRequest())
    if not res.ok:
        terminal.error(f"Failed to list workers: {res.err_msg}")
    ids = [worker.id for worker in res.workers if worker.machine_id == machine_id]
    if not ids:
        terminal.error(f"No workers found for machine '{machine_id}'.")
    return ids


def apply_worker_action(service: ServiceClient, worker_ids: Sequence[str], action: str) -> None:
    request_type, method, completed = {
        "cordon": (CordonWorkerRequest, "cordon_worker", "Cordoned"),
        "uncordon": (UncordonWorkerRequest, "uncordon_worker", "Uncordoned"),
        "drain": (DrainWorkerRequest, "drain_worker", "Cordoned and drained"),
    }[action]
    failures = []
    for worker_id in worker_ids:
        res = getattr(service.gateway, method)(request_type(worker_id=worker_id))
        if not res.ok:
            failures.append(worker_id)
            terminal.warn(f"{worker_id}: {res.err_msg}")

    succeeded = len(worker_ids) - len(failures)
    if succeeded:
        noun = "worker" if succeeded == 1 else "workers"
        terminal.success(f"{completed} {succeeded} {noun}.")
    if failures:
        terminal.error(f"Failed to {action} {len(failures)} worker(s).")
