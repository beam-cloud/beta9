import os
import time
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Callable, Optional

from watchdog.observers import Observer

from ... import terminal
from ...clients.gateway import (
    AttachToContainerRequest,
    ContainerStreamMessage,
    GatewayServiceStub,
    SyncContainerWorkspaceOperation,
    SyncContainerWorkspaceRequest,
)
from ...sync import SyncEventHandler
from .runner import BaseAbstraction

DEFAULT_SYNC_INTERVAL = 0.1


@dataclass
class ContainerAttachResult:
    done: bool
    exit_code: int
    output: str = ""


class Container(BaseAbstraction):
    def __init__(
        self,
        container_id: str,
    ) -> None:
        super().__init__()
        self.gateway_stub = GatewayServiceStub(self.channel)
        self.container_id = container_id

    def attach(
        self,
        *,
        container_id: str,
        sync_dir: Optional[str] = None,
        hide_logs: bool = False,
        exit_on_error: bool = True,
    ) -> ContainerAttachResult:
        """
        Attach to a running container and stream messages back and forth. Also, optionally sync a directory to the container workspace.
        """

        terminal.header(f"Connecting to {container_id}...")

        def _container_stream_generator():
            yield ContainerStreamMessage(
                attach_request=AttachToContainerRequest(container_id=container_id)
            )

            if sync_dir:
                yield from self._sync_dir_to_workspace(
                    dir=sync_dir, container_id=container_id, hide_logs=hide_logs
                )
            else:
                while True:
                    time.sleep(DEFAULT_SYNC_INTERVAL)
                    yield ContainerStreamMessage()

        # Connect to the remote container and stream messages back and forth
        stream = self.gateway_stub.attach_to_container(_container_stream_generator())

        r = None
        for r in stream:
            if r.output and not hide_logs:
                terminal.detail(r.output, end="")

            if r.done or r.exit_code != 0:
                break

        if r is None:
            terminal.error("Container failed")
            return ContainerAttachResult(done=False, exit_code=1)

        result = ContainerAttachResult(
            done=r.done,
            exit_code=r.exit_code,
            output=r.output,
        )

        if r.exit_code != 0:
            terminal.error(f"\nContainer exited with code {r.exit_code}", exit=False)
            if exit_on_error:
                raise SystemExit(r.exit_code)
            return result

        if not r.done:
            terminal.error(f"\n{r.output}")
            return result

        if not hide_logs:
            terminal.header(r.output)

        return result

    def _sync_dir_to_workspace(
        self,
        *,
        dir: str,
        container_id: str,
        on_event: Optional[Callable] = None,
        hide_logs: bool = False,
    ):
        file_update_queue = Queue()
        event_handler = SyncEventHandler(file_update_queue, hide_logs=hide_logs)

        observer = Observer()
        observer.schedule(event_handler, dir, recursive=True)
        observer.start()

        terminal.header(f"Watching '{dir}' for changes...")
        while True:
            try:
                operation, path, new_path = file_update_queue.get_nowait()

                if on_event:
                    on_event(operation, path, new_path)

                req = SyncContainerWorkspaceRequest(
                    container_id=container_id,
                    path=os.path.relpath(path, start=dir),
                    is_dir=os.path.isdir(path),
                    op=operation,
                )

                if operation == SyncContainerWorkspaceOperation.WRITE:
                    if not req.is_dir:
                        with open(path, "rb") as f:
                            req.data = f.read()

                elif operation == SyncContainerWorkspaceOperation.DELETE:
                    pass

                elif operation == SyncContainerWorkspaceOperation.MOVED:
                    req.new_path = os.path.relpath(new_path, start=dir)

                yield ContainerStreamMessage(sync_container_workspace=req)

                file_update_queue.task_done()
            except Empty:
                time.sleep(DEFAULT_SYNC_INTERVAL)
            except BaseException as e:
                terminal.warn(str(e))
