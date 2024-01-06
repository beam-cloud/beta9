import atexit
import json
import os
import signal
import sys
import threading
import time
from multiprocessing import Event, Process, set_start_method
from typing import List, Union

import cloudpickle
from grpclib.client import Channel

from beam.aio import run_sync
from beam.clients.gateway import EndTaskResponse, GatewayServiceStub, StartTaskResponse
from beam.clients.taskqueue import (
    TaskQueuePopResponse,
    TaskQueueServiceStub,
)
from beam.config import with_runner_context
from beam.exceptions import RunnerException
from beam.runner.common import load_handler
from beam.type import TaskExitCode, TaskStatus

TASK_PROCESS_WATCHDOG_INTERVAL = 0.01
TASK_POLLING_INTERVAL = 0.01


class TaskQueueManager:
    def __init__(self) -> None:
        set_start_method("spawn", force=True)

        # Management server attributes
        self.pid: int = os.getpid()
        self.exit_code: int = 0

        # Task worker attributes
        self.task_worker_count: int = 1  # TODO: swap out for concurrency value
        self.task_processes: List[Process] = []
        self.task_workers: List[TaskQueueWorker] = []
        self.task_worker_startup_events: List[Event] = [
            Event() for _ in range(self.task_worker_count)
        ]
        self.task_worker_watchdog_threads: List[threading.Thread] = []

    def run(self):
        for worker_index in range(self.task_worker_count):
            self._start_worker(worker_index)

        for task_process in self.task_processes:
            task_process.join()

    def shutdown(self):
        # Terminate all worker processes
        for task_process in self.task_processes:
            task_process.terminate()
            task_process.join()

        for task_process in self.task_processes:
            if task_process.is_alive():
                task_process.terminate()
                task_process.join(timeout=0)

            if task_process.exitcode != 0:
                self.exit_code = task_process.exitcode

    def _start_worker(self, worker_index: int):
        # Initialize task worker
        self.task_workers.append(
            TaskQueueWorker(
                parent_pid=self.pid,
                worker_startup_event=self.task_worker_startup_events[worker_index],
            )
        )

        # Spawn the task process
        self.task_processes.append(Process(target=self.task_workers[-1].process_tasks))
        self.task_processes[-1].start()

        # Initialize and start watchdog thread
        self.task_worker_watchdog_threads.append(
            threading.Thread(target=self._watchdog, args=(worker_index,), daemon=True)
        )
        self.task_worker_watchdog_threads[-1].start()

    def _watchdog(self, worker_index: int):
        self.task_worker_startup_events[worker_index].wait()

        while True:
            if not self.task_processes[worker_index].is_alive():
                exit_code = self.task_processes[worker_index].exitcode

                # Restart worker if the exit code indicates worker exit
                # was due a task timeout, task cancellation, or gateway disconnect
                if exit_code in [
                    TaskExitCode.Cancelled,
                    TaskExitCode.Timeout,
                    TaskExitCode.Disconnect,
                ]:
                    self.task_processes[worker_index] = Process(
                        target=self.task_workers[worker_index].process_tasks
                    )
                    self.task_processes[worker_index].start()

                    continue

                self.exit_code = exit_code
                if self.exit_code == TaskExitCode.SigKill:
                    print(
                        "Task worker ran out memory! Try deploying again with higher memory limits."
                    )

                os.kill(self.pid, signal.SIGTERM)
                break

            time.sleep(TASK_PROCESS_WATCHDOG_INTERVAL)


class TaskQueueWorker:
    def __init__(
        self,
        *,
        parent_pid: int,
        worker_startup_event: Event,
    ) -> None:
        self.parent_pid: int = parent_pid
        self.worker_startup_event: Event = worker_startup_event

    def _get_next_task(
        self, taskqueue_stub: TaskQueueServiceStub, stub_id: str, container_id: str
    ) -> Union[bytes, None]:
        r: TaskQueuePopResponse = run_sync(
            taskqueue_stub.task_queue_pop(stub_id=stub_id, container_id=container_id)
        )
        if not r.ok or not r.task_msg:
            return None

        return r.task_msg

    @with_runner_context
    def process_tasks(self, channel: Channel) -> None:
        self.worker_startup_event.set()
        handler = load_handler()

        gateway_stub: GatewayServiceStub = GatewayServiceStub(channel)
        taskqueue_stub: TaskQueueServiceStub = TaskQueueServiceStub(channel)

        container_id = os.getenv("CONTAINER_ID")
        container_hostname = os.getenv("CONTAINER_HOSTNAME")
        stub_id = os.getenv("STUB_ID")

        if not container_id or not stub_id:
            raise RunnerException("Invalid runner environment")

        while task := self._get_next_task(taskqueue_stub, stub_id, container_id):
            task = json.loads(task)
            task_id = task["id"]

            # Start the task
            start_time = time.time()
            start_task_response: StartTaskResponse = run_sync(
                gateway_stub.start_task(task_id=task_id, container_id=container_id)
            )
            if not start_task_response.ok:
                raise RunnerException("Unable to start task")

            # Invoke function
            task_status = TaskStatus.Complete
            try:
                result = handler(*task["args"], **task["kwargs"])
                result = cloudpickle.dumps(result)
            except BaseException as exc:
                result = cloudpickle.dumps(exc)
                task_status = TaskStatus.Error
            finally:
                pass

            task_duration = time.time() - start_time

            # End the task
            end_task_response: EndTaskResponse = run_sync(
                gateway_stub.end_task(
                    task_id=task_id,
                    task_duration=task_duration,
                    task_status=task_status,
                    container_id=container_id,
                    container_hostname=container_hostname,
                    scale_down_delay=0,
                )
            )

            if not end_task_response.ok:
                raise RunnerException("Unable to end task")


def main():
    tq = TaskQueueManager()
    atexit.register(tq.shutdown)

    tq.run()

    if tq.exit_code != 0 and tq.exit_code != TaskExitCode.SigTerm:
        sys.exit(tq.exit_code)


if __name__ == "__main__":
    main()
