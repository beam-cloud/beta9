import json
import os
import signal
import sys
import threading
import time
import traceback
from concurrent.futures import CancelledError, ThreadPoolExecutor
from multiprocessing import Event, Process, set_start_method
from multiprocessing.synchronize import Event as TEvent
from typing import Any, List, NamedTuple, Union

import grpc

from ..channel import Channel, with_runner_context
from ..clients.gateway import GatewayServiceStub
from ..clients.taskqueue import (
    TaskQueueCompleteRequest,
    TaskQueueCompleteResponse,
    TaskQueueMonitorRequest,
    TaskQueueMonitorResponse,
    TaskQueuePopRequest,
    TaskQueuePopResponse,
    TaskQueueServiceStub,
)
from ..exceptions import RunnerException
from ..logging import StdoutJsonInterceptor
from ..runner.common import (
    FunctionContext,
    FunctionHandler,
    config,
    execute_lifecycle_method,
    send_callback,
)
from ..type import LifeCycleMethod, TaskExitCode, TaskStatus

TASK_PROCESS_WATCHDOG_INTERVAL = 0.01
TASK_POLLING_INTERVAL = 0.01
TASK_MANAGER_INTERVAL = 0.1


class TaskQueueManager:
    def __init__(self) -> None:
        # Manager attributes
        self.pid: int = os.getpid()
        self.exit_code: int = 0
        self.shutdown_event = Event()

        self._setup_signal_handlers()
        set_start_method("spawn", force=True)

        # Task worker attributes
        self.task_worker_count: int = config.concurrency
        self.task_processes: List[Process] = []
        self.task_workers: List[TaskQueueWorker] = []
        self.task_worker_startup_events: List[TEvent] = [
            Event() for _ in range(self.task_worker_count)
        ]
        self.task_worker_watchdog_threads: List[threading.Thread] = []

    def _setup_signal_handlers(self):
        if os.getpid() == self.pid:
            signal.signal(signal.SIGTERM, self._init_shutdown)

    def _init_shutdown(self, signum=None, frame=None):
        self.shutdown_event.set()

    def run(self):
        for worker_index in range(self.task_worker_count):
            print(f"Starting task worker[{worker_index}]")
            self._start_worker(worker_index)

        while not self.shutdown_event.is_set():
            time.sleep(TASK_MANAGER_INTERVAL)

        self.shutdown()

    def shutdown(self):
        print("Spinning down taskqueue")

        # Terminate all worker processes
        for task_process in self.task_processes:
            task_process.terminate()
            task_process.join(timeout=5)

        for task_process in self.task_processes:
            if task_process.is_alive():
                print("Task process did not join within the timeout. Terminating...")
                task_process.terminate()
                task_process.join(timeout=0)

            if task_process.exitcode != 0:
                self.exit_code = task_process.exitcode

    def _start_worker(self, worker_index: int):
        # Initialize task worker
        self.task_workers.append(
            TaskQueueWorker(
                worker_index=worker_index,
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


class Task(NamedTuple):
    id: str = ""
    args: Any = ()
    kwargs: Any = ()


class TaskQueueWorker:
    def __init__(
        self,
        *,
        worker_index: int,
        parent_pid: int,
        worker_startup_event: TEvent,
    ) -> None:
        self.worker_index: int = worker_index
        self.parent_pid: int = parent_pid
        self.worker_startup_event: TEvent = worker_startup_event

    def _get_next_task(
        self, taskqueue_stub: TaskQueueServiceStub, stub_id: str, container_id: str
    ) -> Union[Task, None]:
        try:
            r: TaskQueuePopResponse = taskqueue_stub.task_queue_pop(
                TaskQueuePopRequest(stub_id=stub_id, container_id=container_id)
            )

            if not r.ok or not r.task_msg:
                return None

            task = json.loads(r.task_msg)
            return Task(
                id=task["task_id"],
                args=task["args"],
                kwargs=task["kwargs"],
            )
        except (grpc.RpcError, OSError):
            return None

    def _monitor_task(
        self,
        *,
        context: FunctionContext,
        stub_id: str,
        container_id: str,
        task: Task,
        taskqueue_stub: TaskQueueServiceStub,
        gateway_stub: GatewayServiceStub,
    ) -> None:
        initial_backoff = 5
        max_retries = 5
        backoff = initial_backoff
        retry = 0

        while retry <= max_retries:
            try:
                for response in taskqueue_stub.task_queue_monitor(
                    TaskQueueMonitorRequest(
                        task_id=task.id,
                        stub_id=stub_id,
                        container_id=container_id,
                    )
                ):
                    response: TaskQueueMonitorResponse
                    if response.cancelled:
                        print(f"Task cancelled: {task.id}")

                        send_callback(
                            gateway_stub=gateway_stub,
                            context=context,
                            payload={},
                            task_status=TaskStatus.Cancelled,
                        )
                        os._exit(TaskExitCode.Cancelled)

                    if response.complete:
                        return

                    if response.timed_out:
                        print(f"Task timed out: {task.id}")

                        send_callback(
                            gateway_stub=gateway_stub,
                            context=context,
                            payload={},
                            task_status=TaskStatus.Timeout,
                        )
                        os._exit(TaskExitCode.Timeout)

                    retry = 0
                    backoff = initial_backoff

                # If successful, it means the stream is finished.
                # Break out of the retry loop
                break

            except (
                grpc.RpcError,
                ConnectionRefusedError,
            ):
                if retry == max_retries:
                    print("Lost connection to task monitor, exiting")
                    os._exit(0)

                time.sleep(backoff)
                backoff *= 2
                retry += 1

            except CancelledError:
                return

            except BaseException:
                print("Unexpected error occurred in task monitor")
                os._exit(0)

    @with_runner_context
    def process_tasks(self, channel: Channel) -> None:
        self.worker_startup_event.set()
        taskqueue_stub = TaskQueueServiceStub(channel)
        gateway_stub = GatewayServiceStub(channel)

        # Load handler and execute on_start method
        handler = FunctionHandler()
        on_start_value = execute_lifecycle_method(name=LifeCycleMethod.OnStart)

        print(f"Worker[{self.worker_index}] ready")
        while True:
            task = self._get_next_task(taskqueue_stub, config.stub_id, config.container_id)
            if not task:
                time.sleep(TASK_POLLING_INTERVAL)
                continue

            with StdoutJsonInterceptor(task_id=task.id), ThreadPoolExecutor() as thread_pool:
                print(f"Running task <{task.id}>")

                context = FunctionContext.new(
                    config=config,
                    task_id=task.id,
                    on_start_value=on_start_value,
                )

                monitor_task = thread_pool.submit(
                    self._monitor_task,
                    context=context,
                    stub_id=config.stub_id,
                    container_id=config.container_id,
                    task=task,
                    taskqueue_stub=taskqueue_stub,
                    gateway_stub=gateway_stub,
                )

                start_time = time.time()
                task_status = TaskStatus.Complete
                result = None
                duration = None

                try:
                    args = task.args or []
                    kwargs = task.kwargs or {}

                    result = handler(context, *args, **kwargs)
                except BaseException:
                    print(traceback.format_exc())
                    task_status = TaskStatus.Error
                finally:
                    duration = time.time() - start_time
                    complete_task_response: TaskQueueCompleteResponse = (
                        taskqueue_stub.task_queue_complete(
                            TaskQueueCompleteRequest(
                                task_id=task.id,
                                stub_id=config.stub_id,
                                task_duration=duration,
                                task_status=task_status,
                                container_id=config.container_id,
                                container_hostname=config.container_hostname,
                                keep_warm_seconds=config.keep_warm_seconds,
                            )
                        )
                    )

                    if not complete_task_response.ok:
                        raise RunnerException("Unable to end task")

                    monitor_task.cancel()
                    print(f"Task completed <{task.id}>, took {duration}s")

                    send_callback(
                        gateway_stub=gateway_stub,
                        context=context,
                        payload=result or {},
                        task_status=task_status,
                    )  # Send callback to callback_url, if defined


if __name__ == "__main__":
    tq = TaskQueueManager()
    tq.run()

    if tq.exit_code != 0 and tq.exit_code != TaskExitCode.SigTerm:
        sys.exit(tq.exit_code)
