import importlib
import json
import os
import signal
import sys
import threading
import time
from multiprocessing import Event, Process, set_start_method
from typing import Callable, List

import cloudpickle
from fastapi import FastAPI
from grpclib.client import Channel
from uvicorn import Config, Server

from beam.aio import run_sync
from beam.clients.gateway import EndTaskResponse, GatewayServiceStub, StartTaskResponse
from beam.clients.taskqueue import TaskQueuePopResponse, TaskQueueServiceStub
from beam.config import with_runner_context
from beam.exceptions import RunnerException
from beam.type import TaskExitCode, TaskStatus

USER_CODE_VOLUME = "/mnt/code"
TASK_PROCESS_WATCHDOG_INTERVAL = 0.01


# TODO: move to some sort of common module
def _load_handler() -> Callable:
    sys.path.insert(0, USER_CODE_VOLUME)

    handler = os.getenv("HANDLER")
    if not handler:
        raise RunnerException()

    try:
        module, func = handler.split(":")
        target_module = importlib.import_module(module)
        method = getattr(target_module, func)
        return method
    except BaseException:
        raise RunnerException()


class TaskQueueManager:
    def __init__(self) -> None:
        set_start_method("spawn", force=True)

        # Management server attributes
        self.app: FastAPI = FastAPI()
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

        @self.app.on_event("startup")
        def startup_event():
            for worker_index in range(self.task_worker_count):
                self._start_worker(worker_index)

        @self.app.on_event("shutdown")
        def shutdown_event():
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

    @with_runner_context
    def process_tasks(self, channel: Channel) -> None:
        self.worker_startup_event.set()
        handler = _load_handler()

        gateway_stub: GatewayServiceStub = GatewayServiceStub(channel)
        taskqueue_stub: TaskQueueServiceStub = TaskQueueServiceStub(channel)

        container_id = os.getenv("CONTAINER_ID")
        container_hostname = os.getenv("CONTAINER_HOSTNAME")
        stub_id = os.getenv("STUB_ID")
        if not container_id or not stub_id:
            raise RunnerException("Invalid runner environment")

        r: TaskQueuePopResponse = run_sync(
            taskqueue_stub.task_queue_pop(stub_id=stub_id, container_id=container_id)
        )
        print(r)

        task_msg = None
        if not r.ok:
            return

        task_msg = json.loads(r.task_msg)
        task_id = task_msg["id"]

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
            result = handler(*task_msg["args"], **task_msg["kwargs"])
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

        # try:
        #     self.task_client: TaskClient = TaskClient()
        # except BaseException:
        #     print("Error occurred during app initialization")
        #     sys.exit(BeamTaskExitCode.ErrorLoadingApp)

        # async def _task_loop():
        #     print("Ready for tasks.")

        #     executor = ThreadPoolExecutor(max_workers=1)

        #     async for task in self.task_client.task_generator(self.instance):
        #         task_status = TaskStatus.COMPLETE

        #         if not await task.before():
        #             print(f"Unable to start task: {task.task_id}")
        #             continue

        #         try:
        #             monitor_task = loop.create_task(self.task_client.monitor_task(task))
        #             task_result, err = await loop.run_in_executor(executor, task.run)
        #             if err:
        #                 task_status = TaskStatus.FAILED

        #             self.set_task_result(
        #                 task_id=task.task_id, task_result=task_result, task_status=task_status
        #             )

        #         except BaseException:
        #             print("Unhandled error occurred in task")
        #             task_status = TaskStatus.Error
        #         finally:
        #             if not await task.after(task_status=task_status):
        #                 print(f"Unable to end task: {task.task_id}")
        #             else:
        #                 task.close(msg=task_result, status=task_status)

        #             monitor_task.cancel()

        # loop.run_until_complete(_task_loop())
        # print("Worker exited.")


def main():
    tq = TaskQueueManager()
    config = Config(app=tq.app, host="0.0.0.0", port=int(os.getenv("BIND_PORT")), workers=1)

    s = Server(config)
    s.run()

    if tq.exit_code != 0 and tq.exit_code != TaskExitCode.SigTerm:
        sys.exit(tq.exit_code)


if __name__ == "__main__":
    main()
