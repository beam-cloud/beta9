import asyncio
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from multiprocessing import Event, Manager, Process, set_start_method
from multiprocessing.managers import DictProxy, SyncManager
from typing import Any, List

from fastapi import FastAPI
from runner.instance import AppInstance, AppInstanceMode
from uvicorn import Config, Server

from beam.clients.gateway import TaskClient
from beam.task import BeamAppTaskStatus, BeamTaskExitCode


class TaskQueue:
    def __init__(self) -> None:
        set_start_method("spawn", force=True)

        # Management server attributes
        self.app: FastAPI = FastAPI()
        self.pid: int = os.getpid()
        self.exit_code: int = 0
        self.manager: SyncManager = Manager()

        # Task worker attributes
        self.task_worker_count: int = WORKERS
        self.task_results: DictProxy = self.manager.dict()
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
            BeamTaskWorker(
                parent_pid=self.pid,
                worker_startup_event=self.task_worker_startup_events[worker_index],
                task_results=self.task_results,
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
                # was due a task timeout, task cancellation, or workbus disconnect
                if exit_code in [
                    TaskExitCode.Canceled,
                    TaskExitCode.Timeout,
                    TaskExitCode.Disconnect,
                ]:
                    self.task_processes[worker_index] = Process(
                        target=self.task_workers[worker_index].process_tasks
                    )
                    self.task_processes[worker_index].start()

                    continue

                self.exit_code = exit_code
                if self.exit_code == BeamTaskExitCode.SigKill:
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
        task_results: dict,
    ) -> None:
        self.parent_pid: int = parent_pid
        self.task_results: dict = task_results
        self.worker_startup_event: Event = worker_startup_event

    def set_task_result(self, *, task_id: str, task_result: Any, task_status: str) -> None:
        status_code = HTTPStatus.OK

        if task_status == BeamAppTaskStatus.FAILED:
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR

        try:
            self.task_results[task_id] = self._create_response(
                body=task_result, status_code=status_code
            )
        except BrokenPipeError:
            print("Failed to communicate with task bus, killing worker.")
            os._exit(TaskExitCode.SigKill)

    # Process tasks in queue one by one
    def process_tasks(self) -> None:
        self.worker_startup_event.set()
        loop = asyncio.get_event_loop()

        try:
            self.task_client: TaskClient = TaskClient()
            self.instance: AppInstance = AppInstance.create(AppInstanceMode.Deployment)
        except BaseException:
            print("Error occurred during app initialization")
            sys.exit(BeamTaskExitCode.ErrorLoadingApp)

        async def _task_loop():
            print("Ready for tasks.")

            executor = ThreadPoolExecutor(max_workers=1)

            async for task in self.task_client.task_generator(self.instance):
                task_status = BeamAppTaskStatus.COMPLETE

                if not await task.before():
                    print(f"Unable to start task: {task.task_id}")
                    continue

                try:
                    monitor_task = loop.create_task(self.task_client.monitor_task(task))
                    task_result, err = await loop.run_in_executor(executor, task.run)
                    if err:
                        task_status = BeamAppTaskStatus.FAILED

                    self.set_task_result(
                        task_id=task.task_id, task_result=task_result, task_status=task_status
                    )

                except BaseException:
                    print("Unhandled error occurred in task")
                    task_status = BeamAppTaskStatus.FAILED
                finally:
                    if not await task.after(task_status=task_status):
                        print(f"Unable to end task: {task.task_id}")
                    else:
                        task.close(msg=task_result, status=task_status)

                    monitor_task.cancel()

        loop.run_until_complete(_task_loop())
        self.task_client.close()
        print("Worker exited.")


if __name__ == "__main__":
    config = Config(app=deployment.app, host="0.0.0.0", port=int(os.getenv("BIND_PORT")), workers=1)
    server = Server(config)
    server.run()

    if deployment.exit_code != 0 and deployment.exit_code != BeamTaskExitCode.SigTerm:
        sys.exit(deployment.exit_code)
