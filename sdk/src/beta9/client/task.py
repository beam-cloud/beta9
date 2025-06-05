import json
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Any, List, Optional

import requests

from ..exceptions import TaskNotFoundError
from ..type import TaskStatus


@dataclass
class Task:
    id: str
    url: str
    _status: Optional[TaskStatus] = None
    _result: Optional[Any] = None
    _outputs: Optional[Any] = field(default_factory=list)

    def __init__(self, id: str, url: str, token: str):
        self.id = id
        self.url = url
        self.__token = token
        self._get()

    def _get(self):
        headers = {
            "Authorization": f"Bearer {self.__token}",
            "Content-Type": "application/json",
        }
        response = requests.get(self.url, headers=headers)
        if response.status_code == HTTPStatus.OK:
            body = response.json()
            self._status = TaskStatus(body["status"])
            self._result = body["result"]
            self._outputs = body["outputs"]
        elif response.status_code == HTTPStatus.NOT_FOUND:
            raise TaskNotFoundError(self.id)
        else:
            response.raise_for_status()

    def status(self) -> TaskStatus:
        """Returns the status of the task"""
        self._get()
        return self._status

    def result(self, wait: bool = False) -> Any:
        """Returns the JSON output of the task"""
        if wait:
            for event in self.subscribe():
                status = TaskStatus(event.get("status", ""))
                if status.is_complete():
                    break

        self._get()
        return self._result

    def outputs(self) -> List:
        """Returns a list of the Output() objects saved during the duration of the task"""
        return self._outputs

    def is_complete(self) -> bool:
        """Returns True if the task is in a terminal state (COMPLETE, ERROR, TIMEOUT, CANCELLED)"""
        self._get()
        return self._status.is_complete()

    def subscribe(self):
        """Subscribe to a task and yield updates as task status changes. Returns an iterable of JSON objects."""
        headers = {
            "Authorization": f"Bearer {self.__token}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }

        try:
            with requests.get(f"{self.url}/subscribe", headers=headers, stream=True) as response:
                response.raise_for_status()
                buffer = ""

                for chunk in response.iter_content(chunk_size=1024, decode_unicode=True):
                    if not chunk:
                        continue

                    buffer += chunk
                    while "\n\n" in buffer:
                        event, buffer = buffer.split("\n\n", 1)
                        event_type = None
                        data = None

                        for line in event.split("\n"):
                            if line.startswith("event: "):
                                event_type = line[7:]
                            elif line.startswith("data: "):
                                data = line[6:]

                        if event_type == "status" and data:
                            try:
                                task_data = json.loads(data)
                                yield task_data

                                # Stop iteration if task is done running
                                status = task_data.get("status")
                                if TaskStatus(status).is_complete():
                                    return

                            except json.JSONDecodeError:
                                continue
        except GeneratorExit:
            raise
        except BaseException as e:
            yield {"error": str(e)}
