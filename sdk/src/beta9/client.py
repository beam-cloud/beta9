import json
from dataclasses import dataclass
from functools import lru_cache
from typing import Union

import requests

from .exceptions import StubNotFoundError, TaskNotFoundError, WorkspaceNotFoundError
from .type import TaskStatus


@dataclass
class Result:
    task_id: str
    url: str

    def __init__(self, task_id: str, url: str, token: str, workspace_id: str):
        self.task_id = task_id
        self.url = url
        self.__token = token
        self.__workspace_id = workspace_id

    def get(self):
        headers = {
            "Authorization": f"Bearer {self.__token}",
            "Content-Type": "application/json",
        }
        response = requests.get(self.url, headers=headers)
        return response.json()

    def subscribe(self):
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

        except Exception as e:
            yield {"error": str(e)}


class Client:
    def __init__(
        self,
        token: str,
        gateway_host: str = "0.0.0.0",
        gateway_port: int = 1994,
        tls: bool = False,
    ) -> None:
        self.token: str = token
        self.gateway_host: str = gateway_host
        self.gateway_port: int = gateway_port
        self.tls: bool = tls
        self.base_url: str = self._get_base_url()

        self._load_workspace()

    def _load_workspace(self):
        response = self._make_request(self.base_url, "GET", "/api/v1/workspace/current")

        if response and "external_id" in response:
            self.workspace_id = response["external_id"]
        else:
            raise WorkspaceNotFoundError("Failed to load workspace")

    def _get_base_url(self):
        return f"{'https' if self.tls else 'http'}://{self.gateway_host}:{self.gateway_port}"

    def _make_request(self, url: str, method: str, path: str, data: dict = {}):
        url = f"{url}/{path}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        response = requests.request(method, url, headers=headers, data=json.dumps(data))
        return response.json()

    def _post(self, *, url: str, path: str, data: dict = {}):
        return self._make_request(url, "POST", path, data)

    @lru_cache(maxsize=128)
    def _get_stub_url(self, id: str) -> Union[str, None]:
        response = self._make_request(self.base_url, "GET", f"/api/v1/stub/{id}/url")
        if response and "url" in response:
            return response["url"]

        return None

    def submit(self, *, id: str, args: dict = {}) -> Union[Result, None]:
        url = self._get_stub_url(id)
        if not url:
            raise TaskNotFoundError(f"Failed to get retrieve URL for task {id}")

        result = self._post(url=url, path="", data=args)
        if "task_id" in result:
            return Result(
                task_id=result["task_id"],
                url=f"{self.base_url}/api/v1/task/{self.workspace_id}/{result['task_id']}",
                token=self.token,
                workspace_id=self.workspace_id,
            )

        return None

    def subscribe(self, *, id: str, args: dict = {}):
        """ """
        url = self._get_stub_url(id)
        if not url:
            raise StubNotFoundError(f"Failed to get retrieve URL for task {id}")

        response = self._post(url=url, path="", data=args)
        if "task_id" not in response:
            raise TaskNotFoundError(f"Failed to get task ID from response for task {id}")

        retrieve_url = f"{self.base_url}/api/v1/task/{self.workspace_id}/{response['task_id']}"
        result = Result(
            task_id=response["task_id"],
            url=retrieve_url,
            token=self.token,
            workspace_id=self.workspace_id,
        )
        return result.subscribe()

    def status(self, *, task_id: str):
        """ """
        result_url = f"{self.base_url}/api/v1/task/{self.workspace_id}/{task_id}"
        return Result(task_id=task_id, url=result_url, token=self.token)

    def upload_file(self, *, file_path: str = ""):
        """ """
        pass
