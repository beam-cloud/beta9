from typing import Union

from ..exceptions import StubNotFoundError, TaskNotFoundError
from . import get_stub_url, post
from .task import Task


class Deployment:
    def __init__(self, base_url: str, id: str, token: str, workspace_id: str):
        self.id = id
        self.token = token
        self.base_url = base_url
        self.workspace_id = workspace_id

    def submit(self, *, args: dict = {}) -> Union[Task, None]:
        url = get_stub_url(token=self.token, url=self.base_url, id=self.id)
        if not url:
            raise TaskNotFoundError(f"Failed to get retrieve URL for task {self.id}")

        result = post(token=self.token, url=url, path="", data=args)
        if "task_id" in result:
            return Task(
                id=result["task_id"],
                url=f"{self.base_url}/api/v1/task/{self.workspace_id}/{result['task_id']}",
                token=self.token,
            )

        return None

    def subscribe(self, *, args: dict = {}):
        url = get_stub_url(token=self.token, url=self.base_url, id=self.id)
        if not url:
            raise StubNotFoundError(f"Failed to get retrieve URL for task {id}")

        response = post(token=self.token, url=url, path="", data=args)
        if "task_id" not in response:
            raise TaskNotFoundError(f"Failed to get task ID from response for task {id}")

        retrieve_url = f"{self.base_url}/api/v1/task/{self.workspace_id}/{response['task_id']}"
        task = Task(
            id=response["task_id"],
            url=retrieve_url,
            token=self.token,
        )
        return task.subscribe()
