from typing import Union

from ..exceptions import StubNotFoundError, TaskNotFoundError
from . import get_deployment_url, get_stub_url, post
from .task import Task


class Deployment:
    def __init__(
        self, *, base_url: str, deployment_id: str, stub_id: str, token: str, workspace_id: str
    ):
        self.id = deployment_id
        self.stub_id = stub_id
        self.token = token
        self.base_url = base_url
        self.workspace_id = workspace_id

        if self.stub_id:
            self.url = get_stub_url(token=self.token, url=self.base_url, id=self.stub_id)
        else:
            self.url = get_deployment_url(token=self.token, url=self.base_url, id=self.id)

    def submit(self, *, args: dict = {}) -> Union[Task, None]:
        if not self.url:
            raise TaskNotFoundError(f"Failed to get retrieve URL for task {self.id}")

        result = post(token=self.token, url=self.url, path="", data=args)
        if "task_id" in result:
            return Task(
                id=result["task_id"],
                url=f"{self.base_url}/api/v1/task/{self.workspace_id}/{result['task_id']}",
                token=self.token,
            )

        print(result)
        return None

    def subscribe(self, *, args: dict = {}):
        if not self.url:
            raise StubNotFoundError(f"Failed to get retrieve URL for task {id}")

        response = post(token=self.token, url=self.url, path="", data=args)
        if "task_id" not in response:
            raise TaskNotFoundError(f"Failed to get task ID from response for task {id}")

        task = Task(
            id=response["task_id"],
            url=f"{self.base_url}/api/v1/task/{self.workspace_id}/{response['task_id']}",
            token=self.token,
        )

        return task.subscribe()
