from typing import Any, Callable, Union

from ..exceptions import DeploymentNotFoundError, TaskNotFoundError
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

    def submit(self, *, input: dict = {}) -> Union[Task, Any]:
        """Submit a task to the deployment. Returns a Task object if the task runs asynchronously, otherwise blocks until the task is complete and returns the JSON response."""

        if not self.url:
            raise TaskNotFoundError(f"Failed to get retrieve URL for task {self.id}")

        response = post(token=self.token, url=self.url, path="", data=input)
        body = response.json()
        if "task_id" in body:
            return Task(
                id=body["task_id"],
                url=f"{self.base_url}/api/v1/task/{self.workspace_id}/{body['task_id']}",
                token=self.token,
            )

        return body

    def subscribe(self, *, input: dict = {}, event_handler: Callable = None) -> Any:
        """Submit a task to the deployment, and subscribe to the task. Yields updates as task status changes. Returns an iterable of JSON objects."""

        if not self.url:
            raise DeploymentNotFoundError(f"Failed to get retrieve URL for task {id}")

        response = post(token=self.token, url=self.url, path="", data=input)
        body = response.json()
        if "task_id" not in body and body is not None:
            return body

        task = Task(
            id=body["task_id"],
            url=f"{self.base_url}/api/v1/task/{self.workspace_id}/{body['task_id']}",
            token=self.token,
        )

        return task.subscribe(event_handler=event_handler)
