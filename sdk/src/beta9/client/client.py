from ..exceptions import WorkspaceNotFoundError
from . import make_request
from .deployment import Deployment
from .task import Task


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
        response = make_request(
            token=self.token,
            url=self.base_url,
            method="GET",
            path="/api/v1/workspace/current",
        )

        if response and "external_id" in response:
            self.workspace_id = response["external_id"]
        else:
            raise WorkspaceNotFoundError("Failed to load workspace")

    def _get_base_url(self):
        return f"{'https' if self.tls else 'http'}://{self.gateway_host}:{self.gateway_port}"

    def upload_file(self, *, file_path: str = ""):
        pass

    def get_task_by_id(self, *, id: str) -> Task:
        return Task(
            id=id, url=f"{self.base_url}/api/v1/task/{self.workspace_id}/{id}", token=self.token
        )

    def get_deployment_by_name(self, name: str) -> Deployment:
        return Deployment(
            base_url=self.base_url,
            id=name,
            token=self.token,
            workspace_id=self.workspace_id,
        )

    def get_deployment_by_id(self, id: str) -> Deployment:
        return Deployment(
            base_url=self.base_url,
            id=id,
            token=self.token,
            workspace_id=self.workspace_id,
        )
