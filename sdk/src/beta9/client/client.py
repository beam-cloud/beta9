import http
import os
import uuid
from pathlib import Path

import requests

from ..config import get_config_context, get_settings
from ..env import is_remote
from ..exceptions import VolumeUploadError, WorkspaceNotFoundError
from . import get, make_request
from .deployment import Deployment
from .task import Task

VOLUME_UPLOAD_PATH = "uploads"


class Client:
    def __init__(
        self,
        token: str = "",
        gateway_host: str = "",
        gateway_port: int = 0,
        tls: bool = False,
    ) -> None:
        self.token: str = token
        self.gateway_host: str = gateway_host
        self.gateway_port: int = gateway_port
        self.tls: bool = tls

        if not self.token:
            self.token = os.environ.get("BETA9_TOKEN", "")

        if is_remote():
            self.gateway_host = os.environ.get("BETA9_GATEWAY_HOST_HTTP", "beta9-gateway")
            self.gateway_port = int(os.environ.get("BETA9_GATEWAY_PORT_HTTP", "1994"))

        else:
            settings = get_settings()
            config_context = get_config_context()

            if not self.token:
                self.token = config_context.token

            if not gateway_host:
                self.gateway_host = settings.api_host

            if not gateway_port:
                self.gateway_port = settings.api_port

        if not self.tls and self.gateway_port == 443:
            self.tls = True

        self.base_url: str = self._get_base_url()
        self._load_workspace()

    def _load_workspace(self):
        response = make_request(
            token=self.token,
            url=self.base_url,
            method="GET",
            path="/api/v1/workspace/current",
        )

        body = response.json()
        if body and "external_id" in body:
            self.workspace_id = body["external_id"]
        else:
            raise WorkspaceNotFoundError("Failed to load workspace")

    def _get_base_url(self):
        if self.gateway_host.startswith(("http://", "https://")):
            return f"{self.gateway_host}:{self.gateway_port}"
        return f"{'https' if self.tls else 'http'}://{self.gateway_host}:{self.gateway_port}"

    def upload_file(self, local_path: str = "") -> str:
        """
        Upload a file to to be used as an input to some function or deployment.

        Args:
            local_path (str): The path to the file to upload.

        Returns:
            str: The path to the uploaded file.

        Raises:
            VolumeUploadError: If the file upload fails.
        """

        path = Path(local_path)
        filename = f"{path.stem}_{uuid.uuid4()}{path.suffix}"
        volume_path = str(path.parent / filename) if path.parent != Path(".") else filename

        try:
            response = get(
                token=self.token,
                url=self.base_url,
                path=f"/volume/{self.workspace_id}/generate-upload-url/{VOLUME_UPLOAD_PATH}/{volume_path}",
            )
            response.raise_for_status()
        except BaseException as e:
            raise VolumeUploadError(f"Failed to get upload URL: {e}")

        if response.status_code == http.HTTPStatus.OK:
            presigned_url = response.json()

            with open(local_path, "rb") as file:
                r = requests.put(presigned_url, data=file)
                if r.status_code != http.HTTPStatus.OK:
                    raise VolumeUploadError(f"Failed to upload file: {r.text}")

        response = get(
            token=self.token,
            url=self.base_url,
            path=f"/volume/{self.workspace_id}/generate-download-url/{VOLUME_UPLOAD_PATH}/{volume_path}",
        )
        response.raise_for_status()
        return response.json()

    def get_task_by_id(self, id: str) -> Task:
        """
        Retrieve a task by task ID.

        Args:
            id (str): The ID of the task to retrieve.

        Returns:
            Task: The task object.
        """
        return Task(
            id=id,
            url=f"{self.base_url}/api/v1/task/{self.workspace_id}/{id}",
            token=self.token,
        )

    def get_deployment_by_id(self, id: str) -> Deployment:
        """
        Retrieve a deployment using its deployment ID.

        Args:
            id (str): The ID of the deployment to retrieve.

        Returns:
            Deployment: The deployment object.
        """
        return Deployment(
            base_url=self.base_url,
            token=self.token,
            workspace_id=self.workspace_id,
            deployment_id=id,
        )

    def get_deployment_by_stub_id(self, stub_id: str) -> Deployment:
        """
        Retrieve a deployment using the associated stub ID.

        Args:
            stub_id (str): The ID of the stub to retrieve.

        Returns:
            Deployment: The deployment object.
        """
        return Deployment(
            base_url=self.base_url,
            token=self.token,
            workspace_id=self.workspace_id,
            stub_id=stub_id,
        )
