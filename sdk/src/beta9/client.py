import json
from dataclasses import dataclass
from functools import lru_cache
from typing import Union

import requests


@dataclass
class Result:
    task_id: str


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
            raise Exception("Failed to get retrieve URL")

        result = self._post(url=url, path="", data=args)
        return Result(task_id=result["task_id"])

    def subscribe(self, *, id: str, args: dict = {}):
        pass

    def status(self, *, id: str):
        pass

    def upload_file(self, *, file_path: str = ""):
        pass
