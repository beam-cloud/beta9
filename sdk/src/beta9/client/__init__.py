import json
from functools import lru_cache
from typing import Union

import requests

from ..exceptions import DeploymentNotFoundError, StubNotFoundError


def make_request(
    *, token: str, url: str, method: str, path: str, data: dict = {}, params: dict = None
) -> requests.Response:
    url = f"{url}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    return requests.request(
        method, url, headers=headers, data=json.dumps(data) if data else None, params=params
    )


def post(*, token: str, url: str, path: str, data: dict = {}):
    return make_request(token=token, url=url, method="POST", path=path, data=data)


def get(*, token: str, url: str, path: str, params: dict = None):
    return make_request(token=token, url=url, method="GET", path=path, params=params)


@lru_cache(maxsize=128)
def get_stub_url(*, token: str, url: str, id: str) -> Union[str, None]:
    response = make_request(token=token, url=url, method="GET", path=f"/api/v1/stub/{id}/url")
    body = response.json()

    if body and "url" in body:
        return body["url"]

    raise StubNotFoundError(id)


@lru_cache(maxsize=128)
def get_deployment_url(*, token: str, url: str, id: str) -> Union[str, None]:
    response = make_request(token=token, url=url, method="GET", path=f"/api/v1/deployment/{id}/url")

    body = response.json()
    if body and "url" in body:
        return body["url"]

    raise DeploymentNotFoundError(id)
