import json
from dataclasses import dataclass

import requests

from ..type import TaskStatus


@dataclass
class Task:
    id: str
    url: str

    def __init__(self, id: str, url: str, token: str):
        self.id = id
        self.url = url
        self.__token = token

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

        except BaseException as e:
            yield {"error": str(e)}
