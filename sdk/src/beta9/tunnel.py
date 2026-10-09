"""SSH byte streams use the worker's shared request replay journal."""

import json
from contextlib import suppress
import threading
from uuid import UUID, uuid4

import websocket

from .channel import _RecoveryWindow


class Tunnel:
    def __init__(self, connect, source, target):
        self.connect = connect
        self.source = source
        self.target = target
        self.session = str(uuid4())
        self.condition = threading.Condition()
        self.remote = None
        self.stopped = False
        self.input_ack = None
        self.error = None

    def _upload(self):
        ack = UUID(int=0)
        try:
            while True:
                data = (
                    self.source.read1(65536)
                    if hasattr(self.source, "read1")
                    else self.source.read(65536)
                )
                request = uuid4()
                sent = None
                with self.condition:
                    while self.input_ack != str(request):
                        if self.stopped:
                            return
                        remote = self.remote
                        if remote is None or remote is sent:
                            self.condition.wait()
                            continue
                        try:
                            if data:
                                remote.send_binary(request.bytes + ack.bytes + data)
                            else:
                                remote.send(
                                    json.dumps({"type": "eof", "id": str(request), "ack": str(ack)})
                                )
                        except (OSError, websocket.WebSocketException):
                            remote.close()
                        sent = remote
                        self.condition.wait()
                if not data:
                    return
                ack = request
        except Exception as error:
            with self.condition:
                self.error = error
                self.stopped = True
                if self.remote is not None:
                    self.remote.close()
                self.condition.notify_all()

    def run(self):
        request, ack = uuid4(), UUID(int=0)
        recovery = _RecoveryWindow(start=False)
        threading.Thread(target=self._upload, daemon=True).start()
        try:
            while not self.stopped:
                remote = None
                try:
                    remote = self.connect(self.session)
                    remote.settimeout(30)
                    with self.condition:
                        self.remote = remote
                        self.condition.notify_all()
                    remote.send(json.dumps({"type": "read", "id": str(request), "ack": str(ack)}))
                    while not self.stopped:
                        message = remote.recv()
                        if not message:
                            raise ConnectionError("Tunnel disconnected")
                        if isinstance(message, bytes):
                            if len(message) < 16 or message[:16] != request.bytes:
                                raise RuntimeError("Invalid tunnel response")
                            pending = memoryview(message)[16:]
                            try:
                                while pending:
                                    written = self.target.write(pending)
                                    if not written:
                                        raise RuntimeError("Local tunnel output closed")
                                    pending = pending[written:]
                                self.target.flush()
                            except OSError as error:
                                raise RuntimeError("Local tunnel output closed") from error
                        else:
                            control = json.loads(message)
                            if control["type"] == "input":
                                with self.condition:
                                    self.input_ack = control["id"]
                                    self.condition.notify_all()
                                continue
                            if control["id"] != str(request):
                                raise RuntimeError("Invalid tunnel response")
                            if control["type"] == "eof":
                                with suppress(OSError, websocket.WebSocketException):
                                    remote.send(json.dumps({"type": "close", "id": str(uuid4())}))
                                return
                            if control["type"] != "output":
                                raise RuntimeError("Invalid tunnel control")
                        # Advancing the request before sending its ack prevents replaying output.
                        ack, request = request, uuid4()
                        remote.send(
                            json.dumps({"type": "read", "id": str(request), "ack": str(ack)})
                        )
                        recovery.reset()
                except (OSError, websocket.WebSocketException) as error:
                    if isinstance(
                        error, websocket.WebSocketBadStatusException
                    ) and error.status_code in (400, 401, 403, 404, 410, 501):
                        raise
                    if not recovery.wait():
                        raise TimeoutError(
                            "Gateway did not reconnect within two minutes"
                        ) from error
                finally:
                    with self.condition:
                        self.remote = None
                        self.condition.notify_all()
                    if remote is not None:
                        remote.close()
            if self.error:
                raise self.error
        finally:
            with self.condition:
                self.stopped = True
                self.condition.notify_all()


def bridge_tunnel(connect, source, target):
    Tunnel(connect, source, target).run()
