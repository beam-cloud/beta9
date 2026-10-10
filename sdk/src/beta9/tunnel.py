"""SSH byte streams use the worker's shared request replay journal."""

import json
from contextlib import suppress
from queue import Full, Queue
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
        self.output = Queue(maxsize=1)
        self.read_id, self.read_ack = uuid4(), UUID(int=0)
        self.downloading = False
        self.sent_read = None

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
            self._fail(error)

    def _fail(self, error):
        with self.condition:
            self.error = error
            self.stopped = True
            if self.remote is not None:
                self.remote.close()
            self.condition.notify_all()

    def _send_read(self):
        with self.condition:
            remote = self.remote
            if self.stopped or self.downloading or remote is None:
                return
            if self.sent_read == (remote, self.read_id):
                return
            self.sent_read = (remote, self.read_id)
            try:
                remote.send(
                    json.dumps({"type": "read", "id": str(self.read_id), "ack": str(self.read_ack)})
                )
            except (OSError, websocket.WebSocketException):
                remote.close()

    def _finish_read(self):
        with self.condition:
            self.read_ack, self.read_id = self.read_id, uuid4()
            self.downloading = False
        self._send_read()

    def _download(self):
        try:
            while True:
                data = self.output.get()
                if data is None or self.stopped:
                    return
                pending = memoryview(data)
                while pending:
                    written = self.target.write(pending)
                    if not written:
                        raise RuntimeError("Local tunnel output closed")
                    pending = pending[written:]
                self.target.flush()
                self._finish_read()
        except Exception as error:
            self._fail(error)

    def run(self):
        recovery = _RecoveryWindow(start=False)
        threading.Thread(target=self._upload, daemon=True).start()
        threading.Thread(target=self._download, daemon=True).start()
        try:
            while not self.stopped:
                remote = None
                try:
                    remote = self.connect(self.session)
                    # The gateway upgrades only after reattaching the worker session.
                    # Keep the backoff until a reply arrives, but give each outage
                    # its own recovery window after successful reattachment.
                    recovery.reset(keep_backoff=True)
                    remote.settimeout(30)
                    with self.condition:
                        self.remote = remote
                        self.condition.notify_all()
                    self._send_read()
                    while not self.stopped:
                        message = remote.recv()
                        if not message:
                            raise ConnectionError("Tunnel disconnected")
                        recovery.reset()
                        if isinstance(message, bytes):
                            with self.condition:
                                if (
                                    len(message) < 16
                                    or message[:16] != self.read_id.bytes
                                    or self.downloading
                                ):
                                    raise RuntimeError("Invalid tunnel response")
                                self.downloading = True
                            # Delivering output must not block receipt of stdin acknowledgements.
                            self.output.put(message[16:])
                        else:
                            control = json.loads(message)
                            if control["type"] == "input":
                                with self.condition:
                                    self.input_ack = control["id"]
                                    self.condition.notify_all()
                                continue
                            if control["id"] != str(self.read_id):
                                raise RuntimeError("Invalid tunnel response")
                            if control["type"] == "eof":
                                with suppress(OSError, websocket.WebSocketException):
                                    remote.send(json.dumps({"type": "close", "id": str(uuid4())}))
                                return
                            if control["type"] != "output":
                                raise RuntimeError("Invalid tunnel control")
                            self._finish_read()
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
                        self.sent_read = None
                        self.condition.notify_all()
                    if remote is not None:
                        remote.close()
            if self.error:
                raise self.error
        finally:
            with self.condition:
                self.stopped = True
                self.condition.notify_all()
            with suppress(Full):
                self.output.put_nowait(None)


def bridge_tunnel(connect, source, target):
    Tunnel(connect, source, target).run()
