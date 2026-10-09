"""A resumable byte stream for SSH, SCP, rsync, and port forwarding."""

import json
import struct
import threading
import time
from uuid import uuid4

import websocket

from .recovery import RECOVERY_TIMEOUT


class Tunnel:
    def __init__(self, connect, source, target):
        self.connect = connect
        self.source = source
        self.target = target
        self.session = str(uuid4())
        self.offset = 0
        self.created = False
        self.condition = threading.Condition()
        self.remote = None
        self.stopped = False
        self.input_offset = 0
        self.input_eof = False
        self.error = None

    def _upload(self):
        offset = 0
        try:
            while True:
                data = (
                    self.source.read1(65536)
                    if hasattr(self.source, "read1")
                    else self.source.read(65536)
                )
                end = offset + len(data)
                sent = None
                while True:
                    with self.condition:
                        if self.stopped:
                            return
                        if self.input_offset >= end and (data or self.input_eof):
                            break
                        remote = self.remote
                        if remote is None or remote is sent:
                            self.condition.wait()
                            continue
                    try:
                        if data:
                            remote.send_binary(struct.pack("!Q", offset) + data)
                        else:
                            remote.send(json.dumps({"type": "eof", "offset": offset}))
                        sent = remote
                    except (OSError, websocket.WebSocketException):
                        remote.close()
                        sent = remote
                if not data:
                    return
                offset = end
        except Exception as error:
            with self.condition:
                self.error = error
                self.stopped = True
                if self.remote is not None:
                    self.remote.close()
                self.condition.notify_all()

    def run(self):
        threading.Thread(target=self._upload, daemon=True).start()
        recovery_started = None
        delay = 0.2
        try:
            while not self.stopped:
                remote = None
                try:
                    remote = self.connect(self.session, self.offset, not self.created)
                    remote.settimeout(30)
                    while not self.stopped:
                        message = remote.recv()
                        if not message:
                            raise ConnectionError("Tunnel disconnected")
                        if isinstance(message, bytes):
                            if len(message) < 8:
                                raise RuntimeError("Invalid tunnel frame")
                            offset = struct.unpack("!Q", message[:8])[0]
                            if offset != self.offset:
                                raise RuntimeError("Invalid tunnel output offset")
                            pending = memoryview(message)[8:]
                            try:
                                while pending:
                                    written = self.target.write(pending)
                                    if not written:
                                        raise RuntimeError("Local tunnel output closed")
                                    pending = pending[written:]
                                self.target.flush()
                            except OSError as error:
                                raise RuntimeError("Local tunnel output closed") from error
                            self.offset += len(message) - 8
                            remote.send(json.dumps({"type": "ack", "offset": self.offset}))
                        else:
                            control = json.loads(message)
                            if control["type"] == "eof":
                                if control["offset"] != self.offset:
                                    raise RuntimeError("Invalid tunnel EOF offset")
                                remote.send(json.dumps({"type": "close"}))
                                return
                            if control["type"] != "input":
                                raise RuntimeError("Invalid tunnel control")
                            with self.condition:
                                self.created = True
                                self.input_offset = control["offset"]
                                self.input_eof = control.get("eof", False)
                                self.remote = remote
                                self.condition.notify_all()
                        recovery_started = None
                        delay = 0.2
                except (OSError, websocket.WebSocketException) as error:
                    if isinstance(
                        error, websocket.WebSocketBadStatusException
                    ) and error.status_code in (400, 401, 403, 404, 410):
                        raise
                    if recovery_started is None:
                        recovery_started = time.monotonic()
                    remaining = RECOVERY_TIMEOUT - (time.monotonic() - recovery_started)
                    if remaining <= 0:
                        raise TimeoutError(
                            "Gateway did not reconnect within two minutes"
                        ) from error
                    time.sleep(min(delay, remaining))
                    delay = min(delay * 1.5, 2.0)
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
