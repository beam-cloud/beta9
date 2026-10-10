import io
import json
import queue
from uuid import UUID

import pytest

from beta9.tunnel import Tunnel


def test_successful_reattachment_resets_recovery_before_the_first_reply(monkeypatch):
    clock = [0.0]
    sessions = []
    monkeypatch.setattr("beta9.channel.time.monotonic", lambda: clock[0])
    monkeypatch.setattr("beta9.channel.time.sleep", lambda _: None)

    class Remote:
        def settimeout(self, _):
            pass

        def close(self):
            pass

        def send(self, message):
            control = json.loads(message)
            if control["type"] == "read":
                self.read_id = control["id"]

        def recv(self):
            if len(sessions) < 5:
                raise ConnectionResetError("gateway cycled before its first reply")
            return json.dumps({"type": "eof", "id": self.read_id})

    def connect(session):
        # Each outage fits the recovery budget; their total does not.
        clock[0] += 50
        sessions.append(session)
        return Remote()

    Tunnel(connect, io.BytesIO(), io.BytesIO()).run()
    assert len(sessions) == 5
    assert len(set(sessions)) == 1


def test_failed_reattachments_still_exhaust_recovery(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr("beta9.channel.time.monotonic", lambda: clock[0])
    monkeypatch.setattr("beta9.channel.time.sleep", lambda _: None)

    def connect(_):
        clock[0] += 50
        raise ConnectionRefusedError("gateway unavailable")

    with pytest.raises(TimeoutError, match="Gateway did not reconnect"):
        Tunnel(connect, io.BytesIO(), io.BytesIO()).run()
    assert clock[0] == 200


def test_tunnel_reuses_requests_when_gateway_loses_replies_and_acknowledgements(monkeypatch):
    monkeypatch.setattr("beta9.channel.time.sleep", lambda _: None)
    payload = bytes(range(256)) * 1024
    received = bytearray()
    output = b"echo:" + payload
    responses = {}
    connects = []
    output_offset = 0
    input_eof = False
    lost_input_ack = lost_output_reply = lost_output_ack = False
    output_delivered = False

    class Remote:
        def __init__(self):
            self.messages = queue.Queue()
            self.read = None

        def settimeout(self, _):
            pass

        def close(self):
            self.messages.put(ConnectionResetError())

        def recv(self):
            message = self.messages.get(timeout=5)
            if isinstance(message, Exception):
                raise message
            return message

        def send_binary(self, frame):
            nonlocal lost_input_ack
            request = str(UUID(bytes=frame[:16]))
            if request not in responses:
                received.extend(frame[32:])
                responses[request] = json.dumps({"type": "input", "id": request})
            if not lost_input_ack:
                lost_input_ack = True
                self.messages.put(ConnectionResetError("lost stdin reply"))
            else:
                self.messages.put(responses[request])

        def send(self, message):
            nonlocal input_eof, lost_output_ack
            control = json.loads(message)
            request = control["id"]
            if control["type"] == "eof":
                input_eof = True
                responses[request] = json.dumps({"type": "input", "id": request})
                self.messages.put(responses[request])
                if self.read:
                    self.reply(self.read)
            elif control["type"] == "read":
                if output_delivered and not lost_output_ack:
                    lost_output_ack = True
                    raise ConnectionResetError("lost stdout ack before next read")
                self.read = request
                if input_eof:
                    self.reply(request)

        def reply(self, request):
            nonlocal output_offset, lost_output_reply, output_delivered
            if request not in responses:
                chunk = output[output_offset : output_offset + 65536]
                output_offset += len(chunk)
                responses[request] = (
                    UUID(request).bytes + chunk
                    if chunk
                    else json.dumps({"type": "eof", "id": request})
                )
            if not lost_output_reply:
                lost_output_reply = True
                self.messages.put(ConnectionResetError("lost stdout reply after reading"))
            else:
                self.messages.put(responses[request])
                output_delivered = True

    def connect(session):
        connects.append(session)
        return Remote()

    class PartialWriter(io.BytesIO):
        def write(self, data):
            return super().write(data[:13])

    target = PartialWriter()
    Tunnel(connect, io.BytesIO(payload), target).run()
    assert bytes(received) == payload
    assert target.getvalue() == output
    assert len(connects) == 4
    assert len(set(connects)) == 1


def test_tunnel_processes_input_ack_while_local_output_is_blocked():
    import threading

    writable = threading.Event()

    class Remote:
        def __init__(self):
            self.messages = queue.Queue()
            self.read = None
            self.input = None
            self.responded = False
            self.writes = 0

        def settimeout(self, _):
            pass

        def close(self):
            self.messages.put(ConnectionResetError())

        def recv(self):
            return self.messages.get(timeout=5)

        def reply(self):
            if self.read and self.input and not self.responded:
                # Output arrives before stdin's ack, just as two RPCs can complete.
                self.messages.put(UUID(self.read).bytes + b"output")
                self.messages.put(json.dumps({"type": "input", "id": self.input}))
                self.responded = True

        def send_binary(self, frame):
            self.writes += 1
            self.input = str(UUID(bytes=frame[:16]))
            if self.writes == 1:
                self.reply()
            else:
                writable.set()
                self.messages.put(json.dumps({"type": "input", "id": self.input}))

        def send(self, message):
            control = json.loads(message)
            if control["type"] == "read":
                self.read = control["id"]
                if self.responded:
                    self.messages.put(json.dumps({"type": "eof", "id": self.read}))
                else:
                    self.reply()

    class Target(io.BytesIO):
        def write(self, data):
            assert writable.wait(1), "output blocked delivery of stdin's ack"
            return super().write(data)

    remote = Remote()
    target = Target()
    Tunnel(lambda _: remote, io.BytesIO(b"x" * 131072), target).run()
    assert remote.writes == 2
    assert target.getvalue() == b"output"
