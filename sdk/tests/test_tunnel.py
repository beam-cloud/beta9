import io
import json
import queue
import struct

from beta9.tunnel import Tunnel


def test_tunnel_preserves_bytes_when_gateway_loses_input_and_output_acknowledgements(monkeypatch):
    monkeypatch.setattr("beta9.tunnel.time.sleep", lambda _: None)
    payload = bytes(range(256)) * 1024
    received = bytearray()
    connects = []
    output = b"echo:" + payload
    lost_input_ack = False
    lost_output_ack = False

    class Remote:
        def __init__(self, offset):
            self.messages = queue.Queue()
            self.messages.put(json.dumps({"type": "input", "offset": len(received)}))
            self.offset = offset
            self.eof = False

        def settimeout(self, _):
            pass

        def close(self):
            pass

        def recv(self):
            message = self.messages.get(timeout=5)
            if isinstance(message, Exception):
                raise message
            return message

        def send_binary(self, frame):
            nonlocal lost_input_ack
            offset = struct.unpack("!Q", frame[:8])[0]
            data = frame[8:]
            assert offset <= len(received) <= offset + len(data)
            received.extend(data[len(received) - offset :])
            if not lost_input_ack:
                lost_input_ack = True
                self.messages.put(ConnectionResetError("gateway stopped after committing stdin"))
                return
            self.messages.put(json.dumps({"type": "input", "offset": len(received)}))

        def send(self, message):
            nonlocal lost_output_ack
            control = json.loads(message)
            if control["type"] == "eof":
                self.eof = True
                self.messages.put(
                    json.dumps({"type": "input", "offset": len(received), "eof": True})
                )
                self.next_output()
            elif control["type"] == "ack":
                self.offset = control["offset"]
                if not lost_output_ack:
                    lost_output_ack = True
                    raise ConnectionResetError("gateway stopped before receiving output ack")
                self.next_output()

        def next_output(self):
            chunk = output[self.offset : self.offset + 65536]
            if chunk:
                self.messages.put(struct.pack("!Q", self.offset) + chunk)
            else:
                self.messages.put(json.dumps({"type": "eof", "offset": self.offset}))

    def connect(session, offset, create):
        connects.append((session, offset, create))
        remote = Remote(offset)
        if offset:
            remote.next_output()
        return remote

    target = io.BytesIO()
    Tunnel(connect, io.BytesIO(payload), target).run()
    assert bytes(received) == payload
    assert target.getvalue() == output
    assert len(connects) == 3
    assert len({session for session, _, _ in connects}) == 1
    assert connects[0][2] is True
    assert all(not create for _, _, create in connects[1:])


def test_legacy_gateway_still_drains_output_after_stdin_eof():
    payload = b"legacy stdin"
    input_data = bytearray()
    messages = queue.Queue()

    class Remote:
        resumable = False

        def send_binary(self, data):
            input_data.extend(data)

        def send(self, data):
            assert data == "EOF"
            messages.put(b"reply:" + bytes(input_data))
            messages.put("")

        def recv(self):
            return messages.get(timeout=5)

        def close(self):
            pass

    target = io.BytesIO()
    Tunnel(lambda *_: Remote(), io.BytesIO(payload), target).run()
    assert bytes(input_data) == payload
    assert target.getvalue() == b"reply:" + payload
