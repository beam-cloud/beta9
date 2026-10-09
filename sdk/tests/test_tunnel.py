import io
import json
import queue
from uuid import UUID

from beta9.tunnel import Tunnel


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
            pass

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
