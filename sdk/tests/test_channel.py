from unittest import TestCase
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from beta9.channel import Channel, GatewayHTTP


class TestChannelIdentity(TestCase):
    def test_cache_key_identifies_gateway_and_token(self):
        # Per-channel caches (image existence, synced objects) are shared by
        # channels to the same gateway and token, and by nothing else.
        channels = [
            Channel("localhost:1993", token="t1"),
            Channel("localhost:1993", token="t1"),
            Channel("localhost:1993", token="t2"),
            Channel("other:1993", token="t1"),
        ]
        try:
            a, b, c, d = (ch.cache_key for ch in channels)
            self.assertEqual(a, b)
            self.assertNotEqual(a, c)
            self.assertNotEqual(a, d)
        finally:
            for ch in channels:
                ch.close()


class TestGatewayHTTP(TestCase):
    def test_sequential_requests_reuse_connection_and_preserve_identity(self):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                requests.append((self.client_address, self.path, self.headers["Authorization"]))
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = GatewayHTTP(f"http://127.0.0.1:{server.server_port}", "workspace", "test-token")
        try:
            self.assertEqual(client.json("GET", "/{ws}/first"), {})
            self.assertEqual(client.json("GET", "/{ws}/second"), {})
            self.assertEqual(requests[0][0], requests[1][0])
            self.assertEqual([r[1] for r in requests], ["/workspace/first", "/workspace/second"])
            self.assertEqual([r[2] for r in requests], ["Bearer test-token"] * 2)
        finally:
            client.close()
            server.shutdown()
            server.server_close()
            thread.join()
