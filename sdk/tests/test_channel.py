from unittest import TestCase

from beta9.channel import Channel


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
