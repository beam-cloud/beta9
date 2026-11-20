from unittest import TestCase

from beta9.logging import StoredStdoutInterceptor, redact_bearer_tokens


class TestRedactBearerTokens(TestCase):
    def test_redacts_curl_style_header(self):
        line = "-H 'Authorization: Bearer sk_abc123-XYZ=' \\"
        self.assertEqual(
            redact_bearer_tokens(line),
            "-H 'Authorization: Bearer [REDACTED]' \\",
        )

    def test_redacts_websocat_style_header(self):
        line = "websocat 'wss://app.example.com' -H 'Authorization: Bearer abc123'"
        self.assertEqual(
            redact_bearer_tokens(line),
            "websocat 'wss://app.example.com' -H 'Authorization: Bearer [REDACTED]'",
        )

    def test_leaves_other_output_untouched(self):
        line = "=> Invocation details"
        self.assertEqual(redact_bearer_tokens(line), line)


class TestStoredStdoutInterceptor(TestCase):
    def test_captured_logs_redact_bearer_tokens(self):
        with StoredStdoutInterceptor(capture_logs=True) as interceptor:
            print("curl -X POST 'https://app.example.com' \\")
            print("-H 'Authorization: Bearer abc123' \\")

        joined = "".join(interceptor.logs)
        self.assertNotIn("abc123", joined)
        self.assertIn("Authorization: Bearer [REDACTED]", joined)
