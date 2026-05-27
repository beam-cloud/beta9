import argparse
import unittest

from benchmarks.sandbox_parallel import (
    DEFAULT_SANDBOX_EXPECTED_OUTPUT,
    expected_exec_output,
    summarize_exec_verification,
    text_tail,
    verify_exec_output,
)


class SandboxParallelTests(unittest.TestCase):
    def test_expected_exec_output_formats_sample_fields(self):
        args = argparse.Namespace(
            sandbox_expected_output=DEFAULT_SANDBOX_EXPECTED_OUTPUT + "-{container_id}-{stub_id}-{warmup}"
        )
        sample = {
            "index": 7,
            "containerId": "sandbox-1",
            "stubId": "stub-1",
            "warmup": False,
        }

        self.assertEqual(
            expected_exec_output(args, sample),
            "beta9-sandbox-ready-7-sandbox-1-stub-1-false",
        )

    def test_verify_exec_output_requires_exit_zero_and_stdout_match(self):
        result = {"execExitCode": 0}

        verify_exec_output(result, "ready-token\n", "ready-token")

        self.assertTrue(result["execVerified"])
        self.assertTrue(result["execOutputMatched"])

    def test_verify_exec_output_records_stdout_mismatch(self):
        result = {"execExitCode": 0}

        verify_exec_output(result, "wrong\n", "ready-token")

        self.assertFalse(result["execVerified"])
        self.assertFalse(result["execOutputMatched"])
        self.assertIn("stdout mismatch", result["execVerificationError"])

    def test_text_tail_preserves_byte_count_and_truncation(self):
        text, truncated, byte_count = text_tail("abcdef", 3)

        self.assertEqual(text, "def")
        self.assertTrue(truncated)
        self.assertEqual(byte_count, 6)

    def test_summarize_exec_verification_counts_failures(self):
        summary = summarize_exec_verification(
            [
                {"warmup": True, "execVerified": False},
                {"warmup": False, "execVerified": True, "execExitCode": 0},
                {
                    "warmup": False,
                    "execVerified": False,
                    "execExitCode": 0,
                    "execOutputMatched": False,
                },
            ]
        )

        self.assertEqual(summary["count"], 2)
        self.assertEqual(summary["completed"], 2)
        self.assertEqual(summary["verified"], 1)
        self.assertEqual(summary["mismatched"], 1)
        self.assertEqual(summary["failed"], 1)


if __name__ == "__main__":
    unittest.main()
