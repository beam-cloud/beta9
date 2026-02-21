#!/usr/bin/env python3
"""Basic inference job test.

Usage:
    BETA9_INFERENCE_HOST=<ollama-host> python test_job.py

Requires Ollama running with llama3.2 model pulled.
"""

import os
import sys

# Allow running from beta9 root dir
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "sdk/src"))

from beta9 import inference


def test_server_health():
    """Test inference server is reachable."""
    print("Testing server health...")
    healthy = inference.health()
    if not healthy:
        print("FAILED: Inference server not healthy")
        return False
    print("PASSED: Server healthy")
    return True


def test_list_models():
    """Test listing available models."""
    print("Testing list_models...")
    models = inference.list_models()
    print(f"Available models: {[m.name for m in models]}")
    print("PASSED")
    return True


def test_chat():
    """Test basic chat completion."""
    print("Testing chat completion...")

    result = inference.chat(
        model="llama3.2",
        messages=[{"role": "user", "content": "Say hello in 5 words or less"}],
    )

    if result.error:
        print(f"FAILED: {result.error}")
        return False

    print(f"Response: {result.content}")
    assert result.content, "Expected non-empty response"
    print("PASSED")
    return True


if __name__ == "__main__":
    host = os.getenv("BETA9_INFERENCE_HOST", "localhost")
    port = int(os.getenv("BETA9_INFERENCE_PORT", "11434"))

    print(f"Connecting to inference server at {host}:{port}")
    inference.configure(host=host, port=port)

    results = []
    results.append(test_server_health())
    results.append(test_list_models())
    results.append(test_chat())

    if all(results):
        print("\nAll tests PASSED")
        sys.exit(0)
    else:
        print("\nSome tests FAILED")
        sys.exit(1)
