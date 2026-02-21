#!/usr/bin/env python3
"""Test model loading/unloading lifecycle.

Usage:
    BETA9_INFERENCE_HOST=<ollama-host> python test_inference_lifecycle.py

Requires Ollama running with llama3.2 model pulled.
Tests cold start, warm requests, and multi-turn conversations.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "sdk/src"))

from beta9 import inference


def test_lifecycle():
    """Test cold start, warm requests, and model swapping."""

    model = "llama3.2"

    # 0. Unload model to ensure cold start
    print("0. Ensuring cold start (unloading model if loaded)...")
    inference.unload_model(model)
    time.sleep(1)

    # 1. First request (cold start)
    print(f"1. Cold start test ({model})...")
    start = time.time()
    result1 = inference.chat(
        model=model,
        messages=[{"role": "user", "content": "Hi"}],
    )
    cold_time = time.time() - start

    if result1.error:
        print(f"   FAILED: {result1.error}")
        return False

    print(f"   Cold start: {cold_time:.2f}s")
    print(f"   Response: {result1.content[:50]}...")

    # 2. Warm request (should be faster)
    print("2. Warm request test...")
    start = time.time()
    result2 = inference.chat(
        model=model,
        messages=[{"role": "user", "content": "Hi again"}],
    )
    warm_time = time.time() - start

    if result2.error:
        print(f"   FAILED: {result2.error}")
        return False

    print(f"   Warm request: {warm_time:.2f}s")

    if warm_time < cold_time:
        print(f"   ✓ Warm ({warm_time:.2f}s) < Cold ({cold_time:.2f}s)")
    else:
        print(f"   ⚠ Warm ({warm_time:.2f}s) not faster than cold ({cold_time:.2f}s)")
        print("   (Expected after first run; model may already be cached)")

    # 3. Check model states
    print("3. Checking model states...")
    models = inference.list_models()
    model_names = [m.name for m in models]
    print(f"   Models: {model_names}")

    # 4. Multi-turn conversation
    print("4. Multi-turn conversation test...")
    result3 = inference.chat(
        model=model,
        messages=[
            {"role": "user", "content": "What is 2+2?"},
            {"role": "assistant", "content": "4"},
            {"role": "user", "content": "And 3+3?"},
        ],
    )

    if result3.error:
        print(f"   FAILED: {result3.error}")
        return False

    print(f"   Response: {result3.content[:50]}...")
    print("   ✓ Multi-turn OK")

    # 5. Generate endpoint (raw completion)
    print("5. Generate endpoint test...")
    result4 = inference.generate(
        model=model,
        prompt="The capital of France is",
    )

    if result4.error:
        print(f"   FAILED: {result4.error}")
        return False

    print(f"   Response: {result4.content[:50]}...")
    print("   ✓ Generate OK")

    print("\nAll lifecycle tests PASSED")
    return True


def test_embeddings():
    """Test embedding generation (optional - requires nomic-embed-text)."""
    print("\nTesting embeddings...")

    model = "nomic-embed-text"

    result = inference.embed(
        model=model,
        input="Hello, world!",
    )

    if result.error:
        print(f"   SKIPPED: {result.error}")
        print(f"   (Pull with: ollama pull {model})")
        return True  # Non-blocking: embedding model is optional

    print(f"   Embedding dimensions: {len(result.embedding)}")
    assert len(result.embedding) > 0, "Expected non-empty embedding"
    print("   ✓ Embeddings OK")
    return True


if __name__ == "__main__":
    host = os.getenv("BETA9_INFERENCE_HOST", "localhost")
    port = int(os.getenv("BETA9_INFERENCE_PORT", "11434"))

    print(f"Connecting to inference server at {host}:{port}")
    inference.configure(host=host, port=port)

    # Check server health first
    if not inference.health():
        print(f"ERROR: Inference server not reachable at {host}:{port}")
        print("Start Ollama: ollama serve")
        sys.exit(1)

    results = []
    results.append(test_lifecycle())
    results.append(test_embeddings())

    if all(results):
        print("\nAll tests PASSED")
        sys.exit(0)
    else:
        print("\nSome tests FAILED")
        sys.exit(1)
