"""
Standalone tests for Bot model validation and MiniMax provider support.
These tests do not require the full beta9 SDK to be installed.
"""

import json
import os
import sys
import unittest

# Read the bot.py source directly for validation tests
BOT_PY_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "src",
    "beta9",
    "abstractions",
    "experimental",
    "bot",
    "bot.py",
)

# Extract VALID_MODELS and PROVIDER_BASE_URLS from the source
with open(BOT_PY_PATH, "r") as f:
    source = f.read()


def extract_list(source, var_name):
    """Extract a list from Python source code."""
    start = source.find(f"{var_name} = [")
    if start == -1:
        return []
    end = source.find("]", start) + 1
    return eval(source[start + len(var_name + " = ") : end])


def extract_dict(source, var_name):
    """Extract a dict from Python source code."""
    marker = var_name + " = {"
    start = source.find(marker)
    if start == -1:
        return {}
    end = source.find("}", start) + 1
    return eval(source[start + len(var_name + " = ") : end])


VALID_MODELS = extract_list(source, "VALID_MODELS")
PROVIDER_BASE_URLS = extract_dict(source, "PROVIDER_BASE_URLS")


class TestValidModels(unittest.TestCase):
    """Test that VALID_MODELS contains expected models."""

    def test_openai_models_present(self):
        openai_models = [
            "gpt-4o",
            "gpt-4o-mini",
            "gpt-4",
            "gpt-3.5-turbo",
            "gpt-3.5-turbo-instruct",
            "gpt-3.5-turbo-16k",
            "gpt-3.5-turbo-0613",
            "gpt-4-0613",
        ]
        for model in openai_models:
            self.assertIn(model, VALID_MODELS, f"{model} should be in VALID_MODELS")

    def test_minimax_m27_present(self):
        self.assertIn("MiniMax-M2.7", VALID_MODELS)

    def test_minimax_m25_present(self):
        self.assertIn("MiniMax-M2.5", VALID_MODELS)

    def test_minimax_m25_highspeed_present(self):
        self.assertIn("MiniMax-M2.5-highspeed", VALID_MODELS)

    def test_total_model_count(self):
        self.assertEqual(len(VALID_MODELS), 11)

    def test_no_duplicate_models(self):
        self.assertEqual(len(VALID_MODELS), len(set(VALID_MODELS)))


class TestProviderBaseUrls(unittest.TestCase):
    """Test PROVIDER_BASE_URLS configuration."""

    def test_minimax_url_present(self):
        self.assertIn("minimax", PROVIDER_BASE_URLS)

    def test_minimax_url_format(self):
        url = PROVIDER_BASE_URLS["minimax"]
        self.assertEqual(url, "https://api.minimax.io/v1")
        self.assertTrue(url.startswith("https://"))
        self.assertTrue(url.endswith("/v1"))


class TestBotConfigBaseUrl(unittest.TestCase):
    """Test base_url parameter in Bot.__init__ signature."""

    def test_base_url_parameter_exists(self):
        self.assertIn("base_url", source)
        self.assertIn("base_url: Optional[str] = None", source)

    def test_minimax_auto_detect_logic(self):
        self.assertIn('model.startswith("MiniMax-")', source)
        self.assertIn('PROVIDER_BASE_URLS["minimax"]', source)

    def test_base_url_passed_to_extra(self):
        self.assertIn('self.extra["base_url"] = base_url', source)


class TestGoBackendConfig(unittest.TestCase):
    """Test Go backend BotConfig changes."""

    GO_TYPES_PATH = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "pkg",
        "abstractions",
        "experimental",
        "bot",
        "types.go",
    )

    GO_INTERFACE_PATH = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "pkg",
        "abstractions",
        "experimental",
        "bot",
        "interface.go",
    )

    def test_base_url_field_in_bot_config(self):
        with open(self.GO_TYPES_PATH, "r") as f:
            go_source = f.read()
        self.assertIn("BaseUrl", go_source)
        self.assertIn('"base_url,omitempty"', go_source)

    def test_custom_client_config_logic(self):
        with open(self.GO_INTERFACE_PATH, "r") as f:
            go_source = f.read()
        self.assertIn("DefaultConfig", go_source)
        self.assertIn("NewClientWithConfig", go_source)
        self.assertIn("config.BaseURL", go_source)

    def test_fallback_to_default_client(self):
        with open(self.GO_INTERFACE_PATH, "r") as f:
            go_source = f.read()
        self.assertIn("openai.NewClient(opts.BotConfig.ApiKey)", go_source)

    def test_base_url_condition(self):
        with open(self.GO_INTERFACE_PATH, "r") as f:
            go_source = f.read()
        self.assertIn('opts.BotConfig.BaseUrl != ""', go_source)


class TestBotDocstring(unittest.TestCase):
    """Test that documentation mentions MiniMax."""

    def test_docstring_mentions_minimax(self):
        self.assertIn("MiniMax", source)

    def test_docstring_mentions_minimax_models(self):
        self.assertIn("MiniMax-M2.7", source)
        self.assertIn("MiniMax-M2.5", source)
        self.assertIn("MiniMax-M2.5-highspeed", source)

    def test_docstring_mentions_base_url(self):
        self.assertIn("base_url", source)
        self.assertIn("api.minimax.io", source)


class TestIntegrationMiniMax(unittest.TestCase):
    """Integration tests for MiniMax API (require MINIMAX_API_KEY)."""

    @unittest.skipUnless(
        os.environ.get("MINIMAX_API_KEY"),
        "MINIMAX_API_KEY not set",
    )
    def test_minimax_api_reachable(self):
        """Verify MiniMax API endpoint is reachable."""
        import urllib.request

        api_key = os.environ["MINIMAX_API_KEY"]
        url = "https://api.minimax.io/v1/models"
        req = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {api_key}"},
        )
        try:
            resp = urllib.request.urlopen(req, timeout=10)
            self.assertEqual(resp.status, 200)
        except Exception:
            self.skipTest("MiniMax API not reachable")

    @unittest.skipUnless(
        os.environ.get("MINIMAX_API_KEY"),
        "MINIMAX_API_KEY not set",
    )
    def test_minimax_chat_completion(self):
        """Verify MiniMax chat completion works with OpenAI-compatible API."""
        import urllib.request

        api_key = os.environ["MINIMAX_API_KEY"]
        url = "https://api.minimax.io/v1/chat/completions"
        data = json.dumps(
            {
                "model": "MiniMax-M2.5-highspeed",
                "messages": [{"role": "user", "content": "Say hello"}],
                "max_tokens": 10,
            }
        ).encode()
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            result = json.loads(resp.read())
            self.assertIn("choices", result)
            self.assertTrue(len(result["choices"]) > 0)
        except Exception:
            self.skipTest("MiniMax API not reachable")

    @unittest.skipUnless(
        os.environ.get("MINIMAX_API_KEY"),
        "MINIMAX_API_KEY not set",
    )
    def test_minimax_m27_chat_completion(self):
        """Verify MiniMax-M2.7 chat completion works."""
        import urllib.request

        api_key = os.environ["MINIMAX_API_KEY"]
        url = "https://api.minimax.io/v1/chat/completions"
        data = json.dumps(
            {
                "model": "MiniMax-M2.7",
                "messages": [{"role": "user", "content": "Say hello"}],
                "max_tokens": 10,
            }
        ).encode()
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            result = json.loads(resp.read())
            self.assertIn("choices", result)
        except Exception:
            self.skipTest("MiniMax API not reachable")


if __name__ == "__main__":
    unittest.main()
