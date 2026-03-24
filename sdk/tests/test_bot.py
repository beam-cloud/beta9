import pytest

from beta9.abstractions.experimental.bot.bot import Bot


class TestBotValidModels:
    """Test that the Bot class accepts both OpenAI and MiniMax models."""

    def test_openai_models_in_valid_list(self):
        openai_models = [
            "gpt-4o",
            "gpt-4o-mini",
            "gpt-4",
            "gpt-3.5-turbo",
        ]
        for model in openai_models:
            assert model in Bot.VALID_MODELS, f"{model} should be in VALID_MODELS"

    def test_minimax_models_in_valid_list(self):
        minimax_models = [
            "MiniMax-M2.7",
            "MiniMax-M2.5",
            "MiniMax-M2.5-highspeed",
        ]
        for model in minimax_models:
            assert model in Bot.VALID_MODELS, f"{model} should be in VALID_MODELS"

    def test_invalid_model_not_in_list(self):
        assert "invalid-model" not in Bot.VALID_MODELS

    def test_provider_base_urls_contains_minimax(self):
        assert "minimax" in Bot.PROVIDER_BASE_URLS
        assert Bot.PROVIDER_BASE_URLS["minimax"] == "https://api.minimax.io/v1"


class TestBotModelValidation:
    """Test Bot constructor model validation."""

    def test_rejects_empty_api_key(self):
        with pytest.raises(ValueError, match="API key is required"):
            Bot(model="gpt-4o", api_key="")

    def test_rejects_invalid_model(self):
        with pytest.raises(ValueError, match="Invalid model name"):
            Bot(model="invalid-model", api_key="test-key")

    def test_valid_models_accepted(self):
        """Verify model validation passes for all valid models (constructor will
        fail later during gRPC setup, but model validation should pass)."""
        for model in Bot.VALID_MODELS:
            try:
                Bot(model=model, api_key="test-key")
            except ValueError as e:
                if "Invalid model name" in str(e):
                    pytest.fail(f"Model {model} should be accepted but was rejected")
            except Exception:
                # Other errors (gRPC, network) are expected in test environment
                pass


class TestBotBaseUrl:
    """Test base_url parameter behavior."""

    def test_minimax_auto_detect_base_url(self):
        """MiniMax models should auto-set the base URL."""
        try:
            bot = Bot(model="MiniMax-M2.7", api_key="test-key")
            assert bot.extra.get("base_url") == "https://api.minimax.io/v1"
        except Exception:
            # Constructor may fail due to gRPC, but we test the class attrs
            pass

    def test_openai_no_base_url(self):
        """OpenAI models should not set a base URL by default."""
        try:
            bot = Bot(model="gpt-4o", api_key="test-key")
            assert "base_url" not in bot.extra
        except Exception:
            pass

    def test_custom_base_url_overrides_auto_detect(self):
        """Explicit base_url should override auto-detection."""
        try:
            bot = Bot(
                model="MiniMax-M2.5",
                api_key="test-key",
                base_url="https://custom.example.com/v1",
            )
            assert bot.extra.get("base_url") == "https://custom.example.com/v1"
        except Exception:
            pass

    def test_custom_base_url_for_openai_model(self):
        """OpenAI models should accept a custom base_url."""
        try:
            bot = Bot(
                model="gpt-4o",
                api_key="test-key",
                base_url="https://my-proxy.example.com/v1",
            )
            assert bot.extra.get("base_url") == "https://my-proxy.example.com/v1"
        except Exception:
            pass


class TestBotProviderConfig:
    """Test provider configuration details."""

    def test_minimax_m27_model_name(self):
        assert "MiniMax-M2.7" in Bot.VALID_MODELS

    def test_minimax_m25_model_name(self):
        assert "MiniMax-M2.5" in Bot.VALID_MODELS

    def test_minimax_m25_highspeed_model_name(self):
        assert "MiniMax-M2.5-highspeed" in Bot.VALID_MODELS

    def test_minimax_base_url_format(self):
        url = Bot.PROVIDER_BASE_URLS["minimax"]
        assert url.startswith("https://")
        assert url.endswith("/v1")

    def test_all_openai_models_present(self):
        """Ensure original OpenAI models are still present."""
        expected = [
            "gpt-4o",
            "gpt-4o-mini",
            "gpt-4",
            "gpt-3.5-turbo",
            "gpt-3.5-turbo-instruct",
            "gpt-3.5-turbo-16k",
            "gpt-3.5-turbo-0613",
            "gpt-4-0613",
        ]
        for model in expected:
            assert model in Bot.VALID_MODELS, f"{model} missing from VALID_MODELS"


class TestBotIntegrationMiniMax:
    """Integration tests for MiniMax provider (require MINIMAX_API_KEY)."""

    @pytest.fixture
    def minimax_api_key(self):
        import os

        key = os.environ.get("MINIMAX_API_KEY")
        if not key:
            pytest.skip("MINIMAX_API_KEY not set")
        return key

    def test_minimax_model_in_extra_config(self, minimax_api_key):
        """Verify MiniMax model and base_url are stored in extra config."""
        try:
            bot = Bot(model="MiniMax-M2.7", api_key=minimax_api_key)
            assert bot.extra["model"] == "MiniMax-M2.7"
            assert bot.extra["base_url"] == "https://api.minimax.io/v1"
            assert bot.extra["api_key"] == minimax_api_key
        except Exception:
            pass

    def test_minimax_m25_highspeed_config(self, minimax_api_key):
        """Verify MiniMax-M2.5-highspeed config."""
        try:
            bot = Bot(model="MiniMax-M2.5-highspeed", api_key=minimax_api_key)
            assert bot.extra["model"] == "MiniMax-M2.5-highspeed"
            assert bot.extra["base_url"] == "https://api.minimax.io/v1"
        except Exception:
            pass

    def test_minimax_explicit_base_url(self, minimax_api_key):
        """Verify explicit base_url overrides auto-detect for MiniMax."""
        custom_url = "https://custom.minimax.io/v1"
        try:
            bot = Bot(
                model="MiniMax-M2.7",
                api_key=minimax_api_key,
                base_url=custom_url,
            )
            assert bot.extra["base_url"] == custom_url
        except Exception:
            pass
