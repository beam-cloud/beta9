import unittest
from unittest.mock import MagicMock, PropertyMock, patch

from beta9.abstractions.experimental.bot.bot import Bot


class TestBotProvider(unittest.TestCase):
    def _make_bot(self, **kwargs):
        params = dict(api_key="sk-orca-test")
        params.update(kwargs)
        with (
            patch(
                "beta9.abstractions.base.runner.RunnerAbstraction.__init__",
                return_value=None,
            ),
            patch(
                "beta9.abstractions.experimental.bot.bot.Bot.gateway_stub",
                new_callable=PropertyMock,
                return_value=MagicMock(),
            ),
            patch("beta9.abstractions.experimental.bot.bot.FileSyncer"),
        ):
            return Bot(**params)

    def test_default_provider_is_openai(self):
        bot = self._make_bot(model="gpt-4o")
        self.assertEqual(bot.extra["provider"], Bot.OPENAI_PROVIDER)

    def test_orcarouter_provider_accepts_namespaced_model(self):
        bot = self._make_bot(
            provider=Bot.ORCAROUTER_PROVIDER, model="openai/gpt-4o-mini"
        )
        self.assertEqual(bot.extra["provider"], Bot.ORCAROUTER_PROVIDER)
        self.assertEqual(bot.extra["model"], "openai/gpt-4o-mini")

    def test_orcarouter_provider_accepts_auto_router(self):
        bot = self._make_bot(provider=Bot.ORCAROUTER_PROVIDER, model="orcarouter/auto")
        self.assertEqual(bot.extra["provider"], Bot.ORCAROUTER_PROVIDER)
        self.assertEqual(bot.extra["model"], "orcarouter/auto")

    def test_invalid_provider_raises(self):
        with self.assertRaises(ValueError):
            self._make_bot(provider="unknown", model="gpt-4o")

    def test_openai_provider_rejects_unknown_model(self):
        with self.assertRaises(ValueError):
            self._make_bot(model="not-a-real-model")

    def test_missing_api_key_raises(self):
        with self.assertRaises(ValueError):
            self._make_bot(api_key="")


if __name__ == "__main__":
    unittest.main()
