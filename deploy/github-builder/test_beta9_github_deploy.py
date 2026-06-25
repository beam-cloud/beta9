import os
import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk" / "src"))

import beta9_github_deploy as builder


class GithubBuilderEntrypointTest(unittest.TestCase):
    def test_validate_relative_path_rejects_escape(self):
        self.assertEqual(builder.validate_relative_path("services/api/Dockerfile"), "services/api/Dockerfile")

        with self.assertRaises(builder.BuilderError):
            builder.validate_relative_path("../Dockerfile")

        with self.assertRaises(builder.BuilderError):
            builder.validate_relative_path("/Dockerfile")

    def test_parse_env_vars_validates_keys(self):
        self.assertEqual(
            builder.parse_env_vars('{"API_URL":"https://example.com","DEBUG":true}'),
            {"API_URL": "https://example.com", "DEBUG": "True"},
        )

        with self.assertRaises(builder.BuilderError):
            builder.parse_env_vars('{"1BAD":"value"}')

    def test_parse_cpu_accepts_dashboard_numeric_values(self):
        with mock.patch.dict(os.environ, {"CPU": "4"}, clear=True):
            self.assertEqual(builder.parse_cpu(), 4)

        with mock.patch.dict(os.environ, {"CPU": "0.5"}, clear=True):
            self.assertEqual(builder.parse_cpu(), 0.5)

        with mock.patch.dict(os.environ, {"CPU": "4000m"}, clear=True):
            self.assertEqual(builder.parse_cpu(), "4000m")

    def test_clone_repo_uses_askpass_without_token_in_command(self):
        commands = []

        def fake_run(cmd, *, cwd=None, env=None):
            commands.append((cmd, env))

        with tempfile.TemporaryDirectory() as tmp:
            clone_dir = Path(tmp) / "repo"
            with mock.patch.object(builder, "run", side_effect=fake_run):
                builder.clone_repo("beam-cloud/example", "main", "secret-token", clone_dir)

        self.assertEqual(len(commands), 1)
        cmd, env = commands[0]
        self.assertNotIn("secret-token", " ".join(cmd))
        self.assertIn("https://github.com/beam-cloud/example.git", cmd)
        self.assertEqual(env["GIT_TERMINAL_PROMPT"], "0")
        self.assertTrue(env["GIT_ASKPASS"].endswith("git-askpass.sh"))

    def test_load_config_validates_deploy_contract(self):
        env = {
            "GIT_REPO_FULL_NAME": "beam-cloud/example",
            "GIT_REF": "main",
            "GIT_TOKEN": "token",
            "APP_NAME": "example",
            "INTERNAL_PORT": "8080",
            "CPU": "1",
            "MEMORY": "512",
            "MIN_REPLICAS": "1",
            "MAX_REPLICAS": "2",
            "KEEP_WARM_SECONDS": "30",
            "DOCKERFILE_PATH": "Dockerfile",
            "ENV_VARS_JSON": '{"API_URL":"https://example.com"}',
        }
        with mock.patch.dict(os.environ, env, clear=True):
            config = builder.load_config()

        self.assertEqual(config["repo_full_name"], "beam-cloud/example")
        self.assertEqual(config["port"], 8080)
        self.assertEqual(config["env_vars"], {"API_URL": "https://example.com"})

    def test_load_config_supports_model_mode(self):
        env = {
            "DEPLOY_MODE": "model",
            "MODEL_ID": "zai-org/GLM-4.5",
            "APP_NAME": "glm",
            "INTERNAL_PORT": "8000",
            "CPU": "4",
            "MEMORY": "32768",
            "GPU": "H100",
            "MIN_REPLICAS": "1",
            "MAX_REPLICAS": "2",
            "KEEP_WARM_SECONDS": "300",
            "SERVED_MODEL_NAME": "glm-4.5",
            "CONTEXT_LENGTH": "131072",
            "CONCURRENT_REQUESTS": "16",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            config = builder.load_config()

        self.assertEqual(config["mode"], "model")
        self.assertEqual(config["model_id"], "zai-org/GLM-4.5")
        self.assertEqual(config["served_model_name"], "glm-4.5")
        self.assertEqual(config["context_length"], 131072)
        self.assertEqual(config["cpu"], 4)
        self.assertEqual(config["base_image"], "vllm/vllm-openai:v0.10.2")
        self.assertEqual(config["concurrent_requests"], 16)

    def test_model_service_uses_python3_vllm_entrypoint(self):
        config = {
            "model_id": "Qwen/Qwen2.5-0.5B-Instruct",
            "engine": "vllm",
            "port": 8000,
            "served_model_name": "qwen",
            "context_length": 4096,
            "base_image": "vllm/vllm-openai:latest",
            "env_vars": {},
            "app_name": "qwen",
            "cpu": 4,
            "memory": "32768",
            "gpu": "RTX4090",
            "keep_warm_seconds": 300,
            "min_replicas": 1,
            "max_replicas": 1,
            "pool": "llm-pool",
            "tokenizer": "",
            "metrics_path": "/metrics",
            "slo_tier": "standard",
            "concurrent_requests": 16,
        }

        service = builder.build_model_service(config)

        self.assertEqual(service.entrypoint[:3], ["python3", "-m", "vllm.entrypoints.openai.api_server"])
        self.assertIn("--model", service.entrypoint)
        self.assertIn("--served-model-name", service.entrypoint)
        self.assertEqual(service.concurrent_requests, 16)

    def test_model_service_uses_any_gpu_for_pool_backed_custom_gpu(self):
        config = {
            "model_id": "Qwen/Qwen2.5-0.5B-Instruct",
            "engine": "vllm",
            "port": 8000,
            "served_model_name": "qwen",
            "context_length": 4096,
            "base_image": "vllm/vllm-openai:latest",
            "env_vars": {},
            "app_name": "qwen",
            "cpu": 4,
            "memory": "32768",
            "gpu": "L4",
            "keep_warm_seconds": 300,
            "min_replicas": 1,
            "max_replicas": 1,
            "pool": "llm-pool",
            "tokenizer": "",
            "metrics_path": "/metrics",
            "slo_tier": "standard",
            "concurrent_requests": 16,
        }

        service = builder.build_model_service(config)

        self.assertEqual(service.gpu, "any")

    def test_result_payload_maps_sdk_deploy_result(self):
        payload = builder.result_payload(
            {"app_name": "example"},
            {
                "deployment_id": "dep-123",
                "deployment_name": "example",
                "invoke_url": "https://example.test",
                "version": 3,
            },
        )

        self.assertEqual(payload["app_id"], "dep-123")
        self.assertEqual(payload["app_name"], "example")
        self.assertEqual(payload["endpoint_url"], "https://example.test")

    def test_main_calls_service_deploy_not_pod_create(self):
        class FakeService:
            deploy_calls = []

            def deploy(self, name=None):
                self.deploy_calls.append(name)
                return {
                    "deployment_id": "dep-123",
                    "deployment_name": "qwen",
                    "invoke_url": "https://qwen.example",
                    "version": 1,
                }, True

            def create(self, *args, **kwargs):
                raise AssertionError("builder must deploy services, not create one-off pods")

        fake_service = FakeService()

        with (
            mock.patch.object(
                builder,
                "load_config",
                return_value={
                    "mode": "model",
                    "model_id": "Qwen/Qwen2.5-0.5B-Instruct",
                    "app_name": "qwen",
                },
            ),
            mock.patch.object(builder, "build_model_service", return_value=fake_service),
            mock.patch("sys.stdout", new_callable=StringIO) as stdout,
        ):
            exit_code = builder.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(fake_service.deploy_calls, ["qwen"])
        self.assertIn(builder.RESULT_PREFIX, stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
