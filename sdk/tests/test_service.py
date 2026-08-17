import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from beta9 import (
    DatabaseServingConfig,
    DurableDisk,
    Image,
    LLMConfig,
    LLMTokenPressureAutoscaler,
    Pod,
    Service,
    ServingConfig,
)
from beta9.abstractions.service import (
    DEFAULT_SERVICE_KEEP_WARM_SECONDS,
    command_from_dockerfile,
    ports_from_dockerfile,
    service_image_implies_default_port,
)
from beta9.cli.deployment import (
    _generate_service_module,
    _merge_port_options,
    _service_image_option,
)
from beta9.cli.extraclick import (
    DockerfileParser,
    handle_config_override,
    image_from_dockerfile_option,
)
from beta9.cli.llm import apply_llm_metadata as _apply_llm_metadata


class TestService(TestCase):
    def test_command_string_maps_to_shell_entrypoint(self):
        service = Service(command="npm run start", port=3000)

        self.assertEqual(service.entrypoint, ["sh", "-lc", "npm run start"])
        self.assertEqual(service.ports, [3000])
        self.assertIn("PORT=3000", service.env)

    def test_command_string_is_quoted_when_wrapped_for_user_code(self):
        service = Service(command="python server.py", port=8080, name="quoted-command")

        with mock.patch.object(service, "prepare_runtime", return_value=False):
            response, ok = service.deploy()

        self.assertFalse(ok)
        self.assertEqual(response, {})
        self.assertEqual(
            service.entrypoint,
            ["sh", "-c", "cd /mnt/code && sh -lc 'python server.py'"],
        )

    def test_service_respects_explicit_port_env(self):
        service = Service(command="npm run start", port=3000, env={"PORT": "9000"})

        self.assertEqual(service.ports, [3000])
        self.assertIn("PORT=9000", service.env)

    def test_bare_memory_string_is_megabytes(self):
        service = Service(command="npm run start", port=3000, memory="512")

        self.assertEqual(service.memory, 512)

    def test_memory_string_accepts_common_units(self):
        self.assertEqual(Service(command="x", memory="512MB").memory, 512)
        self.assertEqual(Service(command="x", memory="512m").memory, 512)
        self.assertEqual(Service(command="x", memory="512Mi").memory, 512)
        self.assertEqual(Service(command="x", memory="512MiB").memory, 512)
        self.assertEqual(Service(command="x", memory="0.5Gi").memory, 512)
        self.assertEqual(Service(command="x", memory="1GB").memory, 1000)
        self.assertEqual(Service(command="x", memory="1g").memory, 1000)
        self.assertEqual(Service(command="x", memory="1Gi").memory, 1024)
        self.assertEqual(Service(command="x", memory="1GiB").memory, 1024)

    def test_image_entrypoint_can_be_used_without_command(self):
        service = Service(image=Image.from_id("img-123"), ports=[8080])

        self.assertEqual(service.entrypoint, [])
        self.assertEqual(service.ports, [8080])
        self.assertEqual(service.keep_warm_seconds, DEFAULT_SERVICE_KEEP_WARM_SECONDS)
        self.assertEqual(service.autoscaler.min_containers, 0)
        self.assertEqual(service.autoscaler.max_containers, 1)

    def test_image_service_without_explicit_port_defaults_to_8000(self):
        service = Service(image=Image.from_id("img-123"))

        self.assertEqual(service.entrypoint, [])
        self.assertEqual(service.ports, [8000])
        self.assertIn("PORT=8000", service.env)

    def test_service_serializes_durable_disk_and_database_metadata(self):
        service = Service(
            image=Image.from_id("img-123"),
            ports=[5432],
            disks=[
                DurableDisk(
                    name="pg-data",
                    size="10Gi",
                    mount_path="/var/lib/postgresql/data",
                )
            ],
            serving=ServingConfig(
                database=DatabaseServingConfig(
                    kind="postgres",
                    port=5432,
                    readiness_probe="pg_isready",
                    connection_env_name="DATABASE_URL",
                    credential_secret_names=["PG_PASSWORD"],
                )
            ),
        )

        req = service._stub_request(
            stub_type="pod/deployment",
            stub_name="pg",
            force_create_stub=True,
            autoscaler_type="queue_depth",
            inputs=None,
            outputs=None,
        )

        self.assertEqual(req.disks[0].name, "pg-data")
        self.assertEqual(req.serving.database.kind, "postgres")
        self.assertEqual(req.serving.database.connection_env_name, "DATABASE_URL")

    def test_command_service_without_image_does_not_default_port(self):
        service = Service(command="python worker.py")

        self.assertEqual(service.entrypoint, ["sh", "-lc", "python worker.py"])
        self.assertEqual(service.ports, [])
        self.assertNotIn("PORT=", service.env)

    def test_always_on_keeps_one_replica_running(self):
        service = Service(image=Image.from_id("img-123"), ports=[8080], always_on=True)

        self.assertEqual(service.autoscaler.min_containers, 1)
        self.assertEqual(service.autoscaler.max_containers, 1)
        self.assertTrue(service.always_on)

    def test_service_replica_bounds_are_explicit(self):
        service = Service(
            image=Image.from_id("img-123"),
            ports=[8080],
            min_replicas=2,
            max_replicas=4,
        )

        self.assertEqual(service.autoscaler.min_containers, 2)
        self.assertEqual(service.autoscaler.max_containers, 4)

    def test_service_replica_bounds_validate(self):
        with self.assertRaises(ValueError):
            Service(image=Image.from_id("img-123"), ports=[8080], min_replicas=3, max_replicas=2)

    def test_service_max_replicas_must_allow_capacity(self):
        with self.assertRaises(ValueError):
            Service(image=Image.from_id("img-123"), ports=[8080], max_replicas=0)

    def test_merge_port_options_deduplicates_ports(self):
        kwargs = {"ports": [8000], "port": (8000, 9000)}

        _merge_port_options(kwargs)

        self.assertEqual(kwargs["ports"], [8000, 9000])
        self.assertNotIn("port", kwargs)

    def test_generate_service_module_uses_dockerfile_image_without_entrypoint(self):
        image = Image.from_id("img-123")
        kwargs = {
            "dockerfile": image,
            "image": None,
            "entrypoint": None,
            "ports": [8080],
            "env": (),
        }

        service = _generate_service_module("dockerfile-app", kwargs)

        self.assertEqual(service.name, "dockerfile-app")
        self.assertEqual(service.image, image)
        self.assertEqual(service.entrypoint, [])
        self.assertEqual(service.ports, [8080])
        self.assertIn("PORT=8080", service.env)
        self.assertEqual(service.keep_warm_seconds, DEFAULT_SERVICE_KEEP_WARM_SECONDS)

    def test_generate_service_module_preserves_explicit_immediate_scale_to_zero(self):
        image = Image.from_id("img-123")
        kwargs = {
            "dockerfile": image,
            "image": None,
            "entrypoint": None,
            "ports": [8080],
            "env": (),
            "keep_warm_seconds": 0,
        }

        service = _generate_service_module("dockerfile-app", kwargs)

        self.assertEqual(service.keep_warm_seconds, 0)

    def test_generate_service_module_defaults_image_service_to_port_8000(self):
        image = Image.from_id("img-123")
        kwargs = {
            "dockerfile": None,
            "image": image,
            "entrypoint": None,
            "ports": [],
            "env": (),
        }

        service = _generate_service_module("image-app", kwargs)

        self.assertEqual(service.ports, [8000])
        self.assertIn("PORT=8000", service.env)

    def test_generate_service_module_applies_resource_options(self):
        image = Image.from_id("img-123")
        kwargs = {
            "dockerfile": None,
            "image": image,
            "entrypoint": None,
            "ports": [8080],
            "env": (),
            "cpu": 0.25,
            "memory": "0.5Gi",
            "secrets": ["API_TOKEN"],
            "tcp": True,
        }

        service = _generate_service_module("image-app", kwargs)

        self.assertEqual(service.cpu, 250)
        self.assertEqual(service.memory, 512)
        self.assertEqual([secret.name for secret in service.secrets], ["API_TOKEN"])
        self.assertTrue(service.tcp)

    def test_generate_service_module_applies_llm_metadata(self):
        image = Image.from_id("img-123")
        kwargs = {
            "dockerfile": None,
            "image": image,
            "entrypoint": None,
            "ports": [8000],
            "env": (),
            "llm_enabled": True,
            "llm_model_id": "Qwen/Qwen2.5-0.5B-Instruct",
            "llm_engine": "vllm",
            "llm_served_model_name": "qwen",
            "llm_context_length": 4096,
            "llm_tokenizer": "qwen-tokenizer",
            "llm_metrics_path": "/metrics",
            "llm_slo_tier": "standard",
        }

        service = _generate_service_module("llm-app", kwargs)

        self.assertEqual(service.entrypoint, [])
        self.assertEqual(service.app_kind, "llm_model")
        self.assertEqual(service.serving_protocol, "openai")
        self.assertEqual(service.llm.model_id, "Qwen/Qwen2.5-0.5B-Instruct")
        self.assertEqual(service.llm.engine, "vllm")
        self.assertEqual(service.llm.served_model_name, "qwen")
        self.assertEqual(service.llm.context_length, 4096)
        self.assertEqual(service.llm.tokenizer, "qwen-tokenizer")
        self.assertEqual(service.llm.metrics_path, "/metrics")
        self.assertEqual(service.llm.slo_tier, "standard")
        self.assertIsInstance(service.autoscaler, LLMTokenPressureAutoscaler)

    def test_llm_service_with_pool_defaults_to_any_gpu(self):
        service = Service(
            image=Image.from_id("img-123"),
            ports=[8000],
            pool="gpu-pool",
            llm=LLMConfig(model_id="Qwen/Qwen2.5-0.5B-Instruct"),
        )

        self.assertEqual(service.gpu, "any")
        self.assertEqual(service.gpu_count, 1)

    def test_apply_llm_metadata_defaults_pool_pod_to_any_gpu(self):
        pod = Pod(
            image=Image.from_id("img-123"),
            entrypoint=["vllm", "serve"],
            ports=[8000],
            pool="gpu-pool",
        )

        ok = _apply_llm_metadata(pod, {"llm_enabled": True})

        self.assertTrue(ok)
        self.assertEqual(pod.gpu, "any")
        self.assertEqual(pod.gpu_count, 1)

    def test_generate_service_module_infers_llm_metadata_from_vllm_dockerfile(self):
        image = Image()
        image.dockerfile = "\n".join(
            [
                "FROM vllm/vllm-openai:latest",
                "ENV MODEL_ID=Qwen/Qwen2.5-0.5B-Instruct",
                'CMD ["--model", "Qwen/Qwen2.5-0.5B-Instruct", "--served-model-name", "qwen", "--max-model-len", "4096"]',
            ]
        )
        kwargs = {
            "dockerfile": image,
            "image": None,
            "entrypoint": None,
            "ports": [8000],
            "env": (),
            "llm_enabled": True,
        }

        service = _generate_service_module("llm-app", kwargs)

        self.assertEqual(service.app_kind, "llm_model")
        self.assertEqual(service.serving_protocol, "openai")
        self.assertEqual(service.llm.model_id, "Qwen/Qwen2.5-0.5B-Instruct")
        self.assertEqual(service.llm.engine, "vllm")
        self.assertEqual(service.llm.served_model_name, "qwen")
        self.assertEqual(service.llm.context_length, 4096)
        self.assertEqual(service.llm.metrics_path, "/metrics")

    def test_generate_service_module_does_not_require_vllm_model_metadata(self):
        image = Image()
        image.dockerfile = (
            'FROM python:3.12-slim\nCMD ["vllm", "serve", "meta-llama/Llama-3.2-1B-Instruct"]\n'
        )
        kwargs = {
            "dockerfile": image,
            "image": None,
            "entrypoint": None,
            "ports": [8000],
            "env": (),
            "llm_enabled": True,
        }

        service = _generate_service_module("llm-app", kwargs)

        self.assertEqual(service.llm.engine, "vllm")
        self.assertEqual(service.llm.model_id, "")
        self.assertEqual(service.llm.context_length, 0)

    def test_generate_service_module_ignores_invalid_llm_context_hint(self):
        image = Image()
        image.dockerfile = "\n".join(
            [
                "FROM vllm/vllm-openai:latest",
                'CMD ["--model", "Qwen/Qwen2.5-0.5B-Instruct", "--max-model-len", "4096tokens"]',
            ]
        )
        kwargs = {
            "dockerfile": image,
            "image": None,
            "entrypoint": None,
            "ports": [8000],
            "env": (),
            "llm_enabled": True,
        }

        service = _generate_service_module("llm-app", kwargs)

        self.assertEqual(service.llm.model_id, "Qwen/Qwen2.5-0.5B-Instruct")
        self.assertEqual(service.llm.context_length, 0)

    def test_generate_service_module_allows_llm_without_inferred_model(self):
        image = Image.from_id("img-123")
        kwargs = {
            "dockerfile": None,
            "image": image,
            "entrypoint": None,
            "ports": [8000],
            "env": (),
            "llm_enabled": True,
        }

        service = _generate_service_module("llm-app", kwargs)

        self.assertEqual(service.app_kind, "llm_model")
        self.assertEqual(service.serving_protocol, "openai")
        self.assertEqual(service.llm.model_id, "")
        self.assertEqual(service.llm.context_length, 0)

    def test_service_llm_config_defaults_metadata(self):
        service = Service(
            image=Image.from_id("img-123"),
            ports=[8000],
            llm=LLMConfig(model_id="Qwen/Qwen2.5-0.5B-Instruct"),
        )
        pod = Pod(
            image=Image.from_id("img-123"),
            entrypoint=["vllm", "serve"],
            ports=[8000],
            llm=LLMConfig(model_id="meta-llama/Llama-3.2-1B-Instruct"),
        )

        self.assertEqual(service.app_kind, "llm_model")
        self.assertEqual(service.serving_protocol, "openai")
        self.assertEqual(service.llm.model_id, "Qwen/Qwen2.5-0.5B-Instruct")
        self.assertIsInstance(service.autoscaler, LLMTokenPressureAutoscaler)
        self.assertEqual(pod.app_kind, "llm_model")
        self.assertEqual(pod.serving_protocol, "openai")
        self.assertEqual(pod.llm.model_id, "meta-llama/Llama-3.2-1B-Instruct")

    def test_service_llm_config_serializes_as_serving_config(self):
        service = Service(
            image=Image.from_id("img-123"),
            ports=[8000],
            serving=ServingConfig(llm=LLMConfig(model_id="Qwen/Qwen2.5-0.5B-Instruct")),
        )

        proto = service._serving_config_proto()

        self.assertIsNotNone(proto)
        self.assertEqual(proto.app_kind, "llm_model")
        self.assertEqual(proto.serving_protocol, "openai")
        self.assertEqual(proto.llm.model_id, "Qwen/Qwen2.5-0.5B-Instruct")

    def test_apply_llm_metadata_tags_existing_pod(self):
        pod = Pod(image=Image.from_id("img-123"), entrypoint=["vllm", "serve"], ports=[8000])
        ok = _apply_llm_metadata(
            pod,
            {
                "llm_model_id": "meta-llama/Llama-3.2-1B-Instruct",
                "llm_engine": "vllm",
                "llm_context_length": 8192,
            },
        )

        self.assertTrue(ok)
        self.assertEqual(pod.app_kind, "llm_model")
        self.assertEqual(pod.serving_protocol, "openai")
        self.assertEqual(pod.llm.model_id, "meta-llama/Llama-3.2-1B-Instruct")
        self.assertEqual(pod.llm.engine, "vllm")
        self.assertEqual(pod.llm.context_length, 8192)
        self.assertIsInstance(pod.autoscaler, LLMTokenPressureAutoscaler)

    def test_empty_cli_port_override_preserves_inferred_service_port(self):
        image = Image.from_id("img-123")
        kwargs = {
            "dockerfile": None,
            "image": image,
            "entrypoint": None,
            "ports": [],
            "env": (),
        }

        service = _generate_service_module("image-app", kwargs)
        self.assertTrue(handle_config_override(service, kwargs))

        self.assertEqual(service.ports, [8000])
        self.assertIn("PORT=8000", service.env)

    def test_cli_replica_override_updates_autoscaler(self):
        service = Service(image=Image.from_id("img-123"), ports=[8080])
        kwargs = {"min_replicas": 2, "max_replicas": 3, "always_on": None}

        self.assertTrue(handle_config_override(service, kwargs))

        self.assertEqual(service.autoscaler.min_containers, 2)
        self.assertEqual(service.autoscaler.max_containers, 3)

    def test_cli_secret_override_keeps_gateway_secret_shape(self):
        service = Service(image=Image.from_id("img-123"), ports=[8080])
        kwargs = {"secrets": ["API_TOKEN"]}

        self.assertTrue(handle_config_override(service, kwargs))

        self.assertEqual([secret.name for secret in service.secrets], ["API_TOKEN"])

    def test_cli_secret_override_accepts_single_secret_string(self):
        service = Service(image=Image.from_id("img-123"), ports=[8080])
        kwargs = {"secrets": "API_TOKEN"}

        self.assertTrue(handle_config_override(service, kwargs))

        self.assertEqual([secret.name for secret in service.secrets], ["API_TOKEN"])

    def test_cli_allow_marketplace_override_is_explicit(self):
        service = Service(image=Image.from_id("img-123"), ports=[8080], allow_marketplace=True)

        self.assertTrue(handle_config_override(service, {"allow_marketplace": None}))
        self.assertTrue(service.allow_marketplace)

        self.assertTrue(handle_config_override(service, {"allow_marketplace": False}))
        self.assertFalse(service.allow_marketplace)

        self.assertTrue(handle_config_override(service, {"allow_marketplace": True}))
        self.assertTrue(service.allow_marketplace)

    def test_service_image_option_rejects_image_and_dockerfile_together(self):
        kwargs = {"dockerfile": "Dockerfile", "image": Image.from_id("img-123")}

        with self.assertRaisesRegex(ValueError, "either --dockerfile or --image"):
            _service_image_option(kwargs)

    def test_generate_service_module_infers_dockerfile_exposed_port(self):
        image = Image()
        image.dockerfile = "FROM python:3.12-slim\nEXPOSE 5000/tcp 9000\n"
        kwargs = {
            "dockerfile": image,
            "image": None,
            "entrypoint": None,
            "ports": [],
            "env": ("PORT=7000",),
        }

        service = _generate_service_module("dockerfile-app", kwargs)

        self.assertEqual(service.ports, [5000, 9000])
        self.assertIn("PORT=7000", service.env)

    def test_service_uses_dockerfile_image_process_without_entrypoint(self):
        image = Image()
        image.dockerfile = 'FROM node:20-alpine\nEXPOSE 8080\nCMD ["node", "server.js"]\n'

        service = Service(image=image)

        self.assertEqual(service.entrypoint, [])
        self.assertEqual(service.ports, [8080])
        self.assertIn("PORT=8080", service.env)
        self.assertEqual(command_from_dockerfile(image), ["node", "server.js"])

    def test_dockerfile_shell_form_cmd_is_available_for_metadata(self):
        image = Image()
        image.dockerfile = "FROM node:20-alpine\nCMD npm start\n"

        service = Service(image=image)

        self.assertEqual(service.entrypoint, [])
        self.assertEqual(command_from_dockerfile(image), ["sh", "-lc", "npm start"])

    def test_dockerfile_entrypoint_and_cmd_are_combined(self):
        image = Image()
        image.dockerfile = 'ENTRYPOINT ["node"]\nCMD ["server.js"]\n'

        self.assertEqual(command_from_dockerfile(image), ["node", "server.js"])

    def test_dockerfile_exec_form_errors_are_actionable(self):
        image = Image()
        image.dockerfile = 'CMD ["node",]\n'

        with self.assertRaisesRegex(ValueError, "JSON string array"):
            command_from_dockerfile(image)

    def test_dockerfile_command_parser_ignores_comments_outside_quotes(self):
        image = Image()
        image.dockerfile = 'CMD ["node", "server#prod.js"] # inline comment\n'

        self.assertEqual(command_from_dockerfile(image), ["node", "server#prod.js"])

    @mock.patch("beta9.abstractions.image.Image.sync_files")
    def test_generate_service_module_materializes_dockerfile_path_without_entrypoint_override(
        self, sync_files_mock
    ):
        with TemporaryDirectory() as tmpdir:
            dockerfile = Path(tmpdir) / "Dockerfile"
            dockerfile.write_text('FROM node:20-alpine\nEXPOSE 8080\nCMD ["node", "server.js"]\n')
            kwargs = {
                "dockerfile": str(dockerfile),
                "image": None,
                "entrypoint": None,
                "ports": [],
                "env": (),
            }

            service = _generate_service_module("dockerfile-app", kwargs)

        self.assertIsInstance(kwargs["dockerfile"], Image)
        self.assertEqual(service.entrypoint, [])
        self.assertEqual(service.ports, [8080])
        self.assertIn("PORT=8080", service.env)
        sync_files_mock.assert_called_once_with(str(Path(tmpdir)))

    def test_dockerfile_parser_does_not_sync_during_click_parsing(self):
        with TemporaryDirectory() as tmpdir:
            dockerfile = Path(tmpdir) / "Dockerfile"
            dockerfile.write_text("FROM node:20-alpine\n")

            with mock.patch("beta9.abstractions.image.Image.from_dockerfile") as from_dockerfile:
                parsed = DockerfileParser().convert(str(dockerfile), None, None)

        self.assertEqual(parsed, str(dockerfile))
        from_dockerfile.assert_not_called()

    @mock.patch("beta9.abstractions.image.Image.sync_files")
    def test_dockerfile_option_uses_current_directory_for_root_dockerfile(self, sync_files_mock):
        with TemporaryDirectory() as tmpdir:
            dockerfile = Path(tmpdir) / "Dockerfile"
            dockerfile.write_text("FROM node:20-alpine\n")
            previous_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                image_from_dockerfile_option("Dockerfile")
            finally:
                os.chdir(previous_cwd)

        sync_files_mock.assert_called_once_with(".")

    def test_ports_from_dockerfile_ignores_comments_and_protocols(self):
        image = Image()
        image.dockerfile = "\n".join(
            [
                "FROM node:22",
                "EXPOSE 3000/tcp 3000/udp # duplicate",
                "# EXPOSE 9000",
            ]
        )

        self.assertEqual(ports_from_dockerfile(image), [3000])

    @mock.patch("beta9.abstractions.image.Image.sync_files")
    def test_from_dockerfile_marks_image_as_dockerfile_build(self, sync_files_mock):
        with TemporaryDirectory() as tmpdir:
            dockerfile = Path(tmpdir) / "Dockerfile"
            dockerfile.write_text("FROM python:3.12-slim\nEXPOSE 5050/tcp\n")

            service = Service.from_dockerfile(str(dockerfile), name="dockerfile-app")

        self.assertEqual(service.name, "dockerfile-app")
        self.assertEqual(service.image.dockerfile, "FROM python:3.12-slim\nEXPOSE 5050/tcp\n")
        self.assertEqual(service.ports, [5050])
        self.assertIn("PORT=5050", service.env)
        self.assertTrue(service.image.ignore_python)

    def test_empty_image_does_not_imply_default_service_port(self):
        self.assertFalse(service_image_implies_default_port(Image()))
