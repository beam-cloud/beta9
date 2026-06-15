from tempfile import TemporaryDirectory
from unittest import TestCase, mock
from pathlib import Path

from beta9 import Image, Service
from beta9.abstractions.service import ports_from_dockerfile
from beta9.cli.deployment import (
    _generate_service_module,
    _merge_port_options,
)
from beta9.cli.extraclick import handle_config_override


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

    def test_image_entrypoint_can_be_used_without_command(self):
        service = Service(image=Image.from_id("img-123"), ports=[8080])

        self.assertEqual(service.entrypoint, [])
        self.assertEqual(service.ports, [8080])
        self.assertEqual(service.autoscaler.min_containers, 0)
        self.assertEqual(service.autoscaler.max_containers, 1)

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
            dockerfile.write_text("FROM python:3.12-slim")

            service = Service.from_dockerfile(str(dockerfile), name="dockerfile-app")

        self.assertEqual(service.name, "dockerfile-app")
        self.assertEqual(service.image.dockerfile, "FROM python:3.12-slim")
        self.assertTrue(service.image.ignore_python)
