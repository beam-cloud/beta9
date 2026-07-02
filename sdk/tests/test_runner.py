import unittest
from concurrent.futures import ThreadPoolExecutor
from inspect import signature
from time import sleep
from unittest.mock import MagicMock

from beta9.abstractions.base import runner as runner_module
from beta9.abstractions.base.runner import RunnerAbstraction
from beta9.abstractions.endpoint import ASGI, Endpoint, RealtimeASGI
from beta9.abstractions.experimental.bot.bot import Bot
from beta9.abstractions.function import Function, Schedule
from beta9.abstractions.image import ImageBuildResult
from beta9.abstractions.integrations.fastmcp import MCPServer
from beta9.abstractions.integrations.vllm import VLLM
from beta9.abstractions.sandbox import Sandbox
from beta9.abstractions.taskqueue import TaskQueue
from beta9.clients.gateway import GetOrCreateStubResponse
from beta9.sync import FileSyncResult
from beta9.type import DurableDisk


class TestRunner(unittest.TestCase):
    def setUp(self):
        runner_module._stub_created_for_workspace = False
        self.runner = RunnerAbstraction()

    def test_cpu_parsing_float_input_within_range(self):
        self.assertEqual(self.runner.parse_cpu(0.5), 500)
        self.assertEqual(self.runner.parse_cpu(2.0), 2000)

    def test_cpu_parsing_integer_input_within_range(self):
        self.assertEqual(self.runner.parse_cpu(1), 1000)
        self.assertEqual(self.runner.parse_cpu(2), 2000)

    def test_cpu_parsing_string_input_within_range(self):
        self.assertEqual(self.runner.parse_cpu("1000m"), 1000)
        self.assertEqual(self.runner.parse_cpu("2000m"), 2000)

    def test_cpu_parsing_float_input_out_of_range(self):
        with self.assertRaises(ValueError):
            self.runner.parse_cpu(0.05)

        with self.assertRaises(ValueError):
            self.runner.parse_cpu(70.0)

    def test_cpu_parsing_string_input_out_of_range(self):
        with self.assertRaises(ValueError):
            self.runner.parse_cpu("50m")

        with self.assertRaises(ValueError):
            self.runner.parse_cpu("65000m")

    def test_cpu_parsing_invalid_string_format(self):
        with self.assertRaises(ValueError):
            self.runner.parse_cpu("100")

        with self.assertRaises(ValueError):
            self.runner.parse_cpu("abc")

        with self.assertRaises(ValueError):
            self.runner.parse_cpu("100mc")

    def test_cpu_parsing_non_numeric_input(self):
        with self.assertRaises(TypeError):
            self.runner.parse_cpu([])  # type:ignore[reportArgumentType]

        with self.assertRaises(TypeError):
            self.runner.parse_cpu({})  # type:ignore[reportArgumentType]

    def test_prepare_runtime_is_single_flight(self):
        def slow_image_build(target_platform=""):
            sleep(0.01)
            return ImageBuildResult(
                success=True, image_id="image-id", python_version="python3.12"
            )

        def slow_file_sync(ignore_patterns=None):
            sleep(0.01)
            return FileSyncResult(success=True, object_id="object-id")

        def slow_stub_create(request):
            sleep(0.01)
            return GetOrCreateStubResponse(ok=True, stub_id="stub-id")

        self.runner.image.build = MagicMock(side_effect=slow_image_build)
        self.runner.syncer.sync = MagicMock(side_effect=slow_file_sync)
        self.runner.gateway_stub.get_or_create_stub = MagicMock(
            side_effect=slow_stub_create
        )

        with ThreadPoolExecutor(max_workers=16) as executor:
            results = list(
                executor.map(
                    lambda _: self.runner.prepare_runtime(
                        stub_type="sandbox",
                        force_create_stub=True,
                    ),
                    range(16),
                )
            )

        self.assertTrue(all(results))
        self.assertEqual(self.runner.image.build.call_count, 1)
        self.runner.image.build.assert_called_once_with(target_platform="")
        self.assertEqual(self.runner.syncer.sync.call_count, 1)
        self.assertEqual(self.runner.gateway_stub.get_or_create_stub.call_count, 1)

    def test_prepare_image_targets_amd64_only_for_marketplace(self):
        self.runner.image.build = MagicMock(
            return_value=ImageBuildResult(
                success=True, image_id="normal-image", python_version="python3.12"
            )
        )

        self.assertTrue(self.runner._prepare_image())
        self.runner.image.build.assert_called_once_with(target_platform="")

        marketplace_runner = RunnerAbstraction(allow_marketplace=True)
        marketplace_runner.image.build = MagicMock(
            return_value=ImageBuildResult(
                success=True, image_id="marketplace-image", python_version="python3.12"
            )
        )

        self.assertTrue(marketplace_runner._prepare_image())
        marketplace_runner.image.build.assert_called_once_with(
            target_platform=runner_module.MARKETPLACE_TARGET_PLATFORM
        )

    def test_stub_creation_flag_is_not_reset_on_read(self):
        runner_module._mark_stub_created_for_workspace()

        self.assertTrue(runner_module._stub_created_for_current_workspace())
        self.assertTrue(runner_module._stub_created_for_current_workspace())

    def test_stub_request_exports_durable_disks(self):
        self.runner.disks = [
            DurableDisk(name="cache", size="10Gi", mount_path="/cache", read_only=True)
        ]

        request = self.runner._stub_request(
            stub_type="function",
            stub_name="function/test:handler",
            force_create_stub=True,
            autoscaler_type="queue_depth",
            inputs=None,
            outputs=None,
        )

        self.assertEqual(len(request.disks), 1)
        self.assertEqual(request.disks[0].name, "cache")
        self.assertEqual(request.disks[0].size, "10Gi")
        self.assertEqual(request.disks[0].mount_path, "/cache")
        self.assertTrue(request.disks[0].read_only)

    def test_public_runtime_abstractions_accept_durable_disks(self):
        for cls in (
            Function,
            Schedule,
            Endpoint,
            ASGI,
            RealtimeASGI,
            TaskQueue,
            Sandbox,
            Bot,
            MCPServer,
            VLLM,
        ):
            self.assertIn("disks", signature(cls.__init__).parameters, cls.__name__)
