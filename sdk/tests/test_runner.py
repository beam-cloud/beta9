import unittest
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from unittest.mock import MagicMock

from beta9.abstractions.base import runner as runner_module
from beta9.abstractions.base.runner import RunnerAbstraction
from beta9.abstractions.image import ImageBuildResult
from beta9.clients.gateway import GetOrCreateStubResponse
from beta9.sync import FileSyncResult


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
        def slow_image_build():
            sleep(0.01)
            return ImageBuildResult(success=True, image_id="image-id", python_version="python3.12")

        def slow_file_sync(ignore_patterns=None):
            sleep(0.01)
            return FileSyncResult(success=True, object_id="object-id")

        def slow_stub_create(request):
            sleep(0.01)
            return GetOrCreateStubResponse(ok=True, stub_id="stub-id")

        self.runner.image.build = MagicMock(side_effect=slow_image_build)
        self.runner.syncer.sync = MagicMock(side_effect=slow_file_sync)
        self.runner.gateway_stub.get_or_create_stub = MagicMock(side_effect=slow_stub_create)

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
        self.assertEqual(self.runner.syncer.sync.call_count, 1)
        self.assertEqual(self.runner.gateway_stub.get_or_create_stub.call_count, 1)

    def test_stub_creation_flag_is_not_reset_on_read(self):
        runner_module._mark_stub_created_for_workspace()

        self.assertTrue(runner_module._stub_created_for_current_workspace())
        self.assertTrue(runner_module._stub_created_for_current_workspace())
