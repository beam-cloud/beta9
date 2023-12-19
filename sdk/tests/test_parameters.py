import copy
import unittest
from collections import namedtuple
from typing import Any

import pytest
from marshmallow import ValidationError

from beam import (
    App,
    Autoscaling,
    Image,
    Output,
    PythonVersion,
    RequestLatencyAutoscaler,
    Runtime,
    Volume,
    build_config,
)
from beam.utils.parse import compose_cpu

try:
    from importlib.metadata import version  # type: ignore
except ImportError:
    from importlib_metadata import version


beam_sdk_version = version("beam-sdk")


class TestParameters(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self.maxDiff = None
        self._default_config = {
            "app_spec_version": "v3",
            "name": "test",
            "sdk_version": beam_sdk_version,
            "mounts": [],
            "runtime": Runtime().data,
            "triggers": [
                {
                    "handler": "tests/test_parameters.py:test",
                    "callback_url": None,
                    "runtime": None,
                    "outputs": [],
                    "autoscaling": None,
                    "autoscaler": None,
                    "task_policy": {
                        "max_retries": 3,
                        "timeout": 3600,
                    },
                    "workers": 1,
                    "authorized": True,
                },
            ],
            "run": None,
        }

        self._default_rest_api_config = {
            **self._default_config,
            "triggers": [
                {
                    **self._default_config["triggers"][0],
                    "path": "/test",
                    "loader": None,
                    "trigger_type": "rest_api",
                    "max_pending_tasks": 100,
                    "keep_warm_seconds": 90,
                    "method": "POST",
                }
            ],
        }

        self._default_task_queue_config = {
            **self._default_config,
            "triggers": [
                {
                    **self._default_rest_api_config["triggers"][0],
                    "trigger_type": "webhook",
                    "keep_warm_seconds": 10,
                }
            ],
        }

        self._default_schedule_config = {
            **self._default_config,
            "triggers": [
                {
                    **self._default_config["triggers"][0],
                    "trigger_type": "cron_job",
                    "when": "every 1m",
                }
            ],
        }

        self._default_run_config = {
            "app_spec_version": "v3",
            "sdk_version": beam_sdk_version,
            "name": "test",
            "mounts": [],
            "triggers": [],
            "runtime": Runtime().data,
            "run": {
                "handler": "tests/test_parameters.py:test",
                "callback_url": None,
                "outputs": [],
                "runtime": None,
                "task_policy": {
                    "max_retries": 3,
                    "timeout": 3600,
                },
            },
        }

    @property
    def default_config(self) -> dict:
        return copy.deepcopy(self._default_config)

    @property
    def default_rest_api_config(self) -> dict:
        return copy.deepcopy(self._default_rest_api_config)

    @property
    def default_task_queue_config(self) -> dict:
        return copy.deepcopy(self._default_task_queue_config)

    @property
    def default_schedule_config(self) -> dict:
        return copy.deepcopy(self._default_schedule_config)

    @property
    def default_run_config(self) -> dict:
        return copy.deepcopy(self._default_run_config)

    def _run_single_trigger_subtest(
        self,
        app_config: dict,
        expected_overall_config: dict,
        trigger_type: str = "rest_api",
        trigger_kwargs: dict = {},
    ):
        with self.subTest(
            app_config=app_config,
            expected_config=expected_overall_config,
            trigger_type=trigger_type,
            trigger_kwargs=trigger_kwargs,
        ):
            app = App(**{"name": "test", "runtime": Runtime(), **app_config})

            if trigger_type == "rest_api":

                @app.rest_api(**trigger_kwargs)
                def test():
                    pass

            elif trigger_type == "webhook":

                @app.task_queue(**trigger_kwargs)
                def test():
                    pass

            elif trigger_type == "cron_job":

                @app.schedule(**trigger_kwargs)
                def test():
                    pass

            config_dict = test()
            print("provided:", trigger_kwargs)
            print("returned:", config_dict)
            print("expected:", expected_overall_config)
            self.assertDictEqual(config_dict, expected_overall_config)

    def _run_single_run_subtest(self, app_config, expected_overall_config, run_kwargs={}):
        with self.subTest(app_config=app_config, expected_config=expected_overall_config):
            app = App(**{"name": "test", "runtime": Runtime(), **app_config})

            @app.run(**run_kwargs)
            def test():
                pass

            config_dict = test()
            self.assertDictEqual(config_dict, expected_overall_config)

    def test_default_trigger_and_app(self):
        self._run_single_trigger_subtest({}, self.default_rest_api_config)
        expected_config = self.default_task_queue_config
        expected_config["triggers"][0]["trigger_type"] = "webhook"
        self._run_single_trigger_subtest(
            {}, expected_overall_config=expected_config, trigger_type="webhook"
        )

        expected_config = self.default_schedule_config

        self._run_single_trigger_subtest(
            {},
            expected_overall_config=expected_config,
            trigger_type="cron_job",
            trigger_kwargs={"when": "every 1m"},
        )

    def test_runtime_app_parameters(self):
        runtimes: list[dict] = [
            {
                "memory": "2Gi",
            },
            {"cpu": compose_cpu(2), "memory": "2Gi", "gpu": "A10G"},
        ]

        for i in range(len(runtimes)):
            expected_config = self.default_rest_api_config
            expected_config["runtime"] = Runtime(**runtimes[i]).data
            expected_config["triggers"][0]["runtime"] = None

            self._run_single_trigger_subtest(
                app_config={"runtime": runtimes[i]},
                expected_overall_config=expected_config,
            )

            self._run_single_trigger_subtest(
                app_config={"runtime": Runtime(**runtimes[i])},
                expected_overall_config=expected_config,
            )

    def test_volumes_app_parameters(self):
        volumes = [
            {
                "input": [
                    {"name": "test", "path": "./test", "volume_type": "shared"},
                ],
                "expected": [{"name": "test", "app_path": "./test", "mount_type": "shared"}],
            },
            {
                "input": [
                    {
                        "name": "test",
                        "path": "./test",
                        "volume_type": "persistent",
                    },
                    {
                        "name": "test2",
                        "path": "./test2",
                        "volume_type": "shared",
                    },
                ],
                "expected": [
                    {"name": "test", "app_path": "./test", "mount_type": "persistent"},
                    {"name": "test2", "app_path": "./test2", "mount_type": "shared"},
                ],
            },
            {
                "input": [
                    {
                        "name": "test",
                        "path": "./test",
                    }
                ],
                "expected": [{"name": "test", "app_path": "./test", "mount_type": "shared"}],
            },
        ]

        for i in range(len(volumes)):
            expected_config = self.default_rest_api_config
            expected_config["mounts"] = volumes[i]["expected"]

            self._run_single_trigger_subtest(
                app_config={"volumes": volumes[i]["input"]},
                expected_overall_config=expected_config,
            )

            self._run_single_trigger_subtest(
                app_config={"volumes": [Volume(**volume) for volume in volumes[i]["input"]]},
                expected_overall_config=expected_config,
            )

    def _create_parameter_permutations(self, parameters, current_param_set, permute_list=[]):
        for i in range(len(parameters)):
            self._create_parameter_permutations(
                parameters[i + 1 :], current_param_set + [parameters[i]], permute_list
            )

            self._create_parameter_permutations(
                parameters[i + 1 :], current_param_set, permute_list
            )

        if len(parameters) == 0:
            combined_params = {}
            for param in current_param_set:
                combined_params = {**combined_params, **param}

            permute_list.append(combined_params)

        return permute_list

    def test_rest_api_and_task_queue_parameters(self):
        parameters = [
            {
                "outputs": [
                    {"path": "./test"},
                    {"path": "./test2"},
                ]
            },
            {
                "autoscaling": {
                    "max_replicas": 10,
                    "desired_latency": 500,
                    "autoscaling_type": "max_request_latency",
                }
            },
            {"max_pending_tasks": 100000},
            {"keep_warm_seconds": 100},
            {"loader": "handler.py:handler"},
        ]

        # Create permutations of parameters
        parameter_permutations = self._create_parameter_permutations(parameters, [], [])

        for param in parameter_permutations:
            expected_task_queue_config = self.default_task_queue_config
            expected_rest_api_config = self.default_rest_api_config

            expected_task_queue_config["triggers"][0] = {
                **expected_task_queue_config["triggers"][0],
                **param,
            }

            expected_rest_api_config["triggers"][0] = {
                **expected_rest_api_config["triggers"][0],
                **param,
            }

            self._run_single_trigger_subtest(
                {},
                expected_overall_config=expected_task_queue_config,
                trigger_type="webhook",
                trigger_kwargs=param,
            )

            self._run_single_trigger_subtest(
                {},
                expected_overall_config=expected_rest_api_config,
                trigger_kwargs=param,
            )

            # Test with classes
            input_param_with_classes = {
                **param,
                "outputs": [
                    Output(**output) for output in (param["outputs"] if "outputs" in param else [])
                ],
                "autoscaling": Autoscaling(**param["autoscaling"])
                if "autoscaling" in param
                else None,
                "runtime": Runtime(**param["runtime"]) if "runtime" in param else None,
            }

            self._run_single_trigger_subtest(
                {},
                expected_overall_config=expected_task_queue_config,
                trigger_type="webhook",
                trigger_kwargs=input_param_with_classes,
            )

            self._run_single_trigger_subtest(
                {},
                expected_overall_config=expected_rest_api_config,
                trigger_kwargs=input_param_with_classes,
            )

    def test_schedule_parameters(self):
        parameters = [
            {
                "when": "0 0 * * *",
            },
            {
                "when": "every 1s",
            },
            {
                "when": "every 1m",
            },
            {
                "when": "every 1h",
            },
            {
                "outputs": [
                    {"path": "./test"},
                    {"path": "./test2"},
                ]
            },
            {
                "runtime": {
                    "cpu": "2000m",
                    "memory": "10Gi",
                    "gpu": "",
                    "image": build_config(Image(), Image),
                },
            },
        ]

        # Create permutations of parameters
        parameter_permutations = self._create_parameter_permutations(parameters, [], [])

        for param in parameter_permutations:
            default_param = {"when": "0 0 * * *", "runtime": Runtime().data}

            param = {**default_param, **param}

            expected_schedule_config = self.default_schedule_config

            if "runtime" in param:
                expected_schedule_config["runtime"] = param["runtime"]

            expected_schedule_config["triggers"][0] = {
                **expected_schedule_config["triggers"][0],
                **param,
            }

            self._run_single_trigger_subtest(
                {},
                expected_overall_config=expected_schedule_config,
                trigger_type="cron_job",
                trigger_kwargs={**param},
            )

            # Test with classes
            input_param_with_classes = {
                **param,
                "outputs": [
                    Output(**output) for output in (param["outputs"] if "outputs" in param else [])
                ],
                "runtime": Runtime(**param["runtime"]) if "runtime" in param else Runtime(),
            }

            self._run_single_trigger_subtest(
                {},
                expected_overall_config=expected_schedule_config,
                trigger_type="cron_job",
                trigger_kwargs=input_param_with_classes,
            )

    def test_outputs_parameters(self):
        parameters = [
            {"outputs": []},
            {
                "outputs": [
                    {"path": "./testdir"},
                    {"path": "./testfile"},
                ]
            },
        ]

        for param in parameters:
            for config in [
                self.default_task_queue_config,
                self.default_rest_api_config,
                self.default_schedule_config,
            ]:
                trigger_type = config["triggers"][0].get("trigger_type")
                config["triggers"][0] = {
                    **config["triggers"][0],
                    **param,
                }

                param = {
                    **param,
                    **(
                        {"when": config["triggers"][0]["when"]}
                        if trigger_type == "cron_job"
                        else {}
                    ),
                }

                self._run_single_trigger_subtest(
                    {},
                    expected_overall_config=config,
                    trigger_type=trigger_type,
                    trigger_kwargs=param,
                )

                input_class_params = {
                    **param,
                    "outputs": [
                        Output(**output)
                        for output in (param["outputs"] if "outputs" in param else [])
                    ],
                }

                self._run_single_trigger_subtest(
                    {},
                    expected_overall_config=config,
                    trigger_type=trigger_type,
                    trigger_kwargs=input_class_params,
                )

    def test_autoscaling_parameters(self):
        default_autoscaling_params = {
            "max_replicas": 1,
            "desired_latency": 100,
            "autoscaling_type": "max_request_latency",
        }

        parameters = [
            {"autoscaling": {"max_replicas": 1, "desired_latency": 1000}},
            {
                "autoscaling": {
                    "max_replicas": 2,
                }
            },
            {"autoscaling": {"desired_latency": 1000}},
        ]

        param: dict[str, Any]
        for param in parameters:
            expected_task_queue_config = self.default_task_queue_config
            expected_rest_api_config = self.default_rest_api_config

            for config in [expected_task_queue_config, expected_rest_api_config]:
                trigger_type = config["triggers"][0].get("trigger_type")
                config["triggers"][0] = {
                    **config["triggers"][0],
                    "autoscaling": {
                        **default_autoscaling_params,
                        **param["autoscaling"],
                    },
                }

                self._run_single_trigger_subtest(
                    {},
                    expected_overall_config=config,
                    trigger_type=trigger_type,
                    trigger_kwargs=param,
                )

                input_class_params = {
                    "autoscaling": Autoscaling(**param["autoscaling"])
                    if "autoscaling" in param
                    else None,
                }

                self._run_single_trigger_subtest(
                    {},
                    expected_overall_config=config,
                    trigger_type=trigger_type,
                    trigger_kwargs=input_class_params,
                )

    def test_image_parameters(self):
        beam_sdk_version = "beam-sdk==0.0.0"

        parameters = [
            {
                "image": {
                    "python_version": PythonVersion.Python310,
                }
            },
            {
                "image": {
                    "python_version": PythonVersion.Python310,
                    "python_packages": ["numpy", "pandas", beam_sdk_version],
                }
            },
            {
                "image": {
                    "python_version": PythonVersion.Python310,
                    "python_packages": ["numpy", "pandas", beam_sdk_version],
                    "commands": ["echo hi"],
                }
            },
            {
                "image": {
                    "python_version": PythonVersion.Python310,
                    "python_packages": ["numpy", "pandas", beam_sdk_version],
                    "commands": ["echo hi"],
                    "base_image": "docker.io/beamcloud/custom-img-test:latest",
                }
            },
        ]

        # Test params for trigger level runtime image
        for param in parameters:
            expected_config = self.default_task_queue_config
            expected_config["triggers"][0]["runtime"] = Runtime().data
            trigger = expected_config["triggers"][0]
            trigger_type = trigger.get("trigger_type")

            input_dict_params = {
                "runtime": {
                    **trigger.get("runtime"),
                    "image": {
                        **trigger.get("runtime").get("image"),
                        **param["image"],
                    },
                }
            }

            expected_config["triggers"][0] = {
                **expected_config["triggers"][0],
                **input_dict_params,
            }
            expected_config["runtime"] = input_dict_params["runtime"]

            self._run_single_trigger_subtest(
                {},
                expected_overall_config=expected_config,
                trigger_type=trigger_type,
                trigger_kwargs=input_dict_params,
            )

            input_class_params = {
                "runtime": {
                    **trigger.get("runtime"),
                    "image": Image(**{**trigger.get("runtime").get("image"), **param["image"]}),
                }
            }

            self._run_single_trigger_subtest(
                {},
                expected_overall_config=expected_config,
                trigger_type=trigger_type,
                trigger_kwargs=input_class_params,
            )

        # Test params for app level runtime image
        for param in parameters:
            expected_config = self.default_task_queue_config
            expected_config["runtime"] = Runtime(image=Image(**param["image"])).data
            trigger = expected_config["triggers"][0]

            input_dict_params = {
                "runtime": {
                    **expected_config["runtime"],
                    "image": {
                        **expected_config["runtime"]["image"],
                        **param["image"],
                    },
                }
            }

            self._run_single_trigger_subtest(
                input_dict_params,
                expected_overall_config=expected_config,
                trigger_type="webhook",
            )

            input_class_params = {
                "runtime": {
                    **expected_config["runtime"],
                    "image": Image(**{**expected_config["runtime"]["image"], **param["image"]}),
                }
            }

            self._run_single_trigger_subtest(
                input_class_params,
                expected_overall_config=expected_config,
                trigger_type="webhook",
            )

    def test_run_parameters(self):
        parameters = [
            {
                "outputs": [
                    {"path": "./tmp1"},
                    {"path": "./tmp2"},
                ]
            },
            {
                "runtime": build_config(Runtime(cpu=3, memory="1Gi"), Runtime),
            },
            {
                "callback_url": "http://test.com",
            },
        ]

        parameter_permutations = self._create_parameter_permutations(parameters, [], [])

        for param in parameter_permutations:
            expected_config = self.default_run_config
            expected_config["run"] = {**expected_config["run"], **param}

            if "runtime" in param:
                expected_config["runtime"] = param["runtime"]

            self._run_single_run_subtest(
                {},
                expected_overall_config=expected_config,
                run_kwargs=param,
            )

    def test_loaders(self):
        def some_function():
            pass

        expected_func_path = "tests/test_parameters.py:some_function"

        expected_config = self.default_task_queue_config
        expected_config["triggers"][0]["loader"] = expected_func_path

        # test the passing in of a function
        self._run_single_trigger_subtest(
            {},
            expected_overall_config=expected_config,
            trigger_type="webhook",
            trigger_kwargs={"loader": some_function},
        )

        # test the passing in of a string
        self._run_single_trigger_subtest(
            {},
            expected_overall_config=expected_config,
            trigger_type="webhook",
            trigger_kwargs={"loader": expected_func_path},
        )

    def test_autoscalers(self):
        Test = namedtuple("Test", ["trigger", "autoscaler", "autoscaler_expected"])

        tests = [
            # TODO: Re-enable when queue depth autoscaler is implemented.
            # Test(
            #     "webhook",
            #     QueueDepthAutoscaler(max_tasks_per_replica=20),
            #     {"queue_depth": {"max_tasks_per_replica": 20, "max_replicas": 1}},
            # ),
            # Test(
            #     "rest_api",
            #     QueueDepthAutoscaler(max_tasks_per_replica=1000),
            #     {"queue_depth": {"max_tasks_per_replica": 1000, "max_replicas": 1}},
            # ),
            Test(
                "webhook",
                RequestLatencyAutoscaler(desired_latency=100),
                {"request_latency": {"desired_latency": 100, "max_replicas": 1}},
            ),
            Test(
                "webhook",
                {
                    "request_latency": {"desired_latency": 100, "max_replicas": 2},
                    "queue_depth": {"max_tasks_per_replica": 11},
                },
                None,
            ),
            Test(
                "rest_api",
                RequestLatencyAutoscaler(desired_latency=60),
                {"request_latency": {"desired_latency": 60, "max_replicas": 1}},
            ),
        ]

        for test in tests:
            expected_config = self.default_config
            if test.trigger == "rest_api":
                expected_config = self.default_rest_api_config
            elif test.trigger == "webhook":
                expected_config = self.default_task_queue_config

            expected_config["triggers"][0]["autoscaler"] = test.autoscaler_expected
            expected_config["triggers"][0]["trigger_type"] = test.trigger

            subtest_kwargs = dict(
                app_config={},
                expected_overall_config=expected_config,
                trigger_type=test.trigger,
                trigger_kwargs={"autoscaler": test.autoscaler},
            )

            if isinstance(test.autoscaler, dict) and len(test.autoscaler.keys()) > 1:
                with pytest.raises(ValidationError):
                    self._run_single_trigger_subtest(**subtest_kwargs)
            else:
                self._run_single_trigger_subtest(**subtest_kwargs)

    def test_memory_limits_should_fail(self):
        tests = [
            {
                "memory": "65Gi",
            }
        ]

        for test in tests:
            with pytest.raises(ValueError):
                runtime = self.default_config["runtime"]
                runtime["memory"] = test["memory"]

                self._run_single_run_subtest(
                    {"runtime": runtime},
                    expected_overall_config=self.default_run_config,
                )

                self._run_single_trigger_subtest(
                    {"runtime": Runtime(**runtime)},
                    expected_overall_config=self.default_run_config,
                )

    def test_task_policy(self):
        Test = namedtuple("Test", ["trigger", "task_policy", "task_policy_expected"])

        tests = [
            Test(
                "webhook",
                {
                    "max_retries": 0,
                },
                {
                    "max_retries": 0,
                    "timeout": 3600,
                },
            ),
            Test(
                "rest_api",
                {
                    "max_retries": 1,
                    "timeout": 4200,
                },
                {
                    "max_retries": 1,
                    "timeout": 4200,
                },
            ),
            Test(
                "cron_job",
                {},
                {
                    "max_retries": 3,
                    "timeout": 3600,
                },
            ),
        ]

        for test in tests:
            expected_trigger_config = self.default_config
            kwargs = {}

            if test.trigger == "webhook":
                expected_trigger_config = self.default_task_queue_config
            elif test.trigger == "rest_api":
                expected_trigger_config = self.default_rest_api_config
            elif test.trigger == "cron_job":
                expected_trigger_config = self.default_schedule_config
                kwargs["when"] = self.default_schedule_config["triggers"][0]["when"]

            expected_trigger_config["triggers"][0]["task_policy"] = test.task_policy_expected
            expected_trigger_config["triggers"][0]["trigger_type"] = test.trigger

            self._run_single_trigger_subtest(
                {},
                trigger_type=test.trigger,
                trigger_kwargs={"task_policy": test.task_policy, **kwargs},
                expected_overall_config=expected_trigger_config,
            )

            # Make sure it works for run as well
            expected_run_config = self.default_run_config
            expected_run_config["run"]["task_policy"] = test.task_policy_expected

            self._run_single_run_subtest(
                {},
                run_kwargs={
                    "task_policy": test.task_policy,
                },
                expected_overall_config=expected_run_config,
            )

            # Now we want to test flatten params
            self._run_single_trigger_subtest(
                {},
                trigger_type=test.trigger,
                trigger_kwargs={**test.task_policy, **kwargs},
                expected_overall_config=expected_trigger_config,
            )

            # Make sure it works for run as well
            run_kwargs = {}

            if test.task_policy.get("timeout"):
                run_kwargs["timeout"] = test.task_policy["timeout"]

            # Runs cant set max_retries, so it will always be 3 (ignored in backend)
            expected_run_config["run"]["task_policy"]["max_retries"] = 3

            self._run_single_run_subtest(
                {},
                run_kwargs=run_kwargs,
                expected_overall_config=expected_run_config,
            )
