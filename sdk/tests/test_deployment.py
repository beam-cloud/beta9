from unittest import TestCase, mock
from unittest.mock import MagicMock

from beta9 import Image, Pod, asgi, endpoint, function, realtime, schedule, task_queue
from beta9.integrations import VLLM, VLLMArgs

PHI_VISION_INSTRUCT = "microsoft/Phi-3.5-vision-instruct"


class GatewayStubMock:
    def __init__(self, deployment_id):
        self.deployment_id = deployment_id

    def deploy_stub(self):
        return MagicMock(deployment_id=self.deployment_id, ok=True)


class TestDeployment(TestCase):
    def test_init(self):
        pass

    @mock.patch(
        "beta9.abstractions.function.Function.prepare_runtime",
        return_value=True,
    )
    @mock.patch(
        "beta9.abstractions.function.Function.gateway_stub",
        return_value=MagicMock(
            deploy_stub=MagicMock(
                return_value=MagicMock(deployment_id="test-deployment-id", ok=True)
            )
        ),
    )
    def test_function_deploy(self, gateway_stub_mock, prepare_runtime_mock):
        @function(name="test-func", cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_func():
            return 1

        resp, ok = test_func.deploy()

        self.assertEqual(ok, gateway_stub_mock.deploy_stub().ok)
        self.assertEqual(resp["deployment_id"], gateway_stub_mock.deploy_stub().deployment_id)

    @mock.patch(
        "beta9.abstractions.function.DeployableMixin.deploy",
        return_value=(MagicMock(), True),
    )
    @mock.patch(
        "beta9.abstractions.function.FunctionServiceStub.function_schedule",
        return_value=MagicMock(),
    )
    def test_schedule_deploy(
        self,
        function_schedule_mock,
        deploy_mock,
    ):
        @schedule(
            when="0 */6 * * *",
            name="test-schedule",
            cpu=1,
            memory=128,
            image=Image(python_version="python3.8"),
        )
        def test_schedule():
            return 1

        resp, ok = test_schedule.deploy()

        self.assertEqual(ok, True)
        self.assertEqual(
            resp["scheduled_job_id"], function_schedule_mock.return_value.scheduled_job_id
        )
        self.assertEqual(resp["deployment_id"], deploy_mock.return_value[0]["deployment_id"])

    @mock.patch(
        "beta9.abstractions.endpoint.Endpoint.prepare_runtime",
        return_value=True,
    )
    @mock.patch(
        "beta9.abstractions.endpoint.Endpoint.gateway_stub",
        return_value=MagicMock(
            deploy_stub=MagicMock(
                return_value=MagicMock(deployment_id="test-deployment-id", ok=True)
            )
        ),
    )
    def test_endpoint_deploy(self, gateway_stub_mock, prepare_runtime_mock):
        @endpoint(name="test-endpoint", cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_endpoint():
            return {"result": 1}

        resp, ok = test_endpoint.deploy()

        self.assertEqual(ok, gateway_stub_mock.deploy_stub().ok)
        self.assertEqual(resp["deployment_id"], gateway_stub_mock.deploy_stub().deployment_id)

    @mock.patch(
        "beta9.abstractions.endpoint.ASGI.prepare_runtime",
        return_value=True,
    )
    @mock.patch(
        "beta9.abstractions.endpoint.ASGI.gateway_stub",
        return_value=MagicMock(
            deploy_stub=MagicMock(
                return_value=MagicMock(deployment_id="test-deployment-id", ok=True)
            )
        ),
    )
    def test_asgi_deploy(self, gateway_stub_mock, prepare_runtime_mock):
        @asgi(name="test-asgi", cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_asgi_app(scope, receive, send):
            pass

        resp, ok = test_asgi_app.deploy()

        self.assertEqual(ok, gateway_stub_mock.deploy_stub().ok)
        self.assertEqual(resp["deployment_id"], gateway_stub_mock.deploy_stub().deployment_id)

    @mock.patch(
        "beta9.abstractions.endpoint.ASGI.prepare_runtime",
        return_value=True,
    )
    @mock.patch(
        "beta9.abstractions.endpoint.ASGI.gateway_stub",
        return_value=MagicMock(
            deploy_stub=MagicMock(
                return_value=MagicMock(deployment_id="test-deployment-id", ok=True)
            )
        ),
    )
    def test_realtime_asgi_deploy(self, gateway_stub_mock, prepare_runtime_mock):
        @realtime(name="test-realtime", cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_realtime_app(scope, receive, send):
            pass

        resp, ok = test_realtime_app.deploy()

        self.assertEqual(ok, gateway_stub_mock.deploy_stub().ok)
        self.assertEqual(resp["deployment_id"], gateway_stub_mock.deploy_stub().deployment_id)

    @mock.patch(
        "beta9.abstractions.taskqueue.TaskQueue.prepare_runtime",
        return_value=True,
    )
    @mock.patch(
        "beta9.abstractions.taskqueue.TaskQueue.gateway_stub",
        return_value=MagicMock(
            deploy_stub=MagicMock(
                return_value=MagicMock(deployment_id="test-deployment-id", ok=True)
            )
        ),
    )
    def test_task_queue_deploy(self, gateway_stub_mock, prepare_runtime_mock):
        @task_queue(name="test-queue", cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_task():
            return 1

        resp, ok = test_task.deploy()

        self.assertEqual(ok, gateway_stub_mock.deploy_stub().ok)
        self.assertEqual(resp["deployment_id"], gateway_stub_mock.deploy_stub().deployment_id)

    @mock.patch(
        "beta9.abstractions.pod.Pod.prepare_runtime",
        return_value=True,
    )
    @mock.patch(
        "beta9.abstractions.pod.Pod.gateway_stub",
        return_value=MagicMock(
            deploy_stub=MagicMock(
                return_value=MagicMock(deployment_id="test-deployment-id", ok=True)
            )
        ),
    )
    def test_pod_deploy(self, gateway_stub_mock, prepare_runtime_mock):
        test_pod = Pod(
            name="test-pod",
            cpu=1,
            memory=128,
            image=Image(python_version="python3.8"),
            entrypoint=["python", "app.py"],
        )

        resp, ok = test_pod.deploy()

        self.assertEqual(ok, gateway_stub_mock.deploy_stub().ok)
        self.assertEqual(resp["deployment_id"], gateway_stub_mock.deploy_stub().deployment_id)

    @mock.patch(
        "beta9.abstractions.integrations.vllm.VLLM.prepare_runtime",
        return_value=True,
    )
    @mock.patch(
        "beta9.abstractions.integrations.vllm.VLLM.gateway_stub",
        return_value=MagicMock(
            deploy_stub=MagicMock(
                return_value=MagicMock(
                    deployment_id="test-deployment-id",
                    ok=True,
                )
            )
        ),
    )
    def test_vllm_deploy(self, gateway_stub_mock, prepare_runtime_mock):
        test_vllm = VLLM(
            name=PHI_VISION_INSTRUCT.split("/")[-1],
            cpu=8,
            memory="16Gi",
            gpu="A100-40",
            vllm_args=VLLMArgs(
                model=PHI_VISION_INSTRUCT,
                served_model_name=[PHI_VISION_INSTRUCT],
                trust_remote_code=True,
                max_model_len=4096,
                limit_mm_per_prompt={"image": 2},
            ),
        )

        resp, ok = test_vllm.deploy()

        self.assertEqual(ok, gateway_stub_mock.deploy_stub().ok)
        self.assertEqual(resp["deployment_id"], gateway_stub_mock.deploy_stub().deployment_id)
