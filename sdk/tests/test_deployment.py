from unittest import TestCase, mock
from unittest.mock import MagicMock

from beta9 import Image, Pod, asgi, endpoint, function, realtime, schedule, task_queue


class TestDeployment(TestCase):
    def test_init(self):
        pass

    @mock.patch(
        "beta9.abstractions.function.DeployableMixin.deploy",
        return_value=(MagicMock(), True),
    )
    def test_function_deploy(self, deploy_mock):
        @function(name="test-func", cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_func():
            return 1

        resp, ok = test_func.deploy()

        self.assertEqual(ok, True)
        self.assertEqual(resp["deployment_id"], deploy_mock.return_value[0]["deployment_id"])

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
        "beta9.abstractions.endpoint.DeployableMixin.deploy",
        return_value=(MagicMock(), True),
    )
    def test_endpoint_deploy(self, deploy_mock):
        @endpoint(name="test-endpoint", cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_endpoint():
            return {"result": 1}

        resp, ok = test_endpoint.deploy()

        self.assertEqual(ok, True)
        self.assertEqual(resp["deployment_id"], deploy_mock.return_value[0]["deployment_id"])

    @mock.patch(
        "beta9.abstractions.endpoint.DeployableMixin.deploy",
        return_value=(MagicMock(), True),
    )
    def test_asgi_deploy(self, deploy_mock):
        @asgi(name="test-asgi", cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_asgi_app(scope, receive, send):
            pass

        resp, ok = test_asgi_app.deploy()

        self.assertEqual(ok, True)
        self.assertEqual(resp["deployment_id"], deploy_mock.return_value[0]["deployment_id"])

    @mock.patch(
        "beta9.abstractions.endpoint.DeployableMixin.deploy",
        return_value=(MagicMock(), True),
    )
    def test_realtime_asgi_deploy(self, deploy_mock):
        @realtime(name="test-realtime", cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_realtime_app(scope, receive, send):
            pass

        resp, ok = test_realtime_app.deploy()

        self.assertEqual(ok, True)
        self.assertEqual(resp["deployment_id"], deploy_mock.return_value[0]["deployment_id"])

    @mock.patch(
        "beta9.abstractions.taskqueue.DeployableMixin.deploy",
        return_value=(MagicMock(), True),
    )
    def test_task_queue_deploy(self, deploy_mock):
        @task_queue(name="test-queue", cpu=1, memory=128, image=Image(python_version="python3.8"))
        def test_task():
            return 1

        resp, ok = test_task.deploy()

        self.assertEqual(ok, True)
        self.assertEqual(resp["deployment_id"], deploy_mock.return_value[0]["deployment_id"])

    @mock.patch(
        "beta9.abstractions.pod.Pod.deploy",
        return_value=(MagicMock(), True),
    )
    def test_pod_deploy(self, deploy_mock):
        test_pod = Pod(
            name="test-pod",
            cpu=1,
            memory=128,
            image=Image(python_version="python3.8"),
            entrypoint=["python", "app.py"],
        )

        resp, ok = test_pod.deploy()

        self.assertEqual(ok, True)
        self.assertEqual(resp["deployment_id"], deploy_mock.return_value[0]["deployment_id"])
