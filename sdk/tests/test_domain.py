import json
import sys
from types import SimpleNamespace
from unittest import TestCase, mock
from unittest.mock import MagicMock

from click.testing import CliRunner

sys.modules.setdefault("websocket", MagicMock())

from beta9 import Image, Pod, Service, asgi, endpoint, function, realtime, task_queue
from beta9.channel import GatewayHTTPError
from beta9.cli import deployment as deployment_cli
from beta9.cli import domain as domain_cli
from beta9.cli import extraclick
from beta9.cli.main import load_cli


class FakeHTTPClient:
    def __init__(self, handler):
        self._handler = handler
        self.calls = []

    def json(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        return self._handler(method, path, **kwargs)


class FakeServiceClient:
    def __init__(self, http_client):
        self.channel = SimpleNamespace(config=SimpleNamespace(token="tok"))
        self.http = http_client

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class TestDomainCLI(TestCase):
    def _invoke(self, args, handler):
        http_client = FakeHTTPClient(handler)
        runner = CliRunner()
        with (
            mock.patch.object(
                extraclick,
                "get_config_context",
                return_value=SimpleNamespace(token="tok", gateway_host="localhost", gateway_port=1993),
            ),
            mock.patch.object(
                extraclick,
                "ServiceClient",
                return_value=FakeServiceClient(http_client),
            ),
        ):
            result = runner.invoke(domain_cli.management, args)
        return result, http_client.calls

    def test_domain_management_commands_registered(self):
        cli = load_cli(check_config=False)

        domain_group = cli.management_group.get_command(None, "domain")
        self.assertIsNotNone(domain_group)
        self.assertEqual(
            sorted(domain_group.commands),
            ["add", "delete", "get", "list", "remove", "show", "verify"],
        )

        deploy_cmd = cli.common_group.get_command(None, "deploy")
        self.assertIn("custom_domain", {p.name for p in deploy_cmd.params})

    def test_add_domain_json_and_table(self):
        payload = {
            "id": "dom-123",
            "domain": "api.customer.io",
            "deployment_name": "my-api",
            "status": "pending",
            "verification_token": "beta9-verify-abc",
            "txt_record_name": "_beta9-challenge.api.customer.io",
            "cname_target": "my-api-1234567.app.example.com",
            "url": "https://api.customer.io",
        }

        result, calls = self._invoke(
            [
                "add",
                "api.customer.io",
                "--deployment",
                "my-api",
                "--port",
                "8000",
                "--format",
                "json",
            ],
            lambda method, path, **kwargs: payload,
        )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(calls[0][0], "POST")
        self.assertEqual(calls[0][1], "/api/v1/domain/{ws}")
        self.assertEqual(
            calls[0][2]["json"],
            {
                "domain": "api.customer.io",
                "deployment_name": "my-api",
                "deployment_id": "",
                "stub_type": "",
                "version": 0,
                "port": 8000,
            },
        )
        self.assertEqual(json.loads(result.output)["domain"], "api.customer.io")

        table_result, _ = self._invoke(
            ["add", "api.customer.io", "--deployment", "my-api"],
            lambda method, path, **kwargs: payload,
        )
        self.assertEqual(table_result.exit_code, 0, table_result.output)
        self.assertIn("api.customer.io", table_result.output)
        self.assertIn("beta9-verify-abc", table_result.output)

    def test_add_domain_requires_deployment_target(self):
        result, calls = self._invoke(
            ["add", "api.customer.io"],
            lambda *args, **kwargs: {},
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertEqual(calls, [])

    def test_list_get_verify_and_remove_domains(self):
        sample = {
            "id": "dom-123",
            "domain": "api.customer.io",
            "deployment_name": "my-api",
            "status": "active",
            "version": 2,
            "port": 8080,
            "url": "https://api.customer.io",
            "created_at": "2026-11-18T17:00:00Z",
        }

        list_res, list_calls = self._invoke(
            ["list", "--deployment", "my-api", "--format", "json"],
            lambda *args, **kwargs: [sample],
        )
        self.assertEqual(list_res.exit_code, 0, list_res.output)
        self.assertEqual(list_calls[0][2]["params"], {"deployment_name": "my-api"})
        self.assertEqual(len(json.loads(list_res.output)), 1)

        show_res, show_calls = self._invoke(
            ["show", "api.customer.io", "--format", "json"],
            lambda *args, **kwargs: sample,
        )
        self.assertEqual(show_res.exit_code, 0, show_res.output)
        self.assertEqual(show_calls[0][1], "/api/v1/domain/{ws}/api.customer.io")

        verify_res, verify_calls = self._invoke(
            ["verify", "api.customer.io", "--format", "json"],
            lambda *args, **kwargs: sample,
        )
        self.assertEqual(verify_res.exit_code, 0, verify_res.output)
        self.assertEqual(verify_calls[0][0], "POST")
        self.assertEqual(verify_calls[0][1], "/api/v1/domain/{ws}/api.customer.io/verify")
        self.assertNotIn("json", verify_calls[0][2])

        del_res, del_calls = self._invoke(
            ["remove", "api.customer.io", "--yes"],
            lambda *args, **kwargs: None,
        )
        self.assertEqual(del_res.exit_code, 0, del_res.output)
        self.assertEqual(del_calls[0][0], "DELETE")
        self.assertEqual(del_calls[0][1], "/api/v1/domain/{ws}/api.customer.io")

        del_json_res, del_json_calls = self._invoke(
            ["remove", "api.customer.io", "--yes", "--format", "json"],
            lambda *args, **kwargs: None,
        )
        self.assertEqual(del_json_res.exit_code, 0, del_json_res.output)
        self.assertEqual(json.loads(del_json_res.output), {"deleted": "api.customer.io"})
        self.assertEqual(del_json_calls[0][0], "DELETE")

        del_no_yes_res, del_no_yes_calls = self._invoke(
            ["remove", "api.customer.io", "--format", "json"],
            lambda *args, **kwargs: None,
        )
        self.assertNotEqual(del_no_yes_res.exit_code, 0)
        self.assertEqual(del_no_yes_calls, [])
        self.assertEqual(json.loads(del_no_yes_res.output)["code"], "NEEDS_CONFIRMATION")

    def test_api_http_error_exits_nonzero(self):
        def fail(*_args, **_kwargs):
            raise GatewayHTTPError(409, "custom domain is already registered")

        result, _ = self._invoke(
            ["add", "api.customer.io", "-d", "my-api"],
            fail,
        )
        self.assertEqual(result.exit_code, 1)


class TestDomainSDKAbstractions(TestCase):
    def test_endpoint_asgi_realtime_pod_and_service_accept_custom_domain(self):
        @endpoint(name="ep-app", custom_domain="API.Example.COM.")
        def my_endpoint():
            return {"ok": True}

        @asgi(name="asgi-app", custom_domain=["a.example.com", "b.example.com", "a.example.com"])
        def my_asgi(scope, receive, send):
            pass

        @realtime(name="ws-app", custom_domain="ws.example.com")
        def my_realtime(ctx):
            return []

        my_pod = Pod(
            name="pod-app",
            entrypoint=["python", "app.py"],
            ports=[8000],
            custom_domain="pod.example.com",
        )
        my_service = Service(
            name="svc-app",
            image=Image.from_id("img-123"),
            ports=[8000],
            custom_domain="svc.example.com",
        )

        self.assertEqual(my_endpoint.parent.custom_domain, ["api.example.com"])
        self.assertEqual(my_asgi.parent.custom_domain, ["a.example.com", "b.example.com"])
        self.assertEqual(my_realtime.parent.custom_domain, ["ws.example.com"])
        self.assertEqual(my_pod.custom_domain, ["pod.example.com"])
        self.assertEqual(my_service.custom_domain, ["svc.example.com"])

        stub_req = my_endpoint.parent._stub_request(
            stub_type="endpoint/deployment",
            stub_name="ep-app",
            force_create_stub=True,
            autoscaler_type="queue_depth",
            inputs=None,
            outputs=None,
        )
        self.assertEqual(json.loads(stub_req.extra)["custom_domain"], ["api.example.com"])

    @mock.patch("beta9.abstractions.endpoint.Endpoint.prepare_runtime", return_value=True)
    @mock.patch(
        "beta9.abstractions.endpoint.Endpoint.gateway_stub",
        new_callable=mock.PropertyMock,
        return_value=MagicMock(
            deploy_stub=MagicMock(
                return_value=MagicMock(
                    deployment_id="dep-1",
                    ok=True,
                    invoke_url="https://app.example.com/endpoint/ep-app/v1",
                    version=1,
                    warn_msg="",
                    rollout_action="created",
                )
            ),
            get_url=MagicMock(
                return_value=MagicMock(ok=True, url="https://ep-app-1234567.app.example.com")
            ),
        ),
    )
    def test_endpoint_deploy_reports_custom_domains(self, _gw_mock, _prep_mock):
        @endpoint(name="ep-app", custom_domain="api.example.com")
        def my_endpoint():
            return {"ok": True}

        resp, ok = my_endpoint.deploy()
        self.assertTrue(ok)
        self.assertEqual(resp["custom_domains"], ["api.example.com"])

    @mock.patch("beta9.abstractions.pod.Pod.prepare_runtime", return_value=True)
    @mock.patch(
        "beta9.abstractions.pod.Pod.gateway_stub",
        new_callable=mock.PropertyMock,
        return_value=MagicMock(
            deploy_stub=MagicMock(
                return_value=MagicMock(
                    deployment_id="dep-2",
                    ok=True,
                    invoke_url="https://app.example.com/pod/pod-app/latest/8000",
                    version=1,
                    warn_msg="",
                    rollout_action="created",
                )
            ),
            get_url=MagicMock(
                return_value=MagicMock(ok=True, url="https://pod-app-1234567-latest-8000.app.example.com")
            ),
        ),
    )
    def test_pod_deploy_reports_custom_domains(self, _gw_mock, _prep_mock):
        pod = Pod(
            name="pod-app",
            entrypoint=["python", "app.py"],
            ports=[8000],
            custom_domain="pod.example.com",
        )

        resp, ok = pod.deploy()
        self.assertTrue(ok)
        self.assertEqual(resp["custom_domains"], ["pod.example.com"])

    def test_cli_override_and_service_generation_forward_custom_domain(self):
        @endpoint(name="ep-app")
        def my_endpoint():
            return {"ok": True}

        ok = extraclick.handle_config_override(
            my_endpoint,
            {"custom_domain": (["cli1.example.com", "cli2.example.com"],)},
        )
        self.assertTrue(ok)
        self.assertEqual(my_endpoint.parent.custom_domain, ["cli1.example.com", "cli2.example.com"])

        service = deployment_cli._generate_service_module(
            "web-svc",
            {
                "entrypoint": ["python", "-m", "http.server", "8000"],
                "ports": [8000],
                "custom_domain": (["svc1.example.com"],),
            },
        )
        self.assertEqual(service.custom_domain, ["svc1.example.com"])

    def test_custom_domain_rejects_tcp_pod_sets_no_ports_and_unsupported_types(self):
        with self.assertRaises(ValueError):
            Pod(
                name="tcp-pod",
                entrypoint=["python", "app.py"],
                ports=[5432],
                tcp=True,
                custom_domain="db.example.com",
            )

        @endpoint(name="ep-set")
        def ep_set():
            return {}

        with self.assertRaises(ValueError):
            ep_set.parent.parse_custom_domain({"a.example.com"})

        @function(name="fn-app")
        def my_fn():
            return 1

        @task_queue(name="tq-app")
        def my_tq():
            return 1

        self.assertFalse(
            extraclick.handle_config_override(my_fn, {"custom_domain": (["fn.example.com"],)})
        )
        self.assertFalse(
            extraclick.handle_config_override(my_tq, {"custom_domain": (["tq.example.com"],)})
        )

        tcp_pod = Pod(name="tcp-pod", entrypoint=["python", "app.py"], ports=[5432], tcp=True)
        self.assertFalse(
            extraclick.handle_config_override(tcp_pod, {"custom_domain": (["db.example.com"],)})
        )

        portless_pod = Pod(name="portless-pod", entrypoint=["python", "app.py"])
        self.assertFalse(
            extraclick.handle_config_override(
                portless_pod, {"custom_domain": (["worker.example.com"],)}
            )
        )
        self.assertTrue(
            extraclick.handle_config_override(
                portless_pod,
                {"ports": [8000], "custom_domain": (["pod.example.com"],)},
            )
        )

        portless_custom = Pod(
            name="portless-custom",
            entrypoint=["python", "app.py"],
            custom_domain="worker.example.com",
        )
        resp, ok = portless_custom.deploy()
        self.assertFalse(ok)
        self.assertEqual(resp, {})

        with self.assertRaises(ValueError):
            deployment_cli._generate_service_module(
                "worker-svc",
                {
                    "entrypoint": ["python", "worker.py"],
                    "no_ports": True,
                    "custom_domain": (["worker.example.com"],),
                },
            )

    @mock.patch("beta9.abstractions.endpoint.Endpoint.prepare_runtime", return_value=True)
    def test_print_invocation_snippet_hides_custom_domain_when_not_deployed_and_failed_deploy(self, _prep_mock):
        @endpoint(name="ep-serve", custom_domain="serve.example.com")
        def ep_serve():
            return {}

        ep_serve.parent.stub_id = "stub-1"
        gw_mock = MagicMock(
            get_url=MagicMock(
                return_value=MagicMock(ok=True, url="https://app.example.com/endpoint/id/stub-1")
            ),
            deploy_stub=MagicMock(
                return_value=MagicMock(
                    deployment_id="",
                    ok=False,
                    invoke_url="",
                    version=0,
                    warn_msg="",
                    err_msg="custom domain rejected",
                    rollout_action="",
                )
            ),
        )
        with mock.patch.object(
            type(ep_serve.parent), "gateway_stub", new_callable=mock.PropertyMock, return_value=gw_mock
        ):
            with mock.patch("beta9.terminal.resource") as resource_mock:
                ep_serve.parent.print_invocation_snippet()
                resource_mock.assert_called_once_with(
                    "Endpoint", {"URL": "https://app.example.com/endpoint/id/stub-1"}
                )

            resp, ok = ep_serve.deploy()
            self.assertFalse(ok)
            self.assertNotIn("custom_domains", resp)
