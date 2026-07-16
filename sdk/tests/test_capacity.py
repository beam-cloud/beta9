import io
from unittest import TestCase, mock
from unittest.mock import MagicMock

from beta9 import terminal
from beta9.abstractions.base.capacity import (
    CAPACITY_STATUS_AVAILABLE,
    CAPACITY_STATUS_NONE,
    DEFAULT_ONDEMAND_TTL,
    INDEFINITE_TTL,
    handle_capacity_verdict,
    requested_gpus,
    select_ttl,
    ttl_label,
    valid_ttl,
    wait_for_pool_ready,
)
from beta9.clients.gateway import (
    GetOrCreateStubRequest,
    GetOrCreateStubResponse,
    ListPoolOffersResponse,
    ListPrivatePoolsResponse,
    PoolOffer,
    PrivatePool,
)


def _runner(pool_config=None, headless=False):
    runner = MagicMock()
    runner.pool_config = pool_config
    runner.headless = headless
    return runner


class TestRequestedGpus(TestCase):
    def test_splits_comma_separated_gpus(self):
        req = GetOrCreateStubRequest(gpu="A6000,T4")
        self.assertEqual(requested_gpus(req), ["A6000", "T4"])

    def test_empty_for_cpu_only(self):
        self.assertEqual(requested_gpus(GetOrCreateStubRequest(gpu="")), [])


class TestHandleCapacityVerdict(TestCase):
    def test_passes_through_when_capacity_available(self):
        runner = _runner()
        response = GetOrCreateStubResponse(
            ok=True, stub_id="stub-1", capacity_status=CAPACITY_STATUS_AVAILABLE
        )

        result = handle_capacity_verdict(
            runner, GetOrCreateStubRequest(gpu="T4"), response, "function"
        )

        self.assertIs(result, response)
        runner.gateway_stub.get_or_create_stub.assert_not_called()

    def test_passes_through_when_user_targeted_a_pool(self):
        runner = _runner(pool_config=MagicMock())
        response = GetOrCreateStubResponse(
            ok=True, stub_id="stub-1", capacity_status=CAPACITY_STATUS_NONE
        )

        result = handle_capacity_verdict(
            runner, GetOrCreateStubRequest(gpu="A6000"), response, "function"
        )

        self.assertIs(result, response)

    def test_headless_no_capacity_fails_with_hint(self):
        runner = _runner(headless=True)
        response = GetOrCreateStubResponse(
            ok=True, stub_id="stub-1", capacity_status=CAPACITY_STATUS_NONE
        )

        with mock.patch.object(terminal, "error") as error_mock:
            result = handle_capacity_verdict(
                runner, GetOrCreateStubRequest(gpu="A6000"), response, "function"
            )

        self.assertIsNone(result)
        error_mock.assert_called_once()
        self.assertIn("A6000", error_mock.call_args.args[0])
        # The hint must be a single short, copy-pasteable command.
        hint = error_mock.call_args.kwargs.get("hint")
        self.assertIn("beta9 machine reserve --gpu A6000", hint)
        self.assertLess(len(hint), 100)

    def test_non_tty_no_capacity_fails(self):
        runner = _runner()
        response = GetOrCreateStubResponse(
            ok=True, stub_id="stub-1", capacity_status=CAPACITY_STATUS_NONE
        )

        with mock.patch.object(terminal, "is_interactive", return_value=False):
            with mock.patch.object(terminal, "error") as error_mock:
                result = handle_capacity_verdict(
                    runner, GetOrCreateStubRequest(gpu="A6000"), response, "function"
                )

        self.assertIsNone(result)
        error_mock.assert_called_once()

    def test_matched_private_pool_reissues_stub_with_pool(self):
        runner = _runner()
        reissued = GetOrCreateStubResponse(ok=True, stub_id="stub-2")
        runner.gateway_stub.get_or_create_stub.return_value = reissued
        request = GetOrCreateStubRequest(gpu="A6000")
        response = GetOrCreateStubResponse(
            ok=True,
            stub_id="stub-1",
            capacity_status=CAPACITY_STATUS_NONE,
            matched_private_pool="ondemand-a6000",
        )

        result = handle_capacity_verdict(runner, request, response, "function")

        self.assertIs(result, reissued)
        self.assertEqual(request.pool.name, "ondemand-a6000")
        self.assertEqual(request.pool.selector, "ondemand-a6000")
        self.assertEqual(runner.pool_config, request.pool)

    def test_deployment_warns_and_proceeds_headless(self):
        runner = _runner()
        response = GetOrCreateStubResponse(
            ok=True, stub_id="stub-1", capacity_status=CAPACITY_STATUS_NONE
        )

        with mock.patch.object(terminal, "is_interactive", return_value=False):
            with mock.patch.object(terminal, "warn") as warn_mock:
                result = handle_capacity_verdict(
                    runner, GetOrCreateStubRequest(gpu="A6000"), response, "endpoint/deployment"
                )

        self.assertIs(result, response)
        warn_mock.assert_called_once()

    def test_deployment_interactive_cancel_exits_cleanly(self):
        runner = _runner()
        response = GetOrCreateStubResponse(
            ok=True, stub_id="stub-1", capacity_status=CAPACITY_STATUS_NONE
        )

        # Cancelling is not a failure: exit code 0, no error output.
        with mock.patch.object(terminal, "is_interactive", return_value=True):
            with mock.patch.object(terminal, "select", return_value="cancel"):
                with mock.patch.object(terminal, "error") as error_mock:
                    with self.assertRaises(SystemExit) as exit_ctx:
                        handle_capacity_verdict(
                            runner,
                            GetOrCreateStubRequest(gpu="A6000"),
                            response,
                            "endpoint/deployment",
                        )

        self.assertEqual(exit_ctx.exception.code, 0)
        error_mock.assert_not_called()

    def test_deployment_interactive_deploy_anyway_proceeds(self):
        runner = _runner()
        response = GetOrCreateStubResponse(
            ok=True, stub_id="stub-1", capacity_status=CAPACITY_STATUS_NONE
        )

        with mock.patch.object(terminal, "is_interactive", return_value=True):
            with mock.patch.object(terminal, "select", return_value="deploy"):
                result = handle_capacity_verdict(
                    runner, GetOrCreateStubRequest(gpu="A6000"), response, "endpoint/deployment"
                )

        self.assertIs(result, response)

    def test_deployment_interactive_reserve_pins_stub_to_ondemand_pool(self):
        runner = _runner()
        offer = PoolOffer(
            id="offer-1", provider="vast", gpu="A6000", gpu_count=1, hourly_cost_micros=450_000
        )
        reissued = GetOrCreateStubResponse(ok=True, stub_id="stub-2")
        runner.gateway_stub.list_pool_offers.return_value = ListPoolOffersResponse(
            ok=True, offers=[offer]
        )
        runner.gateway_stub.get_or_create_stub.return_value = reissued
        request = GetOrCreateStubRequest(gpu="A6000")
        response = GetOrCreateStubResponse(
            ok=True, stub_id="stub-1", capacity_status=CAPACITY_STATUS_NONE
        )

        # First select: "reserve"; then offer picker; then duration.
        with mock.patch.object(terminal, "is_interactive", return_value=True):
            with mock.patch.object(terminal, "select", side_effect=["reserve", offer, "8h"]):
                with mock.patch.object(terminal, "confirm", return_value=True):
                    with mock.patch(
                        "beta9.abstractions.base.capacity.wait_for_pool_ready",
                        return_value=True,
                    ):
                        result = handle_capacity_verdict(
                            runner, request, response, "endpoint/deployment"
                        )

        # The deployment stub is re-issued pinned to the reserved pool.
        self.assertIs(result, reissued)
        self.assertEqual(request.pool.name, "ondemand-a6000")
        self.assertEqual(request.pool.selector, "ondemand-a6000")
        self.assertEqual(request.pool.ttl, "8h")
        self.assertEqual(runner.pool_config, request.pool)

    def test_interactive_tui_launches_pool_and_reissues_stub(self):
        runner = _runner()
        offer = PoolOffer(
            id="offer-1",
            provider="vast",
            gpu="A6000",
            gpu_count=1,
            hourly_cost_micros=450_000,
            region="us-east",
        )
        reissued = GetOrCreateStubResponse(ok=True, stub_id="stub-2")
        runner.gateway_stub.list_pool_offers.return_value = ListPoolOffersResponse(
            ok=True, offers=[offer]
        )
        runner.gateway_stub.get_or_create_stub.return_value = reissued
        request = GetOrCreateStubRequest(gpu="A6000")
        response = GetOrCreateStubResponse(
            ok=True, stub_id="stub-1", capacity_status=CAPACITY_STATUS_NONE
        )

        with mock.patch.object(terminal, "is_interactive", return_value=True):
            with mock.patch.object(terminal, "select", side_effect=[offer, "2h"]):
                with mock.patch.object(terminal, "confirm", return_value=True):
                    with mock.patch(
                        "beta9.abstractions.base.capacity.wait_for_pool_ready",
                        return_value=True,
                    ):
                        result = handle_capacity_verdict(runner, request, response, "function")

        self.assertIs(result, reissued)
        self.assertEqual(request.pool.gpu, ["A6000"])
        self.assertEqual(request.pool.nodes, 1)
        self.assertEqual(request.pool.ttl, "2h")
        self.assertEqual(request.pool.offer_id, "offer-1")
        self.assertEqual(request.pool.providers, ["vast"])
        self.assertEqual(request.pool.name, "ondemand-a6000")
        self.assertGreater(request.pool.max_spend, 0)
        self.assertEqual(runner.pool_config, request.pool)

    def test_interactive_tui_confirm_decline_aborts(self):
        runner = _runner()
        offer = PoolOffer(id="offer-1", provider="vast", gpu="A6000", hourly_cost_micros=450_000)
        runner.gateway_stub.list_pool_offers.return_value = ListPoolOffersResponse(
            ok=True, offers=[offer]
        )
        response = GetOrCreateStubResponse(
            ok=True, stub_id="stub-1", capacity_status=CAPACITY_STATUS_NONE
        )

        with mock.patch.object(terminal, "is_interactive", return_value=True):
            with mock.patch.object(terminal, "select", side_effect=[offer, "1h"]):
                with mock.patch.object(terminal, "confirm", return_value=False):
                    result = handle_capacity_verdict(
                        runner, GetOrCreateStubRequest(gpu="A6000"), response, "function"
                    )

        self.assertIsNone(result)
        runner.gateway_stub.get_or_create_stub.assert_not_called()

    def test_interactive_tui_no_offers_fails_with_hint(self):
        runner = _runner()
        runner.gateway_stub.list_pool_offers.return_value = ListPoolOffersResponse(
            ok=True, offers=[]
        )
        response = GetOrCreateStubResponse(
            ok=True, stub_id="stub-1", capacity_status=CAPACITY_STATUS_NONE
        )

        with mock.patch.object(terminal, "is_interactive", return_value=True):
            with mock.patch.object(terminal, "error") as error_mock:
                result = handle_capacity_verdict(
                    runner, GetOrCreateStubRequest(gpu="A6000"), response, "function"
                )

        self.assertIsNone(result)
        error_mock.assert_called_once()


class TestWaitForPoolReady(TestCase):
    def test_returns_true_when_machine_ready(self):
        gateway_stub = MagicMock()
        gateway_stub.list_private_pools.return_value = ListPrivatePoolsResponse(
            ok=True,
            pools=[
                PrivatePool(
                    name="ondemand-a6000",
                    machine_count=1,
                    ready_machine_count=1,
                    reserved_nodes=1,
                )
            ],
        )

        self.assertTrue(wait_for_pool_ready(gateway_stub, "ondemand-a6000", timeout_s=10))

    def test_times_out_when_pool_never_ready(self):
        gateway_stub = MagicMock()
        gateway_stub.list_private_pools.return_value = ListPrivatePoolsResponse(ok=True, pools=[])

        with mock.patch("beta9.abstractions.base.capacity.PROVISION_POLL_INTERVAL_S", 0):
            with mock.patch.object(terminal, "error") as error_mock:
                self.assertFalse(wait_for_pool_ready(gateway_stub, "ondemand-a6000", timeout_s=0.2))

        error_mock.assert_called_once()


class TestTtlSelection(TestCase):
    def test_valid_ttl(self):
        self.assertTrue(valid_ttl("45m"))
        self.assertTrue(valid_ttl("6h"))
        self.assertTrue(valid_ttl("2d"))
        self.assertFalse(valid_ttl("1"))
        self.assertFalse(valid_ttl("0h"))
        self.assertFalse(valid_ttl("abc"))
        self.assertFalse(valid_ttl(""))

    def test_select_ttl_non_interactive_uses_default(self):
        with mock.patch.object(terminal, "is_interactive", return_value=False):
            self.assertEqual(select_ttl(1.0), DEFAULT_ONDEMAND_TTL)

    def test_select_ttl_picks_choice(self):
        with mock.patch.object(terminal, "is_interactive", return_value=True):
            with mock.patch.object(terminal, "select", return_value="8h"):
                self.assertEqual(select_ttl(1.0), "8h")

    def test_select_ttl_custom_duration_reprompts_until_valid(self):
        with mock.patch.object(terminal, "is_interactive", return_value=True):
            with mock.patch.object(terminal, "select", return_value=""):
                with mock.patch.object(terminal, "prompt", side_effect=["nope", "36h"]):
                    self.assertEqual(select_ttl(1.0), "36h")

    def test_select_ttl_indefinite_option(self):
        def pick_indefinite(title, options):
            values = [o.value for o in options]
            self.assertIn(INDEFINITE_TTL, values)
            return INDEFINITE_TTL

        with mock.patch.object(terminal, "is_interactive", return_value=True):
            with mock.patch.object(terminal, "select", side_effect=pick_indefinite):
                self.assertEqual(select_ttl(1.0), INDEFINITE_TTL)

    def test_ttl_label(self):
        self.assertEqual(ttl_label("6h"), "expires in 6h")
        self.assertIn("until you release it", ttl_label(INDEFINITE_TTL))


class TestSelectFallback(TestCase):
    def test_numbered_fallback_when_not_interactive(self):
        options = [
            terminal.SelectOption(label="first", value=1),
            terminal.SelectOption(label="second", value=2),
        ]

        with mock.patch.object(terminal, "is_interactive", return_value=False):
            with mock.patch.object(terminal, "prompt", return_value="2"):
                result = terminal.select("Pick one", options)

        self.assertEqual(result, 2)

    def test_numbered_fallback_uses_default_on_empty_input(self):
        options = [
            terminal.SelectOption(label="first", value="a"),
            terminal.SelectOption(label="second", value="b"),
        ]

        # terminal.prompt returns the default when the user presses enter.
        with mock.patch.object(terminal, "is_interactive", return_value=False):
            with mock.patch.object(terminal, "prompt", side_effect=lambda **kw: kw["default"]):
                result = terminal.select("Pick one", options)

        self.assertEqual(result, "a")


class TestConfirmFallback(TestCase):
    def test_line_input_fallback(self):
        with mock.patch.object(terminal, "is_interactive", return_value=False):
            with mock.patch.object(terminal, "prompt", return_value="y"):
                self.assertTrue(terminal.confirm("Proceed?"))

        with mock.patch.object(terminal, "is_interactive", return_value=False):
            with mock.patch.object(terminal, "prompt", return_value="n"):
                self.assertFalse(terminal.confirm("Proceed?"))


class TestStepTracker(TestCase):
    def test_prints_check_on_success_and_cross_on_failure(self):
        buffer = io.StringIO()
        with terminal.redirect_terminal_to_buffer(buffer):
            tracker = terminal.StepTracker()
            with tracker.step("Building image", "Image ready"):
                pass
            with tracker.step("Syncing files") as step:
                step.ok = False

        output = buffer.getvalue()
        self.assertIn("✓ Image ready", output)
        self.assertIn("✗ Syncing files", output)
