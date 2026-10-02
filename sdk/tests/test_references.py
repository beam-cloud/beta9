import pytest

from beta9.references import validate_expression


@pytest.mark.parametrize(
    "expr",
    [
        "app.web.URL",
        "app.clickhouse.URL.8123",
        "app.clickhouse.TCP.9000",
        "app.clickhouse.TCP.65535",
        "app.my.dotted.app.URL.1",
    ],
)
def test_app_references_the_gateway_resolves(expr):
    assert validate_expression(expr) == []


@pytest.mark.parametrize(
    "expr",
    [
        "app.clickhouse.TCP.65536",
        "app.clickhouse.URL.99999",
        "app.clickhouse.URL.0",
        "app.clickhouse.TCP",
        "app.web.url",
        "app..URL",
    ],
)
def test_app_references_the_gateway_rejects(expr):
    assert len(validate_expression(expr)) == 1
