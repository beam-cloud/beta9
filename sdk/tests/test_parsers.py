import pytest

from beam.utils.parse import compose_cpu, compose_memory


class TestParsers:
    def test_compose_cpu(self):
        assert compose_cpu(1) == "1000m"
        assert compose_cpu(1.999) == "1999m"
        assert compose_cpu("1000m") == "1000m"

    def test_compose_memory(self):
        assert compose_memory("10gi") == "10Gi"
        assert compose_memory("10 gi ") == "10Gi"
        assert compose_memory("10 Gi ") == "10Gi"

        # Raises if Gi is > 256
        with pytest.raises(ValueError):
            compose_memory("10000Gi")

        assert compose_memory("256mi") == "256Mi"
        assert compose_memory("256 mi ") == "256Mi"
        assert compose_memory("256 Mi ") == "256Mi"

        # Raises if Mi is < 128 or >= 1000
        with pytest.raises(ValueError):
            compose_memory("127Mi")

        with pytest.raises(ValueError):
            compose_memory("1000Mi")

        # Test invalid formats
        with pytest.raises(ValueError):
            compose_memory("1000ti")

        with pytest.raises(ValueError):
            compose_memory("1000.0")

        with pytest.raises(ValueError):
            compose_memory("1000")

        with pytest.raises(ValueError):
            compose_memory("2 gigabytes")
