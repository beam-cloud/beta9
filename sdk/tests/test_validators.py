import pytest

from beam import validators


def test_every():
    with pytest.raises(validators.ValidationError):
        validators.IsValidEvery()("every")

    with pytest.raises(validators.ValidationError):
        validators.IsValidEvery()("every 1")

    with pytest.raises(validators.ValidationError):
        validators.IsValidEvery()("every 1 s")

    with pytest.raises(validators.ValidationError):
        validators.IsValidEvery()("every 1 unknown-s")

    for time_values in [1, 10, 100]:
        for unit in validators.IsValidEvery.time_units:
            # should not fail
            validators.IsValidEvery()(f"every {time_values}{unit}")
