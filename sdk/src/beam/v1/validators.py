from typing import Any, Callable, Type

import validators
from croniter import croniter
from marshmallow import ValidationError
from marshmallow.validate import Validator


class IsValidURL(Validator):
    def _validate(self, value: str):
        if not validators.url(value):  # type: ignore
            raise ValidationError(f"'{value}' - Invalid URL")

    def __call__(self, value: str) -> str:
        self._validate(value)
        return value


class IsValidEvery(Validator):
    time_units = ("ms", "s", "m", "h")

    def _validate(self, value: str) -> None:
        stripped_values = value.strip().lower().split()

        if not stripped_values[0] == "every":
            raise ValueError("Not an 'every' expression")

        if len(stripped_values) != 2:
            raise ValidationError("Expression format should be 'every <number><unit>'")

        time_string = stripped_values[1]

        time_unit = None
        for unit in self.time_units:
            if time_string.endswith(unit):
                time_unit = unit
                break

        if time_unit is None:
            raise ValidationError(
                "An 'every' query must end with a valid time unit:" + " 'ms', 's', 'm', 'h'"
            )

        time_number = time_string[: -len(time_unit)]

        if not time_number.isnumeric():
            raise ValidationError("An 'every' query must have a positive number")

        numerical_value = int(time_number)

        if int(numerical_value) <= 0:
            raise ValidationError("An 'every' query must have a positive number")

    def __call__(self, value: str) -> str:
        self._validate(value)
        return value


class IsValidCron(Validator):
    def _validate(self, value: str):
        if not croniter.is_valid(value):
            raise ValidationError(f"'{value}' - Invalid cron expression")

    def __call__(self, value: str) -> str:
        self._validate(value)
        return value


class IsValidCronOrEvery(Validator):
    def _validate(self, value: str):
        # The only exceptions that are forwarded upwards are validation errors
        try:
            return IsValidEvery()(value)
        except ValueError:
            # If it is a value error, that means it is not a 'every' expression
            pass

        try:
            IsValidCron()(value)
        except ValidationError:
            raise ValidationError(f"'{value}' - Invalid cron or 'every' expression")

    def __call__(self, value: str) -> str:
        self._validate(value)
        return value


class IsInstanceOf(Validator):
    def __init__(self, instanceType: Type):
        self.instanceType = instanceType

    def __call__(self, value: Any) -> Any:
        if isinstance(value, self.instanceType):
            return value

        raise ValidationError([f"needs to be type {self.instanceType}"])


class IsFileMethod(Validator):
    def __init__(self) -> None:
        super().__init__()

    def _raise_validation_error(self):
        raise ValidationError("must be a string in form {pathToFile}:{method}")

    def _validate(self, handler: str):
        if not isinstance(handler, str):
            self._raise_validation_error()

        parsed_handler = handler.split(":")

        if len(parsed_handler) != 2:
            self._raise_validation_error()

    def __call__(self, value: str) -> str:
        self._validate(value)
        return value


class SerializerMethod(Validator):
    def __init__(self, serializer: Callable) -> None:
        self.serializer = serializer

    def __call__(self, value: Any) -> Any:
        return self.serializer(value)
