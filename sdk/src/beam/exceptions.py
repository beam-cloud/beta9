import os

import grpclib


class ConnectionError(Exception):
    pass


def grpc_debug():
    debug = bool(os.environ.get("GRPC_DEBUG", False))

    def catch_and_suppress_grpc_error(func):
        def func_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (
                grpclib.exceptions.GRPCError,
                grpclib.exceptions.StreamTerminatedError,
            ):
                raise ConnectionError from None

        return func_wrapper

    def decorator(cls):
        if debug:
            return cls

        for attr, value in cls.__dict__.items():
            if callable(value) and not attr.startswith("__"):
                setattr(cls, attr, catch_and_suppress_grpc_error(value))

        return cls

    return decorator


class RunnerException(SystemExit):
    pass
