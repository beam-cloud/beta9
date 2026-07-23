from importlib import import_module


_EXPORTS = {
    "Map": (".abstractions.map", "Map"),
    "Image": (".abstractions.image", "Image"),
    "Queue": (".abstractions.queue", "SimpleQueue"),
    "Volume": (".abstractions.volume", "Volume"),
    "CloudBucket": (".abstractions.volume", "CloudBucket"),
    "CloudBucketConfig": (".abstractions.volume", "CloudBucketConfig"),
    "task_queue": (".abstractions.taskqueue", "TaskQueue"),
    "function": (".abstractions.function", "Function"),
    "endpoint": (".abstractions.endpoint", "Endpoint"),
    "asgi": (".abstractions.endpoint", "ASGI"),
    "realtime": (".abstractions.endpoint", "RealtimeASGI"),
    "Container": (".abstractions.base.container", "Container"),
    "env": (".env", None),
    "GpuType": (".type", "GpuType"),
    "DatabaseServingConfig": (".type", "DatabaseServingConfig"),
    "DurableDisk": (".type", "DurableDisk"),
    "LLMConfig": (".type", "LLMConfig"),
    "LLMTokenPressureAutoscaler": (".type", "LLMTokenPressureAutoscaler"),
    "Pool": (".type", "Pool"),
    "PythonVersion": (".type", "PythonVersion"),
    "Output": (".abstractions.output", "Output"),
    "QueueDepthAutoscaler": (".type", "QueueDepthAutoscaler"),
    "ServingConfig": (".type", "ServingConfig"),
    "experimental": (".abstractions.experimental", None),
    "integrations": (".abstractions.integrations", None),
    "schedule": (".abstractions.function", "Schedule"),
    "TaskPolicy": (".type", "TaskPolicy"),
    "Bot": (".abstractions.experimental.bot.bot", "Bot"),
    "BotLocation": (".abstractions.experimental.bot.bot", "BotLocation"),
    "BotEventType": (".abstractions.experimental.bot.bot", "BotEventType"),
    "BotContext": (".abstractions.experimental.bot.types", "BotContext"),
    "Pod": (".abstractions.pod", "Pod"),
    "Service": (".abstractions.service", "Service"),
    "PricingPolicy": (".type", "PricingPolicy"),
    "PricingPolicyCostModel": (".type", "PricingPolicyCostModel"),
    "Client": (".client.client", "Client"),
    "Task": (".client.task", "Task"),
    "Deployment": (".client.deployment", "Deployment"),
    "schema": (".schema", None),
    "Sandbox": (".abstractions.sandbox", "Sandbox"),
    "SandboxInstance": (".abstractions.sandbox", "SandboxInstance"),
    "SandboxProcess": (".abstractions.sandbox", "SandboxProcess"),
    "SandboxProcessStream": (".abstractions.sandbox", "SandboxProcessStream"),
    "SandboxProcessManager": (".abstractions.sandbox", "SandboxProcessManager"),
    "SandboxProcessResponse": (".abstractions.sandbox", "SandboxProcessResponse"),
    "SandboxConnectionError": (".abstractions.sandbox", "SandboxConnectionError"),
    "SandboxProcessError": (".abstractions.sandbox", "SandboxProcessError"),
    "SandboxFileSystemError": (".abstractions.sandbox", "SandboxFileSystemError"),
    "SandboxFilePosition": (".abstractions.sandbox", "SandboxFilePosition"),
    "SandboxFileSearchRange": (".abstractions.sandbox", "SandboxFileSearchRange"),
    "SandboxFileSearchMatch": (".abstractions.sandbox", "SandboxFileSearchMatch"),
    "SandboxFileInfo": (".abstractions.sandbox", "SandboxFileInfo"),
    "SandboxFileSystem": (".abstractions.sandbox", "SandboxFileSystem"),
    "SandboxFileSearchResult": (".abstractions.sandbox", "SandboxFileSearchResult"),
}

__all__ = list(_EXPORTS)


def __getattr__(name):
    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError as error:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from error

    module = import_module(module_name, __name__)
    value = module if attribute is None else getattr(module, attribute)
    globals()[name] = value
    return value


def __dir__():
    return sorted((*globals(), *__all__))
