import warnings
from typing import Callable

from marshmallow import (
    EXCLUDE,
    Schema,
    ValidationError,
    fields,
    validate,
    validates_schema,
)

from beam.utils.parse import compose_cpu, compose_memory
from beam.v1 import validators
from beam.v1.type import AutoscalingType, GpuType, PythonVersion, TriggerType, VolumeType


def warning_on_one_line(message, category, filename, lineno, file=None, line=None):
    return "\033[93m\n{}: {}. Please update your configuration.\033[00m\n\n".format(
        category.__name__,
        message,
    )


warnings.formatwarning = warning_on_one_line


class DeprecatedNestedField(fields.Nested):
    def _validate(self, value):
        if self.metadata.get("deprecated"):
            warnings.warn(f"Field '{self.name}' is deprecated", stacklevel=2)
        super()._validate(value)


class SerializerMethod(fields.Field):
    def __init__(self, method: Callable, **kwargs):
        self.method = method
        super().__init__(**kwargs)

    def serialize(self, value, attr, obj, **kwargs):
        try:
            return self.method(value)
        except Exception as e:
            raise ValidationError(str(e))

    def _deserialize(self, value, attr, data, **kwargs):
        try:
            return self.method(value)
        except Exception as e:
            raise ValidationError(str(e))


class ConfigurationBase(Schema):
    class Meta:
        ordered = True
        unknown = EXCLUDE


class RequestLatencyAutoscalerConfiguration(ConfigurationBase):
    desired_latency = fields.Float(required=True)
    max_replicas = fields.Integer(
        required=True,
        validate=validate.Range(min=1, max=30),
    )


class QueueDepthAutoscalerConfiguration(ConfigurationBase):
    max_tasks_per_replica = fields.Integer(required=True)
    max_replicas = fields.Integer(
        required=True,
        validate=validate.Range(min=1, max=30),
    )


class AutoscalerConfiguration(ConfigurationBase):
    request_latency = fields.Nested(
        RequestLatencyAutoscalerConfiguration,
        required=False,
        allow_none=True,
    )
    queue_depth = fields.Nested(
        QueueDepthAutoscalerConfiguration,
        required=False,
        allow_none=True,
    )

    @validates_schema
    def check_nested(self, data, **kwargs):
        attrs = [
            data.get("queue_depth") is not None,
            data.get("request_latency") is not None,
        ]
        if attrs.count(True) != 1:
            raise ValidationError(
                "Exactly one of queue_depth or request_latency autoscaling strategies must be set."
            )


class AutoscalingConfiguration(ConfigurationBase):
    max_replicas = fields.Integer(required=True)
    desired_latency = fields.Float(required=True)
    autoscaling_type = fields.Enum(
        enum=AutoscalingType,
        by_value=True,
        validate=validate.OneOf(choices=[autoscaling.value for autoscaling in AutoscalingType]),
        required=True,
    )


class VolumeConfiguration(ConfigurationBase):
    name = fields.String(required=True)
    app_path = fields.String(required=True)
    mount_type = fields.Enum(
        enum=VolumeType,
        by_value=True,
        validate=validate.OneOf(choices=[mount.value for mount in VolumeType]),
        required=True,
    )


class OutputConfiguration(ConfigurationBase):
    path = fields.String(required=True)


class ImageConfiguration(ConfigurationBase):
    python_version = fields.Enum(
        enum=PythonVersion,
        by_value=True,
        validate=validate.OneOf(choices=[version.value for version in PythonVersion]),
        dump_default=PythonVersion.Python38,
    )
    python_packages = fields.List(fields.String(), dump_default=[])
    commands = fields.List(fields.String(), dump_default=[])
    base_image = fields.String(allow_none=True, dump_default=None)
    base_image_creds = fields.String(allow_none=True, dump_default=None)


class TaskPolicyConfiguration(ConfigurationBase):
    max_retries = fields.Integer(dump_default=3, required=True)
    timeout = fields.Integer(dump_default=3600, required=True)


class RuntimeConfiguration(ConfigurationBase):
    cpu = SerializerMethod(compose_cpu, required=True)
    memory = SerializerMethod(compose_memory, required=True)
    gpu = fields.Enum(
        enum=GpuType,
        by_value=True,
        validate=validate.OneOf(choices=[gpu.value for gpu in GpuType]),
    )
    image = fields.Nested(ImageConfiguration, required=True)


class RunConfiguration(ConfigurationBase):
    name = fields.String(allow_none=True)
    handler = fields.String(validate=validators.IsFileMethod(), required=True)
    callback_url = fields.Url(allow_none=True)
    outputs = fields.Nested(OutputConfiguration, dump_default=[], many=True)
    runtime = fields.Nested(RuntimeConfiguration, dump_default=None, allow_none=True)
    task_policy = fields.Nested(TaskPolicyConfiguration)


class TriggerConfiguration(ConfigurationBase):
    handler = fields.String(validate=validators.IsFileMethod(), required=True)
    loader = fields.String(
        validate=validators.IsFileMethod(),
        allow_none=True,
    )
    callback_url = fields.Url(
        allow_none=True,
    )
    max_pending_tasks = fields.Integer()
    keep_warm_seconds = fields.Integer()
    trigger_type = fields.Enum(
        enum=TriggerType,
        by_value=True,
        validate=validate.OneOf(choices=[trigger.value for trigger in TriggerType]),
        required=True,
    )
    path = fields.String()
    method = fields.String(validate=validate.OneOf(choices=["GET", "POST"]))
    runtime = fields.Nested(RuntimeConfiguration, dump_default=None, allow_none=True)
    outputs = fields.Nested(OutputConfiguration, dump_default=[], many=True)
    autoscaling = DeprecatedNestedField(
        AutoscalingConfiguration,
        dump_default=None,
        allow_none=True,
        metadata={"deprecated": True},
    )
    autoscaler = fields.Nested(AutoscalerConfiguration, dump_default=None, allow_none=True)
    when = fields.String(
        validate=validators.IsValidCronOrEvery(), dump_default=None, allow_none=True
    )
    task_policy = fields.Nested(TaskPolicyConfiguration)
    workers = fields.Integer()
    authorized = fields.Boolean()


class AppConfiguration(ConfigurationBase):
    app_spec_version = fields.String(required=True)
    sdk_version = fields.String(required=True)
    name = fields.String(required=True)
    runtime = fields.Nested(RuntimeConfiguration, dump_default=None, allow_none=True)
    mounts = fields.List(fields.Nested(VolumeConfiguration), dump_default=[])
    triggers = fields.Nested(TriggerConfiguration, dump_default=[], many=True)
    run = fields.Nested(RunConfiguration, dump_default=None, allow_none=True)
