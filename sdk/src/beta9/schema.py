import ast
import base64
import binascii
import inspect
import json
import os
import tempfile
import urllib.request
from io import BytesIO
from typing import Any, Dict
from urllib.parse import urlparse


class ValidationError(Exception):
    def __init__(self, message, field=None):
        super().__init__(message)
        self.message = message
        self.field = field

    def to_dict(self):
        error = {"error": "ValidationError", "message": self.message}

        if self.field:
            error["field"] = self.field

        return error


class SchemaField:
    def validate(self, value: Any) -> Any:
        raise NotImplementedError()

    def dump(self, value: Any) -> Any:
        return value

    def to_dict(self) -> Dict[str, Any]:
        d = {"type": self.__class__.__name__}
        if isinstance(self, Object):
            d["fields"] = self.schema_cls.to_dict()
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchemaField":
        field_type = data["type"]
        field_classes = {
            "String": String,
            "Integer": Integer,
            "File": File,
            "Image": Image,
            "Object": Object,
        }

        if field_type in ("Object", "object"):
            # Recursively build nested schema
            schema_cls = Schema.from_dict(data["fields"])
            return Object(schema_cls)

        if field_type not in field_classes:
            raise ValidationError(f"Unknown field type '{field_type}'")

        return field_classes[field_type]()


class String(SchemaField):
    def validate(self, value: Any) -> str:
        if not isinstance(value, str):
            raise ValidationError(f"Expected string, got {type(value).__name__}")
        return value


class Integer(SchemaField):
    def validate(self, value: Any) -> int:
        if not isinstance(value, int):
            raise ValidationError(f"Expected integer, got {type(value).__name__}")
        return value


class File(SchemaField):
    def dump(self, value: Any) -> str:
        from .abstractions.output import Output

        o = Output.from_file(value)
        o.save()
        return o.public_url()

    def _is_url(self, value: str) -> bool:
        try:
            result = urlparse(value)
            return all([result.scheme, result.netloc])
        except (ValueError, AttributeError):
            return False

    def _download_url(self, url: str) -> Any:
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            with urllib.request.urlopen(url) as response:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    temp_file.write(chunk)

            temp_file.close()

            return open(temp_file.name, "rb")
        except (urllib.error.URLError, urllib.error.HTTPError, IOError) as e:
            temp_file.close()
            os.unlink(temp_file.name)
            raise ValidationError(f"Failed to download file from URL: {e}")

    def _decode_base64(self, value: str) -> Any:
        try:
            temp_file = tempfile.NamedTemporaryFile(delete=False)

            try:
                decoded_data = base64.b64decode(value)
                temp_file.write(decoded_data)
                temp_file.close()
                return open(temp_file.name, "rb")
            except (IOError, OSError) as e:
                temp_file.close()
                os.unlink(temp_file.name)
                raise ValidationError(f"Failed to decode base64 data: {e}")
        except (ValueError, binascii.Error) as e:
            raise ValidationError(f"Invalid base64 data: {e}")

    def validate(self, value: Any) -> Any:
        if hasattr(value, "read"):
            return value

        if isinstance(value, str):
            # Check if it's a URL first
            if self._is_url(value):
                return self._download_url(value)

            # If not a URL, try to decode as base64
            try:
                return self._decode_base64(value)
            except ValidationError:
                raise ValidationError(
                    "String input must be either a valid URL or base64 encoded data"
                )

        raise ValidationError(
            "Input must be a file-like object, URL string, or base64 encoded string"
        )

    def __del__(self):
        if hasattr(self, "_temp_file") and self._temp_file:
            try:
                self._temp_file.close()
                os.unlink(self._temp_file.name)
            except (OSError, IOError):
                pass


class Image(SchemaField):
    def _import_pil(self):
        try:
            from PIL import Image as PILImage

            return PILImage
        except ImportError:
            raise ValidationError("Pillow library is not installed. Image schema is unavailable.")

    def validate(self, value: Any) -> Any:
        PILImage = self._import_pil()
        if isinstance(value, PILImage.Image):
            return value
        elif isinstance(value, str):
            try:
                image_data = base64.b64decode(value)
                return PILImage.open(BytesIO(image_data))
            except Exception as e:
                raise ValidationError(f"Invalid base64 image data: {e}")
        else:
            raise ValidationError(
                f"Expected PIL.Image or base64 string, got {type(value).__name__}"
            )

    def dump(self, value: Any) -> str:
        PILImage = self._import_pil()
        if not isinstance(value, PILImage.Image):
            raise ValidationError(
                f"Expected PIL.Image for serialization, got {type(value).__name__}"
            )
        buffered = BytesIO()
        value.save(buffered, format="PNG")
        return base64.b64encode(buffered.getvalue()).decode("utf-8")


class Object(SchemaField):
    def __init__(self, schema_cls):
        self.schema_cls = schema_cls

    def validate(self, value: Any) -> Any:
        if isinstance(value, dict):
            return self.schema_cls(**value)
        elif isinstance(value, self.schema_cls):
            return value
        else:
            raise ValidationError(
                f"Expected dict or {self.schema_cls.__name__}, got {type(value).__name__}"
            )

    def dump(self, value: Any) -> Dict[str, Any]:
        if isinstance(value, self.schema_cls):
            return value.dump()
        raise ValidationError(f"Expected {self.schema_cls.__name__} for serialization")


class SchemaMeta(type):
    def __new__(cls, name, bases, attrs):
        fields = {k: v for k, v in attrs.items() if isinstance(v, SchemaField)}
        attrs["_fields"] = fields
        return super().__new__(cls, name, bases, attrs)


class Schema(metaclass=SchemaMeta):
    def __init__(self, **kwargs):
        validated = self.validate(kwargs)

        for key, value in validated.items():
            setattr(self, key, value)

        self._data = validated

    @classmethod
    def validate(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        validated = {}
        for key, field in cls._fields.items():
            if key not in data:
                raise ValidationError(f"Missing required field '{key}'")
            validated[key] = field.validate(data[key])
        return validated

    @classmethod
    def new(cls, data: Dict[str, Any]) -> "Schema":
        return cls(**data)

    def dump(self) -> Dict[str, Any]:
        result = {}

        for key in self._fields:
            value = getattr(self, key)
            if isinstance(value, Schema):
                result[key] = value.dump()
            else:
                field = self._fields[key]
                result[key] = field.dump(value)

        return result

    def dict(self) -> Dict[str, Any]:
        result = {}

        for key in self._fields:
            value = getattr(self, key)
            if isinstance(value, Schema):
                result[key] = value.dict()
            else:
                result[key] = value

        return result

    def json(self) -> str:
        return json.dumps(self.dict())

    @classmethod
    def from_json(cls, json_str: str) -> "Schema":
        schema_config = json.loads(json_str)
        fields_config = schema_config.get("fields", {})
        attrs = {
            name: SchemaField.from_dict(field_dict) for name, field_dict in fields_config.items()
        }
        return SchemaMeta("DynamicSchema", (Schema,), attrs)

    @classmethod
    def to_json(cls) -> str:
        schema_config = {"fields": {name: field.to_dict() for name, field in cls._fields.items()}}
        return json.dumps(schema_config, indent=2)

    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        return {"fields": {name: field.to_dict() for name, field in cls._fields.items()}}

    @classmethod
    def from_dict(cls, schema_dict: Dict[str, Any]) -> "Schema":
        fields_config = schema_dict.get("fields", {})
        attrs = {
            name: SchemaField.from_dict(field_dict) for name, field_dict in fields_config.items()
        }
        return SchemaMeta("DynamicSchema", (Schema,), attrs)

    @classmethod
    def object(cls, fields: dict) -> "Schema":
        """
        Dynamically create a Schema with the given fields and type annotations.
        The class name is inferred from the assignment context if possible.
        """

        # Try to infer the name from the calling context
        try:
            frame = inspect.currentframe().f_back
            line = inspect.getframeinfo(frame).code_context[0]
            tree = ast.parse(line)
            assign = tree.body[0]
            if isinstance(assign, ast.Assign):
                name = assign.targets[0].id
            else:
                name = "DynamicSchema"
        except Exception:
            name = "DynamicSchema"

        processed_fields = {}
        for k, v in fields.items():
            if isinstance(v, dict):
                # Recursively create a nested schema and wrap in Object
                nested_schema = Schema.object(v)
                processed_fields[k] = Object(nested_schema)
            elif isinstance(v, type) and issubclass(v, Schema):
                processed_fields[k] = Object(v)
            else:
                processed_fields[k] = v

        # Build __annotations__ for editor support
        annotations = {}
        for k, v in processed_fields.items():
            if isinstance(v, String):
                annotations[k] = str
            elif isinstance(v, Integer):
                annotations[k] = int
            elif isinstance(v, File):
                annotations[k] = bytes
            elif isinstance(v, Image):
                try:
                    from PIL import Image as PILImage

                    annotations[k] = PILImage.Image
                except ImportError:
                    annotations[k] = object
            elif isinstance(v, Object):
                annotations[k] = v.schema_cls
            else:
                annotations[k] = object

        attrs = dict(processed_fields)
        attrs["__annotations__"] = annotations
        return type(name, (cls,), attrs)
