import base64
import json
from io import BytesIO
from typing import Any, Dict


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

    def serialize(self, value: Any) -> Any:
        return value

    def deserialize(self, value: Any) -> Any:
        return value

    def to_dict(self) -> Dict[str, Any]:
        return {"type": self.__class__.__name__}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchemaField":
        field_type = data["type"]
        field_classes = {
            "String": String,
            "Integer": Integer,
            "File": File,
            "Image": Image,
        }
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
    def validate(self, value: Any) -> Any:
        if not hasattr(value, "read"):
            raise ValidationError("Expected file-like object")
        return value


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

    def serialize(self, value: Any) -> str:
        PILImage = self._import_pil()
        if not isinstance(value, PILImage.Image):
            raise ValidationError(
                f"Expected PIL.Image for serialization, got {type(value).__name__}"
            )
        buffered = BytesIO()
        value.save(buffered, format="PNG")
        return base64.b64encode(buffered.getvalue()).decode("utf-8")

    def deserialize(self, value: str) -> Any:
        PILImage = self._import_pil()
        try:
            image_data = base64.b64decode(value)
            return PILImage.open(BytesIO(image_data))
        except Exception as e:
            raise ValidationError(f"Invalid base64 image data: {e}")


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
        self._data = validated  # Optionally keep the dict

    @classmethod
    def validate(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        validated = {}
        for key, field in cls._fields.items():
            if key not in data:
                raise ValidationError(f"Missing required field '{key}'")
            validated[key] = field.validate(data[key])
        return validated

    @classmethod
    def parse(cls, data: Dict[str, Any]) -> "Schema":
        return cls(**data)

    def dict(self) -> Dict[str, Any]:
        return {key: getattr(self, key) for key in self._fields}

    def json(self) -> str:
        return json.dumps(self.dict())

    @classmethod
    def from_json(cls, json_str: str) -> "Schema":
        schema_config = json.loads(json_str)
        fields_config = schema_config.get("fields", {})
        attrs = {
            name: SchemaField.from_dict(field_dict) for name, field_dict in fields_config.items()
        }
        print(f"Attrs: {attrs}")
        # Dynamically create a new Schema subclass with these fields
        return SchemaMeta("DynamicSchema", (Schema,), attrs)

    @classmethod
    def serialize(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        return {key: field.serialize(data[key]) for key, field in cls._fields.items()}

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        return {key: field.deserialize(data[key]) for key, field in cls._fields.items()}

    @classmethod
    def to_json(cls) -> str:
        schema_config = {"fields": {name: field.to_dict() for name, field in cls._fields.items()}}
        return json.dumps(schema_config, indent=2)

    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        return {
            "fields": {
                name: {"type": field.__class__.__name__} for name, field in cls._fields.items()
            }
        }

    @classmethod
    def from_dict(cls, schema_dict: Dict[str, Any]) -> "Schema":
        fields_config = schema_dict.get("fields", {})
        attrs = {
            name: SchemaField.from_dict(field_dict) for name, field_dict in fields_config.items()
        }
        return SchemaMeta("DynamicSchema", (Schema,), attrs)
