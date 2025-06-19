import ast
import base64
import binascii
import inspect
import json
import os
import tempfile
import urllib.request
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple, Union
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
    """Base class for all schema fields."""

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
        """Create a SchemaField from a dictionary."""
        field_type = data["type"]
        field_classes = {
            "String": String,
            "Integer": Integer,
            "File": File,
            "Image": Image,
            "Object": Object,
            "JSON": JSON,
        }

        if field_type in ("Object", "object"):
            # Recursively build nested schema
            schema_cls = Schema.from_dict(data["fields"])
            return Object(schema_cls)
        elif field_type in ("JSON", "json"):
            return JSON()

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
    """Schema field for file-like objects, URLs, or base64 data."""

    def dump(self, value: Any) -> str:
        """Serialize the file and return a public URL."""
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
    """Schema field for validating and serializing images."""

    def __init__(
        self,
        max_size: Optional[Tuple[int, int]] = None,
        min_size: Optional[Tuple[int, int]] = None,
        allowed_formats: Optional[List[str]] = None,
        quality: int = 85,
        preserve_metadata: bool = False,
    ):
        """
        Initialize an Image schema field.

        Args:
            max_size: Optional (width, height) maximum.
            min_size: Optional (width, height) minimum.
            allowed_formats: Allowed image formats.
            quality: JPEG/WEBP quality (1-100).
            preserve_metadata: Preserve EXIF metadata if True.
        """
        self.max_size = max_size
        self.min_size = min_size
        self.allowed_formats = allowed_formats or ["PNG", "JPEG", "WEBP"]
        self.quality = max(1, min(100, quality))
        self.preserve_metadata = preserve_metadata

    def _import_pil(self):
        try:
            from PIL import Image as PILImage
            from PIL.ExifTags import TAGS

            return PILImage, TAGS
        except ImportError:
            raise ValidationError("Pillow library is not installed. Image schema is unavailable.")

    def _validate_dimensions(self, img):
        """Validate image dimensions against constraints."""
        width, height = img.size

        if self.max_size:
            max_w, max_h = self.max_size
            if width > max_w or height > max_h:
                raise ValidationError(
                    f"Image dimensions {width}x{height} exceed maximum allowed {max_w}x{max_h}"
                )

        if self.min_size:
            min_w, min_h = self.min_size
            if width < min_w or height < min_h:
                raise ValidationError(
                    f"Image dimensions {width}x{height} below minimum required {min_w}x{min_h}"
                )

    def _get_image_format(self, img):
        """Determine the best format for the image."""
        if img.format and img.format.upper() in self.allowed_formats:
            return img.format.upper()
        return "PNG"  # Default to PNG if format not in allowed list

    def _extract_metadata(self, img):
        """Extract image metadata if preservation is enabled."""
        if not self.preserve_metadata:
            return None

        metadata = {}
        if hasattr(img, "_getexif") and img._getexif():
            _, TAGS = self._import_pil()
            for tag_id, value in img._getexif().items():
                tag = TAGS.get(tag_id, tag_id)
                metadata[tag] = value
        return metadata

    def _is_url(self, value: str) -> bool:
        """Check if string is a valid URL."""
        try:
            result = urlparse(value)
            return all([result.scheme, result.netloc])
        except (ValueError, AttributeError):
            return False

    def _download_url(self, url: str) -> Any:
        """Download image from URL."""
        temp_file = tempfile.NamedTemporaryFile(delete=False)

        try:
            with urllib.request.urlopen(url) as response:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    temp_file.write(chunk)

            temp_file.close()
            PILImage, _ = self._import_pil()
            return PILImage.open(temp_file.name)
        except Exception as e:
            temp_file.close()
            os.unlink(temp_file.name)
            raise ValidationError(f"Failed to download image from URL: {e}")

    def validate(self, value: Any) -> Any:
        """Validate and process image input."""
        PILImage, _ = self._import_pil()

        # Handle PIL Image directly
        if isinstance(value, PILImage.Image):
            img = value
        # Handle file path
        elif isinstance(value, str) and os.path.isfile(value):
            try:
                img = PILImage.open(value)
            except Exception as e:
                raise ValidationError(f"Failed to open image file: {e}")
        # Handle URL
        elif isinstance(value, str) and self._is_url(value):
            img = self._download_url(value)
        # Handle base64
        elif isinstance(value, str):
            try:
                image_data = base64.b64decode(value)
                img = PILImage.open(BytesIO(image_data))
            except Exception as e:
                raise ValidationError(f"Invalid base64 image data: {e}")
        else:
            raise ValidationError(
                f"Expected PIL.Image, file path, URL, or base64 string, got {type(value).__name__}"
            )

        # Validate image format
        if img.format and img.format.upper() not in self.allowed_formats:
            raise ValidationError(
                f"Image format {img.format} not in allowed formats: {self.allowed_formats}"
            )

        # Validate dimensions
        self._validate_dimensions(img)

        # Store metadata if needed
        if self.preserve_metadata:
            img._metadata = self._extract_metadata(img)

        return img

    def dump(self, value: Any) -> str:
        """Serialize image to a file and return a public URL."""
        PILImage, _ = self._import_pil()
        if not isinstance(value, PILImage.Image):
            raise ValidationError(
                f"Expected PIL.Image for serialization, got {type(value).__name__}"
            )

        # Determine output format
        output_format = self._get_image_format(value)

        # Prepare save parameters
        save_params = {}
        if output_format == "JPEG":
            save_params["quality"] = self.quality
            save_params["optimize"] = True
        elif output_format == "WEBP":
            save_params["quality"] = self.quality
            save_params["lossless"] = False

        # Convert to RGB if needed (for JPEG)
        if output_format == "JPEG" and value.mode in ("RGBA", "LA"):
            value = value.convert("RGB")

        # Save to a temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{output_format.lower()}")
        try:
            value.save(temp_file, format=output_format, **save_params)
            temp_file.close()
            from .abstractions.output import Output

            with open(temp_file.name, "rb") as f:
                o = Output.from_file(f)
                o.save()
                return o.public_url()
        finally:
            try:
                os.unlink(temp_file.name)
            except Exception:
                pass

    def __del__(self):
        if hasattr(self, "_temp_file") and self._temp_file:
            try:
                self._temp_file.close()
                os.unlink(self._temp_file.name)
            except (OSError, IOError):
                pass


class Object(SchemaField):
    """Schema field for nested objects."""

    def __init__(self, schema_cls):
        self.schema_cls = schema_cls

    def validate(self, value: Any) -> Any:
        """Validate and load a nested object."""
        if isinstance(value, dict):
            return self.schema_cls(**value)
        elif isinstance(value, self.schema_cls):
            return value
        else:
            raise ValidationError(
                f"Expected dict or {self.schema_cls.__name__}, got {type(value).__name__}"
            )

    def dump(self, value: Any) -> Dict[str, Any]:
        """Serialize a nested object."""
        if isinstance(value, self.schema_cls):
            return value.dump()
        raise ValidationError(f"Expected {self.schema_cls.__name__} for serialization")


class JSON(SchemaField):
    """Schema field for JSON data."""

    def validate(self, value: Any) -> Any:
        """Validate and process JSON input."""

        # If value is already a dict/list, use it directly
        if isinstance(value, (dict, list, str, int, float, bool)) or value is None:
            return value
        elif isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError as e:
                raise ValidationError(f"Invalid JSON string: {e}")
        else:
            raise ValidationError(
                f"Expected dict, list, or JSON string, got {type(value).__name__}"
            )

    def dump(self, value: Any) -> Union[Dict[str, Any], List[Any]]:
        """Serialize JSON data."""
        if isinstance(value, (dict, list)):
            return value
        else:
            raise ValidationError(
                f"Expected dict or list for serialization, got {type(value).__name__}"
            )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {"type": "JSON"}


class SchemaMeta(type):
    """Metaclass for collecting schema fields."""

    def __new__(cls, name, bases, attrs):
        fields = {k: v for k, v in attrs.items() if isinstance(v, SchemaField)}
        attrs["_fields"] = fields
        return super().__new__(cls, name, bases, attrs)


class Schema(metaclass=SchemaMeta):
    """Base class for  input/output schemas."""

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
        """Create a new schema instance from a dictionary."""
        return cls(**data)

    def dump(self) -> Dict[str, Any]:
        """Serialize the schema to a dictionary."""
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
        """Return the schema definition as a plain dictionary."""
        result = {}

        for key in self._fields:
            value = getattr(self, key)
            if isinstance(value, Schema):
                result[key] = value.dict()
            else:
                result[key] = value

        return result

    def json(self) -> str:
        """Return the schema definition as a JSON string."""
        return json.dumps(self.dict())

    @classmethod
    def from_json(cls, json_str: str) -> "Schema":
        """Create a schema class from a JSON schema definition."""
        schema_config = json.loads(json_str)
        fields_config = schema_config.get("fields", {})
        attrs = {
            name: SchemaField.from_dict(field_dict) for name, field_dict in fields_config.items()
        }
        return SchemaMeta("DynamicSchema", (Schema,), attrs)

    @classmethod
    def to_json(cls) -> str:
        """Return the schema definition as a JSON string."""
        schema_config = {"fields": {name: field.to_dict() for name, field in cls._fields.items()}}
        return json.dumps(schema_config, indent=2)

    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        """Return the schema definition as a dictionary."""
        return {"fields": {name: field.to_dict() for name, field in cls._fields.items()}}

    @classmethod
    def from_dict(cls, schema_dict: Dict[str, Any]) -> "Schema":
        """Create a schema class from a dictionary definition."""
        fields_config = schema_dict.get("fields", {})
        attrs = {
            name: SchemaField.from_dict(field_dict) for name, field_dict in fields_config.items()
        }
        return SchemaMeta("DynamicSchema", (Schema,), attrs)

    @classmethod
    def object(cls, fields: dict) -> "Schema":
        """Dynamically create a schema class from a fields dictionary."""

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
            elif isinstance(v, JSON):
                # For JSON fields, use Union[Dict, List] as the annotation
                annotations[k] = Union[Dict[str, Any], List[Any]]
            else:
                annotations[k] = object

        attrs = dict(processed_fields)
        attrs["__annotations__"] = annotations
        return type(name, (cls,), attrs)
