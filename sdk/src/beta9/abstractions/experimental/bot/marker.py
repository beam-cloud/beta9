import json
from typing import Any, Type, Union

from pydantic import BaseModel


class BotLocation:
    def __init__(self, *, marker: Union[Type[BaseModel], Any], expose: bool = True) -> None:
        if not issubclass(marker, BaseModel):
            raise TypeError("marker must be a subclass of a pydantic BaseModel")

        self.marker: Type[BaseModel] = marker
        self.expose: bool = expose
        self.name: str = marker.__name__

    def to_dict(self) -> dict:
        """
        Convert BotLocation instance to a dictionary.
        """
        marker_spec = {
            str(name): str(field.annotation) for name, field in self.marker.__fields__.items()
        }

        return {"name": self.name, "marker": marker_spec, "expose": self.expose}

    def to_json(self) -> str:
        """
        Convert BotLocation instance to a JSON string.
        """
        return json.dumps(self.to_dict(), indent=2)

    def __str__(self):
        """
        Return the class name as a string representation.
        """
        return self.marker.__name__

    def __hash__(self):
        """
        Use the class name for hashing.
        """
        return hash(self.marker.__name__)

    def __eq__(self, other):
        """
        Compare based on class name.
        """
        if isinstance(other, BotLocation):
            return self.marker.__name__ == other.marker.__name__
        return False
