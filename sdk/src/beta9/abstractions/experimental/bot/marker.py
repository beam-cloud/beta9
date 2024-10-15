import json
from dataclasses import dataclass, fields, is_dataclass
from typing import Type, get_type_hints


class BotLocation:
    def __init__(self, *, marker: Type[dataclass]) -> None:
        if not is_dataclass(marker):
            raise TypeError("marker must be a dataclass")

        self.marker: Type[dataclass] = marker
        self.name: str = marker.__name__

    def to_dict(self) -> dict:
        """
        Convert BotLocation instance to a dictionary.
        """
        type_hints = get_type_hints(self.marker)
        marker_spec = {field.name: type_hints[field.name].__name__ for field in fields(self.marker)}

        return {"name": self.name, "marker": marker_spec}

    def to_json(self) -> str:
        """
        Convert BotLocation instance to a JSON string.
        """
        return json.dumps(self.to_dict(), indent=2)
