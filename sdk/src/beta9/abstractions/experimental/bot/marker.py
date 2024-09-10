import json


class BotMarker:
    pass


class BotLocation:
    def __init__(self, *, name: str) -> None:
        self.name: str = name

    def to_dict(self) -> dict:
        """
        Convert BotLocation instance to a dictionary.
        """
        return {"name": self.name}

    def to_json(self) -> str:
        """
        Convert BotLocation instance to a JSON string.
        """
        return json.dumps(self.to_dict())
