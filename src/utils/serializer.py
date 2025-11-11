import json
import pickle
from typing import Any

from ..exceptions import SerializationError


class JSONSerializer:
    def serialize(self, obj: Any) -> str:
        try:
            return json.dumps(obj)
        except (TypeError, ValueError) as e:
            raise SerializationError(f"Failed to serialize: {e}") from e

    def deserialize(self, data: str) -> Any:
        try:
            return json.loads(data)
        except (json.JSONDecodeError, ValueError) as e:
            raise SerializationError(f"Failed to deserialize: {e}") from e


class PickleSerializer:
    def serialize(self, obj: Any) -> str:
        try:
            return pickle.dumps(obj).hex()
        except Exception as e:
            raise SerializationError(f"Failed to serialize: {e}") from e

    def deserialize(self, data: str) -> Any:
        try:
            return pickle.loads(bytes.fromhex(data))
        except Exception as e:
            raise SerializationError(f"Failed to deserialize: {e}") from e
