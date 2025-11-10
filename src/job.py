from dataclasses import dataclass, asdict
import json, time
from typing import Any


@dataclass
class Job:
    id: str
    name: str
    payload: Any
    retries: int = 0
    max_retries: int = 3
    created_at: float = 0.0

    def __post_init__(self):
        if not self.created_at:
            self.created_at = time.time()

    def to_json(self):
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(data: str):
        return Job(**json.loads(data))
