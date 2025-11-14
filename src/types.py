from enum import Enum
from typing import Any, Callable, Protocol, TypeAlias

JobPayload: TypeAlias = dict[str, Any]
ProcessorFunc: TypeAlias = Callable[[JobPayload], None]


class JobState(str, Enum):
    WAITING = "waiting"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    DELAYED = "delayed"
    RETRYING = "retrying"
    DEAD_LETTER = "dead_letter"


class Priority(int, Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class RetryStrategy(Protocol):
    def calculate_delay(self, attempt: int) -> int: ...


class RateLimiter(Protocol):
    def acquire(self, key: str) -> bool: ...

    def release(self, key: str) -> None: ...


class Serializer(Protocol):
    def serialize(self, obj: Any) -> str: ...

    def deserialize(self, data: str) -> Any: ...
