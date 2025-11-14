from datetime import datetime, UTC
from typing import Any, Optional
from uuid import uuid4
from pydantic import BaseModel, Field

from .types import JobState, Priority, JobPayload


class JobOptions(BaseModel):
    max_retries: int = Field(default=3, ge=0)
    timeout: Optional[int] = Field(default=None, ge=1)
    priority: Priority = Field(default=Priority.NORMAL)
    delay: int = Field(default=0, ge=0)
    backoff_type: str = Field(default="exponential")
    backoff_delay: int = Field(default=1000, ge=0)


class Job(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    payload: JobPayload
    state: JobState = Field(default=JobState.WAITING)
    attempts: int = Field(default=0, ge=0)
    options: JobOptions = Field(default_factory=JobOptions)

    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None

    error: Optional[str] = None
    result: Optional[Any] = None
    progress: int = Field(default=0, ge=0, le=100)

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, data: str) -> "Job":
        return cls.model_validate_json(data)

    def mark_active(self) -> None:
        self.state = JobState.ACTIVE
        if self.started_at is None:
            self.started_at = datetime.now(UTC)
        self.attempts += 1

    def mark_completed(self, result: Any = None) -> None:
        self.state = JobState.COMPLETED
        self.completed_at = datetime.now(UTC)
        self.result = result
        self.progress = 100

    def mark_failed(self, error: str) -> None:
        self.state = JobState.FAILED
        self.failed_at = datetime.now(UTC)
        self.error = error

    def should_retry(self) -> bool:
        return self.attempts <= self.options.max_retries

    def is_exhausted(self) -> bool:
        return self.attempts > self.options.max_retries

    def update_progress(self, progress: int) -> None:
        self.progress = max(0, min(100, progress))
