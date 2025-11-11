from typing import Optional, Any
import time

from .connection import RedisConnection
from .job import Job, JobOptions, JobState, JobPayload
from .types import Priority, Serializer
from .exceptions import JobNotFoundError
from .utils.logger import get_logger
from .utils.serializer import JSONSerializer


class TaskQueue:
    def __init__(
        self,
        connection: RedisConnection,
        name: str = "default",
        serializer: Optional[Serializer] = None,
    ):
        self.redis = connection.client
        self.name = name
        self.serializer = serializer or JSONSerializer()
        self.logger = get_logger(f"queue.{name}")
        self._init_keys()

    def _init_keys(self):
        self.waiting_key = f"queue:{self.name}:waiting"
        self.active_key = f"queue:{self.name}:active"
        self.delayed_key = f"queue:{self.name}:delayed"
        self.completed_key = f"queue:{self.name}:completed"
        self.failed_key = f"queue:{self.name}:failed"
        self.priority_keys = {
            Priority.CRITICAL: f"queue:{self.name}:priority:critical",
            Priority.HIGH: f"queue:{self.name}:priority:high",
            Priority.NORMAL: f"queue:{self.name}:priority:normal",
            Priority.LOW: f"queue:{self.name}:priority:low",
        }

    def add(
        self, name: str, payload: JobPayload, options: Optional[JobOptions] = None
    ) -> Job:
        opts = options or JobOptions()
        job = Job(name=name, payload=payload, options=opts)

        serialized_data = self.serializer.serialize(job.model_dump(mode="json"))

        with self.redis.pipeline() as pipe:
            pipe.set(self._job_key(job.id), serialized_data)

            if opts.delay > 0:
                pipe.zadd(self.delayed_key, {job.id: time.time() + opts.delay})
                job.state = JobState.DELAYED
            elif opts.priority != Priority.NORMAL:
                pipe.rpush(self.priority_keys[opts.priority], job.id)
            else:
                pipe.rpush(self.waiting_key, job.id)

            pipe.execute()

        self.logger.info(
            "Job added", job_id=job.id, job_name=name, priority=opts.priority.name
        )
        return job

    def get_next_job(self, timeout: int = 1) -> Optional[Job]:
        self._move_delayed_to_waiting()

        for priority in [
            Priority.CRITICAL,
            Priority.HIGH,
            Priority.NORMAL,
            Priority.LOW,
        ]:
            job_id = self.redis.lpop(self.priority_keys[priority])
            if job_id:
                return self._activate_job(job_id)

        result = self.redis.blpop(self.waiting_key, timeout)
        if result:
            return self._activate_job(result[1])
        return None

    def complete(self, job: Job, result: Any = None) -> None:
        job.mark_completed(result)
        self._update_job_state(job, self.completed_key)
        self.logger.info("Job completed", job_id=job.id, job_name=job.name)

    def fail(self, job: Job, error: str) -> None:
        job.mark_failed(error)
        self._update_job_state(job, self.failed_key)
        self.logger.error("Job failed", job_id=job.id, error=error)

    def retry(self, job: Job, delay: int) -> None:
        job.state = JobState.RETRYING
        with self.redis.pipeline() as pipe:
            pipe.set(self._job_key(job.id), job.to_json())
            pipe.lrem(self.active_key, 1, job.id)
            pipe.zadd(self.delayed_key, {job.id: time.time() + delay / 1000})
            pipe.execute()
        self.logger.info("Job retry", job_id=job.id, attempt=job.attempts, delay=delay)

    def get_job(self, job_id: str) -> Job:
        data = self.redis.get(self._job_key(job_id))
        if not data:
            raise JobNotFoundError(f"Job {job_id} not found")
        job_dict = self.serializer.deserialize(data)
        return Job.model_validate(job_dict)

    def update_progress(self, job_id: str, progress: int) -> None:
        job = self.get_job(job_id)
        job.update_progress(progress)
        self.redis.set(self._job_key(job.id), job.to_json())

    def get_counts(self) -> dict[str, int]:
        with self.redis.pipeline() as pipe:
            pipe.llen(self.waiting_key)
            pipe.llen(self.active_key)
            pipe.zcard(self.delayed_key)
            pipe.zcard(self.completed_key)
            pipe.zcard(self.failed_key)
            for key in self.priority_keys.values():
                pipe.llen(key)
            results = pipe.execute()

        return {
            "waiting": results[0],
            "active": results[1],
            "delayed": results[2],
            "completed": results[3],
            "failed": results[4],
            "priority_critical": results[5],
            "priority_high": results[6],
            "priority_normal": results[7],
            "priority_low": results[8],
        }

    def clean(self, grace_period: int = 86400) -> int:
        cutoff = time.time() - grace_period
        completed = self.redis.zrangebyscore(self.completed_key, 0, cutoff)
        failed = self.redis.zrangebyscore(self.failed_key, 0, cutoff)

        if completed or failed:
            with self.redis.pipeline() as pipe:
                for job_id in completed + failed:
                    pipe.delete(self._job_key(job_id))
                pipe.zremrangebyscore(self.completed_key, 0, cutoff)
                pipe.zremrangebyscore(self.failed_key, 0, cutoff)
                pipe.execute()

        return len(completed) + len(failed)

    def pause(self) -> None:
        self.redis.set(f"queue:{self.name}:paused", "1")

    def resume(self) -> None:
        self.redis.delete(f"queue:{self.name}:paused")

    def is_paused(self) -> bool:
        return bool(self.redis.get(f"queue:{self.name}:paused"))

    def _activate_job(self, job_id: str) -> Optional[Job]:
        data = self.redis.get(self._job_key(job_id))
        if not data:
            return None

        job = Job.from_json(data)
        job.mark_active()

        with self.redis.pipeline() as pipe:
            pipe.set(self._job_key(job.id), job.to_json())
            pipe.rpush(self.active_key, job.id)
            pipe.execute()

        return job

    def _update_job_state(self, job: Job, target_key: str) -> None:
        with self.redis.pipeline() as pipe:
            pipe.set(self._job_key(job.id), job.to_json())
            pipe.lrem(self.active_key, 1, job.id)
            pipe.zadd(target_key, {job.id: time.time()})
            pipe.execute()

    def _move_delayed_to_waiting(self) -> None:
        delayed = self.redis.zrangebyscore(self.delayed_key, 0, time.time())
        if delayed:
            with self.redis.pipeline() as pipe:
                for job_id in delayed:
                    pipe.rpush(self.waiting_key, job_id)
                    pipe.zrem(self.delayed_key, job_id)
                pipe.execute()

    def _job_key(self, job_id: str) -> str:
        return f"queue:{self.name}:job:{job_id}"
