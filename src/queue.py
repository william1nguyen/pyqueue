from typing import Optional, Any
import time

from .connection import RedisConnection
from .job import Job, JobOptions, JobState, JobPayload
from .types import Priority
from .exceptions import JobNotFoundError
from .utils.logger import get_logger


class TaskQueue:
    def __init__(self, connection: RedisConnection, name: str = "default"):
        self.connection = connection
        self.name = name
        self.redis = connection.client
        self.logger = get_logger(f"queue.{name}")

        self.waiting_key = f"queue:{name}:waiting"
        self.active_key = f"queue:{name}:active"
        self.delayed_key = f"queue:{name}:delayed"
        self.completed_key = f"queue:{name}:completed"
        self.failed_key = f"queue:{name}:failed"
        self.job_key_prefix = f"queue:{name}:job:"

        self.priority_keys = {
            Priority.CRITICAL: f"queue:{name}:priority:critical",
            Priority.HIGH: f"queue:{name}:priority:high",
            Priority.NORMAL: f"queue:{name}:priority:normal",
            Priority.LOW: f"queue:{name}:priority:low",
        }

    def add(
        self,
        name: str,
        payload: JobPayload,
        options: Optional[JobOptions],
    ) -> Job:
        opts = options or JobOptions()
        job = Job(name=name, payload=payload, options=opts)

        pipe = self.redis.pipeline()
        pipe.set(self._job_key(job.id), job.to_json())

        if opts.delay > 0:
            execute_at = time.time() + opts.delay
            pipe.zadd(self.delayed_key, {job.id: execute_at})
            job.state = JobState.DELAYED

        elif opts.priority != Priority.NORMAL:
            priority_key = self.priority_keys[opts.priority]
            pipe.rpush(priority_key, job.id)

        else:
            pipe.rpush(self.waiting_key, job.id)

        pipe.execute()

        self.logger.info(
            "Job added",
            job_id=job.id,
            job_name=name,
            priority=opts.priority.name,
            delay=opts.delay,
        )

        return job

    def get_next_job(self, timeout: int = 1) -> Optional[Job]:
        self._process_delayed_jobs()

        for priority in [
            Priority.CRITICAL,
            Priority.HIGH,
            Priority.NORMAL,
            Priority.LOW,
        ]:
            priority_key = self.priority_keys[priority]
            job_id = self.redis.lpop(priority_key)
            if job_id:
                return self._fetch_and_active_job(job_id)

        res = self.redis.blpop(self.waiting_key, timeout)
        if not res:
            return None

        _, job_id = res
        return self._fetch_and_active_job(job_id)

    def _fetch_and_active_job(self, job_id: str) -> Optional[Job]:
        job_data = self.redis.get(self._job_key(job_id))
        if not job_data:
            return None

        job = Job.from_json(job_data)
        job.mark_active()

        pipe = self.redis.pipeline()
        pipe.set(self._job_key(job.id), job.to_json())
        pipe.rpush(self.active_key, job.id)
        pipe.execute()

        return job

    def complete(self, job: Job, result: Any = None) -> None:
        job.mark_completed(result)

        pipe = self.redis.pipeline()
        pipe.set(self._job_key(job.id), job.to_json())
        pipe.lrem(self.active_key, 1, job.id)
        pipe.zadd(self.completed_key, {job.id: time.time()})
        pipe.execute()

    def fail(self, job: Job, error: str) -> None:
        job.mark_failed(error)

        pipe = self.redis.pipeline()
        pipe.set(self._job_key(job.id), job.to_json())
        pipe.lrem(self.active_key, 1, job.id)
        pipe.zadd(self.failed_key, {job.id: time.time()})
        pipe.execute()

        self.logger.error("Job failed", job_id=job.id, job_name=job.name, error=error)

    def retry(self, job: Job, delay: int) -> None:
        job.state = JobState.RETRYING

        execute_at = time.time() + delay
        pipe = self.redis.pipeline()
        pipe.set(
            self._job_key(job.id),
            job.to_json(),
        )
        pipe.lrem(self.active_key, 1, job.id)
        pipe.zadd(self.delayed_key, {job.id: execute_at})
        pipe.execute()

        self.logger.info(
            "Job scheduled for retry",
            job_id=job.id,
            attempt=job.attempts,
            delay=delay,
        )

    def get_job(self, job_id: str):
        job_data = self.redis.get(self._job_key(job_id))
        if not job_data:
            raise JobNotFoundError(f"Job {job_id} not found")
        return Job.from_json(job_data)

    def update_progress(self, job_id: str, progress: int) -> None:
        job = self.get_job(job_id)
        job.update_progress(progress)
        self.redis.set(self._job_key(job.id), job.to_json())

    def _process_delayed_jobs(self) -> None:
        now = time.time()
        delayed_jobs = self.redis.zrangebyscore(self.delayed_key, 0, now)

        if not delayed_jobs:
            return

        pipe = self.redis.pipeline()
        for job_id in delayed_jobs:
            pipe.rpush(self.waiting_key, job_id)
            pipe.zrem(self.delayed_key, job_id)
        pipe.execute()

    def get_counts(self) -> dict[str, int]:
        pipe = self.redis.pipeline()
        pipe.llen(self.waiting_key)
        pipe.llen(self.active_key)
        pipe.zcard(self.delayed_key)
        pipe.zcard(self.completed_key)
        pipe.zcard(self.failed_key)

        for priority_key in self.priority_keys.values():
            pipe.llen(priority_key)

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

        pipe = self.redis.pipeline()
        completed = self.redis.zrangebyscore(self.completed_key, 0, cutoff)
        failed = self.redis.zrangebyscore(self.failed_key, 0, cutoff)

        for job_id in completed + failed:
            pipe.delete(self._job_key(job_id))

        pipe.zremrangebyscore(self.completed_key, 0, cutoff)
        pipe.zremrangebyscore(self.failed_key, 0, cutoff)
        pipe.execute()

        return len(completed) + len(failed)

    def _job_key(self, job_id: str) -> str:
        return f"{self.job_key_prefix}{job_id}"
