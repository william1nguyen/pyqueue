import uuid
from typing import Optional
from redis_client import RedisClient
from job import Job


class TaskQueue:
    def __init__(self, redis: RedisClient, name: str = "default"):
        self.redis = redis
        self.name = name
        self.queue_key = f"queue:{name}"

    def enqueue(self, name: str, payload) -> str:
        job_id = str(uuid.uuid4())
        job = Job(id=job_id, name=name, payload=payload)
        self.redis.rpush(self.queue_key, job.to_json())
        return job_id

    def dequeue(self, timeout: int = 1) -> Optional[Job]:
        res = self.redis.blpop(self.queue_key, timeout)
        if not res:
            return None
        _, data = res
        return Job.from_json(data)
