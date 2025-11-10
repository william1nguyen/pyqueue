import redis
import json
from typing import Optional, Any


class RedisClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        db: int = 0,
    ):
        self.client = redis.Redis(
            host=host,
            port=port,
            password=password,
            db=db,
            decode_responses=True,
        )

    def set_json(self, key: str, value: Any, ex: Optional[int] = None):
        return self.client.set(key, json.dumps(value), ex=ex)

    def get_json(self, key: str):
        data = self.client.get(key)
        if not data:
            return None
        return json.loads(data)

    def rpush(self, key: str, *values: str):
        return self.client.rpush(key, *values)

    def blpop(self, key: str, timeout: int = 0):
        return self.client.blpop([key], timeout)

    def delete(self, *keys: str):
        return self.client.delete(*keys)
