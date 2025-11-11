import redis
from .exceptions import ConnectionError


class RedisConnection:
    def __init__(self, host: str = "localhost", port: int = 6379, **kwargs):
        try:
            self.client = redis.Redis(
                host=host,
                port=port,
                decode_responses=kwargs.get("decode_responses", True),
                **kwargs,
            )
            self.client.ping()
        except redis.RedisError as e:
            raise ConnectionError(f"Redis connection failed: {e}") from e

    def close(self) -> None:
        self.client.close()
