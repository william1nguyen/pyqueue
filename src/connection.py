from typing import Optional
import redis
from redis.connection import ConnectionPool

from exceptions import ConnectionError


class RedisConnection:
    def __init__(
        self,
        host: str,
        port: int,
        password: Optional[str],
        db: int,
        max_connections: int,
        socket_timeout: int,
        socket_connect_timeout: int,
        decode_responses: bool,
        **kwargs,
    ):
        try:
            self.pool = ConnectionPool(
                host=host,
                port=port,
                password=password,
                db=db,
                max_connections=max_connections,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_connect_timeout,
                decode_responses=decode_responses,
                **kwargs,
            )
            self.client = redis.Redis(connection_pool=self.pool)
            self.client.ping()
        except redis.RedisError as e:
            raise ConnectionError(f"Failed to connect to Redis: {e}") from e

    def close(self) -> None:
        self.pool.disconnect()
