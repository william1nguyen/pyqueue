import time
from redis import Redis


class TokenBucketRateLimiter:
    def __init__(
        self,
        redis: Redis,
        max_tokens: int,
        refill_rate: int,
        key_prefix: str = "rate_limit",
    ):
        self.redis = redis
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate
        self.key_prefix = key_prefix

    def acquire(self, key: str, tokens: int = 1) -> bool:
        rate_key = f"{self.key_prefix}:{key}"
        now = time.time()

        with self.redis.pipeline() as pipe:
            pipe.hget(rate_key, "tokens")
            pipe.hget(rate_key, "last_refill")
            results = pipe.execute()

        current_tokens = float(results[0] or self.max_tokens)
        last_refill = float(results[1] or now)

        time_passed = now - last_refill
        refill_tokens = time_passed * self.refill_rate
        current_tokens = min(self.max_tokens, current_tokens + refill_tokens)

        if current_tokens >= tokens:
            new_tokens = current_tokens - tokens
            with self.redis.pipeline() as pipe:
                pipe.hset(rate_key, "tokens", new_tokens)
                pipe.hset(rate_key, "last_refill", now)
                pipe.expire(rate_key, 3600)
                pipe.execute()
            return True

        return False

    def release(self, key: str) -> None:
        pass


class SlidingWindowRateLimiter:
    def __init__(
        self,
        redis: Redis,
        max_requests: int,
        window_seconds: int,
        key_prefix: str = "rate_limit",
    ):
        self.redis = redis
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.key_prefix = key_prefix

    def acquire(self, key: str) -> bool:
        rate_key = f"{self.key_prefix}:{key}"
        now = time.time()
        window_start = now - self.window_seconds

        with self.redis.pipeline() as pipe:
            pipe.zremrangebyscore(rate_key, 0, window_start)
            pipe.zcard(rate_key)
            pipe.zadd(rate_key, {str(now): now})
            pipe.expire(rate_key, self.window_seconds)
            results = pipe.execute()

        current_count = results[1]
        return current_count < self.max_requests

    def release(self, key: str) -> None:
        pass


class LeakyBucketRateLimiter:
    def __init__(
        self,
        redis: Redis,
        capacity: int,
        leak_rate: int,
        key_prefix: str = "rate_limit",
    ):
        self.redis = redis
        self.capacity = capacity
        self.leak_rate = leak_rate
        self.key_prefix = key_prefix

    def acquire(self, key: str) -> bool:
        rate_key = f"{self.key_prefix}:{key}"
        now = time.time()

        with self.redis.pipeline() as pipe:
            pipe.hget(rate_key, "level")
            pipe.hget(rate_key, "last_leak")
            results = pipe.execute()

        current_level = float(results[0] or 0)
        last_leak = float(results[1] or now)

        time_passed = now - last_leak
        leaked = time_passed * self.leak_rate
        current_level = max(0, current_level - leaked)

        if current_level < self.capacity:
            new_level = current_level + 1
            with self.redis.pipeline() as pipe:
                pipe.hset(rate_key, "level", new_level)
                pipe.hset(rate_key, "last_leak", now)
                pipe.expire(rate_key, 3600)
                pipe.execute()
            return True

        return False

    def release(self, key: str) -> None:
        pass
