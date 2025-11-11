import random
from abc import ABC, abstractmethod


class BackoffStrategy(ABC):
    def __init__(self, max_delay: int = 300000):
        self.max_delay = max_delay

    @abstractmethod
    def calculate_delay(self, attempt: int, base_delay: int = 1000) -> int:
        pass


class ExponentialBackoff(BackoffStrategy):
    def calculate_delay(self, attempt: int, base_delay: int = 1000) -> int:
        return min(base_delay * (2 ** (attempt - 1)), self.max_delay)


class ExponentialBackoffWithJitter(BackoffStrategy):
    def calculate_delay(self, attempt: int, base_delay: int = 1000) -> int:
        delay = min(base_delay * (2 ** (attempt - 1)), self.max_delay)
        jitter = random.uniform(0, delay * 0.1)
        return int(delay + jitter)


class LinearBackoff(BackoffStrategy):
    def calculate_delay(self, attempt: int, base_delay: int = 1000) -> int:
        return min(base_delay * attempt, self.max_delay)


class FixedBackoff(BackoffStrategy):
    def calculate_delay(self, attempt: int, base_delay: int = 1000) -> int:
        return base_delay


STRATEGIES = {
    "exponential": ExponentialBackoff,
    "exponential_jitter": ExponentialBackoffWithJitter,
    "linear": LinearBackoff,
    "fixed": FixedBackoff,
}


def get_backoff_strategy(strategy_type: str) -> BackoffStrategy:
    return STRATEGIES.get(strategy_type, ExponentialBackoff)()
