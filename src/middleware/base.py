from abc import ABC, abstractmethod
from typing import Any, Callable, List

from ..job import Job


class Middleware(ABC):
    @abstractmethod
    def before_process(self, job: Job) -> None:
        pass

    @abstractmethod
    def after_process(self, job: Job, result: Any) -> None:
        pass

    @abstractmethod
    def on_error(self, job: Job, error: Exception) -> None:
        pass


class MiddlewareChain:
    def __init__(self):
        self.middlewares: List[Middleware] = []

    def use(self, middleware: Middleware) -> "MiddlewareChain":
        self.middlewares.append(middleware)

    def execute(self, job: Job, processor: Callable[[dict[str, Any]], Any]) -> Any:
        for middleware in self.middlewares:
            middleware.before_process(job)

        try:
            result = processor(job.payload)

            for middlware in reversed(self.middlewares):
                middleware.after_process(job, result)

            return result

        except Exception as e:
            for middleware in reversed(self.middlewares):
                middleware.on_error(job, e)
            raise
