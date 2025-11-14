import time
import sys
import signal
from typing import Optional, Dict
from concurrent.futures import ThreadPoolExecutor, Future

from .connection import RedisConnection
from .job import Job
from .types import ProcessorFunc, RateLimiter
from .queue import TaskQueue
from .utils.backoff import get_backoff_strategy
from .utils.logger import get_logger
from .middleware.base import MiddlewareChain


class Worker:
    def __init__(
        self,
        connection: RedisConnection,
        queue_name: str = "default",
        concurrency: int = 1,
        rate_limiter: Optional[RateLimiter] = None,
        middleware_chain: Optional[MiddlewareChain] = None,
    ):
        self.queue = TaskQueue(connection, queue_name)
        self.concurrency = concurrency
        self.rate_limiter = rate_limiter
        self.middleware = middleware_chain or MiddlewareChain()
        self.logger = get_logger(f"worker.{queue_name}")

        self.processors: Dict[str, ProcessorFunc] = {}
        self.running = False
        self.executor: Optional[ThreadPoolExecutor] = None
        self.active_jobs: Dict[str, Future] = {}

        self._setup_signals()

    def process(self, job_name: str) -> None:
        def decorator(handler: ProcessorFunc):
            self.processors[job_name] = handler
            return handler

        return decorator

    def start(self) -> None:
        if self.running:
            return

        self.running = True
        self.executor = ThreadPoolExecutor(max_workers=self.concurrency)
        self.logger.info("Worker started", concurrency=self.concurrency)

        try:
            while self.running:
                if self.queue.is_paused():
                    time.sleep(1)
                    continue
                self._poll_and_process()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def stop(self) -> None:
        if not self.running:
            return

        self.logger.info("Stopping worker...")
        self.running = False

        if self.executor:
            self.executor.shutdown(wait=True)
            self.executor = None

        self.logger.info("Worker stopped")

    def _poll_and_process(self) -> None:
        if len(self.active_jobs) >= self.concurrency:
            time.sleep(0.1)
            self._cleanup_done()
            return

        job = self.queue.get_next_job(timeout=1)
        if not job:
            return

        if job.name not in self.processors:
            self.queue.fail(job, f"No processor for: {job.name}")
            return

        if self.rate_limiter and not self.rate_limiter.acquire(job.name):
            self.queue.retry(job, 1000)
            self.logger.warning("Rate limited", job_id=job.id, job_name=job.name)
            return

        future = self.executor.submit(self._execute, job)
        self.active_jobs[job.id] = future

    def _execute(self, job: Job) -> None:
        try:
            processor = self.processors[job.name]

            if self.middleware.middlewares:
                result = self.middleware.execute(job, processor)
            else:
                result = processor(job.payload)

            self.queue.complete(job, result)

            if self.rate_limiter:
                self.rate_limiter.release(job.name)

        except Exception as e:
            error = str(e)
            self.logger.error("Job execution failed", job_id=job.id, error=error)

            fresh_job = self.queue.get_job(job.id)

            if fresh_job.should_retry():
                strategy = get_backoff_strategy(fresh_job.options.backoff_type)
                delay = strategy.calculate_delay(
                    fresh_job.attempts, fresh_job.options.backoff_delay
                )
                self.queue.retry(fresh_job, delay)
            else:
                self.queue.fail(fresh_job, error)

            if self.rate_limiter:
                self.rate_limiter.release(job.name)

    def _cleanup_done(self) -> None:
        done = [job_id for job_id, future in self.active_jobs.items() if future.done()]
        for job_id in done:
            del self.active_jobs[job_id]

    def _setup_signals(self) -> None:
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame) -> None:
        self.logger.info(f"Signal {signum} received")
        self.stop()
        sys.exit(0)

    def get_stats(self) -> dict:
        return {
            "running": self.running,
            "active_jobs": len(self.active_jobs),
            "concurrency": self.concurrency,
            "queue_counts": self.queue.get_counts(),
        }
