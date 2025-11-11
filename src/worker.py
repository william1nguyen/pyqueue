import time
import sys
import signal
from typing import Optional, Callable, Dict
from concurrent.futures import ThreadPoolExecutor, Future
from connection import RedisConnection

from .job import Job
from .types import ProcessorFunc
from .queue import TaskQueue
from .utils.backoff import get_backoff_strategy
from .utils.logger import get_logger


class Worker:
    def __init__(
        self,
        connection: RedisConnection,
        queue_name: str = "default",
        concurrency: int = 1,
    ):
        self.queue = TaskQueue(connection, queue_name)
        self.concurrency = concurrency
        self.logger = get_logger(f"worker.{queue_name}")

        self.processors: Dict[str, ProcessorFunc] = {}
        self.running = False
        self.executor: Optional[ThreadPoolExecutor] = None
        self.active_jobs: Dict[str, Future] = {}

    def process(self, job_name: str, handler: ProcessorFunc) -> None:
        self.processors[job_name] = handler

    def start(self) -> None:
        if self.running:
            return

        self.running = True
        self.executor = ThreadPoolExecutor(max_workers=self.concurrency)
        self.logger.info("Worker started", concurrency=self.concurrency)

        try:
            while self.running:
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

        future = self.executor.submit(self._execute, job)
        self.active_jobs[job.id] = future

    def _execute(self, job: Job) -> None:
        try:
            res = self.processors[job.name](job.payload)
            self.queue.complete(job, res)
        except Exception as e:
            error = str(e)

            if job.should_retry():
                strategy = get_backoff_strategy(job.options.backoff_type)
                delay = strategy.caculate_delay(job.attempts, job.options.backoff_delay)
                self.queue.retry(job, delay)
            else:
                self.queue.fail(job, error)

    def _cleanup_done(self) -> None:
        done = [job_id for job_id, future in self.active_jobs.items() if future.done()]
        for job_id in done:
            del self.active_jobs[job_id]

    def _shutdown(self, signum, frame) -> None:
        self.logger.info(f"Signal {signum} received")
        self.stop()
        sys.exit(0)
