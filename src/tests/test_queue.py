import pytest
import time

from ..scheduler import JobScheduler
from ..queue import TaskQueue, BatchOptions
from ..job import Job, JobOptions
from ..types import Priority, JobState
from ..connection import RedisConnection
from ..exceptions import JobNotFoundError


@pytest.fixture
def redis_connection():
    conn = RedisConnection(host="localhost", port=6379, password="local")
    yield conn
    conn.client.flushdb()
    conn.close()


@pytest.fixture
def queue(redis_connection) -> TaskQueue:
    return TaskQueue(
        redis_connection,
        name="test_queue",
    )


@pytest.fixture
def batch_queue(redis_connection) -> TaskQueue:
    return TaskQueue(
        redis_connection,
        name="test_batch_queue",
        batch_options=BatchOptions(batch_size=500, flush_interval=30),
    )


@pytest.fixture
def job_scheduler(queue) -> JobScheduler:
    return JobScheduler(queue=queue)


class TestJobCreation:
    def test_create_job_with_defaults(self):
        job = Job(name="test_job", payload={"data": "value"})
        assert job.name == "test_job"
        assert job.state == JobState.WAITING
        assert job.attempts == 0
        assert job.options.max_retries == 3
        assert job.options.priority == Priority.NORMAL

    def test_create_job_with_custom_options(self):
        options = JobOptions(
            max_retries=5, priority=Priority.HIGH, timeout=5000, delay=1000
        )
        job = Job(name="test_job", payload={"data": "value"}, options=options)
        assert job.options.max_retries == 5
        assert job.options.priority == Priority.HIGH
        assert job.options.timeout == 5000
        assert job.options.delay == 1000

    def test_job_serialization(self):
        job = Job(name="test_job", payload={"data": "value"})
        json_str = job.to_json()
        restored_job = Job.from_json(json_str)
        assert restored_job.id == job.id
        assert restored_job.name == job.name
        assert restored_job.payload == job.payload


class TestQueueOperations:
    def test_add_job_to_queue(self, queue: TaskQueue):
        job = queue.add("test_job", {"data": "value"})
        assert job.id is not None
        assert job.name == "test_job"

        counts = queue.get_counts()
        assert counts["waiting"] == 1

    def test_add_job_with_priority(self, queue: TaskQueue):
        options = JobOptions(priority=Priority.HIGH)
        job = queue.add("test_job", {"data": "value"}, options)

        counts = queue.get_counts()
        assert counts["priority_high"] == 1

    def test_add_delayed_job(self, queue: TaskQueue):
        options = JobOptions(delay=2)
        queue.add("test_job", {"data": "value"}, options)

        counts = queue.get_counts()
        assert counts["delayed"] == 1
        assert counts["waiting"] == 0

    def test_get_next_job_fifo(self, queue: TaskQueue):
        job1 = queue.add("job1", {"order": 1})
        queue.add("job2", {"order": 2})

        next_job = queue.get_next_job()
        assert next_job.id == job1.id
        assert next_job.state == JobState.ACTIVE

    def test_priority_ordering(self, queue: TaskQueue):
        queue.add("low", {"p": "low"}, JobOptions(priority=Priority.LOW))
        job_high = queue.add("high", {"p": "high"}, JobOptions(priority=Priority.HIGH))
        job_critical = queue.add(
            "critical", {"p": "crit"}, JobOptions(priority=Priority.CRITICAL)
        )

        next_job = queue.get_next_job()
        assert next_job.id == job_critical.id

        next_job = queue.get_next_job()
        assert next_job.id == job_high.id

    def test_delayed_job_processing(self, queue: TaskQueue):
        options = JobOptions(delay=1)
        job = queue.add("delayed_job", {"data": "value"}, options)

        next_job = queue.get_next_job(timeout=5)
        assert next_job is None

        time.sleep(1)

        next_job = queue.get_next_job(timeout=5)
        assert next_job is not None
        assert next_job.id == job.id

    def test_complete_job(self, queue: TaskQueue):
        job = queue.add("test_job", {"data": "value"})
        job = queue.get_next_job()

        queue.complete(job, result={"status": "success"})

        completed_job = queue.get_job(job.id)
        assert completed_job.state == JobState.COMPLETED
        assert completed_job.result == {"status": "success"}
        assert completed_job.progress == 100

    def test_fail_job(self, queue: TaskQueue):
        job = queue.add("test_job", {"data": "value"})
        job = queue.get_next_job()

        queue.fail(job, "Something went wrong")

        failed_job = queue.get_job(job.id)
        assert failed_job.state == JobState.FAILED
        assert failed_job.error == "Something went wrong"

    def test_retry_job(self, queue: TaskQueue):
        job = queue.add("test_job", {"data": "value"})
        job = queue.get_next_job()

        queue.retry(job, delay=1000)

        retried_job = queue.get_job(job.id)
        assert retried_job.state == JobState.RETRYING

        counts = queue.get_counts()
        assert counts["delayed"] == 1

    def test_get_job_not_found(self, queue: TaskQueue):
        with pytest.raises(JobNotFoundError):
            queue.get_job("non_existent_id")

    def test_update_progress(self, queue: TaskQueue):
        job = queue.add("test_job", {"data": "value"})

        queue.update_progress(job.id, 50)

        updated_job = queue.get_job(job.id)
        assert updated_job.progress == 50

    def test_get_counts(self, queue: TaskQueue):
        queue.add("job1", {})
        queue.add("job2", {}, JobOptions(priority=Priority.HIGH))
        queue.add("job3", {}, JobOptions(delay=5))

        counts = queue.get_counts()
        assert counts["waiting"] == 1
        assert counts["priority_high"] == 1
        assert counts["delayed"] == 1
        assert counts["active"] == 0
        assert counts["completed"] == 0
        assert counts["failed"] == 0


class TestQueueMaintenance:
    def test_pause_and_resume(self, queue: TaskQueue):
        assert not queue.is_paused()

        queue.pause()
        assert queue.is_paused()

        queue.resume()
        assert not queue.is_paused()

    def test_clean_old_jobs(self, queue: TaskQueue):
        job1 = queue.add("job1", {})
        job1 = queue.get_next_job()
        queue.complete(job1)

        job2 = queue.add("job2", {})
        job2 = queue.get_next_job()
        queue.fail(job2, "error")

        removed = queue.clean(grace_period=0)
        assert removed == 2

        counts = queue.get_counts()
        assert counts["completed"] == 0
        assert counts["failed"] == 0


class TestJobRetryLogic:
    def test_should_retry_with_attempts_remaining(self):
        job = Job(name="test", payload={}, options=JobOptions(max_retries=3))
        job.attempts = 2
        assert job.should_retry() is True

    def test_should_not_retry_max_attempts_reached(self):
        job = Job(name="test", payload={}, options=JobOptions(max_retries=3))
        job.attempts = 4
        assert job.should_retry() is False

    def test_is_exhausted_logic(self):
        job = Job(name="test", payload={}, options=JobOptions(max_retries=2))

        job.attempts = 1
        assert job.is_exhausted() is False

        job.attempts = 2
        assert job.is_exhausted() is False

        job.attempts = 3
        assert job.is_exhausted() is True

    def test_mark_active_increments_attempts(self):
        job = Job(name="test", payload={})
        initial_attempts = job.attempts

        job.mark_active()

        assert job.attempts == initial_attempts + 1
        assert job.state == JobState.ACTIVE
        assert job.started_at is not None


class TestConcurrentOperations:
    def test_multiple_workers_getting_jobs(self, queue: TaskQueue):
        for i in range(5):
            queue.add(f"job_{i}", {"index": i})

        jobs = []
        for _ in range(3):
            job = queue.get_next_job(timeout=0)
            if job:
                jobs.append(job)

        assert len(jobs) == 3
        assert len(set(j.id for j in jobs)) == 3

        for job in jobs:
            assert job.state == JobState.ACTIVE


class TestQueueBatch:
    def test_batch_add_to_queue(self, batch_queue: TaskQueue):
        jobs = []
        job_size = 500

        for i in range(job_size):
            job = batch_queue.add(f"test {i}", {"id": i})
            jobs.append(job)

        counts = batch_queue.get_counts()
        assert counts["waiting"] == job_size

        for job in jobs:
            fetched = batch_queue.get_job(job.id)
            assert fetched.state == JobState.WAITING


class TestQueueScheduler:

    def test_add_job_with_scheduler(self, job_scheduler: JobScheduler):
        job_scheduler.add_cron_job(
            job_name="test",
            payload={},
            cron_expression="* * * * *",
        )

        jobs = job_scheduler.scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].id.startswith("cron_test_")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
