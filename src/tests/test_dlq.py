import pytest
import time
import threading

from ..queue import TaskQueue
from ..worker import Worker
from ..job import Job, JobOptions
from ..types import Priority, JobState
from ..connection import RedisConnection


@pytest.fixture
def redis_connection():
    conn = RedisConnection(host="localhost", port=6379, password="local")
    yield conn
    conn.client.flushdb()
    conn.close()


@pytest.fixture
def queue_with_dlq(redis_connection):
    return TaskQueue(
        redis_connection, name="test_dlq", enable_dlq=True, auto_retry_dlq=False
    )


@pytest.fixture
def queue_with_auto_retry(redis_connection) -> TaskQueue:
    return TaskQueue(
        redis_connection,
        name="test_auto_retry",
        enable_dlq=True,
        auto_retry_dlq=True,
        auto_retry_delay=2,
    )


@pytest.fixture
def queue_no_dlq(redis_connection):
    return TaskQueue(redis_connection, name="test_no_dlq", enable_dlq=False)


class TestDLQBasics:
    def test_job_moves_to_dlq_when_exhausted(self, queue_with_dlq: TaskQueue):
        job = queue_with_dlq.add(
            "test_job", {"data": "test"}, JobOptions(max_retries=0)
        )
        job = queue_with_dlq.get_next_job()

        assert job.attempts == 1
        assert job.is_exhausted() is True

        queue_with_dlq.fail(job, "Test error")

        retrieved_job = queue_with_dlq.get_job(job.id)
        assert retrieved_job.state == JobState.DEAD_LETTER

        counts = queue_with_dlq.get_counts()
        assert counts["dead_letter"] == 1
        assert counts["failed"] == 0

    def test_job_stays_in_failed_when_not_exhausted(self, queue_with_dlq: TaskQueue):
        job = queue_with_dlq.add(
            "test_job", {"data": "test"}, JobOptions(max_retries=3)
        )
        job = queue_with_dlq.get_next_job()

        assert job.attempts == 1
        assert job.should_retry() is True
        assert job.is_exhausted() is False

        queue_with_dlq.fail(job, "Test error")

        retrieved_job = queue_with_dlq.get_job(job.id)
        assert retrieved_job.state == JobState.FAILED

        counts = queue_with_dlq.get_counts()
        assert counts["failed"] == 1
        assert counts["dead_letter"] == 0

    def test_dlq_disabled_goes_to_failed(self, queue_no_dlq: TaskQueue):
        job = queue_no_dlq.add("test_job", {"data": "test"}, JobOptions(max_retries=0))
        job = queue_no_dlq.get_next_job()

        assert job.attempts == 1
        assert job.is_exhausted() is True

        queue_no_dlq.fail(job, "Test error")

        retrieved_job = queue_no_dlq.get_job(job.id)
        assert retrieved_job.state == JobState.FAILED

        counts = queue_no_dlq.get_counts()
        assert counts["failed"] == 1
        assert counts["dead_letter"] == 0


class TestMoveToDeadLetter:

    def test_move_to_dead_letter_sets_state(self, queue_with_dlq: TaskQueue):
        job = queue_with_dlq.add("test_job", {"data": "test"})
        job = queue_with_dlq.get_next_job()

        job.mark_failed("Test error")
        queue_with_dlq.move_to_dead_letter(job)

        retrieved_job = queue_with_dlq.get_job(job.id)
        assert retrieved_job.state == JobState.DEAD_LETTER

    def test_move_to_dead_letter_updates_redis(self, queue_with_dlq: TaskQueue):
        job = queue_with_dlq.add("test_job", {"data": "test"})
        job = queue_with_dlq.get_next_job()

        job.mark_failed("Test error")
        queue_with_dlq.move_to_dead_letter(job)

        counts = queue_with_dlq.get_counts()
        assert counts["dead_letter"] == 1
        assert counts["active"] == 0

    def test_move_to_dead_letter_preserves_job_data(self, queue_with_dlq: TaskQueue):
        job = queue_with_dlq.add(
            "test_job", {"important": "data"}, JobOptions(priority=Priority.HIGH)
        )
        job = queue_with_dlq.get_next_job()

        job.mark_failed("Test error")
        queue_with_dlq.move_to_dead_letter(job)

        retrieved_job = queue_with_dlq.get_job(job.id)
        assert retrieved_job.payload == {"important": "data"}
        assert retrieved_job.options.priority == Priority.HIGH
        assert retrieved_job.error == "Test error"


class TestGetDeadLetterJobs:

    def test_get_dead_letter_jobs_returns_empty_list(self, queue_with_dlq: TaskQueue):
        jobs = queue_with_dlq.get_dead_letter_jobs()
        assert jobs == []

    def test_get_dead_letter_jobs_returns_jobs(self, queue_with_dlq: TaskQueue):
        for i in range(3):
            job = queue_with_dlq.add(f"job_{i}", {"id": i}, JobOptions(max_retries=0))
            job = queue_with_dlq.get_next_job()
            queue_with_dlq.fail(job, f"Error {i}")

        dlq_jobs = queue_with_dlq.get_dead_letter_jobs()
        assert len(dlq_jobs) == 3
        assert all(j.state == JobState.DEAD_LETTER for j in dlq_jobs)

    def test_get_dead_letter_jobs_pagination(self, queue_with_dlq: TaskQueue):
        for i in range(10):
            job = queue_with_dlq.add(f"job_{i}", {"id": i}, JobOptions(max_retries=0))
            job = queue_with_dlq.get_next_job()
            queue_with_dlq.fail(job, f"Error {i}")

        first_5 = queue_with_dlq.get_dead_letter_jobs(start=0, end=4)
        assert len(first_5) == 5

        next_5 = queue_with_dlq.get_dead_letter_jobs(start=5, end=9)
        assert len(next_5) == 5

    def test_get_dead_letter_jobs_handles_missing_jobs(
        self, queue_with_dlq, redis_connection
    ):
        job = queue_with_dlq.add("test_job", {}, JobOptions(max_retries=0))
        job = queue_with_dlq.get_next_job()
        queue_with_dlq.fail(job, "Error")

        redis_connection.client.delete(queue_with_dlq._job_key(job.id))

        dlq_jobs = queue_with_dlq.get_dead_letter_jobs()
        assert dlq_jobs == []


class TestRetryDeadLetter:

    def test_retry_dead_letter_moves_to_waiting(self, queue_with_dlq: TaskQueue):
        job = queue_with_dlq.add(
            "test_job", {"data": "test"}, JobOptions(max_retries=0)
        )
        job = queue_with_dlq.get_next_job()
        queue_with_dlq.fail(job, "Error")

        counts_before = queue_with_dlq.get_counts()
        assert counts_before["dead_letter"] == 1

        queue_with_dlq.retry_dead_letter(job.id)

        counts_after = queue_with_dlq.get_counts()
        assert counts_after["dead_letter"] == 0
        assert counts_after["waiting"] == 1

    def test_retry_dead_letter_resets_job_state(self, queue_with_dlq: TaskQueue):
        job = queue_with_dlq.add(
            "test_job", {"data": "test"}, JobOptions(max_retries=0)
        )
        job = queue_with_dlq.get_next_job()
        queue_with_dlq.fail(job, "Error")

        queue_with_dlq.retry_dead_letter(job.id)

        retried_job = queue_with_dlq.get_job(job.id)
        assert retried_job.state == JobState.WAITING
        assert retried_job.attempts == 0
        assert retried_job.error is None

    def test_retry_dead_letter_preserves_priority(self, queue_with_dlq: TaskQueue):
        job = queue_with_dlq.add(
            "test_job",
            {"data": "test"},
            JobOptions(max_retries=0, priority=Priority.HIGH),
        )
        job = queue_with_dlq.get_next_job()
        queue_with_dlq.fail(job, "Error")

        queue_with_dlq.retry_dead_letter(job.id)

        counts = queue_with_dlq.get_counts()
        assert counts["priority_high"] == 1
        assert counts["waiting"] == 0

    def test_retry_dead_letter_raises_for_non_dlq_job(self, queue_with_dlq: TaskQueue):
        job = queue_with_dlq.add("test_job", {})

        with pytest.raises(ValueError, match="not in dead letter queue"):
            queue_with_dlq.retry_dead_letter(job.id)

    def test_retry_dead_letter_job_can_be_processed_again(
        self, queue_with_dlq: TaskQueue
    ):
        job = queue_with_dlq.add(
            "test_job", {"data": "test"}, JobOptions(max_retries=0)
        )
        job = queue_with_dlq.get_next_job()
        queue_with_dlq.fail(job, "Error")

        queue_with_dlq.retry_dead_letter(job.id)

        retried_job = queue_with_dlq.get_next_job()
        assert retried_job is not None
        assert retried_job.id == job.id
        assert retried_job.state == JobState.ACTIVE


class TestAutoRetryDLQ:
    def test_auto_retry_schedules_job(self, queue_with_auto_retry: TaskQueue):
        job = queue_with_auto_retry.add("test_job", {}, JobOptions(max_retries=0))
        job = queue_with_auto_retry.get_next_job()

        queue_with_auto_retry.fail(job, "Error")

        scheduled_key = f"{queue_with_auto_retry.dead_letter_key}:scheduled_retry"
        scheduled = queue_with_auto_retry.redis.zrange(scheduled_key, 0, -1)
        assert job.id in scheduled

    def test_auto_retry_retries_after_delay(self, queue_with_auto_retry: TaskQueue):
        job = queue_with_auto_retry.add("test_job", {}, JobOptions(max_retries=0))
        job = queue_with_auto_retry.get_next_job()
        queue_with_auto_retry.fail(job, "Error")

        counts = queue_with_auto_retry.get_counts()
        assert counts["dead_letter"] == 1
        assert counts["waiting"] == 0

        time.sleep(2.5)

        next_job = queue_with_auto_retry.get_next_job()

        counts_after = queue_with_auto_retry.get_counts()
        assert counts_after["dead_letter"] == 0
        assert next_job is not None
        assert next_job.id == job.id

    def test_auto_retry_does_not_retry_before_delay(
        self, queue_with_auto_retry: TaskQueue
    ):
        job = queue_with_auto_retry.add("test_job", {}, JobOptions(max_retries=0))
        job = queue_with_auto_retry.get_next_job()
        queue_with_auto_retry.fail(job, "Error")

        counts_before = queue_with_auto_retry.get_counts()
        assert counts_before["dead_letter"] == 1

        time.sleep(1)

        next_job = queue_with_auto_retry.get_next_job(timeout=5)
        assert next_job is None

        counts = queue_with_auto_retry.get_counts()
        assert counts["dead_letter"] == 1
        assert counts["waiting"] == 0

    def test_auto_retry_disabled_does_not_schedule(self, queue_with_dlq: TaskQueue):
        job = queue_with_dlq.add("test_job", {}, JobOptions(max_retries=0))
        job = queue_with_dlq.get_next_job()

        queue_with_dlq.fail(job, "Error")

        scheduled_key = f"{queue_with_dlq.dead_letter_key}:scheduled_retry"
        scheduled = queue_with_dlq.redis.zrange(scheduled_key, 0, -1)
        assert job.id not in scheduled


class TestWorkerWithDLQ:
    def test_worker_moves_job_to_dlq_on_exhausted_retries(self, redis_connection):
        queue = TaskQueue(redis_connection, name="worker_test", enable_dlq=True)
        worker = Worker(queue=queue, concurrency=1)

        @worker.process("failing_job")
        def failing_job(payload):
            raise Exception("Test failure")

        job = queue.add("failing_job", {"data": "test"}, JobOptions(max_retries=1))

        worker_thread = threading.Thread(target=worker.start, daemon=True)
        worker_thread.start()

        time.sleep(3)

        counts = queue.get_counts()
        assert counts["dead_letter"] == 1

        processed_job = queue.get_job(job.id)
        assert processed_job.state == JobState.DEAD_LETTER
        assert processed_job.attempts == 2

        worker.stop()

    def test_worker_with_auto_retry_processes_again(self, redis_connection):
        queue = TaskQueue(
            redis_connection,
            name="worker_auto_retry_test",
            enable_dlq=True,
            auto_retry_dlq=True,
            auto_retry_delay=2,
        )

        service_available = [False]
        attempt_count = [0]

        worker = Worker(queue=queue, concurrency=1)

        @worker.process("flaky_job")
        def flaky_job(payload):
            attempt_count[0] += 1
            if not service_available[0]:
                raise Exception("Service down")
            return {"success": True}

        job = queue.add("flaky_job", {"data": "test"}, JobOptions(max_retries=1))

        worker_thread = threading.Thread(target=worker.start, daemon=True)
        worker_thread.start()

        time.sleep(2)

        counts = queue.get_counts()
        assert counts["dead_letter"] == 1

        service_available[0] = True

        time.sleep(3)

        counts_after = queue.get_counts()
        assert counts_after["completed"] == 1
        assert counts_after["dead_letter"] == 0
        assert attempt_count[0] == 3

        worker.stop()


class TestDLQWithPriorities:

    def test_retry_dlq_maintains_priority(self, queue_with_dlq: TaskQueue):
        priorities = [Priority.CRITICAL, Priority.HIGH, Priority.NORMAL, Priority.LOW]

        for priority in priorities:
            job = queue_with_dlq.add(
                f"job_{priority.name}", {}, JobOptions(max_retries=0, priority=priority)
            )
            job = queue_with_dlq.get_next_job()
            queue_with_dlq.fail(job, "Error")

        dlq_jobs = queue_with_dlq.get_dead_letter_jobs()
        assert len(dlq_jobs) == 4

        for job in dlq_jobs:
            queue_with_dlq.retry_dead_letter(job.id)

        counts = queue_with_dlq.get_counts()
        assert counts["priority_critical"] == 1
        assert counts["priority_high"] == 1
        assert counts["priority_normal"] == 0
        assert counts["waiting"] == 1
        assert counts["priority_low"] == 1


class TestDLQConcurrent:

    def test_multiple_jobs_can_be_in_dlq(self, queue_with_dlq: TaskQueue):
        jobs = []
        for i in range(5):
            job = queue_with_dlq.add(f"job_{i}", {"id": i}, JobOptions(max_retries=0))
            jobs.append(job)

        for job in jobs:
            job = queue_with_dlq.get_next_job()
            queue_with_dlq.fail(job, f"Error {job.payload['id']}")

        counts = queue_with_dlq.get_counts()
        assert counts["dead_letter"] == 5

        dlq_jobs = queue_with_dlq.get_dead_letter_jobs()
        assert len(dlq_jobs) == 5

    def test_retry_multiple_jobs_from_dlq(self, queue_with_dlq: TaskQueue):
        job_ids = []
        for i in range(3):
            job = queue_with_dlq.add(f"job_{i}", {"id": i}, JobOptions(max_retries=0))
            job_ids.append(job.id)
            job = queue_with_dlq.get_next_job()
            queue_with_dlq.fail(job, f"Error {i}")

        for job_id in job_ids:
            queue_with_dlq.retry_dead_letter(job_id)

        counts = queue_with_dlq.get_counts()
        assert counts["dead_letter"] == 0
        assert counts["waiting"] == 3


class TestDLQEdgeCases:

    def test_fail_with_dlq_disabled_ignores_dlq(self, queue_no_dlq: TaskQueue):
        job = queue_no_dlq.add("test_job", {}, JobOptions(max_retries=0))
        job = queue_no_dlq.get_next_job()

        queue_no_dlq.fail(job, "Error")

        counts = queue_no_dlq.get_counts()
        assert counts["failed"] == 1
        assert counts["dead_letter"] == 0

    def test_get_counts_includes_dlq(self, queue_with_dlq: TaskQueue):
        job = queue_with_dlq.add("test_job", {}, JobOptions(max_retries=0))
        job = queue_with_dlq.get_next_job()
        queue_with_dlq.fail(job, "Error")

        counts = queue_with_dlq.get_counts()
        assert "dead_letter" in counts
        assert counts["dead_letter"] == 1

    def test_clean_does_not_affect_dlq(self, queue_with_dlq: TaskQueue):
        job = queue_with_dlq.add("test_job", {}, JobOptions(max_retries=0))
        job = queue_with_dlq.get_next_job()
        queue_with_dlq.fail(job, "Error")

        removed = queue_with_dlq.clean(grace_period=0)
        assert removed == 0

        counts = queue_with_dlq.get_counts()
        assert counts["dead_letter"] == 1


class TestDLQIntegration:
    def test_full_lifecycle_with_dlq(self, redis_connection):
        queue = TaskQueue(redis_connection, name="lifecycle_test", enable_dlq=True)
        worker = Worker(queue=queue)

        call_count = [0]

        @worker.process("lifecycle_job")
        def lifecycle_job(payload):
            call_count[0] += 1
            if call_count[0] <= 2:
                raise Exception("Not ready yet")
            return {"success": True}

        job = queue.add("lifecycle_job", {"data": "test"}, JobOptions(max_retries=1))

        worker_thread = threading.Thread(target=worker.start, daemon=True)
        worker_thread.start()

        time.sleep(2)

        counts = queue.get_counts()
        assert counts["dead_letter"] == 1

        dlq_jobs = queue.get_dead_letter_jobs()
        assert len(dlq_jobs) == 1

        queue.retry_dead_letter(job.id)

        time.sleep(1)

        final_counts = queue.get_counts()
        assert final_counts["completed"] == 1
        assert final_counts["dead_letter"] == 0

        worker.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
