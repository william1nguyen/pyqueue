# PyQueue - A BullMQ-Inspired Distributed Job Queue

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Redis](https://img.shields.io/badge/redis-7.0%2B-red.svg)](https://redis.io/)
[![Concurrent](https://img.shields.io/badge/concurrent-ThreadPoolExecutor-green.svg)](https://docs.python.org/3/library/concurrent.futures.html)
[![Architecture](https://img.shields.io/badge/architecture-distributed-orange.svg)](https://en.wikipedia.org/wiki/Distributed_computing)

PyQueue is a lightweight, Redis-backed distributed job queue system built from scratch in Python. It is inspired by BullMQ and implements reliable job processing with automatic retries, rate limiting, and priority queues.

This project was built for educational purposes to explore distributed systems, concurrent processing patterns, and the design of production-quality job queues.

## Key Features

**Redis-Backed Storage**: Reliable job persistence using Redis lists, sorted sets, and hashes for atomic operations and data durability.

**Priority Queue System**: Four priority levels (CRITICAL, HIGH, NORMAL, LOW) ensuring important jobs are processed first while preventing starvation of lower-priority jobs.

**Retry Strategies**: Implements multiple backoff strategies from scratch:
- Exponential Backoff: Doubles delay on each retry attempt
- Exponential with Jitter: Adds randomness to prevent thundering herd
- Linear Backoff: Increases delay linearly
- Fixed Backoff: Constant delay between retries

**Rate Limiting**: Three rate limiting algorithms for protecting external services:
- Token Bucket: Allows bursts while maintaining average rate
- Sliding Window: Fixed requests per time window
- Leaky Bucket: Constant, smooth processing rate

**Dead Letter Queue**: Automatically handles jobs that exhaust all retry attempts, with optional auto-retry after configurable delay.

**Concurrent Processing**: Uses ThreadPoolExecutor for parallel job execution with configurable concurrency.

**Graceful Shutdown**: Proper signal handling (SIGINT, SIGTERM) ensures active jobs complete before worker termination.

## Getting Started

```bash
git clone https://github.com/william1nguyen/pyqueue
cd pyqueue
uv sync

# Start Redis
./start-dev-env.sh

# Run benchmark
make benchmark
```

```python
from src.connection import RedisConnection
from src.queue import TaskQueue
from src.worker import Worker

conn = RedisConnection(host="localhost", port=6379)
queue = TaskQueue(conn, name="tasks")
worker = Worker(queue=queue, concurrency=5)

@worker.process("my_job")
def handler(payload):
    return {"status": "done"}

worker.start()
```

## Configuration

### Queue Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `name` | str | default | Queue name |
| `serializer` | Serializer | JSONSerializer | Payload serializer |
| `enable_dlq` | bool | False | Enable dead letter queue |
| `auto_retry_dlq` | bool | False | Auto-retry from DLQ |
| `auto_retry_delay` | int | 600 | DLQ retry delay (seconds) |

### Job Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_retries` | int | 3 | Maximum retry attempts |
| `timeout` | int | None | Job timeout (seconds) |
| `priority` | Priority | NORMAL | Priority level |
| `delay` | int | 0 | Execution delay (seconds) |
| `backoff_type` | str | exponential | Backoff strategy |
| `backoff_delay` | int | 1000 | Base delay (ms) |

## Supported Features

| Category | Features |
|----------|----------|
| Job States | WAITING, ACTIVE, COMPLETED, FAILED, DELAYED, RETRYING, DEAD_LETTER |
| Priority | CRITICAL, HIGH, NORMAL, LOW |
| Backoff | exponential, exponential_jitter, linear, fixed |
| Rate Limit | TokenBucket, SlidingWindow, LeakyBucket |
| Serializer | JSON, Pickle |

## Queue Methods

| Method | Description |
|--------|-------------|
| `add(name, payload, options)` | Add job to queue |
| `get_next_job(timeout)` | Get next job (blocking) |
| `complete(job, result)` | Mark job completed |
| `fail(job, error)` | Mark job failed |
| `retry(job, delay)` | Schedule job retry |
| `get_job(job_id)` | Get job by ID |
| `update_progress(job_id, progress)` | Update progress (0-100) |
| `get_counts()` | Get queue statistics |
| `clean(grace_period)` | Remove old jobs |
| `pause()` / `resume()` | Pause/resume processing |
| `get_dead_letter_jobs()` | List DLQ jobs |
| `retry_dead_letter(job_id)` | Retry from DLQ |

## Roadmap

- [x] Rate Limiting Algorithms (Token Bucket, Sliding Window, Leaky Bucket)
- [x] Backoff Strategies (Exponential, Exponential with Jitter, Linear, Fixed)
- [x] Dead Letter Queue with auto-retry
- [x] Concurrent Processing with ThreadPoolExecutor
- [x] Middleware System (before/after hooks, error handling)
- [x] Priority Queue (CRITICAL, HIGH, NORMAL, LOW)
- [x] Delayed Job Scheduling
- [x] Graceful Shutdown (SIGINT, SIGTERM)
- [x] Cron Scheduling with APScheduler
- [ ] Job dependencies (parent-child relationships)
- [ ] Redis Cluster support
- [ ] Web UI for monitoring