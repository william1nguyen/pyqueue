# PyQueue

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Redis](https://img.shields.io/badge/redis-7.0%2B-red.svg)](https://redis.io/)

A lightweight, Redis-backed distributed job queue system for Python, inspired by BullMQ.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Basic Usage](#basic-usage)
- [Core Components](#core-components)
  - [Job](#job)
  - [Queue](#queue)
  - [Worker](#worker)
- [Supported Features](#supported-features)
  - [Priority Queues](#priority-queues)
  - [Delayed Jobs](#delayed-jobs)
  - [Job Retry with Backoff](#job-retry-with-backoff)
  - [Rate Limiting](#rate-limiting)
  - [Middleware System](#middleware-system)
  - [Dead Letter Queue (DLQ)](#dead-letter-queue-dlq)
- [Examples](#examples)
  - [Simple Email Queue](#simple-email-queue)
  - [Image Processing Pipeline](#image-processing-pipeline)
  - [Rate-Limited API Calls](#rate-limited-api-calls)
  - [Scheduled Daily Reports](#scheduled-daily-reports)
- [Monitoring](#monitoring)
  - [Queue Statistics](#queue-statistics)
  - [Worker Statistics](#worker-statistics)
  - [Job Progress Tracking](#job-progress-tracking)
  - [Structured Logging](#structured-logging)
- [Configuration](#configuration)
  - [Redis Connection](#redis-connection)
  - [Queue Configuration](#queue-configuration)
  - [Worker Configuration](#worker-configuration)
  - [Job Options](#job-options)
- [Architecture](#architecture)
  - [Project Structure](#project-structure)
  - [Redis Keys Structure](#redis-keys-structure)
  - [Key Design Decisions](#key-design-decisions)
- [Benchmark](#benchmark)
- [Development](#development)
  - [Setup Development Environment](#setup-development-environment)
  - [Running the Project](#running-the-project)
  - [Docker Compose Configuration](#docker-compose-configuration)
- [Performance Considerations](#performance-considerations)
  - [Design Choices](#design-choices)
  - [Scalability](#scalability)
  - [Recommendations](#recommendations)
- [TODO](#todo)

## Overview

PyQueue is a Redis-based distributed job queue system for Python applications. Inspired by BullMQ, this side project explores building a job queue with features like automatic retries, rate limiting, priority queues, and monitoring capabilities.

**Key Features:**

- **Reliable Job Processing**: Redis-backed persistence for job durability
- **Flexible Configuration**: Fine-grained control over job execution and retry behavior
- **Easy to Use**: Simple API that integrates with Python applications
- **Learning Resource**: Well-documented code for understanding job queue internals

**Use Cases:**

- Background email and notification sending
- Image and video processing pipelines
- API rate-limited request handling
- Scheduled task execution
- Data import/export operations
- Webhook delivery with retry logic

## Features

### Core Capabilities

- **Redis-Backed Storage**: Job data persisted in Redis - [connection.py](connection.py)
- **Job States**: WAITING, ACTIVE, COMPLETED, FAILED, DELAYED, RETRYING, DEAD_LETTER
- **Priority Queues**: CRITICAL, HIGH, NORMAL, and LOW priority levels - [types.py](types.py)
- **Retry Strategies** - [backoff.py](backoff.py)
  - Exponential backoff with optional jitter
  - Linear backoff
  - Fixed delay backoff
- **Rate Limiting** - [rate_limit.py](rate_limit.py)
  - Token Bucket
  - Sliding Window
  - Leaky Bucket
- **Middleware System**: Custom processing logic hooks - [base.py](base.py)
- **Dead Letter Queue (DLQ)**: Handle permanently failed jobs with auto-retry
- **Concurrent Processing**: ThreadPoolExecutor-based job processing
- **Structured Logging**: JSON-formatted logs - [logger.py](logger.py)
- **Serialization**: JSON and Pickle support - [serializer.py](serializer.py)

## Quick Start

### Prerequisites

- Python 3.12 or higher
- Redis 7.0.1 or higher

### Installation

```bash
# Clone the repository
git clone https://github.com/william1nguyen/pyqueue
cd pyqueue

# Install dependencies
uv sync
```

### Basic Usage

```python
from connection import RedisConnection
from queue import TaskQueue
from worker import Worker
from job import JobOptions
from types import Priority

# 1. Connect to Redis
conn = RedisConnection(host="localhost", port=6379)

# 2. Create a queue
queue = TaskQueue(conn, name="email-queue")

# 3. Add a job
job = queue.add(
    name="send_email",
    payload={"to": "user@example.com", "subject": "Welcome!"},
    options=JobOptions(
        priority=Priority.HIGH,
        max_retries=3,
        timeout=30
    )
)
print(f"Job added: {job.id}")

# 4. Create a worker
worker = Worker(queue=queue, concurrency=5)

# 5. Define job processor
@worker.process("send_email")
def send_email_handler(payload):
    email = payload["to"]
    subject = payload["subject"]
    print(f"Sending email to {email}: {subject}")
    return {"status": "sent", "timestamp": "2025-01-01T00:00:00Z"}

# 6. Start processing
worker.start()
```

## Core Components

### Job

Implementation: [job.py](job.py)

The Job class represents a unit of work with comprehensive metadata tracking.

**Key Features:**

- Unique ID generation using UUID
- State management (WAITING, ACTIVE, COMPLETED, FAILED, DELAYED, RETRYING, DEAD_LETTER)
- Automatic timestamp tracking (created_at, started_at, completed_at, failed_at)
- Progress tracking (0-100%)
- Configurable options (retries, timeout, priority, backoff)
- JSON serialization/deserialization

**JobOptions:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_retries` | int | 3 | Maximum retry attempts |
| `timeout` | int | None | Job timeout in seconds |
| `priority` | Priority | NORMAL | Job priority level |
| `delay` | int | 0 | Delay before execution (seconds) |
| `backoff_type` | str | "exponential" | Retry backoff strategy |
| `backoff_delay` | int | 1000 | Base delay for backoff (ms) |

**Example:**

```python
from job import Job, JobOptions
from types import Priority

# Create a job with custom options
options = JobOptions(
    max_retries=5,
    timeout=60,
    priority=Priority.HIGH,
    backoff_type="exponential_jitter",
    backoff_delay=2000
)

job = Job(
    name="process_order",
    payload={"order_id": "12345", "amount": 99.99},
    options=options
)

# Update job state
job.mark_active()
job.update_progress(50)
job.mark_completed(result={"status": "success"})

# Serialize for storage
json_data = job.to_json()
```

### Queue

Implementation: [queue.py](queue.py)

The TaskQueue manages job lifecycle and provides efficient job retrieval with priority support.

**Key Features:**

- Priority-based job processing (CRITICAL > HIGH > NORMAL > LOW)
- Delayed job scheduling using Redis sorted sets
- Pause/resume functionality
- Job state tracking across multiple Redis keys
- Atomic operations using Redis pipelines
- Queue statistics and monitoring
- Cleanup of old completed/failed jobs
- Dead Letter Queue support

**Methods:**

| Method | Description | Example |
|--------|-------------|---------|
| `add(name, payload, options)` | Add a job to the queue | `queue.add("send_email", {...})` |
| `get_next_job(timeout)` | Get the next job to process (blocks) | `job = queue.get_next_job(timeout=5)` |
| `complete(job, result)` | Mark job as completed | `queue.complete(job, {"sent": True})` |
| `fail(job, error)` | Mark job as failed | `queue.fail(job, "SMTP error")` |
| `retry(job, delay)` | Retry a failed job | `queue.retry(job, delay=5000)` |
| `get_job(job_id)` | Retrieve job by ID | `job = queue.get_job("uuid")` |
| `update_progress(job_id, progress)` | Update job progress | `queue.update_progress(job.id, 75)` |
| `get_counts()` | Get queue statistics | `counts = queue.get_counts()` |
| `clean(grace_period)` | Clean old jobs | `queue.clean(grace_period=86400)` |
| `pause()` | Pause queue processing | `queue.pause()` |
| `resume()` | Resume queue processing | `queue.resume()` |
| `get_dead_letter_jobs(start, end)` | Get jobs from DLQ | `dlq_jobs = queue.get_dead_letter_jobs()` |
| `retry_dead_letter(job_id)` | Retry a job from DLQ | `queue.retry_dead_letter(job.id)` |

**Example:**

```python
from connection import RedisConnection
from queue import TaskQueue
from job import JobOptions
from types import Priority

conn = RedisConnection()
queue = TaskQueue(conn, name="tasks", enable_dlq=True)

# Add a high-priority job
job = queue.add(
    name="urgent_task",
    payload={"data": "important"},
    options=JobOptions(priority=Priority.HIGH)
)

# Add a delayed job (execute in 1 hour)
delayed_job = queue.add(
    name="scheduled_task",
    payload={"data": "later"},
    options=JobOptions(delay=3600)
)

# Get queue statistics
counts = queue.get_counts()
print(f"Waiting: {counts['waiting']}, Active: {counts['active']}, DLQ: {counts['dead_letter']}")

# Clean old jobs (older than 24 hours)
cleaned = queue.clean(grace_period=86400)
print(f"Cleaned {cleaned} old jobs")

# Pause processing
queue.pause()
```

### Worker

Implementation: [worker.py](worker.py)

The Worker class handles concurrent job processing with automatic retry and rate limiting.

**Key Features:**

- Concurrent job processing using ThreadPoolExecutor
- Configurable concurrency level
- Automatic retry with backoff strategies
- Rate limiting support
- Middleware chain execution
- Graceful shutdown with signal handling (SIGINT, SIGTERM)
- Real-time job statistics
- Pause-aware processing
- Automatic DLQ handling

**Configuration:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `queue` | TaskQueue | Required | TaskQueue instance to process |
| `concurrency` | int | 1 | Number of concurrent jobs |
| `rate_limiter` | RateLimiter | None | Rate limiter instance |
| `middleware_chain` | MiddlewareChain | None | Middleware chain |

**Example:**

```python
from connection import RedisConnection
from queue import TaskQueue
from worker import Worker
from rate_limit import TokenBucketRateLimiter
from middleware.base import Middleware, MiddlewareChain

# Setup
conn = RedisConnection()
queue = TaskQueue(conn, name="my-queue")

# Optional: Add rate limiting (10 requests per second)
rate_limiter = TokenBucketRateLimiter(
    redis=conn.client,
    max_tokens=10,
    refill_rate=10
)

# Optional: Add middleware
class LoggingMiddleware(Middleware):
    def before_process(self, job):
        print(f"Starting job {job.id}")
    
    def after_process(self, job, result):
        print(f"Completed job {job.id}")
    
    def on_error(self, job, error):
        print(f"Job {job.id} failed: {error}")

middleware_chain = MiddlewareChain()
middleware_chain.use(LoggingMiddleware())

# Create worker with 5 concurrent processors
worker = Worker(
    queue=queue,
    concurrency=5,
    rate_limiter=rate_limiter,
    middleware_chain=middleware_chain
)

# Register job processors
@worker.process("send_email")
def send_email(payload):
    # Process email
    return {"sent": True}

@worker.process("generate_report")
def generate_report(payload):
    # Generate report
    return {"report_id": "123"}

# Start processing (blocks until stopped)
worker.start()

# In another thread/process, check stats
stats = worker.get_stats()
print(f"Active jobs: {stats['active_jobs']}")
```

## Supported Features

### Priority Queues

Process critical jobs first while ensuring lower-priority jobs aren't starved:

```python
from types import Priority
from job import JobOptions

# Critical job - processed first
critical = queue.add(
    "payment_processing",
    {"amount": 1000},
    JobOptions(priority=Priority.CRITICAL)
)

# Normal job - processed after critical and high
normal = queue.add(
    "send_notification",
    {"message": "Hello"},
    JobOptions(priority=Priority.NORMAL)
)

# Low priority - processed when queue is empty
low = queue.add(
    "cleanup_temp_files",
    {},
    JobOptions(priority=Priority.LOW)
)
```

### Delayed Jobs

Schedule jobs for future execution:

```python
# Execute in 1 hour (3600 seconds)
delayed_job = queue.add(
    "send_reminder",
    {"user_id": 123},
    JobOptions(delay=3600)
)

# Execute at midnight
import time
from datetime import datetime, timedelta

midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
midnight += timedelta(days=1)
delay_seconds = int((midnight - datetime.now()).total_seconds())

scheduled_job = queue.add(
    "daily_report",
    {},
    JobOptions(delay=delay_seconds)
)
```

### Job Retry with Backoff

Implementation: [backoff.py](backoff.py)

Automatic retry with intelligent backoff:

#### Exponential Backoff

Doubles delay on each retry attempt.

```python
options = JobOptions(
    max_retries=5,
    backoff_type="exponential",
    backoff_delay=1000  # Start with 1 second
)
# Retry delays: 1s, 2s, 4s, 8s, 16s
```

#### Exponential with Jitter

Adds randomness to prevent thundering herd.

```python
options = JobOptions(
    max_retries=5,
    backoff_type="exponential_jitter",
    backoff_delay=1000
)
# Retry delays: ~1s, ~2s, ~4s, ~8s, ~16s (with random jitter)
```

#### Linear Backoff

Increases delay linearly.

```python
options = JobOptions(
    max_retries=3,
    backoff_type="linear",
    backoff_delay=5000  # 5 seconds
)
# Retry delays: 5s, 10s, 15s
```

#### Fixed Backoff

Constant delay between retries.

```python
options = JobOptions(
    max_retries=10,
    backoff_type="fixed",
    backoff_delay=2000  # Always 2 seconds
)
```

### Rate Limiting

Implementation: [rate_limit.py](rate_limit.py)

Protect external services from overload with multiple rate limiting strategies.

#### Token Bucket

Allows bursts while maintaining average rate. Best for bursty traffic patterns.

```python
from rate_limit import TokenBucketRateLimiter

limiter = TokenBucketRateLimiter(
    redis=conn.client,
    max_tokens=100,      # Bucket capacity
    refill_rate=10,      # Tokens per second
    key_prefix="limiter"
)

worker = Worker(queue=queue, rate_limiter=limiter)
```

**How it works:**
- Bucket starts with `max_tokens` tokens
- Each job consumes 1 token (configurable)
- Tokens refill at `refill_rate` per second
- Allows bursts up to `max_tokens`

#### Sliding Window

Fixed number of requests per time window. Best for strict rate limits.

```python
from rate_limit import SlidingWindowRateLimiter

limiter = SlidingWindowRateLimiter(
    redis=conn.client,
    max_requests=100,    # Max requests
    window_seconds=60,   # Per 60 seconds
    key_prefix="limiter"
)
```

**How it works:**
- Tracks timestamps of all requests in a sliding window
- Removes expired timestamps automatically
- Rejects if count exceeds `max_requests` in `window_seconds`

#### Leaky Bucket

Constant processing rate. Best for smooth, predictable processing.

```python
from rate_limit import LeakyBucketRateLimiter

limiter = LeakyBucketRateLimiter(
    redis=conn.client,
    capacity=50,         # Bucket capacity
    leak_rate=5,         # Jobs per second
    key_prefix="limiter"
)
```

**How it works:**
- Bucket fills with incoming requests
- Leaks at constant `leak_rate`
- Rejects when bucket is full

### Middleware System

Implementation: [base.py](base.py)

Add cross-cutting concerns like logging, metrics, or validation:

```python
from base import Middleware, MiddlewareChain

class MetricsMiddleware(Middleware):
    def before_process(self, job):
        job.start_time = time.time()
    
    def after_process(self, job, result):
        duration = time.time() - job.start_time
        # Send metrics to monitoring system
        metrics.timing(f"job.{job.name}.duration", duration)
    
    def on_error(self, job, error):
        metrics.increment(f"job.{job.name}.error")

class ValidationMiddleware(Middleware):
    def before_process(self, job):
        # Validate payload
        if "user_id" not in job.payload:
            raise ValueError("user_id required")
    
    def after_process(self, job, result):
        pass
    
    def on_error(self, job, error):
        pass

# Chain multiple middleware
chain = MiddlewareChain()
chain.use(ValidationMiddleware())
chain.use(MetricsMiddleware())

worker = Worker(
    queue=queue,
    middleware_chain=chain
)
```

### Dead Letter Queue (DLQ)

Implementation: [queue.py](queue.py)

Handle jobs that have exhausted all retry attempts. DLQ provides a safety net for permanently failed jobs, allowing you to investigate issues and manually retry when ready.

**Key Features:**
- Automatic move to DLQ when job exhausts all retries
- Manual retry from DLQ
- Auto-retry with configurable delay
- List and inspect failed jobs
- Preserve job priority on retry

#### Basic DLQ Usage

```python
from connection import RedisConnection
from queue import TaskQueue
from job import JobOptions

# Enable DLQ
queue = TaskQueue(
    connection=conn,
    name="my-queue",
    enable_dlq=True
)

# Add a job with limited retries
job = queue.add(
    "risky_task",
    {"data": "important"},
    JobOptions(max_retries=3)
)

# When job fails 3 times, it moves to DLQ automatically
# Check DLQ
dlq_jobs = queue.get_dead_letter_jobs()
print(f"Jobs in DLQ: {len(dlq_jobs)}")

for job in dlq_jobs:
    print(f"Job {job.id}: {job.name} - {job.error}")
```

#### Auto-Retry from DLQ

Automatically retry failed jobs after a delay, useful for transient failures (network issues, service downtime):

```python
# Enable auto-retry from DLQ
queue = TaskQueue(
    connection=conn,
    name="my-queue",
    enable_dlq=True,
    auto_retry_dlq=True,
    auto_retry_delay=600  # Retry after 10 minutes
)

# Jobs in DLQ will be automatically retried after 10 minutes
# The worker will pick them up when they're ready
```

**Use Cases:**
- External API temporarily unavailable
- Database connection timeout
- Rate limit exceeded
- Service maintenance window

#### Manual Retry from DLQ

Manually retry specific jobs after investigating and fixing issues:

```python
# Get jobs in DLQ
dlq_jobs = queue.get_dead_letter_jobs()

# Inspect a failed job
failed_job = dlq_jobs[0]
print(f"Job failed with: {failed_job.error}")
print(f"Attempts: {failed_job.attempts}")
print(f"Last failed: {failed_job.failed_at}")

# After fixing the issue (e.g., external service is back)
# Retry the job
queue.retry_dead_letter(failed_job.id)

# Job moves back to waiting queue and will be processed again
```

#### Pagination for Large DLQ

```python
# Get first 10 jobs
first_batch = queue.get_dead_letter_jobs(start=0, end=9)

# Get next 10 jobs
next_batch = queue.get_dead_letter_jobs(start=10, end=19)

# Get all jobs
all_dlq_jobs = queue.get_dead_letter_jobs()
```

#### DLQ with Worker

Worker automatically handles DLQ when jobs exhaust retries:

```python
from worker import Worker

queue = TaskQueue(conn, name="api-calls", enable_dlq=True)
worker = Worker(queue=queue, concurrency=5)

@worker.process("call_external_api")
def call_api(payload):
    # This might fail due to network issues
    response = requests.post(payload["url"], json=payload["data"])
    response.raise_for_status()
    return response.json()

# If job fails max_retries times, worker moves it to DLQ
# You can later inspect and retry when the API is healthy
worker.start()
```

#### Queue Statistics with DLQ

```python
counts = queue.get_counts()
print(f"""
Queue Statistics:
- Waiting: {counts['waiting']}
- Active: {counts['active']}
- Delayed: {counts['delayed']}
- Completed: {counts['completed']}
- Failed: {counts['failed']}
- Dead Letter: {counts['dead_letter']}
""")
```

#### Best Practices

**When to Use DLQ:**
- Jobs with external dependencies that may fail permanently
- Critical jobs that need manual review before retry
- Jobs that require investigation when they fail
- Preventing infinite retry loops

**When to Use Auto-Retry:**
- Transient network failures
- Temporary service outages
- Rate limiting recovery
- Database connection pool exhaustion

**Configuration Guidelines:**

```python
# For critical jobs - no auto-retry, manual review required
queue = TaskQueue(
    conn,
    name="payments",
    enable_dlq=True,
    auto_retry_dlq=False  # Manual review required
)

# For resilient jobs - auto-retry with longer delay
queue = TaskQueue(
    conn,
    name="notifications",
    enable_dlq=True,
    auto_retry_dlq=True,
    auto_retry_delay=1800  # 30 minutes
)

# For non-critical jobs - disable DLQ, just fail
queue = TaskQueue(
    conn,
    name="analytics",
    enable_dlq=False  # Just move to failed state
)
```

## Examples

### Simple Email Queue

```python
from connection import RedisConnection
from queue import TaskQueue
from worker import Worker
from job import JobOptions
from types import Priority
import smtplib
from email.mime.text import MIMEText

# Setup
conn = RedisConnection(host="localhost", port=6379)
queue = TaskQueue(conn, name="email-queue")

# Add jobs
queue.add(
    name="send_email",
    payload={
        "to": "user@example.com",
        "subject": "Welcome!",
        "body": "Thanks for signing up!"
    },
    options=JobOptions(priority=Priority.HIGH, max_retries=3)
)

# Worker
worker = Worker(queue=queue, concurrency=5)

@worker.process("send_email")
def send_email(payload):
    msg = MIMEText(payload["body"])
    msg["Subject"] = payload["subject"]
    msg["To"] = payload["to"]
    
    with smtplib.SMTP("localhost") as server:
        server.send_message(msg)
    
    return {"status": "sent"}

worker.start()
```

### Image Processing Pipeline

```python
from connection import RedisConnection
from queue import TaskQueue
from worker import Worker
from job import JobOptions
from PIL import Image

conn = RedisConnection()
queue = TaskQueue(conn, name="image-processing")

# Add image processing job
queue.add(
    name="resize_image",
    payload={
        "input_path": "/path/to/image.jpg",
        "output_path": "/path/to/thumb.jpg",
        "width": 300,
        "height": 300
    },
    options=JobOptions(
        timeout=60,
        max_retries=2,
        backoff_type="exponential"
    )
)

# Worker with progress tracking
worker = Worker(queue=queue)

@worker.process("resize_image")
def resize_image(payload):
    img = Image.open(payload["input_path"])
    
    # Update progress
    queue.update_progress(payload["job_id"], 50)
    
    img.thumbnail((payload["width"], payload["height"]))
    img.save(payload["output_path"])
    
    # Update progress
    queue.update_progress(payload["job_id"], 100)
    
    return {"output": payload["output_path"]}

worker.start()
```

### Rate-Limited API Calls

```python
from connection import RedisConnection
from queue import TaskQueue
from worker import Worker
from rate_limit import TokenBucketRateLimiter
import requests

conn = RedisConnection()
queue = TaskQueue(conn, name="api-calls")

# Rate limiter: 100 requests per minute
rate_limiter = TokenBucketRateLimiter(
    redis=conn.client,
    max_tokens=100,
    refill_rate=100/60  # ~1.67 per second
)

worker = Worker(
    queue=queue,
    concurrency=10,
    rate_limiter=rate_limiter
)

@worker.process("fetch_data")
def fetch_data(payload):
    response = requests.get(payload["url"])
    return response.json()

# Add jobs
for url in urls:
    queue.add("fetch_data", {"url": url})

worker.start()
```

### Scheduled Daily Reports

```python
from connection import RedisConnection
from queue import TaskQueue
from job import JobOptions
from datetime import datetime, timedelta

conn = RedisConnection()
queue = TaskQueue(conn, name="reports")

# Schedule report for midnight
midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
midnight += timedelta(days=1)
delay_seconds = int((midnight - datetime.now()).total_seconds())

queue.add(
    name="daily_report",
    payload={"report_date": midnight.isoformat()},
    options=JobOptions(delay=delay_seconds)
)
```

## Monitoring

### Queue Statistics

Get comprehensive queue statistics for monitoring:

```python
# Get comprehensive queue statistics
counts = queue.get_counts()
print(f"""
Queue Statistics:
- Waiting: {counts['waiting']}
- Active: {counts['active']}
- Delayed: {counts['delayed']}
- Completed: {counts['completed']}
- Failed: {counts['failed']}
- Dead Letter: {counts['dead_letter']}
- Priority Critical: {counts['priority_critical']}
- Priority High: {counts['priority_high']}
- Priority Normal: {counts['priority_normal']}
- Priority Low: {counts['priority_low']}
""")
```

### Worker Statistics

Monitor worker performance and active jobs:

```python
# Get worker statistics
stats = worker.get_stats()
print(f"""
Worker Statistics:
- Running: {stats['running']}
- Active Jobs: {stats['active_jobs']}
- Concurrency: {stats['concurrency']}
- Queue Counts: {stats['queue_counts']}
""")
```

### Job Progress Tracking

Track progress for long-running jobs:

```python
# In your job processor
def process_large_file(payload):
    file_path = payload["file_path"]
    total_lines = count_lines(file_path)
    
    for i, line in enumerate(open(file_path)):
        process_line(line)
        
        # Update progress every 100 lines
        if i % 100 == 0:
            progress = int((i / total_lines) * 100)
            queue.update_progress(payload["job_id"], progress)
    
    return {"processed": total_lines}
```

### Structured Logging

Implementation: [logger.py](logger.py)

All components use structured logging for easy monitoring:

```python
# Log output is JSON-formatted
{
  "level": "INFO",
  "message": "Job added",
  "logger": "queue.email-queue",
  "timestamp": "2025-01-01T12:00:00.000000",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "job_name": "send_email",
  "priority": "HIGH"
}

{
  "level": "ERROR",
  "message": "Job execution failed",
  "logger": "worker.email-queue",
  "timestamp": "2025-01-01T12:00:05.000000",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "error": "SMTP connection timeout"
}
```

## Configuration

### Redis Connection

Configure Redis connection with various options:

```python
from connection import RedisConnection

conn = RedisConnection(
    host="localhost",
    port=6379,
    db=0,
    password=None,
    socket_timeout=5,
    socket_connect_timeout=5,
    decode_responses=True
)
```

### Queue Configuration

Configure queue behavior:

```python
from queue import TaskQueue
from serializer import JSONSerializer, PickleSerializer

# Use JSON serializer (default)
queue = TaskQueue(
    connection=conn,
    name="tasks",
    serializer=JSONSerializer()
)

# Or use Pickle serializer for complex objects
queue = TaskQueue(
    connection=conn,
    name="tasks",
    serializer=PickleSerializer()
)

# Enable Dead Letter Queue
queue = TaskQueue(
    connection=conn,
    name="tasks",
    enable_dlq=True,              # Enable DLQ (default: False)
    auto_retry_dlq=True,          # Auto-retry from DLQ (default: False)
    auto_retry_delay=600          # Retry delay in seconds (default: 600)
)
```

### Worker Configuration

Configure worker behavior:

```python
worker = Worker(
    queue=queue,
    concurrency=1,           # Number of concurrent jobs
    rate_limiter=None,       # Optional rate limiter
    middleware_chain=None    # Optional middleware chain
)
```

### Job Options

Configure individual job behavior:

```python
from job import JobOptions
from types import Priority

options = JobOptions(
    max_retries=5,              # Maximum retry attempts (default: 3)
    timeout=120,                # Job timeout in seconds (default: None)
    priority=Priority.HIGH,     # Job priority level (default: NORMAL)
    delay=3600,                 # Delay before execution in seconds (default: 0)
    backoff_type="exponential_jitter",  # Backoff strategy (default: "exponential")
    backoff_delay=2000          # Base delay for backoff in ms (default: 1000)
)
```

## Architecture

### Project Structure

```
pyqueue/
├── connection.py           # Redis connection management
├── job.py                  # Job model and state management
├── queue.py                # Queue operations and job lifecycle
├── worker.py               # Worker with concurrent processing
├── types.py                # Type definitions and enums
├── exceptions.py           # Custom exceptions
├── utils/
│   ├── backoff.py         # Retry backoff strategies
│   ├── logger.py          # Structured logging
│   └── serializer.py      # JSON and Pickle serializers
├── middleware/
│   └── base.py            # Middleware system
└── rate_limit.py          # Rate limiting implementations
```

### Redis Keys Structure

```
queue:{name}:waiting              # List of waiting jobs
queue:{name}:active               # List of active jobs
queue:{name}:delayed              # Sorted set of delayed jobs (score = timestamp)
queue:{name}:completed            # Sorted set of completed jobs
queue:{name}:failed               # Sorted set of failed jobs
queue:{name}:dead_letter          # Sorted set of dead letter jobs
queue:{name}:dead_letter:scheduled_retry  # Sorted set for auto-retry from DLQ
queue:{name}:priority:{level}     # Lists for each priority level
queue:{name}:job:{id}             # Job data hash
queue:{name}:paused               # Pause flag
```

### Key Design Decisions

**1. Redis as Backend**
- Atomic operations ensure data consistency
- Sorted sets for efficient delayed job scheduling
- Lists for FIFO queue semantics

**2. Priority Queue Implementation**
- Separate Redis lists for each priority level
- Worker checks CRITICAL → HIGH → NORMAL → LOW in order
- Prevents priority inversion while maintaining fairness

**3. Retry Mechanism**
- Configurable backoff strategies
- Automatic retry count tracking
- Failed jobs move to delayed queue with backoff delay
- Max retries prevents infinite loops

**4. Dead Letter Queue**
- Jobs that exhaust all retries move to DLQ
- Optional auto-retry with configurable delay
- Manual retry capability for investigation
- Preserves job data and metadata for debugging

**5. Concurrency Model**
- ThreadPoolExecutor for concurrent job processing
- Main thread polls for new jobs
- Worker threads execute job processors
- Graceful shutdown waits for active jobs

**6. State Management**
- Jobs transition through well-defined states
- All state changes persisted to Redis
- Timestamps track state transitions
- Enables monitoring and debugging

## Benchmark

| Workers | Jobs  | Work (ms) | Jobs/min | Success% |
|--------:|------:|-----------:|----------:|----------:|
| 4       | 2000  | 10         | 2020      | 100% |
| 8       | 5000  | 10         | 3910      | 100% |
| 16      | 10000 | 10         | 7641      | 100% |
| 32      | 20000 | 10         | **17240** | 100% |
| 16      | 10000 | 5          | 7367      | 100% |
| 32      | 20000 | 5          | 17114     | 100% |

**Maximum throughput:** **~17k jobs/min**  
**Best configuration:** **32 workers @ 10ms per job**

> Note: These numbers come from a local machine. Real-world performance will vary depending on server hardware, Redis configuration, and workload characteristics.

### **Run the benchmark**

```bash
make benchmark
```

## Development

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/william1nguyen/pyqueue.git
cd pyqueue

# Install dependencies
uv sync
```

### Running the Project

```bash
# Start Redis using Docker Compose
./start-dev-env.sh

# Stop Redis
./stop-dev-env.sh
```

### Docker Compose Configuration

The project includes a Docker Compose configuration for Redis:

```yaml
services:
  redis:
    image: bitnamilegacy/redis:7.2.5
    container_name: redis
    hostname: redis
    volumes:
      - redis_data:/bitnami/redis
    environment:
      REDIS_PASSWORD: ${REDIS_PASSWORD:-redisadmin}
    ports:
      - "6379:6379"
```

## Performance Considerations

### Design Choices

- **Redis Pipelining**: Batch operations reduce network round-trips
- **Priority Separation**: Separate lists for each priority level for efficient lookup
- **Sorted Sets**: Time-based queries for delayed jobs using Redis sorted sets
- **ThreadPoolExecutor**: Efficient concurrent job processing
- **Non-blocking Operations**: Uses blocking Redis operations (BLPOP) to reduce CPU usage

### Scalability

- **Horizontal Scaling**: Add more workers to increase throughput
- **Vertical Scaling**: Increase worker concurrency for I/O-bound tasks
- **Multiple Queues**: Separate queues for different job types

### Recommendations

- Keep job payloads small (< 1MB recommended)
- Use automatic cleanup to prevent unbounded Redis memory growth
- Configure appropriate concurrency based on job type (CPU vs I/O bound)
- Monitor queue depths to identify bottlenecks
- Use DLQ for critical jobs that need manual review
- Enable auto-retry DLQ for transient failures

**Note**: Performance varies based on job complexity, network latency, and Redis configuration. Test with your specific workload before deploying at scale.

## TODO

- [x] Redis connection management
- [x] Job model with state management
- [x] Priority queue implementation
- [x] Worker with concurrent processing
- [x] Automatic retry with backoff strategies
- [x] Rate limiting (Token Bucket, Sliding Window, Leaky Bucket)
- [x] Middleware system
- [x] Structured logging
- [x] Delayed job scheduling
- [x] Job progress tracking
- [x] Graceful shutdown
- [x] Queue pause/resume
- [x] Queue statistics and monitoring
- [x] Job cleanup with grace period
- [x] Dead letter queue for permanently failed jobs
- [ ] Job dependencies (parent-child relationships)
- [ ] Repeatable jobs (cron-like scheduling)
- [ ] Redis Cluster support for high availability
- [x] Comprehensive test suite and benchmarks
- [ ] Web UI for monitoring and management