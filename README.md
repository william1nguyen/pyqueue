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
- **Job States**: WAITING, ACTIVE, COMPLETED, FAILED, DELAYED, RETRYING
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
worker = Worker(conn, queue_name="email-queue", concurrency=5)

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
- State management (WAITING, ACTIVE, COMPLETED, FAILED, DELAYED, RETRYING)
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

**Example:**

```python
from connection import RedisConnection
from queue import TaskQueue
from job import JobOptions
from types import Priority

conn = RedisConnection()
queue = TaskQueue(conn, name="tasks")

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
print(f"Waiting: {counts['waiting']}, Active: {counts['active']}")

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

**Configuration:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `connection` | RedisConnection | Required | Redis connection instance |
| `queue_name` | str | "default" | Queue name to process |
| `concurrency` | int | 1 | Number of concurrent jobs |
| `rate_limiter` | RateLimiter | None | Rate limiter instance |
| `middleware_chain` | MiddlewareChain | None | Middleware chain |

**Example:**

```python
from connection import RedisConnection
from worker import Worker
from rate_limit import TokenBucketRateLimiter
from middleware.base import Middleware, MiddlewareChain

# Setup
conn = RedisConnection()

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
    connection=conn,
    queue_name="my-queue",
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

worker = Worker(conn, "api-calls", rate_limiter=limiter)
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
    connection=conn,
    queue_name="tasks",
    middleware_chain=chain
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
worker = Worker(conn, queue_name="email-queue", concurrency=5)

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
worker = Worker(conn, queue_name="image-processing")

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
    conn,
    queue_name="api-calls",
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
```

### Worker Configuration

Configure worker behavior:

```python
worker = Worker(
    connection=conn,
    queue_name="default",
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

### Redis Keys Structure

```
queue:{name}:waiting              # List of waiting jobs
queue:{name}:active               # List of active jobs
queue:{name}:delayed              # Sorted set of delayed jobs (score = timestamp)
queue:{name}:completed            # Sorted set of completed jobs
queue:{name}:failed               # Sorted set of failed jobs
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

**4. Concurrency Model**
- ThreadPoolExecutor for concurrent job processing
- Main thread polls for new jobs
- Worker threads execute job processors
- Graceful shutdown waits for active jobs

**5. State Management**
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
- [ ] Job dependencies (parent-child relationships)
- [ ] Repeatable jobs (cron-like scheduling)
- [ ] Dead letter queue for permanently failed jobs
- [ ] Redis Cluster support for high availability
- [x] Comprehensive test suite and benchmarks
- [ ] Web UI for monitoring and management
