# Thrun

> **⚠️ Work in progress**  
> This library is actively developed. APIs may change between commits.

Async queue worker for PHP built on [TrueAsync](https://github.com/true-async) — an alternative PHP core that implements true asynchrony by modifying the Zend engine, I/O libraries, database and socket handling.

## Goal

The fastest async queue worker for PHP — one worker process that handles both IO-bound and CPU-bound tasks efficiently. Uses real OS threads instead of forked processes, consumes significantly less memory, and aims to outperform Symfony Messenger and Laravel Horizon.

## Benchmarks

Measured on WSL2, 8GB RAM, PHP 8.6 TrueAsync fork:

| Scenario | IO throughput | CPU throughput | Stable RSS |
|---|---|---|---|
| Horizon 1 worker | 18/s | 73/s | 72 MB |
| Horizon 12 workers | 210/s | 514/s | 949 MB |
| TrueAsync 1x100 | 1,869/s | 452/s | 44 MB |
| TrueAsync 12x10 | 2,355/s | 2,059/s | 54 MB |

TrueAsync 12x10 uses **17x less RSS** than Horizon 12 workers, **11x more IO throughput**.

## Requirements

- TrueAsync PHP 8.6+ (`trueasync/php-true-async` Docker image)
- ext-pcntl (for signal handling)
- ext-redis (Edmond's TrueAsync-compatible fork for Redis transport)

## Installation

```bash
composer require yangusik/thrun
```

Package is in development and may change name.

## Quick Start

```php
use Thrun\Envelope\Envelope;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

$transport = new InMemoryTransport();

// Send messages
$transport->send(Envelope::wrap(new SendEmailMessage('user@example.com', 'Hello')));

$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: $transport,
        handlers: [
            SendEmailMessage::class => fn(SendEmailMessage $m) => mail($m->to, $m->subject, '...'),
        ],
        options: new WorkerOptions(threads: 2, concurrency: 4),
    ),
    options: new SupervisorOptions(),
);

$supervisor->run();
```

## Features

**N OS Threads × M Coroutines**

Each Worker spawns N real OS threads. Each thread runs a TaskGroup of M coroutines. Total concurrency is N × M. ThreadChannel provides backpressure and blocks when all coroutines are busy.

**Per-Message Timeout (Hard Cancel)**

Attach a `TimeoutStamp` to any message. If the handler exceeds the limit, it receives a hard cancellation that interrupts blocking operations like `sleep`, `file_get_contents`, or DB queries. `finally` blocks still execute.

```php
use Thrun\Envelope\Stamp\TimeoutStamp;

$transport->send(Envelope::wrap(
    new SlowApiCall(),
    new TimeoutStamp(5000), // 5 seconds
));
```

**Retry with Delay**

Two stamps work together:

- `RetryStamp` — defines the retry policy (backoff intervals and max attempts).
- `RedeliveryStamp` — added automatically by the worker on every redelivery. Keeps history of attempts and timestamps.

```php
use Thrun\Envelope\Stamp\RetryStamp;
use Thrun\Envelope\Stamp\MessageIdStamp;

$transport->send(Envelope::wrap(
    new SendEmailMessage('user@example.com', 'Hello'),
    new RetryStamp(backoff: [1000, 2000, 4000], maxAttempts: 3),
    new MessageIdStamp('msg-42'),
));
```

Alternatively, the handler can trigger a retry explicitly via `Acknowledger` without pre-attaching a stamp (see `examples/retry_without_stamp.php`).

**Metrics**

Inject a `MetricsInterface` to track throughput, failures, retries, timeouts, and average processing time.

```php
use Thrun\Worker\Metrics\InMemoryMetrics;

$metrics = new InMemoryMetrics();
$worker = new Worker(transport: $transport, handlers: $handlers, metrics: $metrics);

// $metrics->processed, $metrics->failed, $metrics->retried, $metrics->timedOut
// $metrics->averageTime()
```

**Multi-Queue and Scheduling**

One Worker can serve multiple queues simultaneously with pluggable scheduling:

- `RoundRobinStrategy` — equal distribution across queues
- `PriorityStrategy` — weighted credits, proportional distribution

**Dispatch Policies**

Limit concurrency per partition (tenant, user, etc.) via `PartitionStamp`:

```php
use Thrun\Envelope\Stamp\PartitionStamp;
use Thrun\Transport\Policy\MaxConcurrencyPolicy;
use Thrun\Transport\PolicyAwareReceiver;

$receiver = new PolicyAwareReceiver(
    inner: $transport,
    policy: new MaxConcurrencyPolicy(maxPerPartition: 5),
);

$transport->send(Envelope::wrap($msg, new PartitionStamp('tenant-42')));
```

**Graceful Shutdown**

Supervisor handles SIGINT/SIGTERM gracefully. `Worker::stop()` cancels the scope and closes internal resources. The producer unblocks from `receive()`. Pending jobs complete or timeout.

**Explicit Acknowledgement**

Handlers can accept an `Acknowledger` for explicit control:

```php
use Thrun\Worker\Acknowledger;

SendEmailMessage::class => function (SendEmailMessage $m, Acknowledger $ack) {
    if ($m->to === 'blocked@example.com') {
        $ack->fail(new \RuntimeException('Blocked'));
        return;
    }
    // process...
    $ack->ack();
}
```

Methods: `$ack->ack()`, `$ack->retry(int $delayMs)`, `$ack->fail(?Throwable)`.

**Middleware**

Wrap message processing in reusable middleware:

```php
use Thrun\Middleware\CatchMessageMiddleware;

$worker = new Worker(
    transport: $transport,
    handlers: $handlers,
    options: new WorkerOptions(middleware: [new CatchMessageMiddleware()]),
);
```

**Stamps**

Messages carry stamps for cross-cutting concerns:

| Stamp | Purpose |
|---|---|
| `DelayStamp` | Defer message delivery |
| `ErrorDetailsStamp` | Captures exception info on failure |
| `MessageIdStamp` | Unique message identifier |
| `PartitionStamp` | Partition key for dispatch policies |
| `QueueStamp` | Queue name (set by MultiQueueReceiver) |
| `RedeliveryStamp` | History of redelivery attempts |
| `RetryStamp` | Retry policy configuration |
| `TimeoutStamp` | Hard timeout for handler execution |

**Redis Transport**

At-least-once delivery via `LMOVE` from ready to processing. Delayed messages use a sorted set (`ZADD`). Reclaims the processing list on startup for crash recovery.

> Uses a custom TrueAsync-compatible fork of `phpredis` (early stage). Connection pooling **must** be enabled:

```php
use Thrun\Transport\Redis\RedisTransport;
use Thrun\Transport\Redis\RedisConnection;
use Thrun\Serialization\JsonSerializer;

$redis = new \Redis([
    'pool' => [
        'enabled' => true,
        'min'     => 0,
        'max'     => 1,
        'mux'     => 0,
    ],
]);
$redis->connect('redis', 6379);

$transport = new RedisTransport(
    connection: new RedisConnection($redis, 'thrun:queue'),
    serializer: new JsonSerializer(),
    queue: 'emails',
);
```

**Failure Transport (Dead Letter)**

Exhausted retries can be sent to a separate transport for inspection:

```php
$failureTransport = new InMemoryTransport(); // or RedisTransport

$worker = new Worker(
    transport: $transport,
    handlers: $handlers,
    failureTransport: $failureTransport,
);
```

Failed messages carry an `ErrorDetailsStamp` with the exception class, message, code, and trace.

## Examples

Run examples directly if you have TrueAsync PHP installed, or via Docker (see below).

### `one_queue.php` — basic single-queue worker
Sends six emails to one queue and processes them sequentially.

```text
[Email] - one@example.com processed
[Email] - two@example.com processed
[Email] - three@example.com processed
[Email] - four@example.com processed
[Email] - five@example.com processed
[Email] - six@example.com processed
```

### `two_queue.php` — two queues with priority
Emails and notifications share one worker. Priority strategy favors emails (weight 3) over notifications (weight 1).

```text
[Email] - one@example.com processed
[Email] - two@example.com processed
[Email] - three@example.com processed
[Notification] - 1 processed
[Email] - four@example.com processed
[Email] - five@example.com processed
[Email] - six@example.com processed
[Notification] - 2 processed
```

### `round_robin.php` — round-robin scheduling
Emails and notifications alternate evenly.

```text
[Email] - one@example.com processed
[Notification] - 1 processed
[Email] - two@example.com processed
[Notification] - 2 processed
[Email] - three@example.com processed
...
```

### `priority.php` — three queues with credits
Emails (3), ping (2), and notifications (1) compete. Higher-priority queues get more slots.

```text
[Email] - one@example.com processed
[Ping] Pong
[Email] - two@example.com processed
[Email] - three@example.com processed
[Ping] Pong
[Email] - four@example.com processed
[Email] - five@example.com processed
[Email] - six@example.com processed
[Notification] - 1 processed
[Notification] - 2 processed
```

### `max_concurrency.php` — per-partition limits
Max 2 concurrent jobs per partition. With 2 threads, the worker processes two messages in parallel, then waits for completion before taking the next batch.

```text
[10:29:46][Email] - one@example.com pending
[10:29:46][Notification] - 1 pending
[10:29:46][Email] - one@example.com completed
[10:29:46][Notification] - 1 completed
[10:29:47][Email] - two@example.com pending
[10:29:47][Notification] - 2 pending
...
```

### `retry.php` — retry with backoff
Messages retry on failure with configurable delays. `RetryStamp` sets the policy; `RedeliveryStamp` tracks each attempt.

```text
[13:44:55][Email][times:0] - one@example.com error
[13:44:55][Email][times:0] - two@example.com error
[13:44:55][Email][times:0] - three@example.com error
[13:44:56][Email][times:1] - one@example.com error
[13:44:56][Email][times:1] - two@example.com error
[13:44:56][Email][times:1] - three@example.com error
[13:44:57][Email][times:2] - one@example.com error
[13:44:57][Email][times:2] - two@example.com processed
[13:44:57][Email][times:2] - three@example.com processed
[13:44:58][Email][times:3] - one@example.com processed
```

### `retry_without_stamp.php` — explicit retry via Acknowledger
Handler decides to retry on its own; no `RetryStamp` is attached upfront.

```text
job retry
job retry (1,2026-06-05T11:39:44+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
job retry (2,2026-06-05T11:39:45+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
...
```

### `middleware.php` — error logging middleware
`CatchMessageMiddleware` prints every exception with message class and location before retry logic kicks in.

```text
[WorkerThread][SendEmailMessage:one] Exception: Custom error (middleware.php:32)
[WorkerThread][SendEmailMessage:two] Exception: Custom error (middleware.php:32)
...
```

### `metrics.php` — live metrics reporter
Real-time console output of processed / failed / retried / timed out counts plus average processing time.

```text
Thrun Metrics Live (stop: Ctrl+C or wait 15s)

  processed: 5   failed: 5   retried: 5   timed out: 0   avg: 0.04ms
  processed: 9   failed: 6   retried: 5   timed out: 0   avg: 0.07ms
...
Done. Processed: 10, Failed: 6, Retried: 5
```

### `redis.php` — Redis-backed transport
Pushes 20 CPU-bound jobs to Redis, then processes them across 12 threads with live progress reporting.

```text
Connected to Redis at redis:6379
Pushed 20 jobs
pending: 20  active: 0  processed: 0  failed: 0  (this run pushed: 20)
pending: 8   active: 12 processed: 0  failed: 0  (this run pushed: 20)
...
All jobs done.
Supervisor finished.
```

## Testing

Tests use `testo`. No mocking of TrueAsync internals — tests run with `InMemoryTransport` and real async execution.

Some transport tests require Redis running on `localhost:6379`.

```bash
vendor/bin/testo
```

## Docker

If you want to test this project in Docker, here is an example setup.

**Dockerfile:**

```dockerfile
FROM trueasync/php-true-async:0.7.0-alpha.9-php8.6

RUN apt-get update && apt-get install -y \
    build-essential \
    autoconf \
    libtool \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Build and install phpredis from trueasync fork
RUN git clone --depth 1 --branch true-async https://github.com/true-async/phpredis.git /tmp/phpredis \
    && cd /tmp/phpredis \
    && phpize \
    && ./configure \
    && make -j$(nproc) \
    && make install \
    && echo 'extension=redis.so' > /etc/php.d/redis.ini \
    && rm -rf /tmp/phpredis
```

**docker-compose.yml:**

```yaml
services:
  dev:
    build:
      context: .
      dockerfile: Dockerfile
    working_dir: /app
    volumes:
      - .:/app

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
```

## License

MIT
