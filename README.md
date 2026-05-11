# Thrun

Async queue worker for PHP built on [TrueAsync](https://github.com/true-async) - alternative PHP core that implements true asynchrony by modifying the Zend engine, I/O libraries, database and socket handling.

## Goal

The fastest async queue worker for PHP - one worker process that handles both IO-bound and CPU-bound tasks efficiently. Uses real OS threads instead of forked processes, consumes significantly less memory, and aims to outperform Symfony Messenger and Laravel Horizon.

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

**N OS Threads x M Coroutines**

Each Worker spawns N real OS threads. Each thread runs a TaskGroup of M coroutines. Total concurrency is N * M. ThreadChannel provides backpressure and blocks when all coroutines are busy.

**Per-Message Timeout (Hard Cancel)**

Attach a TimeoutStamp to any message. If the handler exceeds the limit, it receives a hard cancellation that interrupts blocking operations like `sleep`, `file_get_contents`, or DB queries. `finally` blocks still execute.

```php
use Thrun\Envelope\Stamp\TimeoutStamp;

$transport->send(Envelope::wrap(
    new SlowApiCall(),
    new TimeoutStamp(5000), // 5 seconds
));
```

**Retry with Delay**

Attach a RetryStamp with a strategy. Failed messages are retried with a configurable delay.

```php
use Thrun\Envelope\Stamp\RetryStamp;
use Thrun\Worker\Retry\ExponentialBackoffStrategy;

$transport->send(Envelope::wrap(
    new SendEmailMessage('user@example.com', 'Hello'),
    new RetryStamp(strategy: new ExponentialBackoffStrategy(1000, 3)),
));
```

Strategies: `NoRetryStrategy`, `FixedDelayStrategy`, `ExponentialBackoffStrategy`.

**Metrics**

Inject a MetricsInterface to track throughput, failures, retries, timeouts, and average processing time.

```php
use Thrun\Worker\Metrics\InMemoryMetrics;

$metrics = new InMemoryMetrics();
$worker = new Worker(transport: $transport, handlers: $handlers, metrics: $metrics);

// $metrics->processed, $metrics->failed, $metrics->retried, $metrics->timedOut
// $metrics->averageTime()
```

**Multi-Queue and Scheduling**

One Worker can serve multiple queues simultaneously with pluggable scheduling:

- `RoundRobinStrategy` - equal distribution
- `PriorityStrategy` - weighted credits, proportional distribution

**Dispatch Policies**

Limit concurrency per partition (tenant, user, etc.) via PartitionStamp:

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

Supervisor handles SIGINT/SIGTERM gracefully. `Worker::stop()` cancels the scope and closes the transport. The producer unblocks from `receive()`. Pending jobs complete or timeout.

**Explicit Acknowledgement**

Handlers can accept an Acknowledger for explicit control:

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

**Redis Transport**

At-least-once delivery via `LMOVE` from ready to processing. Delayed messages use a sorted set (`ZADD`). Reclaims the processing list on startup for crash recovery.

```php
use Thrun\Transport\Redis\RedisTransport;
use Thrun\Transport\Redis\RedisConnection;
use Thrun\Serialization\JsonSerializer;

$redis = new \Redis();
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

Failed messages carry an ErrorDetailsStamp with the exception class, message, code, and trace.

## Examples

See the `examples/` directory:

- `one_queue.php` - basic single-queue worker
- `two_queue.php` - multi-queue with priority
- `round_robin.php` - round-robin scheduling
- `priority.php` - priority strategy
- `max_concurrency.php` - per-partition concurrency limits
- `retry.php` - retry with fixed delay
- `metrics.php` - live console metrics reporter

Run examples directly if you have TrueAsync PHP installed, or via Docker (see below).

## Architecture

Detailed architecture: `docs/architecture.md`
Development plan: `docs/development-plan.md`

## Testing

Tests use `testo`. No mocking of TrueAsync internals - test with `InMemoryTransport` and real async execution.

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
