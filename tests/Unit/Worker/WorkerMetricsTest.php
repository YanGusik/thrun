<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Worker;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\RetryStamp;
use Thrun\Envelope\Stamp\TimeoutStamp;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Metrics\InMemoryMetrics;
use Thrun\Worker\Retry\FixedDelayStrategy;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

final class WorkerMetricsTest extends AsyncTestCase
{
    public function countsProcessed(): void
    {
        $transport = new InMemoryTransport();
        $metrics = new InMemoryMetrics();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [PingMessage::class => static fn() => null],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 1);

        Assert::same($metrics->processed, 1);
        Assert::same($metrics->failed, 0);
        Assert::same($metrics->retried, 0);
        Assert::same($metrics->timedOut, 0);
    }

    public function countsFailed(): void
    {
        $transport = new InMemoryTransport();
        $metrics = new InMemoryMetrics();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [PingMessage::class => static fn() => throw new \RuntimeException('fail')],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 1);

        Assert::same($metrics->processed, 0);
        Assert::same($metrics->failed, 1);
        Assert::same($metrics->retried, 0);
    }

    public function countsRetried(): void
    {
        $transport = new InMemoryTransport();
        $metrics = new InMemoryMetrics();
        $transport->send(Envelope::wrap(
            new PingMessage(),
            new RetryStamp(strategy: new FixedDelayStrategy(delayMs: 0, maxAttempts: 1)),
        ));

        $worker = new Worker(
            transport: $transport,
            handlers: [PingMessage::class => static fn() => throw new \RuntimeException('fail')],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 2);

        Assert::same($metrics->failed, 2);
        Assert::same($metrics->retried, 1);
    }

    public function countsTimedOut(): void
    {
        $transport = new InMemoryTransport();
        $metrics = new InMemoryMetrics();
        $transport->send(Envelope::wrap(
            new PingMessage(),
            new TimeoutStamp(timeoutMs: 50),
        ));

        $worker = new Worker(
            transport: $transport,
            handlers: [PingMessage::class => static function (): void { sleep(3); }],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 1);

        Assert::same($metrics->failed, 1);
        Assert::same($metrics->timedOut, 1);
    }

    public function recordsProcessingTime(): void
    {
        $transport = new InMemoryTransport();
        $metrics = new InMemoryMetrics();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [PingMessage::class => static function (): void { \Async\delay(50); }],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 1);

        Assert::same(count($metrics->processingTimes), 1);
        Assert::same($metrics->processingTimes[0] > 0, true);
    }

    private function runWorkerAndWait(
        Worker $worker,
        InMemoryTransport $transport,
        int $expectedAcked = 0,
        int $expectedRejected = 0,
    ): void {
        $coro = \Async\spawn(function () use ($worker): void {
            $worker->run();
        });

        $start = hrtime(true);
        $timeoutNs = 3 * 1e9;
        while (true) {
            if ($transport->ackedCount >= $expectedAcked
                && $transport->rejectedCount >= $expectedRejected
            ) {
                \Async\delay(500);
                if ($transport->ackedCount >= $expectedAcked
                    && $transport->rejectedCount >= $expectedRejected
                ) {
                    break;
                }
            }
            if ((hrtime(true) - $start) > $timeoutNs) {
                break;
            }
            \Async\delay(10);
        }

        $transport->close();
        \Async\await($coro);
    }
}
