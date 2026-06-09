<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Worker;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\TimeoutStamp;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Tests\Fixture\SlowMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Metrics\InMemoryMetrics;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

final class WorkerTimeoutTest extends AsyncTestCase
{
    public function timesOutBlockingSleep(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(Envelope::wrap(
            new SlowMessage(sleepMs: 3000),
            new TimeoutStamp(timeoutMs: 200),
        ));
        $transport->close();

        $metrics = new InMemoryMetrics();
        $worker = new Worker(
            transport: $transport,
            handlers: [
                SlowMessage::class => static function (SlowMessage $m): void {
                    sleep((int) ceil($m->sleepMs / 1000));
                    echo "You weren't supposed to see this message.\n";
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 1);

        var_dump($transport);
        var_dump($metrics);
        Assert::same($metrics->timedOut, 1);
        Assert::same($transport->ackedCount, 0);
        Assert::same($transport->rejectedCount, 1);
    }

    public function timesOutAsyncSleep(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(Envelope::wrap(
            new PingMessage(),
            new TimeoutStamp(timeoutMs: 200),
        ));
        $transport->close();

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function (): void {
                    \Async\delay(5000);
                    echo "You weren't supposed to see this message.\n";
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 1);

        var_dump($transport);
        Assert::same($transport->ackedCount, 0);
        Assert::same($transport->rejectedCount, 1);
    }

    public function finallyRunsOnTimeout(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(Envelope::wrap(
            new PingMessage(),
            new TimeoutStamp(timeoutMs: 200),
        ));
        $transport->close();

        $markerFile = sys_get_temp_dir() . '/thrun_finally_test_' . uniqid() . '.txt';

        $metrics = new InMemoryMetrics();
        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function () use ($markerFile): void {
                    try {
                        sleep(3);
                    } finally {
                        file_put_contents($markerFile, '1');
                    }
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport);

        Assert::same($metrics->timedOut, 1);
        Assert::same($transport->ackedCount, 0);
        Assert::same($transport->rejectedCount, 1);
        Assert::true(file_exists($markerFile));
        Assert::same(file_get_contents($markerFile), '1');
        @unlink($markerFile);
    }

    public function noTimeoutWhenStampIsZero(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(Envelope::wrap(
            new PingMessage(),
            new TimeoutStamp(timeoutMs: 0),
        ));
        $transport->close();

        $metrics = new InMemoryMetrics();

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static fn() => null,
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport);

        Assert::same($transport->ackedCount, 1);
        Assert::same($transport->rejectedCount, 0);
        Assert::same($metrics->timedOut, 0);
    }
}
