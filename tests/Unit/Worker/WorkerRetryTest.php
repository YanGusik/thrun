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
use Thrun\Worker\Retry\FixedDelayStrategy;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

final class WorkerRetryTest extends AsyncTestCase
{
    public function retrySuccessOnSecondAttempt(): void
    {
        $transport = new InMemoryTransport();
        $failureTransport = new InMemoryTransport();
        $transport->send(Envelope::wrap(
            new PingMessage(),
            new RetryStamp(strategy: new FixedDelayStrategy(delayMs: 0, maxAttempts: 3)),
        ));

        $counterFile = sys_get_temp_dir() . '/thrun_retry_counter_' . uniqid() . '.txt';
        file_put_contents($counterFile, '0');

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function () use ($counterFile): void {
                    $count = (int) file_get_contents($counterFile);
                    file_put_contents($counterFile, (string) ($count + 1));
                    if ($count < 1) {
                        throw new \RuntimeException('fail');
                    }
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            failureTransport: $failureTransport,
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 1, expectedRejected: 1);

        Assert::same($transport->ackedCount, 1);
        Assert::same($transport->rejectedCount, 1);
        Assert::same(count($failureTransport->sentEnvelopes), 0);

        @unlink($counterFile);
    }

    public function retryExhaustedGoesToFailureTransport(): void
    {
        $transport = new InMemoryTransport();
        $failureTransport = new InMemoryTransport();
        $transport->send(Envelope::wrap(
            new PingMessage(),
            new RetryStamp(strategy: new FixedDelayStrategy(delayMs: 0, maxAttempts: 2)),
        ));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function (): void {
                    throw new \RuntimeException('always fails');
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            failureTransport: $failureTransport,
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 3);

        Assert::same($transport->rejectedCount, 3);
        Assert::same($transport->ackedCount, 0);

        Assert::same(count($failureTransport->sentEnvelopes), 1);
        $failedEnvelope = $failureTransport->sentEnvelopes[0];
        Assert::true($failedEnvelope->has(\Thrun\Envelope\Stamp\ErrorDetailsStamp::class));
    }

    public function noRetryWithoutStamp(): void
    {
        $transport = new InMemoryTransport();
        $failureTransport = new InMemoryTransport();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function (): void {
                    throw new \RuntimeException('fail');
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            failureTransport: $failureTransport,
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 1);

        Assert::same($transport->rejectedCount, 1);
        Assert::same($transport->ackedCount, 0);
        Assert::same(count($failureTransport->sentEnvelopes), 1);
        Assert::true($failureTransport->sentEnvelopes[0]->has(\Thrun\Envelope\Stamp\ErrorDetailsStamp::class));
    }

    public function timeoutIsRetryable(): void
    {
        $transport = new InMemoryTransport();
        $failureTransport = new InMemoryTransport();
        $transport->send(Envelope::wrap(
            new PingMessage(),
            new RetryStamp(strategy: new FixedDelayStrategy(delayMs: 0, maxAttempts: 1)),
            new TimeoutStamp(timeoutMs: 100),
        ));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function (): void {
                    sleep(3);
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            failureTransport: $failureTransport,
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 2);

        Assert::same($transport->rejectedCount, 2);
        Assert::same($transport->ackedCount, 0);
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
        $timeoutNs = 5 * 1e9;
        while (true) {
            if ($transport->ackedCount >= $expectedAcked
                && $transport->rejectedCount >= $expectedRejected
            ) {
                \Async\delay(200);
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
