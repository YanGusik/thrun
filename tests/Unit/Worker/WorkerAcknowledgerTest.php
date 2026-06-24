<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Worker;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\DelayStamp;
use Thrun\Envelope\Stamp\RedeliveryStamp;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Acknowledger;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;
use function Async\await;

final class WorkerAcknowledgerTest extends AsyncTestCase
{
    public function handlerWithAcknowledgerCanRetry(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function (PingMessage $msg, Acknowledger $ack): void {
                    $attempts = $ack->envelope->last(RedeliveryStamp::class)?->attempt ?? 0;
                    if ($attempts === 0) {
                        $ack->retry(50);
                    } else {
                        $ack->ack();
                    }
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 1, expectedRejected: 1);

        Assert::same($transport->ackedCount, 1, 'ackedCount');
        Assert::same($transport->rejectedCount, 1, 'rejectedCount');
        // Retry envelope has DelayStamp
        Assert::same(count($transport->sentEnvelopes), 2);
        Assert::true($transport->sentEnvelopes[1]->has(DelayStamp::class));
    }

    public function handlerWithAcknowledgerCanFail(): void
    {
        $transport        = new InMemoryTransport();
        $failureTransport = new InMemoryTransport();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function (PingMessage $msg, Acknowledger $ack): void {
                    $ack->fail(new \RuntimeException('Custom fail'));
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            failureTransport: $failureTransport,
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 1);

        Assert::same($transport->rejectedCount, 1);
        Assert::same(count($failureTransport->sentEnvelopes), 1);
    }

    public function singleParamHandlerStillWorks(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [PingMessage::class => static fn(PingMessage $msg) => null],
            options: new WorkerOptions(threads: 1, concurrency: 1),
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 1);

        Assert::same($transport->ackedCount, 1);
    }

    public function rejectIfNotAcceptedAck(): void
    {
        $transport        = new InMemoryTransport();
        $failureTransport = new InMemoryTransport();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function (PingMessage $msg, Acknowledger $ack): void {
                    // nothing
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            failureTransport: $failureTransport,
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 1);

        Assert::same($transport->ackedCount, 0, 'acked');
        Assert::same($transport->rejectedCount, 1, 'rejected');
        Assert::same(count($failureTransport->sentEnvelopes), 1, 'failureTransport');
    }
}
