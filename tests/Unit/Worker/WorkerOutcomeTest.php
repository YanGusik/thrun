<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Worker;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Acknowledger;
use Thrun\Worker\Metrics\InMemoryMetrics;
use Thrun\Worker\Outcome;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

/**
 * A handler that settled the message itself reports the outcome through
 * observe(). The worker counts it and acknowledges the message; rejecting it,
 * storing it or retrying it would duplicate whatever settled it.
 */
final class WorkerOutcomeTest extends AsyncTestCase
{
    public function countsAnObservedFailureWithoutActingOnIt(): void
    {
        $transport       = new InMemoryTransport();
        $failedTransport = new InMemoryTransport();
        $metrics         = new InMemoryMetrics();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function (PingMessage $m, Acknowledger $ack): void {
                    $ack->observe(Outcome::Failure, new \RuntimeException('settled elsewhere'));
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            failureTransport: $failedTransport,
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 1);

        Assert::same($metrics->failed, 1, 'failed');
        Assert::same($metrics->processed, 0, 'processed');
        Assert::same($transport->ackedCount, 1, 'ackedCount');
        Assert::same($transport->rejectedCount, 0, 'rejectedCount');
        Assert::same(count($failedTransport->sentEnvelopes), 0, 'sentEnvelopes');
    }

    public function countsAnObservedRetryAsRetriedAndSendsNoCopy(): void
    {
        $transport = new InMemoryTransport();
        $metrics   = new InMemoryMetrics();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function (PingMessage $m, Acknowledger $ack): void {
                    $ack->observe(Outcome::Retried);
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 1);

        Assert::same($metrics->retried, 1, 'retried');
        Assert::same($metrics->processed, 0, 'processed');
        Assert::same($metrics->failed, 0, 'failed');
        // The retry belongs to whoever scheduled it, so the transport received
        // no copy of the message.
        Assert::same(count($transport->sentEnvelopes), 1, 'sentEnvelopes');
        Assert::same($transport->ackedCount, 1, 'ackedCount');
    }

    public function countsNothingForASkippedMessage(): void
    {
        $transport = new InMemoryTransport();
        $metrics   = new InMemoryMetrics();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function (PingMessage $m, Acknowledger $ack): void {
                    $ack->observe(Outcome::Skipped);
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 1);

        Assert::same($metrics->processed, 0, 'processed');
        Assert::same($metrics->failed, 0, 'failed');
        Assert::same($metrics->retried, 0, 'retried');
        Assert::same($transport->ackedCount, 1, 'ackedCount');
    }

    public function anObservedOutcomeWinsOverALaterFail(): void
    {
        $transport       = new InMemoryTransport();
        $failedTransport = new InMemoryTransport();
        $metrics         = new InMemoryMetrics();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function (PingMessage $m, Acknowledger $ack): void {
                    $ack->observe(Outcome::Failure);
                    $ack->fail(new \RuntimeException('too late'));
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            failureTransport: $failedTransport,
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 1);

        Assert::same($metrics->failed, 1, 'failed');
        Assert::same($transport->rejectedCount, 0, 'rejectedCount');
        Assert::same(count($failedTransport->sentEnvelopes), 0, 'sentEnvelopes');
    }

    public function aHandlerThatOnlyAcksStillCountsAsProcessed(): void
    {
        $transport = new InMemoryTransport();
        $metrics   = new InMemoryMetrics();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static fn(PingMessage $m) => null,
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 1);

        Assert::same($metrics->processed, 1, 'processed');
        Assert::same($metrics->failed, 0, 'failed');
    }
}
