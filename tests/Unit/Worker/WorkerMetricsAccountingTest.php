<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Worker;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\OutcomeMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Acknowledger;
use Thrun\Worker\Metrics\InMemoryMetrics;
use Thrun\Worker\Outcome;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

/**
 * Every message the worker takes has to move exactly one counter, and the
 * counter of running jobs has to come back to zero. A run that mixes outcomes -
 * a job retried a few times before it finally fails, next to one that succeeds -
 * is where a miscount shows.
 */
final class WorkerMetricsAccountingTest extends AsyncTestCase
{
    public function eachMessageMovesExactlyOneCounter(): void
    {
        $transport = new InMemoryTransport();
        $metrics   = new InMemoryMetrics();

        // What one failing job looks like from the queue's side: two attempts
        // Laravel released for a retry, then the attempt that ran out of tries.
        foreach ([Outcome::Retried, Outcome::Retried, Outcome::Failure, Outcome::Success] as $outcome) {
            $transport->send(Envelope::wrap(new OutcomeMessage($outcome->value)));
        }

        $worker = new Worker(
            transport: $transport,
            handlers: [
                OutcomeMessage::class => static function (OutcomeMessage $message, Acknowledger $ack): void {
                    $ack->observe(Outcome::from($message->outcome));
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 4);

        Assert::same($metrics->processed, 1, 'processed');
        Assert::same($metrics->failed, 1, 'failed');
        Assert::same($metrics->retried, 2, 'retried');
        Assert::same($metrics->timedOut, 0, 'timedOut');

        // Four messages in, four counted, none counted twice.
        Assert::same(
            $metrics->processed + $metrics->failed + $metrics->retried,
            4,
            'counters total',
        );

        Assert::same($transport->ackedCount, 4, 'ackedCount');
        Assert::same($transport->rejectedCount, 0, 'rejectedCount');
        Assert::same($metrics->active, 0, 'active');
        Assert::same(count($metrics->processingTimes), 4, 'processing times');
    }

    public function aSkippedMessageIsAcknowledgedAndCountedNowhere(): void
    {
        $transport = new InMemoryTransport();
        $metrics   = new InMemoryMetrics();

        $transport->send(Envelope::wrap(new OutcomeMessage(Outcome::Skipped->value)));
        $transport->send(Envelope::wrap(new OutcomeMessage(Outcome::Success->value)));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                OutcomeMessage::class => static function (OutcomeMessage $message, Acknowledger $ack): void {
                    $ack->observe(Outcome::from($message->outcome));
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 2);

        Assert::same($metrics->processed, 1, 'processed');
        Assert::same($metrics->failed, 0, 'failed');
        Assert::same($metrics->retried, 0, 'retried');
        Assert::same($transport->ackedCount, 2, 'ackedCount');
        Assert::same($metrics->active, 0, 'active');
    }

    public function theRunningCounterReturnsToZeroAfterAFailureTheWorkerHandles(): void
    {
        $transport       = new InMemoryTransport();
        $failedTransport = new InMemoryTransport();
        $metrics         = new InMemoryMetrics();

        $transport->send(Envelope::wrap(new OutcomeMessage('unused')));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                OutcomeMessage::class => static function (OutcomeMessage $message, Acknowledger $ack): void {
                    $ack->fail(new \RuntimeException('handler gave up'));
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            failureTransport: $failedTransport,
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 1);

        // The path the worker owns: the failure is counted once and the message
        // goes to the failure transport rather than being acknowledged.
        Assert::same($metrics->failed, 1, 'failed');
        Assert::same($metrics->processed, 0, 'processed');
        Assert::same($metrics->active, 0, 'active');
        Assert::same($transport->rejectedCount, 1, 'rejectedCount');
        Assert::same(count($failedTransport->sentEnvelopes), 1, 'sentEnvelopes');
    }
}
