<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Worker;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\ErrorDetailsStamp;
use Thrun\Envelope\UnprocessableMessage;
use Thrun\Tests\AsyncTestCase;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Metrics\InMemoryMetrics;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

/**
 * What the worker does with a payload the transport could not deserialize.
 * The transport hands it over as an UnprocessableMessage; no handler is
 * registered for that class, and the raw text is the only thing left of the
 * original message.
 */
final class WorkerUnprocessableTest extends AsyncTestCase
{
    private static function unprocessable(string $raw = 'not valid json'): Envelope
    {
        return Envelope::wrap(
            new UnprocessableMessage(rawPayload: $raw, queue: 'default'),
            new ErrorDetailsStamp(
                exceptionClass: \JsonException::class,
                message: 'Syntax error',
                code: 4,
            ),
        );
    }

    public function routesToFailureTransportWithTheParseError(): void
    {
        $transport       = new InMemoryTransport();
        $failedTransport = new InMemoryTransport();
        $transport->send(self::unprocessable());

        $worker = new Worker(
            transport: $transport,
            handlers: [],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            failureTransport: $failedTransport,
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 1);

        Assert::same($transport->ackedCount, 0, 'ackedCount');
        Assert::same($transport->rejectedCount, 1, 'rejectedCount');
        Assert::same(count($failedTransport->sentEnvelopes), 1, 'sentEnvelopes');

        // The cause that reaches the failure transport is the parse error, not a
        // missing handler for UnprocessableMessage.
        $failed = $failedTransport->sentEnvelopes[0];
        Assert::same(count($failed->all(ErrorDetailsStamp::class)), 1, 'error stamps');

        $error = $failed->last(ErrorDetailsStamp::class);
        Assert::same($error->exceptionClass, \JsonException::class);
        Assert::same($error->message, 'Syntax error');
    }

    public function reportsTheDropWhenThereIsNoFailureTransport(): void
    {
        $transport = new InMemoryTransport();
        $metrics   = new InMemoryMetrics();
        $results   = [];
        $transport->send(self::unprocessable());

        $worker = new Worker(
            transport: $transport,
            handlers: [],
            options: new WorkerOptions(
                threads:     1,
                concurrency: 1,
                onResult:    static function (array $result) use (&$results): void {
                    $results[] = $result;
                },
            ),
            metrics: $metrics,
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 1);

        Assert::same($transport->rejectedCount, 1, 'rejectedCount');
        Assert::same($metrics->failed, 1, 'failed');
        Assert::same(count($results), 1, 'results');
        Assert::same($results[0]['ok'], false);
        Assert::same($results[0]['error']['class'], \JsonException::class);
    }

    public function yieldsToAHandlerRegisteredForUnprocessableMessage(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(self::unprocessable('{"broken"'));

        // The handler runs in a worker thread, so it cannot report back through a
        // captured variable; acking the envelope is the observable proof it ran.
        $worker = new Worker(
            transport: $transport,
            handlers: [
                UnprocessableMessage::class => static function (UnprocessableMessage $message): void {
                    if ($message->rawPayload === '') {
                        throw new \RuntimeException('empty payload');
                    }
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 1);

        Assert::same($transport->ackedCount, 1, 'ackedCount');
        Assert::same($transport->rejectedCount, 0, 'rejectedCount');
    }
}
