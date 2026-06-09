<?php

namespace Thrun\Tests\Unit\Worker;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\ErrorDetailsStamp;
use Thrun\Envelope\Stamp\JobIdStamp;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Acknowledger;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

class WorkerFailedTransportTest extends AsyncTestCase
{
    public function handlerWithAcknowledgerCanRetry(): void
    {
        $transport       = new InMemoryTransport();
        $failedTransport = new InMemoryTransport();
        $transport->send(Envelope::wrap(new PingMessage()));

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static function (PingMessage $msg, Acknowledger $ack): void {
                    $ack->fail(new \Exception("Custom error message"));
                },
            ],
            options: new WorkerOptions(threads: 1, concurrency: 1),
            failureTransport: $failedTransport,
        );

        $this->runWorkerAndWait($worker, $transport, expectedRejected: 1);

        Assert::same($transport->ackedCount, 0, 'ackedCount');
        Assert::same($transport->rejectedCount, 1, 'rejectedCount');


        Assert::same($failedTransport->ackedCount, 0, 'failed ackedCount');
        Assert::same($failedTransport->rejectedCount, 0, 'failed rejectedCount');
        // Retry envelope has DelayStamp
        Assert::same(count($failedTransport->sentEnvelopes), 1, 'sentEnvelopes');
        var_dump($failedTransport->sentEnvelopes);

        $failedEnvelope = $failedTransport->sentEnvelopes[0];
        $errorStamp = $failedEnvelope->last(ErrorDetailsStamp::class);
        Assert::true($failedEnvelope->has(JobIdStamp::class));
        Assert::true($failedEnvelope->has(ErrorDetailsStamp::class));
        Assert::same($errorStamp->exceptionClass, 'Exception');
        Assert::same($errorStamp->message, 'Custom error message');
        Assert::true(str_contains($errorStamp->file, '/tests/Unit/Worker/WorkerFailedTransportTest.php'));
    }
}