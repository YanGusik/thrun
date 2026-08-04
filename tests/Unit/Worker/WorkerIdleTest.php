<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Worker;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

use function Async\delay;

/**
 * When Worker::isIdle() reports true, everything the worker received has been
 * acked or rejected. A caller that stops the worker on an idle report - the
 * supervisor as much as the test helper - would otherwise close the result
 * channel over a result that has not been applied yet, and the message would be
 * left in the transport's in-flight state.
 */
final class WorkerIdleTest extends AsyncTestCase
{
    public function staysBusyWhileAResultIsStillBeingApplied(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(Envelope::wrap(new PingMessage()));
        $transport->close();

        $idleDuringResult = null;
        $worker           = null;

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static fn(PingMessage $m) => null,
            ],
            options: new WorkerOptions(
                threads:     1,
                concurrency: 1,
                onResult:    static function (array $result) use (&$idleDuringResult, &$worker): void {
                    // The result has left the thread, so the pool reports no
                    // pending and no running task; the delay makes sure that
                    // bookkeeping has settled before the question is asked.
                    delay(50);
                    $idleDuringResult ??= $worker->isIdle();
                },
            ),
        );

        $this->runWorkerAndWait($worker, $transport, expectedAcked: 1);

        Assert::same($idleDuringResult, false, 'idle while the result was in flight');
        Assert::same($transport->ackedCount, 1, 'ackedCount');
    }
}
