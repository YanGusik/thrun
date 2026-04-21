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

final class WorkerTest extends AsyncTestCase
{
    public function processesMessage(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(Envelope::wrap(new PingMessage()));
        $transport->close();

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static fn(PingMessage $m) => null,
            ],
            options: new WorkerOptions(threads: 1, concurrency: 2),
        );

        $worker->run();

        Assert::same($transport->ackedCount, 1);
        Assert::same($transport->rejectedCount, 0);
    }

    public function processesMultipleMessages(): void
    {
        $transport = new InMemoryTransport();

        for ($i = 0; $i < 5; $i++) {
            $transport->send(Envelope::wrap(new PingMessage()));
        }
        $transport->close();

        $worker = new Worker(
            transport: $transport,
            handlers: [
                PingMessage::class => static fn(PingMessage $m) => null,
            ],
            options: new WorkerOptions(threads: 2, concurrency: 3),
        );

        $worker->run();

        Assert::same($transport->ackedCount, 5);
        Assert::same($transport->rejectedCount, 0);
    }

    public function rejectsUnknownMessage(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(Envelope::wrap(new PingMessage()));
        $transport->close();

        $worker = new Worker(
            transport: $transport,
            handlers: [], // no handler for PingMessage
            options: new WorkerOptions(threads: 1, concurrency: 1),
        );

        $worker->run();

        Assert::same($transport->ackedCount, 0);
        Assert::same($transport->rejectedCount, 1);
    }
}
