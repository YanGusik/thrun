<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Transport;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Transport\InMemory\InMemoryTransport;

final class InMemoryTransportTest extends AsyncTestCase
{
    public function sendAndReceive(): void
    {
        $transport = new InMemoryTransport();
        $envelope  = Envelope::wrap(new PingMessage());

        $transport->send($envelope);
        $received = $transport->receive();

        Assert::same($received !== null, true);
        Assert::same($received->message::class, PingMessage::class);
    }

    public function receiveReturnsNullWhenEmptyAndClosed(): void
    {
        $transport = new InMemoryTransport();
        $transport->close();
        Assert::same($transport->receive(), null);
    }

    public function receiveDrainsBufferThenReturnsNullAfterClose(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(Envelope::wrap(new PingMessage()));
        $transport->close();

        // Buffered message is still available after close() (at-least-once)
        Assert::same($transport->receive() !== null, true);
        // After buffer drained, receive() returns null
        Assert::same($transport->receive(), null);
    }

    public function tryReceiveReturnsEnvelopeWhenAvailable(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(Envelope::wrap(new PingMessage()));

        Assert::same($transport->tryReceive() !== null, true);
    }

    public function tryReceiveReturnsNullWhenEmpty(): void
    {
        $transport = new InMemoryTransport();
        Assert::same($transport->tryReceive(), null);
    }

    public function ackAndRejectAreTracked(): void
    {
        $transport = new InMemoryTransport();
        $envelope = Envelope::wrap(new PingMessage());

        $transport->ack($envelope);
        Assert::same($transport->ackedCount, 1);
        Assert::same($transport->rejectedCount, 0);

        $transport->reject($envelope);
        Assert::same($transport->ackedCount, 1);
        Assert::same($transport->rejectedCount, 1);
    }

    public function fifoOrder(): void
    {
        $transport = new InMemoryTransport();

        $first  = Envelope::wrap(new PingMessage());
        $second = Envelope::wrap(new PingMessage());

        $transport->send($first);
        $transport->send($second);

        Assert::same($transport->receive(), $first);
        Assert::same($transport->receive(), $second);
        // Empty channel: tryReceive is non-blocking
        Assert::same($transport->tryReceive(), null);

        $transport->close();
    }
}
