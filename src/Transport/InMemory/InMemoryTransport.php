<?php

declare(strict_types=1);

namespace Thrun\Transport\InMemory;

use Async\Channel;
use Async\ChannelException;
use Async\OperationCanceledException;
use Thrun\Contract\TransportInterface;
use Thrun\Envelope\Envelope;

final class InMemoryTransport implements TransportInterface
{
    // for unit tests
    public int $ackedCount    = 0;
    public int $rejectedCount = 0;

    private Channel $channel;

    public function __construct(int $capacity = 100)
    {
        $this->channel = new Channel($capacity);
    }

    /**
     * @throws ChannelException
     * @throws OperationCanceledException
     */
    public function send(Envelope $envelope): void
    {
        $this->channel->send($envelope);
    }

    public function receive(): ?Envelope
    {
        try {
            return $this->channel->recv();
        } catch (\Async\ChannelException|OperationCanceledException) {
            return null;
        }
    }

    public function tryReceive(): ?Envelope
    {
        if ($this->channel->isEmpty()) {
            return null;
        }

        try {
            return $this->channel->recv();
        } catch (\Async\ChannelException|OperationCanceledException) {
            return null;
        }
    }

    public function ack(Envelope $envelope): void
    {
        $this->ackedCount++;
    }

    public function reject(Envelope $envelope): void
    {
        $this->rejectedCount++;
    }

    public function close(): void
    {
        $this->channel->close();
    }
}