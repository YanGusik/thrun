<?php

declare(strict_types=1);

namespace Thrun\Transport\InMemory;

use Async\Channel;
use Async\ChannelException;
use Async\OperationCanceledException;
use Thrun\Contract\TransportInterface;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\DelayStamp;

final class InMemoryTransport implements TransportInterface
{
    // for unit tests
    public int $ackedCount    = 0;
    public int $rejectedCount = 0;
    /** @var Envelope[] */
    public array $sentEnvelopes = [];

    private Channel $channel;
    private ?\Async\Scope $delayScope = null;

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
        $this->sentEnvelopes[] = $envelope;

        $delayStamp = $envelope->last(DelayStamp::class);
        if ($delayStamp !== null && $delayStamp->delayMs > 0) {
            $this->delayScope ??= new \Async\Scope();
            $this->delayScope->spawn(function () use ($envelope, $delayStamp): void {
                \Async\delay($delayStamp->delayMs);
                try {
                    $this->channel->send($envelope);
                } catch (\Async\ChannelException) {
                    // channel closed while waiting
                }
            });
            return;
        }

        $this->channel->send($envelope);
    }

    public function receive(): ?Envelope
    {
        try {
            return $this->channel->recv();
        } catch (\Async\ChannelException) {
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
        } catch (\Async\ChannelException) {
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

    public function isEmpty(): bool
    {
        return $this->channel->isEmpty();
    }

    public function close(): void
    {
        $this->delayScope?->cancel();
        $this->channel->close();
    }
}