<?php

declare(strict_types=1);

namespace Thrun\Transport\Redis;

use Thrun\Contract\SerializerInterface;
use Thrun\Contract\TransportInterface;
use Thrun\Envelope\Envelope;
use function Async\delay;

final class RedisTransport implements TransportInterface
{
    private bool $running = false;

    public function __construct(
        private readonly RedisConnection $connection,
        private readonly SerializerInterface $serializer,
        private readonly string $queue,
    ) {
        // Startup reclaim: return any stuck messages from processing back to ready
        $this->connection->reclaimProcessing($this->queue);
    }

    public function send(Envelope $envelope): void
    {
        $payload = $this->serializer->serialize($envelope);
        $this->connection->pushReady($this->queue, $payload);
    }

    public function receive(): ?Envelope
    {
        $this->running = true;

        while ($this->running) {
            $raw = $this->connection->popToProcessing($this->queue);

            if ($raw !== null) {
                $envelope = $this->deserialize($raw);

                if ($envelope !== null) {
                    return $envelope;
                }

                // Deserialization failed - already moved to failed by deserialize()
                continue;
            }

            delay(50);
        }

        return null;
    }

    public function tryReceive(): ?Envelope
    {
        $raw = $this->connection->popToProcessing($this->queue);

        if ($raw === null) {
            return null;
        }

        return $this->deserialize($raw);
    }

    public function ack(Envelope $envelope): void
    {
        $stamp = $envelope->last(RedisStamp::class);

        if ($stamp instanceof RedisStamp) {
            $this->connection->ack($this->queue, $stamp->rawPayload);
        }
    }

    public function reject(Envelope $envelope): void
    {
        $stamp = $envelope->last(RedisStamp::class);

        if ($stamp instanceof RedisStamp) {
            $this->connection->reject($this->queue, $stamp->rawPayload);
        }
    }

    public function close(): void
    {
        $this->running = false;
    }

    private function deserialize(string $raw): ?Envelope
    {
        try {
            $envelope = $this->serializer->deserialize($raw);

            return $envelope->with(new RedisStamp($raw, $this->queue));
        } catch (\Throwable $e) {
            $this->connection->pushFailed($this->queue, $raw, $e->getMessage());

            return null;
        }
    }
}
