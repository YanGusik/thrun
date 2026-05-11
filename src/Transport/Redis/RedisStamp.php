<?php

declare(strict_types=1);

namespace Thrun\Transport\Redis;

use Thrun\Contract\StampInterface;

/**
 * Transport-specific stamp carrying the raw serialized payload.
 * Required for ack()/reject() to remove the exact message from Redis lists.
 */
final class RedisStamp implements StampInterface
{
    public function __construct(
        public readonly string $rawPayload,
        public readonly string $queue,
    ) {}
}
