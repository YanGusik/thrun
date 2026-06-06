<?php

declare(strict_types=1);

namespace Thrun\Transport\Redis;

/**
 * Low-level Redis operations for queue lists.
 *
 * Keys:
 *   {prefix}:{queue}:ready       - list, RPUSH / LPOP / LMOVE
 *   {prefix}:{queue}:processing  - list, atomic move from ready
 *   {prefix}:{queue}:delayed     - sorted set, ZADD / ZRANGEBYSCORE
 */
final class RedisConnection
{
    public function __construct(
        private readonly \Redis $redis,
        private readonly string $prefix = 'thrun:queue',
    ) {}

    public function pushReady(string $queue, string $payload): void
    {
        $this->redis->rPush($this->key($queue, 'ready'), $payload);
    }

    /**
     * Atomically move a message from ready to processing.
     * Returns the raw payload or null if queue is empty.
     */
    public function popToProcessing(string $queue): ?string
    {
        $result = $this->redis->lMove(
            $this->key($queue, 'ready'),
            $this->key($queue, 'processing'),
            \Redis::RIGHT,
            \Redis::LEFT,
        );

        return $result === false ? null : $result;
    }

    /**
     * Move all messages from processing back to ready (startup reclaim).
     */
    public function reclaimProcessing(string $queue): void
    {
        $processing = $this->key($queue, 'processing');
        $ready      = $this->key($queue, 'ready');

        while (true) {
            $raw = $this->redis->rPop($processing);
            if ($raw === false) {
                break;
            }
            $this->redis->lPush($ready, $raw);
        }
    }

    /**
     * Remove a message from the processing list.
     */
    public function ack(string $queue, string $rawPayload): void
    {
        $this->redis->lRem($this->key($queue, 'processing'), $rawPayload, 0);
    }

    /**
     * Remove from processing list only (unack).
     */
    public function reject(string $queue, string $rawPayload): void
    {
        $this->redis->lRem($this->key($queue, 'processing'), $rawPayload, 0);
    }

    public function pushDelayed(string $queue, string $payload, int $delayMs): void
    {
        $score = microtime(true) + ($delayMs / 1000);
        $this->redis->zAdd($this->key($queue, 'delayed'), $score, $payload);
    }

    /**
     * Pop a due delayed message (score <= now) and return it.
     * Returns null if no due messages or lost race.
     */
    public function popDelayed(string $queue): ?string
    {
        $items = $this->redis->zRangeByScore(
            $this->key($queue, 'delayed'),
            '-inf',
            (string) microtime(true),
            ['limit' => [0, 1]],
        );

        if (empty($items)) {
            return null;
        }

        $removed = $this->redis->zRem($this->key($queue, 'delayed'), $items[0]);
        if ($removed === 0) {
            return null; // lost race
        }

        return $items[0];
    }

    /**
     * Delete all keys for a queue (useful in tests).
     */
    public function pendingCount(string $queue): int
    {
        return (int) $this->redis->lLen($this->key($queue, 'ready'))
            + (int) $this->redis->lLen($this->key($queue, 'processing'))
            + (int) $this->redis->zCard($this->key($queue, 'delayed'));
    }

    public function activeCount(string $queue): int
    {
        return (int) $this->redis->lLen($this->key($queue, 'processing'));
    }

    public function purge(string $queue): void
    {
        foreach (['ready', 'processing', 'delayed'] as $suffix) {
            $this->redis->del($this->key($queue, $suffix));
        }
    }

    private function key(string $queue, string $suffix): string
    {
        return "{$this->prefix}:{$queue}:{$suffix}";
    }
}
