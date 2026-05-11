<?php

declare(strict_types=1);

namespace Thrun\Transport\Redis;

/**
 * Low-level Redis operations for queue lists.
 *
 * Keys:
 *   {prefix}:{queue}:ready       - list, RPUSH / LPOP / LMOVE
 *   {prefix}:{queue}:processing  - list, atomic move from ready
 *   {prefix}:{queue}:failed      - list, wrapped JSON with error metadata
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
     * Remove from processing and push to failed queue with metadata.
     */
    public function reject(string $queue, string $rawPayload, ?string $error = null): void
    {
        $this->redis->lRem($this->key($queue, 'processing'), $rawPayload, 0);
        $this->pushFailed($queue, $rawPayload, $error);
    }

    public function pushFailed(string $queue, string $rawPayload, ?string $error = null): void
    {
        $wrapped = json_encode([
            'payload'  => $rawPayload,
            'error'    => $error,
            'failedAt' => time(),
        ], JSON_THROW_ON_ERROR);

        $this->redis->lPush($this->key($queue, 'failed'), $wrapped);
    }

    /**
     * Delete all keys for a queue (useful in tests).
     */
    public function purge(string $queue): void
    {
        foreach (['ready', 'processing', 'failed'] as $suffix) {
            $this->redis->del($this->key($queue, $suffix));
        }
    }

    private function key(string $queue, string $suffix): string
    {
        return "{$this->prefix}:{$queue}:{$suffix}";
    }
}
