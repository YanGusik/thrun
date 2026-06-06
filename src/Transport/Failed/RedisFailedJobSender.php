<?php

declare(strict_types=1);

namespace Thrun\Transport\Failed;

use Thrun\Contract\FailedJobStoreInterface;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\ErrorDetailsStamp;
use Thrun\Envelope\Stamp\JobIdStamp;
use Thrun\Envelope\Stamp\MessageIdStamp;
use Thrun\Envelope\Stamp\QueueStamp;
use Thrun\Serialization\JsonSerializer;

final class RedisFailedJobSender implements FailedJobStoreInterface
{
    public function __construct(
        private readonly \Redis $redis,
        private readonly JsonSerializer $serializer,
        private readonly string $prefix = 'thrun:failed',
    ) {}

    public function send(Envelope $envelope): void
    {
        $jobId = $envelope->last(JobIdStamp::class)?->id
            ?? throw new \RuntimeException('Missing JobIdStamp in failed envelope');

        $queue = $envelope->last(QueueStamp::class)?->queue ?? 'unknown';

        $record = [
            'job_id' => $jobId,
            'message_id' => $envelope->last(MessageIdStamp::class)?->id,
            'type' => $envelope->type,
            'queue' => $queue,
            'payload' => json_decode(json_encode($envelope->message), true),
            'stamps' => $this->serializer->extractStamps($envelope),
            'exception' => $envelope->last(ErrorDetailsStamp::class)?->exceptionClass,
            'exception_message' => $envelope->last(ErrorDetailsStamp::class)?->message,
            'trace' => $envelope->last(ErrorDetailsStamp::class)?->trace,
            'file' => $envelope->last(ErrorDetailsStamp::class)?->file,
            'line' => $envelope->last(ErrorDetailsStamp::class)?->line,
            'failed_at' => time(),
        ];

        $json = json_encode($record, JSON_THROW_ON_ERROR);

        $this->redis->hSet("{$this->prefix}:jobs", $jobId, $json);
        $this->redis->zAdd("{$this->prefix}:queue:{$queue}", $record['failed_at'], $jobId);
        $this->redis->zAdd("{$this->prefix}:by-time", $record['failed_at'], $jobId);
    }

    public function find(string $jobId): ?array
    {
        $raw = $this->redis->hGet("{$this->prefix}:jobs", $jobId);

        return $raw !== false ? json_decode($raw, true) : null;
    }

    public function allByQueue(string $queue, int $limit = 50): array
    {
        $ids = $this->redis->zRevRange("{$this->prefix}:queue:{$queue}", 0, $limit - 1);

        return $this->fetchMany($ids);
    }

    public function all(int $limit = 50): array
    {
        $ids = $this->redis->zRevRange("{$this->prefix}:by-time", 0, $limit - 1);

        return $this->fetchMany($ids);
    }

    public function forget(string $jobId): void
    {
        $record = $this->find($jobId);
        $queue = $record['queue'] ?? 'unknown';

        $this->redis->hDel("{$this->prefix}:jobs", $jobId);
        $this->redis->zRem("{$this->prefix}:queue:{$queue}", $jobId);
        $this->redis->zRem("{$this->prefix}:by-time", $jobId);
    }

    public function flush(): void
    {
        $this->redis->del("{$this->prefix}:jobs");

        // Delete all queue-specific sorted sets
        $keys = $this->redis->keys("{$this->prefix}:queue:*");
        if ($keys !== false && $keys !== []) {
            $this->redis->del(...$keys);
        }

        $this->redis->del("{$this->prefix}:by-time");
    }

    /** @param list<string> $ids */
    private function fetchMany(array $ids): array
    {
        $records = [];
        foreach ($ids as $id) {
            $raw = $this->redis->hGet("{$this->prefix}:jobs", $id);
            if ($raw !== false) {
                $records[] = json_decode($raw, true);
            }
        }

        return $records;
    }
}
