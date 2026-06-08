<?php

declare(strict_types=1);

namespace Thrun\Transport\Failed;

use Thrun\Contract\FailedJobStoreInterface;
use Thrun\Envelope\Envelope;

final class NullFailedJobSender implements FailedJobStoreInterface
{
    public function send(Envelope $envelope): void
    {
        // No-op
    }

    public function flush(): void
    {
        // No-op
    }

    public function find(string $jobId): ?array
    {
        return null;
    }

    public function all(int $limit = 50): array
    {
        return [];
    }

    public function allByQueue(string $queue, int $limit = 50): array
    {
        return [];
    }

    public function forget(string $jobId): void
    {
        // No-op
    }
}
