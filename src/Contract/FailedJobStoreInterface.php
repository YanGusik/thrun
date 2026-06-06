<?php

declare(strict_types=1);

namespace Thrun\Contract;

interface FailedJobStoreInterface extends SenderInterface
{
    public function find(string $jobId): ?array;

    public function all(int $limit = 50): array;

    public function allByQueue(string $queue, int $limit = 50): array;

    public function forget(string $jobId): void;

    /**
     * Remove all failed jobs.
     */
    public function flush(): void;
}
