<?php

declare(strict_types=1);

namespace Thrun\Worker\Retry;

final class FixedDelayStrategy implements RetryStrategyInterface
{
    public function __construct(
        private readonly int $delayMs,
        private readonly int $maxAttempts,
    ) {}

    public function isRetryable(\Throwable $exception, int $attempt): bool
    {
        return $attempt <= $this->maxAttempts;
    }

    public function getDelay(int $attempt): int
    {
        return $this->delayMs;
    }
}
