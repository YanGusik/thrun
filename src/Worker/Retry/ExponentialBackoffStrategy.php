<?php

declare(strict_types=1);

namespace Thrun\Worker\Retry;

final class ExponentialBackoffStrategy implements RetryStrategyInterface
{
    public function __construct(
        private readonly int $baseDelayMs,
        private readonly int $maxAttempts,
        private readonly float $multiplier = 2.0,
        private readonly ?int $maxDelayMs = null,
    ) {}

    public function isRetryable(\Throwable $exception, int $attempt): bool
    {
        return $attempt <= $this->maxAttempts;
    }

    public function getDelay(int $attempt): int
    {
        $delay = (int) ($this->baseDelayMs * ($this->multiplier ** ($attempt - 1)));

        if ($this->maxDelayMs !== null && $delay > $this->maxDelayMs) {
            return $this->maxDelayMs;
        }

        return $delay;
    }
}
