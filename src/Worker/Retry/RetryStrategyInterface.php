<?php

declare(strict_types=1);

namespace Thrun\Worker\Retry;

interface RetryStrategyInterface
{
    /**
     * Whether the failed attempt should be retried.
     *
     * @param int $attempt The attempt number that just failed (1 = first failure)
     */
    public function isRetryable(\Throwable $exception, int $attempt): bool;

    /**
     * Delay in milliseconds before the next attempt.
     *
     * @param int $attempt The next attempt number (1 = first retry)
     */
    public function getDelay(int $attempt): int;
}
