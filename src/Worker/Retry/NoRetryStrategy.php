<?php

declare(strict_types=1);

namespace Thrun\Worker\Retry;

final class NoRetryStrategy implements RetryStrategyInterface
{
    public function isRetryable(\Throwable $exception, int $attempt): bool
    {
        return false;
    }

    public function getDelay(int $attempt): int
    {
        return 0;
    }
}
