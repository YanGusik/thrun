<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Worker\Retry;

use Testo\Assert;
use Testo\Test;
use Thrun\Worker\Retry\FixedDelayStrategy;

#[Test]
final class FixedDelayStrategyTest
{
    public function retryableWithinMaxAttempts(): void
    {
        $strategy = new FixedDelayStrategy(delayMs: 1000, maxAttempts: 3);

        Assert::true($strategy->isRetryable(new \RuntimeException('fail'), 1));
        Assert::true($strategy->isRetryable(new \RuntimeException('fail'), 2));
        Assert::true($strategy->isRetryable(new \RuntimeException('fail'), 3));
        Assert::false($strategy->isRetryable(new \RuntimeException('fail'), 4));
    }

    public function delayIsFixed(): void
    {
        $strategy = new FixedDelayStrategy(delayMs: 1000, maxAttempts: 3);

        Assert::same($strategy->getDelay(1), 1000);
        Assert::same($strategy->getDelay(2), 1000);
        Assert::same($strategy->getDelay(3), 1000);
    }

    public function zeroMaxAttemptsMeansNeverRetryable(): void
    {
        $strategy = new FixedDelayStrategy(delayMs: 1000, maxAttempts: 0);

        Assert::false($strategy->isRetryable(new \RuntimeException('fail'), 1));
    }
}
