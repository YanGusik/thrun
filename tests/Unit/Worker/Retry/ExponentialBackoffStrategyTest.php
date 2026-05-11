<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Worker\Retry;

use Testo\Assert;
use Testo\Test;
use Thrun\Worker\Retry\ExponentialBackoffStrategy;

#[Test]
final class ExponentialBackoffStrategyTest
{
    public function retryableWithinMaxAttempts(): void
    {
        $strategy = new ExponentialBackoffStrategy(
            baseDelayMs: 1000,
            maxAttempts: 3,
            multiplier: 2.0,
        );

        Assert::true($strategy->isRetryable(new \RuntimeException('fail'), 1));
        Assert::true($strategy->isRetryable(new \RuntimeException('fail'), 2));
        Assert::true($strategy->isRetryable(new \RuntimeException('fail'), 3));
        Assert::false($strategy->isRetryable(new \RuntimeException('fail'), 4));
    }

    public function delayExponentially(): void
    {
        $strategy = new ExponentialBackoffStrategy(
            baseDelayMs: 1000,
            maxAttempts: 4,
            multiplier: 2.0,
        );

        Assert::same($strategy->getDelay(1), 1000);
        Assert::same($strategy->getDelay(2), 2000);
        Assert::same($strategy->getDelay(3), 4000);
        Assert::same($strategy->getDelay(4), 8000);
    }

    public function delayRespectsMaxDelay(): void
    {
        $strategy = new ExponentialBackoffStrategy(
            baseDelayMs: 1000,
            maxAttempts: 5,
            multiplier: 2.0,
            maxDelayMs: 3000,
        );

        Assert::same($strategy->getDelay(1), 1000);
        Assert::same($strategy->getDelay(2), 2000);
        Assert::same($strategy->getDelay(3), 3000);
        Assert::same($strategy->getDelay(4), 3000);
        Assert::same($strategy->getDelay(5), 3000);
    }

    public function customMultiplier(): void
    {
        $strategy = new ExponentialBackoffStrategy(
            baseDelayMs: 100,
            maxAttempts: 3,
            multiplier: 3.0,
        );

        Assert::same($strategy->getDelay(1), 100);
        Assert::same($strategy->getDelay(2), 300);
        Assert::same($strategy->getDelay(3), 900);
    }
}
