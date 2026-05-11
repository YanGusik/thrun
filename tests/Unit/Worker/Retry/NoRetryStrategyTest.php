<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Worker\Retry;

use Testo\Assert;
use Testo\Test;
use Thrun\Worker\Retry\NoRetryStrategy;

#[Test]
final class NoRetryStrategyTest
{
    public function neverRetryable(): void
    {
        $strategy = new NoRetryStrategy();

        Assert::false($strategy->isRetryable(new \RuntimeException('fail'), 1));
        Assert::false($strategy->isRetryable(new \RuntimeException('fail'), 999));
    }

    public function delayIsAlwaysZero(): void
    {
        $strategy = new NoRetryStrategy();

        Assert::same($strategy->getDelay(1), 0);
        Assert::same($strategy->getDelay(100), 0);
    }
}
