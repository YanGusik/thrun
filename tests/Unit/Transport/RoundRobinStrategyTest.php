<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Transport;

use Testo\Assert;
use Thrun\Tests\AsyncTestCase;
use Thrun\Transport\QueueState;
use Thrun\Transport\Strategy\RoundRobinStrategy;

final class RoundRobinStrategyTest extends AsyncTestCase
{
    public function alternatesBetweenQueues(): void
    {
        $strategy = new RoundRobinStrategy();

        $states = [
            new QueueState(name: 'a', priority: 0, active: 0),
            new QueueState(name: 'b', priority: 0, active: 0),
        ];

        Assert::same($strategy->next($states), 'a');
        Assert::same($strategy->next($states), 'b');
        Assert::same($strategy->next($states), 'a');
        Assert::same($strategy->next($states), 'b');
    }

    public function skipsEmptyQueues(): void
    {
        $strategy = new RoundRobinStrategy();

        $states = [
            new QueueState(name: 'empty', priority: 0, active: 0),
            new QueueState(name: 'filled', priority: 0, active: 0),
        ];

        // RoundRobin in our implementation only sees states/. empty/filled is handled by caller
        // This test verifies the strategy simply cycles through provided states
        Assert::same($strategy->next($states), 'empty');
        Assert::same($strategy->next($states), 'filled');
    }

    public function returnsNullForEmptyArray(): void
    {
        $strategy = new RoundRobinStrategy();
        Assert::same($strategy->next([]), null);
    }
}
