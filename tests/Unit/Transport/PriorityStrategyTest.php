<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Transport;

use Testo\Assert;
use Thrun\Tests\AsyncTestCase;
use Thrun\Transport\QueueState;
use Thrun\Transport\Strategy\PriorityStrategy;

final class PriorityStrategyTest extends AsyncTestCase
{
    public function prefersHigherPriority(): void
    {
        $strategy = new PriorityStrategy();

        $states = [
            new QueueState(name: 'low', priority: 1, active: 0),
            new QueueState(name: 'high', priority: 5, active: 0),
        ];

        Assert::same($strategy->next($states), 'high');
    }

    public function fallsBackToLowerPriorityWhenHigherIsEmpty(): void
    {
        $strategy = new PriorityStrategy();

        $states = [
            new QueueState(name: 'high', priority: 5, active: 0),
            new QueueState(name: 'low', priority: 1, active: 0),
        ];

        // PriorityStrategy always picks highest priority from provided states
        // Empty / non-empty logic is handled by MultiQueueReceiver caller
        Assert::same($strategy->next($states), 'high');
    }

    public function samePriorityCyclesRoundRobin(): void
    {
        $strategy = new PriorityStrategy();

        $states = [
            new QueueState(name: 'a', priority: 3, active: 0),
            new QueueState(name: 'b', priority: 3, active: 0),
        ];

        Assert::same($strategy->next($states), 'a');
        Assert::same($strategy->next($states), 'b');
        Assert::same($strategy->next($states), 'a');
    }

    public function picksFromAStateListWhoseKeysAreNotSequential(): void
    {
        $strategy = new PriorityStrategy();

        // What MultiQueueReceiver hands over after array_filter() drops the empty
        // queues: the survivors keep their original keys, so index 0 can be gone.
        $states = [1 => new QueueState(name: 'notifications', priority: 1, active: 0)];

        $this->withWarningsAsErrors(static function () use ($strategy, $states): void {
            Assert::same($strategy->next($states), 'notifications');
            Assert::same($strategy->next($states), 'notifications');
        });
    }

    public function returnsNullForEmptyArray(): void
    {
        $strategy = new PriorityStrategy();
        Assert::same($strategy->next([]), null);
    }
}
