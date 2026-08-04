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

        // What MultiQueueReceiver::pickFromBuffers() hands over after array_filter()
        // drops the empty queues: the survivors keep their original keys, so index 0
        // can be gone.
        $states = [1 => new QueueState(name: 'notifications', priority: 1, active: 0)];

        $this->withWarningsAsErrors(static function () use ($strategy, $states): void {
            Assert::same($strategy->next($states), 'notifications');
            Assert::same($strategy->next($states), 'notifications');
        });
    }

    public function picksTheHighestPriorityFromASparseStateList(): void
    {
        $strategy = new PriorityStrategy();

        // Three configured queues, the first one filtered out.
        $states = [
            1 => new QueueState(name: 'notifications', priority: 3, active: 0),
            2 => new QueueState(name: 'laravel_jobs', priority: 1, active: 0),
        ];

        $this->withWarningsAsErrors(static function () use ($strategy, $states): void {
            Assert::same($strategy->next($states), 'notifications');
        });
    }

    public function cyclesEqualPrioritiesOnASparseStateList(): void
    {
        $strategy = new PriorityStrategy();

        $states = [
            2 => new QueueState(name: 'a', priority: 2, active: 0),
            3 => new QueueState(name: 'b', priority: 2, active: 0),
        ];

        // Equal credits leave the winner to the first entry in iteration order,
        // whatever key it carries.
        $this->withWarningsAsErrors(static function () use ($strategy, $states): void {
            Assert::same($strategy->next($states), 'a');
            Assert::same($strategy->next($states), 'b');
            Assert::same($strategy->next($states), 'a');
        });
    }

    public function keepsPriorityRatioWhenTheStateListIsSparse(): void
    {
        $strategy = new PriorityStrategy();

        $states = [
            1 => new QueueState(name: 'emails', priority: 3, active: 0),
            2 => new QueueState(name: 'laravel_jobs', priority: 1, active: 0),
        ];

        $order = [];
        $this->withWarningsAsErrors(static function () use ($strategy, $states, &$order): void {
            for ($i = 0; $i < 8; $i++) {
                $order[] = $strategy->next($states);
            }
        });

        Assert::same($order, [
            'emails', 'emails', 'emails', 'laravel_jobs',
            'emails', 'emails', 'emails', 'laravel_jobs',
        ]);
    }

    public function returnsNullForEmptyArray(): void
    {
        $strategy = new PriorityStrategy();
        Assert::same($strategy->next([]), null);
    }
}
