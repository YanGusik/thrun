<?php

declare(strict_types=1);

namespace Thrun\Transport\Strategy;

use Thrun\Contract\SchedulingStrategyInterface;
use Thrun\Transport\QueueState;

final class PriorityStrategy implements SchedulingStrategyInterface
{
    /** @var array<string, float> */
    private array $credits = [];

    public function next(array $queues): ?string
    {
        if ($queues === []) {
            return null;
        }

        // accumulate credits proportional to priority
        foreach ($queues as $state) {
            $this->credits[$state->name] = ($this->credits[$state->name] ?? 0.0) + $state->priority;
        }

        // pick queue with highest credit
        $best       = null;
        $bestCredit = PHP_INT_MIN;
        foreach ($queues as $state) {
            $credit = $this->credits[$state->name] ?? 0.0;
            if ($credit > $bestCredit) {
                $bestCredit = $credit;
                $best       = $state->name;
            }
        }

        // deduct winner's own priority so others catch up next round
        if ($best !== null) {
            $this->credits[$best] -= $queues[array_search($best, array_column($queues, 'name'), true)]->priority;
        }

        return $best;
    }
}
