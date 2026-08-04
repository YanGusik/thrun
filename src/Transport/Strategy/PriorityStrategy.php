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

        // Priorities are kept by name because the list may be keyed arbitrarily:
        // MultiQueueReceiver::pickFromBuffers() filters it with array_filter(),
        // which preserves the original keys. Any lookup by position reads the
        // wrong entry, or none at all.
        $priorities = [];

        // accumulate credits proportional to priority
        foreach ($queues as $state) {
            $priorities[$state->name]    = $state->priority;
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
            $this->credits[$best] -= $priorities[$best];
        }

        return $best;
    }
}
