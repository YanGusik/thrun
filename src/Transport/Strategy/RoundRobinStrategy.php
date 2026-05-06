<?php

declare(strict_types=1);

namespace Thrun\Transport\Strategy;

use Thrun\Contract\SchedulingStrategyInterface;
use Thrun\Transport\QueueState;

final class RoundRobinStrategy implements SchedulingStrategyInterface
{
    private int $index = 0;

    public function next(array $queues): ?string
    {
        if ($queues === []) {
            return null;
        }

        $queues = array_values($queues);
        $queue  = $queues[$this->index % count($queues)]->name;
        $this->index++;

        return $queue;
    }
}
