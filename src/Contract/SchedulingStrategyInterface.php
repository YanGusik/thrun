<?php

declare(strict_types=1);

namespace Thrun\Contract;

use Thrun\Transport\QueueState;

interface SchedulingStrategyInterface
{
    /**
     * @param QueueState[] $queues
     */
    public function next(array $queues): ?string;
}
