<?php

declare(strict_types=1);

namespace Thrun\Transport\Stamp;

use Thrun\Contract\StampInterface;

final class QueueStamp implements StampInterface
{
    public function __construct(public readonly string $queue) {}
}
