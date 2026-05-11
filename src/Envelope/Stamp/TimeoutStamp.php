<?php

declare(strict_types=1);

namespace Thrun\Envelope\Stamp;

use Thrun\Contract\StampInterface;

final class TimeoutStamp implements StampInterface
{
    public function __construct(public readonly int $timeoutMs) {}
}
