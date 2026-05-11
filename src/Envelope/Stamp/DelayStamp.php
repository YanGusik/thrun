<?php

declare(strict_types=1);

namespace Thrun\Envelope\Stamp;

use Thrun\Contract\StampInterface;

final class DelayStamp implements StampInterface
{
    public function __construct(public readonly int $delayMs) {}
}
