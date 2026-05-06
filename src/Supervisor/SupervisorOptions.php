<?php

declare(strict_types=1);

namespace Thrun\Supervisor;

final class SupervisorOptions
{
    public function __construct(
        public readonly int   $maxCrashes     = 3,
        public readonly int   $restartWindow  = 300,
        public readonly float $restartBackoff = 1.0,
    ) {}
}
