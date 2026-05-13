<?php

declare(strict_types=1);

namespace Thrun\Worker;

final class WorkerOptions
{
    /**
     * @param array<int, WorkerMiddlewareInterface> $middleware
     */
    public function __construct(
        public readonly int      $threads     = 4,
        public readonly int      $concurrency = 10,
        public readonly ?\Closure $bootloader  = null,
        public readonly array    $middleware  = [],
        public readonly ?\Closure $onDispatch  = null,
        public readonly ?\Closure $onResult    = null,
    ) {}
}
