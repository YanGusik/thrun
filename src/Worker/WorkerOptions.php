<?php

declare(strict_types=1);

namespace Thrun\Worker;

final readonly class WorkerOptions
{
    /**
     * @param  array<int, WorkerMiddlewareInterface>  $middleware
     */
    public function __construct(
        public int $threads = 4,
        public int $concurrency = 10,
        public int $queueSize = 0, // workers * 4
        public ?\Closure $bootloader = null,
        public array $middleware = [],
        public ?\Closure $onDispatch = null,
        public ?\Closure $onResult = null
    ) {
    }
}
