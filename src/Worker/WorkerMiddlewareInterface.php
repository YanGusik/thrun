<?php

declare(strict_types=1);

namespace Thrun\Worker;

interface WorkerMiddlewareInterface
{
    public function handle(object $message, Acknowledger $ack, \Closure $next): void;
}
