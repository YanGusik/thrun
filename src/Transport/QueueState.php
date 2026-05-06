<?php

declare(strict_types=1);

namespace Thrun\Transport;

final class QueueState
{
    public function __construct(
        public readonly string $name,
        public readonly int    $priority = 0,
        public readonly ?int   $depth    = null, // null if transport does not support it
        public readonly int    $active   = 0,
    ) {}
}
