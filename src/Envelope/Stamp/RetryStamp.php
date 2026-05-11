<?php

declare(strict_types=1);

namespace Thrun\Envelope\Stamp;

use Thrun\Contract\StampInterface;

final class RetryStamp implements StampInterface
{
    public function __construct(
        public readonly int $attempts = 0,
        public readonly ?int $maxAttempts = null,
    ) {}
}
