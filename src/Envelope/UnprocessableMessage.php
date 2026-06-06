<?php

declare(strict_types=1);

namespace Thrun\Envelope;

final readonly class UnprocessableMessage
{
    public function __construct(
        public string $rawPayload,
        public string $queue,
    ) {}
}
