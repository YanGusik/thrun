<?php

declare(strict_types=1);

namespace Thrun\Envelope\Stamp;

use Thrun\Contract\NormalizableStampInterface;
use Thrun\Contract\StampInterface;

final class RedeliveryStamp implements StampInterface, NormalizableStampInterface
{
    public function __construct(
        public readonly int $attempt,
        public readonly ?string $retriedAt = null,
    ) {}

    public function normalize(): array
    {
        return [
            'attempt'   => $this->attempt,
            'retriedAt' => $this->retriedAt,
        ];
    }

    public static function denormalize(array $data): self
    {
        return new self(
            attempt:   $data['attempt'] ?? 0,
            retriedAt: $data['retriedAt'] ?? null,
        );
    }
}
