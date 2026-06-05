<?php

declare(strict_types=1);

namespace Thrun\Envelope\Stamp;

use Thrun\Contract\NormalizableStampInterface;
use Thrun\Contract\StampInterface;

final class RetryStamp implements StampInterface, NormalizableStampInterface
{
    /**
     * @param list<int> $backoff Delays in milliseconds for each retry attempt.
     */
    public function __construct(
        public readonly array $backoff = [],
        public readonly ?int $maxAttempts = null,
    ) {}

    public function normalize(): array
    {
        return [
            'backoff'     => $this->backoff,
            'maxAttempts' => $this->maxAttempts,
        ];
    }

    public static function denormalize(array $data): self
    {
        return new self(
            backoff:     $data['backoff'] ?? [],
            maxAttempts: $data['maxAttempts'] ?? null,
        );
    }

    public function isRetryable(int $attempt): bool
    {
        if ($this->backoff === []) {
            return false;
        }

        if ($this->maxAttempts !== null && $attempt >= $this->maxAttempts) {
            return false;
        }

        return true;
    }

    public function getDelayForAttempt(int $attempt): int
    {
        if ($this->backoff === []) {
            return 0;
        }

        $index = min($attempt - 1, count($this->backoff) - 1);

        return $this->backoff[$index];
    }
}
