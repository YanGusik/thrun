<?php

declare(strict_types=1);

namespace Thrun\Transport\Policy;

use Thrun\Contract\DispatchPolicyInterface;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\PartitionStamp;

final class MaxConcurrencyPolicy implements DispatchPolicyInterface
{
    /** @var array<string, int> */
    private array $active = [];

    public function __construct(private readonly int $maxPerPartition = 1) {}

    public function allows(Envelope $envelope): bool
    {
        $key = $this->key($envelope);
        return ($this->active[$key] ?? 0) < $this->maxPerPartition;
    }

    public function acquire(Envelope $envelope): void
    {
        $key = $this->key($envelope);
        $this->active[$key] = ($this->active[$key] ?? 0) + 1;
    }

    public function release(Envelope $envelope): void
    {
        $key = $this->key($envelope);
        $this->active[$key] = max(0, ($this->active[$key] ?? 1) - 1);
    }

    private function key(Envelope $envelope): string
    {
        return $envelope->last(PartitionStamp::class)?->key ?? 'default';
    }
}
