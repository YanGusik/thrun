<?php

declare(strict_types=1);

namespace Thrun\Transport\Policy;

use Thrun\Contract\ConcurrencyResolverInterface;
use Thrun\Contract\DispatchPolicyInterface;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\PartitionStamp;

final class MaxConcurrencyPolicy implements DispatchPolicyInterface
{
    private readonly ConcurrencyResolverInterface $resolver;

    /** @var array<string, int> */
    private array $active = [];

    public function __construct(
        int $maxPerPartition = 1,
        ?ConcurrencyResolverInterface $resolver = null,
    ) {
        $this->resolver = $resolver ?? new StaticConcurrencyResolver($maxPerPartition);
    }

    public function allows(Envelope $envelope): bool
    {
        $key   = $this->key($envelope);
        $limit = $this->resolver->resolve($key, $envelope);

        if ($limit === null) {
            return true;
        }

        return ($this->active[$key] ?? 0) < $limit;
    }

    public function acquire(Envelope $envelope): void
    {
        $key   = $this->key($envelope);
        $limit = $this->resolver->resolve($key, $envelope);

        if ($limit === null) {
            return;
        }

        $this->active[$key] = ($this->active[$key] ?? 0) + 1;
    }

    public function release(Envelope $envelope): void
    {
        $key   = $this->key($envelope);
        $limit = $this->resolver->resolve($key, $envelope);

        if ($limit === null) {
            return;
        }

        $this->active[$key] = max(0, ($this->active[$key] ?? 1) - 1);
    }

    private function key(Envelope $envelope): string
    {
        return $envelope->last(PartitionStamp::class)?->key ?? 'default';
    }
}
