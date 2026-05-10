<?php

declare(strict_types=1);

namespace Thrun\Contract;

use Thrun\Envelope\Envelope;

/**
 * Resolves the concurrency limit for a given partition.
 * Return null to disable the limit (allow unlimited).
 */
interface ConcurrencyResolverInterface
{
    /**
     * @return int|null Limit for this partition, or null for no limit
     */
    public function resolve(string $partitionKey, Envelope $envelope): ?int;
}
