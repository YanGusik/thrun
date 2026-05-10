<?php

declare(strict_types=1);

namespace Thrun\Transport\Policy;

use Thrun\Contract\ConcurrencyResolverInterface;
use Thrun\Envelope\Envelope;

/**
 * Static limit per partition.
 * Reserved keys 'ignore' and 'ignored' bypass the limit (return null).
 */
final class StaticConcurrencyResolver implements ConcurrencyResolverInterface
{
    public const IGNORE  = 'ignore';
    public const IGNORED = 'ignored';

    public function __construct(private readonly int $limit) {}

    public function resolve(string $partitionKey, Envelope $envelope): ?int
    {
        if ($partitionKey === self::IGNORE || $partitionKey === self::IGNORED) {
            return null;
        }

        return $this->limit;
    }
}
