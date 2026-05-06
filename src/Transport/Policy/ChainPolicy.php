<?php

declare(strict_types=1);

namespace Thrun\Transport\Policy;

use Thrun\Contract\DispatchPolicyInterface;
use Thrun\Envelope\Envelope;

final class ChainPolicy implements DispatchPolicyInterface
{
    /** @param DispatchPolicyInterface[] $policies */
    public function __construct(private readonly array $policies) {}

    public function allows(Envelope $envelope): bool
    {
        foreach ($this->policies as $policy) {
            if (!$policy->allows($envelope)) {
                return false;
            }
        }

        return true;
    }

    public function acquire(Envelope $envelope): void
    {
        foreach ($this->policies as $policy) {
            $policy->acquire($envelope);
        }
    }

    public function release(Envelope $envelope): void
    {
        foreach ($this->policies as $policy) {
            $policy->release($envelope);
        }
    }
}
