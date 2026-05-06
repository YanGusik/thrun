<?php

declare(strict_types=1);

namespace Thrun\Contract;

use Thrun\Envelope\Envelope;

interface DispatchPolicyInterface
{
    // can this envelope be dispatched right now?
    public function allows(Envelope $envelope): bool;

    // called when envelope enters processing
    public function acquire(Envelope $envelope): void;

    // called when envelope is acked or rejected
    public function release(Envelope $envelope): void;
}
