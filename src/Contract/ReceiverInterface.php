<?php

declare(strict_types=1);

namespace Thrun\Contract;

use Thrun\Envelope\Envelope;

interface ReceiverInterface
{
    /**
     * Blocks until a message is available.
     * Returns null when the worker should stop.
     */
    public function receive(): ?Envelope;

    public function ack(Envelope $envelope): void;

    public function reject(Envelope $envelope): void;
}
