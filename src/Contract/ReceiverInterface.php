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

    /**
     * Non-blocking. Returns null immediately if no message is available.
     */
    public function tryReceive(): ?Envelope;

    public function ack(Envelope $envelope): void;

    public function reject(Envelope $envelope): void;

    /**
     * Close any background resources (channels, scopes, watchers).
     * After close(), receive() should return null.
     */
    public function close(): void;
}
